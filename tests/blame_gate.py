#!/usr/bin/env python3
"""Prepare immutable inputs and supervise the NOT-YET-QUALIFIED blame gate.

Preparation builds outside measurement. Execution requires an independently
bounded systemd unit and a dedicated aggregate slice, including Docker fixtures.
All application/test executables run in the recorded read-only test image.
"""
import argparse
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from types import SimpleNamespace

from cache_gate import FAILURE_EXCERPT_BYTES, failed_child_excerpt

ROOT = Path(__file__).resolve().parents[1]
LEGACY_REPOSITORY = "https://github.com/borislitv/git-rs"
LEGACY_COMMIT = "43d79301993c88cdde044c5dead260a4b0424079"
GIT_VERSION = "git version 2.56.0"
GIT_SOURCE = ("git 2.56.0\n"
              "source https://www.kernel.org/pub/software/scm/git/git-2.56.0.tar.xz\n"
              "sha256 26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3\n")
SUITES = ("cache_contract", "cache_coordinator", "cache_budget", "blame_build",
          "blame_artifacts", "blame_checkpoints", "blame_reuse", "blame_core")
EXCLUDE = {"blame_artifacts": {"artifact_process"},
           "blame_reuse": {"checkpoint_reuse_child", "warm_cache_outage_child"},
           "blame_core": {"runtime_support::runtime_child"}}
ROOT_RUNTIME = "runtime_support::owned_source_runtime_fences_ready_cache_http_response_staging_and_publication"
HARNESS_FILES = ("tests/blame_gate.py", "tests/blame_artifacts.py", "tests/derived_cache.py",
                 "tests/integration.py", "tests/blame_concurrency.py",
                 "tests/support/cache_faults.py", "tests/support/blame_artifact_faults.py",
                 "tests/cache_gate.py")
LABEL = "git-rs.blame-gate="
PREPARATION_LABEL = "git-rs.blame-preparation="
SLICE_PATTERN = r"git-rs-blame-[a-zA-Z0-9_-]{8,80}\.slice"
RUNTIME_SECONDS = 900


def require(condition, message):
    if not condition:
        raise ValueError(message)


def command(argv, *, timeout=30, **kwargs):
    argv = [str(x) for x in argv]
    if argv[0] == "docker":
        # A remote Docker context cannot share this host's attested cgroup slice.
        env = dict(kwargs.get("env") or os.environ)
        env["DOCKER_HOST"] = "unix:///var/run/docker.sock"
        env.pop("DOCKER_CONTEXT", None)
        kwargs["env"] = env
    return subprocess.run(argv, check=True, timeout=timeout, **kwargs)


def output(argv, **kwargs):
    return command(argv, capture_output=True, text=True, **kwargs).stdout.strip()


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def invocation():
    value = os.environ.get("INVOCATION_ID", "")
    require(re.fullmatch(r"[0-9a-f]{32}", value), "real systemd INVOCATION_ID required")
    return value


def inventory(text, suite):
    names = {line[:-6] for line in text.splitlines() if line.endswith(": test")}
    names -= EXCLUDE.get(suite, set())
    require(names, f"no ignored contracts discovered for {suite}")
    return sorted(names)


def relative_file(bundle, name):
    path = Path(name)
    require(not path.is_absolute() and ".." not in path.parts, "unsafe bundle path")
    target = bundle / path
    require(target.is_file() and not target.is_symlink(), "missing/linked bundle input")
    require(target.resolve().is_relative_to(bundle.resolve()), "bundle input escaped root")
    return target


def load_manifest(bundle):
    require(not bundle.is_symlink(), "linked bundle root")
    manifest_file = relative_file(bundle, "manifest.json")
    for path in (bundle, manifest_file):
        require(path.stat().st_uid in (0, os.getuid()) and path.stat().st_mode & 0o222 == 0,
                "bundle root/manifest must be owned and immutable")
    manifest = json.loads(manifest_file.read_text())
    binding = dict(manifest)
    expected = binding.pop("namespace_sha256")
    require(hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest() == expected,
            "manifest namespace binding mismatch")
    require(manifest["format"] == 1, "unsupported gate manifest")
    require(manifest["legacy"]["repository"] == LEGACY_REPOSITORY
            and manifest["legacy"]["commit"] == LEGACY_COMMIT, "wrong legacy provenance")
    require(re.fullmatch(r"[0-9a-f]{40}", manifest["current_commit"]), "invalid current revision")
    for key in ("cargo_manifest_sha256", "cargo_lock_sha256"):
        require(re.fullmatch(r"[0-9a-f]{64}", manifest["legacy"][key]), "legacy input hash missing")
    require(manifest["git_source"] == GIT_SOURCE, "wrong runtime Git provenance")
    require(manifest["builder_packages"] == manifest["runtime_packages"],
            "build/runtime OS package closure differs")
    for key in ("runtime_image", "builder_image"):
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", manifest[key]), "mutable image reference")
    required = {"current", "fixture", "cache_process", "legacy", "legacy_process", *SUITES}
    require(set(manifest["binaries"]) == required, "incomplete prebuilt binary set")
    require(set(manifest["inventory"]) == set(SUITES), "incomplete contract inventory")
    require(set(manifest["harness_sources"]) == set(HARNESS_FILES), "incomplete harness identity")
    require(all(re.fullmatch(r"[0-9a-f]{64}", value)
                for value in manifest["harness_sources"].values()), "invalid harness digest")
    for record in manifest["binaries"].values():
        path = relative_file(bundle, record["path"])
        require(os.access(path, os.X_OK) and digest(path) == record["sha256"],
                "prebuilt binary digest/executable mismatch")
        require(path.stat().st_mode & 0o222 == 0 and path.stat().st_uid in (0, os.getuid()),
                "bundle binary must be owned and immutable")
    for suite, names in manifest["inventory"].items():
        require(names and names == sorted(set(names))
                and not (set(names) & EXCLUDE.get(suite, set())), "invalid exact inventory")
    require(ROOT_RUNTIME in manifest["inventory"]["blame_core"], "root-runtime contract omitted")
    return manifest


def image_id(name):
    return output(["docker", "image", "inspect", "--format", "{{.Id}}", name])


def image_output(image, argv, volumes=()):
    return preparation_output(["--read-only", "--network", "none",
                   "--user", f"{os.getuid()}:{os.getgid()}",
                   *volumes, "--entrypoint", argv[0], image, *argv[1:]], timeout=60)


def preparation_token(value):
    require(re.fullmatch(r"[0-9a-f]{32}", value), "preparation nonce required")
    return value


def preparation_output(arguments, *, timeout):
    token = preparation_token(os.environ.get("BLAME_GATE_PREP_TOKEN", ""))
    slice_name = os.environ.get("BLAME_GATE_PREP_SLICE", "")
    require(re.fullmatch(SLICE_PATTERN, slice_name), "preparation slice required")
    cpus = sorted(os.sched_getaffinity(0))
    require(1 <= len(cpus) <= 2, "preparation affinity exceeds two CPUs")
    try:
        return output(["docker", "run", "--rm", "--pull=never",
                       "--label", PREPARATION_LABEL + token, "--cgroup-parent", slice_name,
                       "--cpuset-cpus", ",".join(map(str, cpus)), "--cpus", "2",
                       "--memory", "4g", "--memory-swap", "4g", "--pids-limit", "256",
                       *arguments], timeout=min(timeout, RUNTIME_SECONDS))
    finally:
        # Includes daemon-side creation whose response was lost by the client.
        # ExecStopPost repeats this independently if the controller is killed.
        cleanup_preparation(token)


def checkout_identity(path, expected=None):
    revision = output(["git", "--no-optional-locks", "-C", path, "rev-parse", "HEAD"])
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "full source revision required")
    require(expected is None or revision == expected, "wrong legacy source revision")
    require(not output(["git", "--no-optional-locks", "-C", path, "status", "--porcelain", "--untracked-files=no"]),
            "tracked source changes: prepare from an immutable committed checkout")
    if expected:
        origin = output(["git", "--no-optional-locks", "-C", path, "remote", "get-url", "origin"])
        require(origin.removesuffix(".git") == LEGACY_REPOSITORY, "wrong legacy origin")
    return revision


def cargo_artifacts(builder, source, target, arguments):
    target.mkdir(mode=0o700)
    started = time.monotonic()
    try:
        text = preparation_output(["--network", "host", "--user", f"{os.getuid()}:{os.getgid()}",
                       "--env", "HOME=/tmp", "--env", "CARGO_HOME=/target/cargo-home",
                       "--volume", f"{source}:/source:ro", "--volume", f"{target}:/target",
                       "--workdir", "/source", "--entrypoint", "cargo", builder,
                       *arguments, "--locked", "--target-dir", "/target",
                       "--message-format=json"], timeout=RUNTIME_SECONDS)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        # Keep Cargo JSON stdout private. Reuse the gate's bounded, escaped,
        # environment-secret redaction policy; never retain the raw stderr tail.
        stderr = error.stderr or b""
        if isinstance(stderr, str):
            stderr = stderr.encode("utf-8", errors="replace")
        try:
            with tempfile.TemporaryDirectory(prefix=".cargo-failure-", dir=target) as directory:
                log = Path(directory) / "stderr"
                log.write_bytes(stderr[-FAILURE_EXCERPT_BYTES:])
                excerpt = io.StringIO()
                with contextlib.redirect_stdout(excerpt):
                    failed_child_excerpt(log, os.environ, error, time.monotonic() - started)
                diagnostic = excerpt.getvalue()
                (target / "cargo-failure.log").write_text(diagnostic)
                print(diagnostic, end="", file=sys.stderr, flush=True)
        except OSError as diagnostic_error:
            print(f"Cargo stderr unavailable: {type(diagnostic_error).__name__}", file=sys.stderr)
        raise
    found = {}
    for line in text.splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message.get("executable"):
            name = message["target"]["name"]
            executable = Path(message["executable"])
            require(executable.is_relative_to("/target"), "Cargo executable escaped target")
            require(name not in found, "ambiguous Cargo executable")
            found[name] = target / executable.relative_to("/target")
    return found


def prepare(args):
    token = preparation_token(os.environ.get("BLAME_GATE_PREP_TOKEN", ""))
    verify_unit(os.environ.get("BLAME_GATE_PREP_UNIT", ""),
                os.environ.get("BLAME_GATE_PREP_SLICE", ""),
                cleanup_command="blame_gate.py cleanup --prep-token " + token)
    try:
        prepare_bundle(args)
    finally:
        cleanup_preparation(token)


def prepare_bundle(args):
    bundle = args.bundle.resolve()
    require(bundle.parent == ROOT / ".local", "bundle must be a fresh direct .local child")
    (ROOT / ".local").mkdir(mode=0o700, exist_ok=True)
    bundle.mkdir(mode=0o700)  # never replace an earlier build's identity
    legacy = ROOT / ".local/cache-v1"
    revision = checkout_identity(ROOT)
    checkout_identity(legacy, LEGACY_COMMIT)
    builder, runtime = image_id(args.builder_image), image_id(args.image)
    for image in (builder, runtime):
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", image), "immutable image ID required")
    packages = [image_output(image, ["dpkg-query", "-W", "-f=${Package}\\t${Version}\\n"])
                for image in (builder, runtime)]
    require(packages[0] == packages[1], "build/runtime OS packages differ")
    require(image_output(runtime, ["/usr/bin/git", "--version"]) == GIT_VERSION,
            "runtime Git version mismatch")
    provenance = image_output(runtime, ["/usr/bin/cat", "/usr/share/git-rs/git-source.txt"]) + "\n"
    require(provenance == GIT_SOURCE, "runtime Git provenance mismatch")
    harness_sources = {name: digest(ROOT / name) for name in HARNESS_FILES}
    for name, expected in harness_sources.items():
        actual = image_output(runtime, ["sha256sum", "/opt/git-rs/" + name]).split()[0]
        require(actual == expected, "test image harness differs from checkout: " + name)
    target = bundle.parent / (bundle.name + "-current-target")
    current = cargo_artifacts(builder, ROOT, target,
                              ["build", "--bin", "git-rs", "--example", "cache-process",
                               "--example", "blame-http-fixture"])
    contracts = cargo_artifacts(builder, ROOT, bundle.parent / (bundle.name + "-test-target"),
                                ["test", "--no-run", *[x for s in SUITES for x in ("--test", s)]])
    old = cargo_artifacts(builder, legacy, bundle.parent / (bundle.name + "-legacy-target"),
                          ["build", "--bin", "git-rs", "--example", "cache-process"])
    paths = {"current": current["git-rs"], "fixture": current["blame-http-fixture"],
             "cache_process": current["cache-process"], "legacy": old["git-rs"],
             "legacy_process": old["cache-process"], **{s: contracts[s] for s in SUITES}}
    records, names = {}, {}
    (bundle / "bin").mkdir()
    for name, path in paths.items():
        destination = bundle / "bin" / name
        shutil.copyfile(path, destination)
        destination.chmod(0o555)
        records[name] = {"path": f"bin/{name}", "sha256": digest(destination)}
        if name in SUITES:
            text = image_output(runtime, [f"/gate/bin/{name}", "--ignored", "--list"],
                                ["--volume", f"{bundle}:/gate:ro"])
            names[name] = inventory(text, name)
    require(ROOT_RUNTIME in names["blame_core"], "isolated root-runtime contract missing")
    manifest = {"format": 1, "current_commit": revision,
                "legacy": {"repository": LEGACY_REPOSITORY, "commit": LEGACY_COMMIT,
                           "cargo_manifest_sha256": digest(legacy / "Cargo.toml"),
                           "cargo_lock_sha256": digest(legacy / "Cargo.lock")},
                "runtime_image": runtime, "builder_image": builder,
                "builder_packages": hashlib.sha256(packages[0].encode()).hexdigest(),
                "runtime_packages": hashlib.sha256(packages[1].encode()).hexdigest(),
                "git_source": provenance, "binaries": records, "inventory": names,
                "harness_sources": harness_sources}
    manifest["namespace_sha256"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (bundle / "manifest.json").chmod(0o444)
    (bundle / "bin").chmod(0o555)
    bundle.chmod(0o555)
    load_manifest(bundle)
    print(json.dumps({"bundle": bundle.name, "manifest_sha256": digest(bundle / "manifest.json"),
                      "ignored_contracts": {s: len(n) for s, n in names.items()}}, indent=2))


def seconds(value):
    units = {"us": .000001, "ms": .001, "s": 1, "min": 60, "h": 3600}
    parts = re.findall(r"([0-9]+(?:\.[0-9]+)?)(us|ms|min|s|h)", value)
    require(parts and "".join(a+b for a, b in parts) == value.replace(" ", ""),
            "unrecognized/infinite systemd duration")
    return sum(float(a) * units[b] for a, b in parts)


def verify_limits(directory):
    memory = (directory / "memory.max").read_text().strip()
    pids = (directory / "pids.max").read_text().strip()
    cpu = (directory / "cpu.max").read_text().split()
    require(memory.isdigit() and 0 < int(memory) <= 4 * 1024**3, "aggregate memory >4 GiB/unbounded")
    require(pids.isdigit() and 0 < int(pids) <= 256, "aggregate tasks >256/unbounded")
    require(len(cpu) == 2 and all(x.isdigit() for x in cpu)
            and 0 < int(cpu[0]) <= 2 * int(cpu[1]), "aggregate CPU >2/unbounded")
    require((directory / "memory.swap.max").read_text().strip() == "0", "aggregate swap enabled")
    cpus = (directory / "cpuset.cpus.effective").read_text().strip()
    count = sum(int(b)-int(a)+1 if b else 1
                for a, b in re.findall(r"(\d+)(?:-(\d+))?", cpus))
    require(0 < count <= 2, "aggregate CPU affinity must select at most two CPUs")


def verify_unit(unit, slice_name, cleanup_command="blame_gate.py cleanup"):
    token = invocation()
    require(re.fullmatch(SLICE_PATTERN, slice_name), "dedicated gate slice required")
    require(re.fullmatch(r"git-rs-blame-[a-zA-Z0-9_-]{8,80}\.service", unit), "owned gate unit required")
    fields = ("InvocationID", "ControlGroup", "Slice", "RuntimeMaxUSec", "TimeoutStopUSec",
              "KillMode", "ExecStopPost")
    text = output(["systemctl", "show", unit, *["--property=" + x for x in fields]])
    props = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    require(props["InvocationID"] == token and props["Slice"] == slice_name, "unit ownership mismatch")
    require(0 < seconds(props["RuntimeMaxUSec"]) <= RUNTIME_SECONDS, "unit runtime exceeds 900s")
    require(0 < seconds(props["TimeoutStopUSec"]) <= 40 and props["KillMode"] == "control-group",
            "bounded control-group shutdown required")
    require(cleanup_command in props["ExecStopPost"], "independent cleanup required")
    group = output(["systemctl", "show", slice_name, "--property=ControlGroup", "--value"])
    require(group.startswith("/") and ".." not in Path(group).parts
            and props["ControlGroup"].startswith(group + "/"), "unit is outside aggregate slice")
    require(Path("/proc/self/cgroup").read_text().strip() == "0::" + props["ControlGroup"],
            "controller is not in its attested systemd unit")
    verify_limits(Path("/sys/fs/cgroup") / group.lstrip("/"))
    require(output(["docker", "info", "--format", "{{.CgroupDriver}} {{.CgroupVersion}}"])
            == "systemd 2", "systemd Docker cgroup-v2 driver required")
    return token


def cleanup():
    cleanup_label(LABEL + invocation())


def cleanup_preparation(token):
    cleanup_label(PREPARATION_LABEL + preparation_token(token))


def cleanup_label(label):
    query = ["docker", "ps", "-aq", "--filter", "label=" + label]
    ids = output(query, timeout=5).split()
    require(all(re.fullmatch(r"[0-9a-f]{12,64}", x) for x in ids), "invalid Docker container ID")
    if ids:
        command(["docker", "rm", "-f", "-v", *ids], timeout=20)
    require(not output(query, timeout=5), "owned blame gate containers remain")


def run_gate(args):
    token = verify_unit(args.unit, args.slice)
    bundle = args.bundle.resolve()
    manifest = load_manifest(bundle)
    require(digest(bundle / "manifest.json") == args.manifest_sha256,
            "manifest differs from independently recorded preparation hash")
    local = ROOT / ".local" / ("blame-gate-" + token)
    local.mkdir(mode=0o700)
    cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))))
    require(1 <= len(os.sched_getaffinity(0)) <= 2, "controller affinity exceeds two CPUs")
    report = {"completed": False, "manifest_sha256": digest(bundle / "manifest.json"),
              "runtime_image": manifest["runtime_image"], "unit": args.unit, "slice": args.slice,
              "limit_scope": "aggregate controller, runtime and PG/S3 cgroups; not Docker daemon overhead"}
    (local / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    started = time.monotonic()
    try:
        with (local / "controller.log").open("xb") as log:
            command(["docker", "run", "--rm", "--pull=never", "--name", "git-rs-blame-" + token,
                     "--init", "--read-only", "--network", "host",
                     "--cgroup-parent", args.slice, "--cpuset-cpus", cpus, "--cpus", "1",
                     "--memory", "2g", "--memory-swap", "2g", "--pids-limit", "108",
                     "--label", LABEL + token, "--user", f"{os.getuid()}:{os.getgid()}",
                     "--group-add", str(Path("/var/run/docker.sock").stat().st_gid),
                     "--volume", "/var/run/docker.sock:/var/run/docker.sock",
                     "--volume", f"{shutil.which('docker')}:/usr/bin/docker:ro",
                     "--volume", f"{bundle}:/gate:ro",
                     "--volume", f"{local}:/opt/git-rs/.local:rw",
                     "--tmpfs", "/tmp:rw,nosuid,size=512m,mode=1777",
                     "--env", "INVOCATION_ID=" + token, "--env", "GIT_RS_GATE_SLICE=" + args.slice,
                     "--env", "LANG=C", "--env", "LC_ALL=C",
                     "--entrypoint", "/usr/bin/python3", manifest["runtime_image"],
                     "-B", "/opt/git-rs/tests/blame_gate.py", "inside"],
                    timeout=max(1, RUNTIME_SECONDS - (time.monotonic() - started)),
                    stdout=log, stderr=subprocess.STDOUT)
        require(digest(bundle / "manifest.json") == report["manifest_sha256"], "manifest changed")
        report["work_completed"] = True
    finally:
        try:
            cleanup()
            report["cleanup_completed"] = True
            report["completed"] = report.get("work_completed", False)
        finally:
            report["elapsed_seconds"] = time.monotonic() - started
            (local / "report.json").write_text(json.dumps(report, indent=2) + "\n")


def docker_proxy(arguments):
    token = invocation()
    slice_name = os.environ.get("GIT_RS_GATE_SLICE", "")
    require(re.fullmatch(SLICE_PATTERN, slice_name), "missing aggregate slice")
    if arguments and arguments[0] == "run":
        forbidden = ("--cgroup-parent", "--privileged", "--cgroupns", "--pid")
        require(not any(a.split("=", 1)[0] in forbidden for a in arguments[1:]),
                "fixture attempted to escape aggregate cgroup")
        arguments = ["run", "--cgroup-parent", slice_name, "--pull=never",
                     "--label", LABEL + token, *arguments[1:]]
    os.environ["DOCKER_HOST"] = "unix:///var/run/docker.sock"
    os.environ.pop("DOCKER_CONTEXT", None)
    os.execv("/usr/bin/docker", ["/usr/bin/docker", *arguments])


def root_environment(fixture):
    from derived_cache import run, wait_until
    from integration import S3_IMAGE

    env = fixture.test_environment()
    # Source corruption PUTs use the established source-fixture signing key.
    # Keep that authority separate from the cache-only S3 root and credentials.
    source = fixture.container("256m", .25, 32, ["--tmpfs", "/data:rw,size=128m",
        "-p", "127.0.0.1::7070", "-e", "ROOT_ACCESS_KEY_ID=test-access",
        "-e", "ROOT_SECRET_ACCESS_KEY=synthetic-secret-key",
        S3_IMAGE, "--quiet", "posix", "/data"])
    endpoint = "http://127.0.0.1:" + fixture.port(source, "7070/tcp")
    request = ["curl", "--disable", "--silent", "--show-error", "--fail", "--max-time", "5",
               "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", "test-access:synthetic-secret-key",
               "--request", "PUT", endpoint + "/source-test"]
    wait_until(lambda: run(request, ok=False).returncode == 0, "root source S3 bucket")
    env.update(GIT_RS_S3_ENDPOINT=endpoint, GIT_RS_S3_BUCKET="source-test",
               GIT_RS_S3_ACCESS_KEY="test-access", GIT_RS_S3_SECRET_KEY="synthetic-secret-key")
    return env


def runtime_container(reference):
    records = json.loads(output(["docker", "inspect", reference], timeout=5))
    require(len(records) == 1, "runtime container identity is ambiguous")
    return records[0]


def runtime_outer(token, slice_name, expected_id=None, cpus=(1000000000,)):
    name = "git-rs-blame-" + token
    value = runtime_container(expected_id or name)
    identity = value["Id"]
    require(re.fullmatch(r"[0-9a-f]{64}", identity), "invalid runtime outer ID")
    require(expected_id is None or identity == expected_id, "runtime outer ID changed")
    environment = dict(item.split("=", 1) for item in value["Config"]["Env"] if "=" in item)
    require(value["Name"] == "/" + name and value["State"]["Running"]
            and value["Config"]["Labels"].get(LABEL[:-1]) == token
            and environment.get("INVOCATION_ID") == token
            and environment.get("GIT_RS_GATE_SLICE") == slice_name
            and value["HostConfig"]["CgroupParent"] == slice_name,
            "runtime outer ownership/invocation mismatch")
    require(value["HostConfig"]["NanoCpus"] in cpus, "unexpected runtime outer CPU allocation")
    # Host networking can inherit a non-ID hostname. Bind the dispatcher to the
    # actual isolated PID namespace instead of trusting that hostname convention.
    require(value["HostConfig"].get("PidMode", "") == "", "runtime outer shares a PID namespace")
    require(output(["docker", "exec", identity, "/usr/bin/readlink", "/proc/self/ns/pid"], timeout=5)
            == os.readlink("/proc/self/ns/pid"), "runtime dispatcher is not this outer container")
    return value


def runtime_children(name, identity=None):
    found = output(["docker", "ps", "-aq", "--no-trunc", "--filter", "name=^/" + name + "$"], timeout=5).split()
    if identity:
        found += output(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + identity], timeout=5).split()
    require(all(re.fullmatch(r"[0-9a-f]{64}", item) for item in found), "invalid runtime child discovery")
    return set(found)


def runtime_child(child_id, name, token, outer_id, slice_name):
    child = runtime_container(child_id)
    labels = child["Config"]["Labels"]
    environment = dict(item.split("=", 1) for item in child["Config"]["Env"] if "=" in item)
    require(child["Id"] == child_id and child["Name"] == "/" + name
            and labels.get(LABEL[:-1]) == token
            and labels.get("git-rs.blame-runtime-outer") == outer_id
            and environment.get("INVOCATION_ID") == token
            and environment.get("GIT_RS_GATE_SLICE") == slice_name
            and child["HostConfig"]["CgroupParent"] == slice_name
            and child["HostConfig"]["NanoCpus"] == 1000000000
            and child["HostConfig"]["Memory"] == 256 * 1024**2
            and child["HostConfig"]["MemorySwap"] == 256 * 1024**2
            and child["HostConfig"]["PidsLimit"] == 32,
            "refusing unowned or changed runtime child")
    return child


def remove_runtime_child(name, identity, token, outer_id, slice_name):
    found = runtime_children(name, identity)
    for child_id in found:
        require(identity is None or child_id == identity, "runtime child ID changed")
        runtime_child(child_id, name, token, outer_id, slice_name)
    for child_id in found:
        command(["docker", "rm", "-f", "-v", child_id], timeout=20)
    require(not runtime_children(name, identity), "runtime child removal was not acknowledged")


def run_root_runtime(manifest, executable, env, stream):
    """Transfer an idle dispatcher's CPU, retaining its 1.25 combined allowance.

    Never restore the donor until exact owned child removal is acknowledged,
    including an ambiguous create/start/update failure. Global cleanup remains
    independently responsible for interruption of the dispatcher itself.
    """
    token = invocation()
    slice_name = os.environ.get("GIT_RS_GATE_SLICE", "")
    require(re.fullmatch(SLICE_PATTERN, slice_name), "invalid runtime aggregate slice")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", manifest["runtime_image"]), "mutable runtime image")
    require(Path(executable) == Path("/gate/bin/blame_core"), "unexpected isolated runtime executable")
    outer_id = runtime_outer(token, slice_name)["Id"]
    name = "git-rs-blame-runtime-" + token + "-" + uuid.uuid4().hex
    require(not runtime_children(name), "runtime child name already exists")
    for key, value in (("INVOCATION_ID", token), ("GIT_RS_GATE_SLICE", slice_name)):
        require(key not in env or env[key] == value, "conflicting runtime invocation environment")
    child_id, donated, failure, completed = None, False, None, None
    try:
        command(["docker", "update", "--cpus", ".25", outer_id], timeout=10)
        runtime_outer(token, slice_name, outer_id, (250000000,))
        donated = True
        arguments = ["docker", "create", "--pull=never", "--init", "--read-only", "--network", "host", "--user", "0:0",
                     "--name", name, "--label", LABEL + token,
                     "--label", "git-rs.blame-runtime-outer=" + outer_id,
                     "--cgroup-parent", slice_name, "--cpus", "1", "--memory", "256m",
                     "--memory-swap", "256m", "--pids-limit", "32",
                     "--volumes-from", outer_id + ":ro",
                     "--mount", "type=volume,destination=/usr/lib/x86_64-linux-gnu/gconv",
                     "--tmpfs", "/tmp:rw,nosuid,size=128m,mode=1777",
                     "--env", "GIT_RS_OWNED_CODEC_CONTAINER=1",
                     "--env", "INVOCATION_ID=" + token, "--env", "GIT_RS_GATE_SLICE=" + slice_name,
                     *[x for key in sorted(env) for x in ("--env", key + "=" + env[key])],
                     "--entrypoint", str(executable), manifest["runtime_image"],
                     "--ignored", "--exact", ROOT_RUNTIME, "--test-threads=1", "--nocapture"]
        created = output(arguments, timeout=30)
        require(re.fullmatch(r"[0-9a-f]{64}", created), "invalid created runtime child ID")
        child_id = created
        runtime_child(child_id, name, token, outer_id, slice_name)
        runtime_outer(token, slice_name, outer_id, (250000000,))
        completed = command(["docker", "start", "--attach", child_id], env=dict(os.environ, **env),
                            timeout=300, stdout=stream, stderr=subprocess.STDOUT)
    except BaseException as error:
        failure = error
    try:
        remove_runtime_child(name, child_id, token, outer_id, slice_name)
        current = runtime_outer(token, slice_name, outer_id,
                                (250000000,) if donated else (250000000, 1000000000))
        if current["HostConfig"]["NanoCpus"] == 250000000:
            command(["docker", "update", "--cpus", "1", outer_id], timeout=10)
            runtime_outer(token, slice_name, outer_id)
    except BaseException as cleanup_error:
        if failure is not None:
            raise BaseExceptionGroup("runtime work and cleanup/restore failed", [failure, cleanup_error])
        raise
    if failure is not None:
        raise failure
    return completed


def inside():
    token = invocation()
    require(Path("/.dockerenv").is_file(), "gate helpers run only inside the prepared test image")
    bundle = Path("/gate")
    manifest = load_manifest(bundle)
    for name, expected in manifest["harness_sources"].items():
        require(digest(ROOT / name) == expected, "execution harness source mismatch")
    require(output(["/usr/bin/git", "--version"]) == GIT_VERSION, "wrong execution Git")
    require(Path("/usr/share/git-rs/git-source.txt").read_text() == manifest["git_source"],
            "wrong execution Git source")
    packages = output(["dpkg-query", "-W", "-f=${Package}\\t${Version}\\n"])
    require(hashlib.sha256(packages.encode()).hexdigest() == manifest["runtime_packages"],
            "execution OS differs from manifest")
    paths = {key: relative_file(bundle, record["path"]) for key, record in manifest["binaries"].items()}
    for suite in SUITES:
        require(inventory(output([paths[suite], "--ignored", "--list"]), suite)
                == manifest["inventory"][suite], "contract inventory drift")
    shim = ROOT / ".local/docker-bin"
    shim.mkdir(mode=0o700)
    (shim / "docker").write_text("#!/bin/sh\nexec /usr/bin/python3 -B /opt/git-rs/tests/blame_gate.py docker \"$@\"\n")
    (shim / "docker").chmod(0o700)
    os.environ["PATH"] = str(shim) + ":" + os.environ["PATH"]
    owner = "git-rs.blame-fixture-owner=" + token
    os.environ["GIT_RS_TEST_OWNER_LABEL"] = owner
    args = ["--provision", "--owner-label", owner, "--binary", paths["current"],
            "--build-contract-binary", paths["blame_build"],
            "--artifact-contract-binary", paths["blame_artifacts"],
            "--checkpoint-contract-binary", paths["blame_checkpoints"],
            "--opaque-contract-binary", paths["cache_contract"],
            "--coordinator-contract-binary", paths["cache_coordinator"],
            "--budget-contract-binary", paths["cache_budget"],
            "--legacy-binary", paths["legacy"], "--legacy-process-binary", paths["legacy_process"],
            "--legacy-sha256", manifest["binaries"]["legacy"]["sha256"],
            "--legacy-process-sha256", manifest["binaries"]["legacy_process"]["sha256"],
            "--phase", "all"]
    log = ROOT / ".local/artifacts.json"
    with log.open("x") as stdout, (ROOT / ".local/artifacts.stderr").open("x") as stderr:
        command([sys.executable, "-B", ROOT / "tests/blame_artifacts.py", *args],
                timeout=RUNTIME_SECONDS, stdout=stdout, stderr=stderr)
    result = json.loads(log.read_text())
    require(result.get("completed") is True, "artifact/migration harness incomplete")
    for suite in SUITES[:6]:
        require(result["contract_inventory"][suite] == manifest["inventory"][suite],
                "artifact/opaque harness omitted contracts")
    # Reuse the same owned fixture implementation for the two root-only suites.
    from blame_artifacts import Fixture
    settings = SimpleNamespace(binary=paths["current"], legacy_process_binary=paths["legacy_process"],
                               opaque_contract_binary=paths["cache_contract"],
                               coordinator_contract_binary=paths["cache_coordinator"],
                               budget_contract_binary=paths["cache_budget"],
                               artifact_contract_binary=paths["blame_artifacts"], contract_filter=None)
    fixture = Fixture(settings)
    try:
        fixture.setup()
        env = root_environment(fixture)
        env["GIT_RS_BLAME_HTTP_FIXTURE"] = str(paths["fixture"])
        for suite in ("blame_reuse", "blame_core"):
            for index, case in enumerate(manifest["inventory"][suite]):
                with (ROOT / f".local/{suite}-{index}.log").open("x") as stream:
                    arguments = [paths[suite], "--ignored", "--exact", case,
                                 "--test-threads=1", "--nocapture"]
                    environment = env
                    if suite == "blame_core" and case == ROOT_RUNTIME:
                        completed = run_root_runtime(manifest, paths[suite], env, stream)
                    else:
                        completed = command(arguments, env=environment,
                                            timeout=300, stdout=stream, stderr=subprocess.STDOUT)
                require(completed.returncode == 0, "root contract failed")
                require("1 passed; 0 failed; 0 ignored" in
                        (ROOT / f".local/{suite}-{index}.log").read_text(),
                        "exact root contract did not execute")
            result["contract_inventory"][suite] = manifest["inventory"][suite]
    finally:
        fixture.close()
    focused_gate()
    (ROOT / ".local/inventory.json").write_text(json.dumps(result["contract_inventory"], indent=2) + "\n")


def focused_gate():
    command([sys.executable, "-B", ROOT / "tests/integration.py", "--provision",
             "--blame-core-only", "--binary", "/usr/local/bin/git-rs",
             "--blame-fixture-binary", "/usr/local/bin/examples/blame-http-fixture",
             "--fixture-dir", ROOT / ".local/focused"], timeout=RUNTIME_SECONDS)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--bundle", type=Path, required=True)
    prep.add_argument("--builder-image", required=True)
    prep.add_argument("--image", required=True)
    run = sub.add_parser("run")
    run.add_argument("--bundle", type=Path, required=True)
    run.add_argument("--unit", required=True)
    run.add_argument("--slice", required=True)
    run.add_argument("--manifest-sha256", required=True)
    clean = sub.add_parser("cleanup")
    clean.add_argument("--prep-token")
    sub.add_parser("inside")
    sub.add_parser("images")
    proxy = sub.add_parser("docker")
    proxy.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args)
    elif args.action == "run":
        run_gate(args)
    elif args.action == "cleanup":
        if args.prep_token is not None:
            cleanup_preparation(args.prep_token)
        else:
            cleanup()
    elif args.action == "docker":
        docker_proxy(args.arguments)
    elif args.action == "images":
        import ast
        tree = ast.parse((ROOT / "tests/integration.py").read_text())
        images = {n.targets[0].id: ast.literal_eval(n.value) for n in tree.body
                  if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
                  and n.targets[0].id in ("PG_IMAGE", "S3_IMAGE")}
        for key in ("PG_IMAGE", "S3_IMAGE"):
            require(re.fullmatch(r"[a-zA-Z0-9/_.:-]+@sha256:[0-9a-f]{64}", images[key]),
                    "fixture image must have immutable digest")
            print(images[key])
    else:
        inside()


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(f"Blame gate refused/failed: {error}", file=sys.stderr)
        sys.exit(1)
