#!/usr/bin/env python3
"""Bound Docker build workers and transfer commit-bound build, oracle and server images."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import time

import blame_gate as gate

ROOT = gate.ROOT
TARGETS = ("test-fixture-build", "test", "release", "test-server")
INPUT_TARGETS = ("codec-base", "toolchain", "build-inputs")
COMPILED_TARGETS = ("codec-base", "toolchain", "build")
BUNDLES = {"inputs": INPUT_TARGETS, "compiled": COMPILED_TARGETS, "final": TARGETS}
BUILD_TARGETS = (*INPUT_TARGETS, "build", *TARGETS)
ENV = dict(os.environ, DOCKER_BUILDKIT="0", DOCKER_HOST="unix:///var/run/docker.sock")
ENV.pop("DOCKER_CONTEXT", None)


def run(argv, timeout=20, **kwargs):
    return subprocess.run(list(map(str, argv)), env=ENV, timeout=timeout, **kwargs)


def output(argv, timeout=20):
    return run(argv, timeout, check=True, capture_output=True, text=True).stdout.strip()


def slice_name(value):
    gate.require(re.fullmatch(r"git-rs-blame-images-[a-zA-Z0-9_-]{8,80}\.slice", value),
                 "dedicated image-worker slice required")
    return value


def workers(slice_value, *, deadline):
    def remaining():
        seconds = deadline - time.monotonic()
        gate.require(seconds > 0, "image worker cleanup absolute deadline expired")
        return min(5, seconds)

    ids = output(["docker", "ps", "-aq", "--no-trunc"], timeout=remaining()).split()
    gate.require(all(re.fullmatch(r"[0-9a-f]{64}", item) for item in ids), "invalid container IDs")
    if not ids:
        return []
    result = run(["docker", "inspect", *ids], timeout=remaining(), capture_output=True, text=True)
    if result.returncode:
        # A build's --force-rm may race inspection, but daemon/access failures
        # must not be mistaken for an empty owned set.
        lines = result.stderr.splitlines()
        gate.require(lines and all(re.fullmatch(
            r"(?:Error response from daemon: No such container:|error: no such object:) [0-9a-f]{12,64}",
            line) for line in lines), "container inspection failed")
    return [row["Id"] for row in json.loads(result.stdout)
            if row["HostConfig"].get("CgroupParent") == slice_value]


def cleanup(value):
    value = slice_name(value)
    deadline = time.monotonic() + 20
    while ids := workers(value, deadline=deadline):
        gate.require(all(re.fullmatch(r"[0-9a-f]{64}", item) for item in ids), "invalid worker IDs")
        remaining = deadline - time.monotonic()
        gate.require(remaining > 0, "image worker cleanup absolute deadline expired")
        result = run(["docker", "rm", "-f", "-v", *ids], timeout=min(5, remaining),
                     capture_output=True, text=True)
        if result.returncode:
            allowed = {message for identity in ids for message in (
                "Error response from daemon: removal of container " + identity + " is already in progress",
                "Error response from daemon: No such container: " + identity,
            )}
            lines = result.stderr.splitlines()
            gate.require(lines and all(line in allowed for line in lines), "owned image worker removal failed")
        # --force-rm can race us. A successful removal response alone is not drain.
        # Reinspect the exact slice until empty, within the original cleanup budget.
        time.sleep(min(.05, max(0, deadline - time.monotonic())))


def commit(value):
    gate.require(re.fullmatch(r"[0-9a-f]{40}", value), "full commit required")
    gate.require(output(["git", "--no-optional-locks", "-C", ROOT, "rev-parse", "HEAD"]) == value,
                 "image bundle commit differs from checkout")
    return value


def image_id(value):
    gate.require(re.fullmatch(r"sha256:[0-9a-f]{64}", value), "immutable image ID required")
    return value


def manifest_ids(directory, revision, targets):
    manifest_file = directory / "manifest.json"
    gate.require(manifest_file.stat().st_size <= 64 * 1024, "image bundle manifest too large")
    manifest = json.loads(manifest_file.read_text())
    gate.require(manifest["format"] == 2 and manifest["commit"] == revision, "wrong image bundle commit")
    gate.require(manifest["dockerfile_sha256"] == gate.digest(ROOT / "Dockerfile"), "wrong Dockerfile")
    gate.require(set(manifest["images"]) == set(targets), "wrong image targets")
    ids = {target: image_id(manifest["images"][target]) for target in targets}
    gate.require(len(set(ids.values())) == len(targets), "image targets must be distinct")
    return manifest, ids


def build_recipe(directory, revision, target):
    gate.require(target == "build" or target in TARGETS, "invalid prepared build target")
    compiling = target == "build"
    _, ids = manifest_ids(directory, revision, INPUT_TARGETS if compiling else COMPILED_TARGETS)
    runtime = b"FROM codec-base AS runtime-base\n"
    toolchain = b"FROM codec-base AS toolchain\n"
    build_stage = b"FROM build-inputs AS build\n"
    fixture_stage = b"FROM build AS test-fixture-build\n"
    data = (ROOT / "Dockerfile").read_bytes()
    boundaries = (runtime, toolchain, build_stage, fixture_stage)
    gate.require(all(data.count(line) == 1 for line in boundaries), "image phase boundary changed")
    a, b, c, d = (data.index(line) for line in boundaries)
    gate.require(a < b < c < d, "image phase boundaries are out of order")
    tags = {}
    for target, identity in ids.items():
        # Use local content-specific names for the legacy Dockerfile parser.
        # The imported identity must still exist, including after `load`.
        tag = f"git-rs-verified-input:{target}-{identity.removeprefix('sha256:')}"
        run(["docker", "image", "tag", identity, tag], check=True)
        gate.require(output(["docker", "image", "inspect", "--format", "{{.Id}}", tag]) == identity,
                     "prepared image identity mismatch")
        tags[target] = tag
    dependency, start = ("build-inputs", c) if compiling else ("build", d)
    return (f"FROM {tags['codec-base']} AS codec-base\n".encode() + data[a:b]
            + f"FROM {tags['toolchain']} AS toolchain\n".encode()
            + f"FROM {tags[dependency]} AS {dependency}\n\n".encode() + data[start:])


def build(args):
    gate.verify_unit(args.unit, slice_name(args.slice),
                     cleanup_command="blame_images.py cleanup --slice " + args.slice)
    revision = gate.checkout_identity(ROOT)
    help_text = output(["docker", "build", "--help"])
    for option in ("--cgroup-parent", "--cpu-period", "--cpu-quota", "--cpuset-cpus",
                   "--memory-swap", "--force-rm", "--iidfile", "--network", "--target"):
        gate.require(option in help_text, "legacy Docker build does not support " + option)
    args.directory.mkdir(parents=True, exist_ok=True)
    iid = args.directory / (args.target + ".iid")
    gate.require(not iid.exists(), "image identity already exists")
    cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))))
    mirror = ["--build-arg", "GIT_RS_CI_AZURE_ARCHIVE=1"] if args.azure_archive else []
    recipe = None
    if args.prepared is not None:
        gate.require(not args.azure_archive, "prepared builds cannot acquire a new OS base")
        gate.require("--file" in help_text, "legacy Docker build does not support --file")
        recipe = build_recipe(args.prepared, revision, args.target)
    dockerfile = ["--file", "-"] if recipe is not None else []
    try:
        run(["docker", "build", *dockerfile, "--target", args.target, "--tag", args.tag,
             "--build-arg", "VCS_REF=" + revision, *mirror,
             "--network", "host", "--cgroup-parent", args.slice, "--cpuset-cpus", cpus,
             "--cpu-period", "100000", "--cpu-quota", "180000", "--memory", "3712m",
             "--memory-swap", "3712m", "--force-rm", "--iidfile", iid, ROOT],
            timeout=840, check=True, input=recipe)
        image_id(iid.read_text().strip())
    finally:
        cleanup(args.slice)


def export(args):
    revision = commit(args.commit)
    targets = BUNDLES[args.bundle]
    gate.require((args.prepared is not None) == (args.bundle == "compiled"),
                 "only the compiled bundle inherits prepared images")
    inherited = manifest_ids(args.prepared, revision, INPUT_TARGETS)[1] if args.prepared is not None else {}
    ids = {target: inherited[target] if target in inherited
           else image_id((args.directory / (target + ".iid")).read_text().strip()) for target in targets}
    gate.require(len(set(ids.values())) == len(targets), "image targets must be distinct")
    archive = args.directory / "images.tar"
    gate.require(not archive.exists(), "image archive already exists")
    run(["docker", "image", "save", "--output", archive, *ids.values()], timeout=180, check=True)
    manifest = {"format": 2, "commit": revision, "images": ids,
                "dockerfile_sha256": gate.digest(ROOT / "Dockerfile"),
                "archive_sha256": gate.digest(archive), "archive_bytes": archive.stat().st_size}
    (args.directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


def load(args):
    revision = commit(args.commit)
    targets = BUNDLES[args.bundle]
    gate.require((args.bundle == "final") == (args.env_file is not None), "only final image loads export an environment")
    manifest, ids = manifest_ids(args.directory, revision, targets)
    archive = args.directory / "images.tar"
    gate.require(archive.stat().st_size == manifest["archive_bytes"]
                 and gate.digest(archive) == manifest["archive_sha256"], "image archive checksum mismatch")
    # Inspect only metadata/config bytes, never extract archive paths. Reject a
    # declared-ID substitution before importing anything into the daemon.
    with tarfile.open(archive, "r:") as stream:
        def metadata(name, limit):
            member = stream.getmember(name)
            gate.require(member.isfile() and 0 <= member.size <= limit, "invalid archive metadata")
            return stream.extractfile(member).read()
        records = json.loads(metadata("manifest.json", 64 * 1024))
        gate.require(len(records) == len(targets) and all(not row.get("RepoTags") for row in records),
                     "archive must contain exactly the declared untagged image IDs")
        actual = {"sha256:" + hashlib.sha256(metadata(row["Config"], 1024 * 1024)).hexdigest()
                  for row in records}
        identities = [actual]
        if "index.json" in stream.getnames():
            # Docker's containerd image store exposes the OCI manifest digest
            # as .Id. Classic save-by-ID can include a partial OCI index, but
            # still lists every config in manifest.json. A partial index cannot
            # authorize any OCI-manifest-addressed inventory.
            index = json.loads(metadata("index.json", 64 * 1024))
            descriptors = index["manifests"]
            gate.require(len(descriptors) <= len(targets), "wrong OCI image inventory")
            configs, manifests = set(), set()
            for descriptor in descriptors:
                identity = image_id(descriptor["digest"])
                data = metadata("blobs/sha256/" + identity.removeprefix("sha256:"), 64 * 1024)
                gate.require("sha256:" + hashlib.sha256(data).hexdigest() == identity,
                             "OCI manifest digest mismatch")
                configs.add(json.loads(data)["config"]["digest"])
                manifests.add(identity)
            gate.require(configs <= actual, "OCI configs differ from archive inventory")
            if configs == actual and len(manifests) == len(targets):
                identities.append(manifests)
    gate.require(set(ids.values()) in identities, "archive image IDs differ from declaration")
    run(["docker", "image", "load", "--input", archive], timeout=180, check=True)
    for identity in ids.values():
        gate.require(output(["docker", "image", "inspect", "--format", "{{.Id}}", identity]) == identity,
                     "loaded image identity mismatch")
    if args.env_file is not None:
        with args.env_file.open("a") as stream:
            stream.write("BLAME_GATE_BUILDER_IMAGE=" + ids["test-fixture-build"] + "\n")
            stream.write("BLAME_GATE_IMAGE=" + ids["test"] + "\n")
            stream.write("GIT_RS_TEST_RUNTIME_IMAGE=" + ids["release"] + "\n")
            stream.write("GIT_RS_TEST_RUNTIME_FIXTURE_IMAGE=" + ids["test-server"] + "\n")
            stream.write("GIT_RS_TEST_REVISION=" + revision + "\n")
    print(json.dumps({"verified_commit": revision, "images": ids}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    builder = sub.add_parser("build")
    builder.add_argument("--target", choices=BUILD_TARGETS, required=True)
    builder.add_argument("--tag", required=True)
    builder.add_argument("--unit", required=True)
    builder.add_argument("--slice", required=True)
    builder.add_argument("--directory", type=Path, required=True)
    builder.add_argument("--azure-archive", action="store_true")
    builder.add_argument("--prepared", type=Path)
    for action in ("export", "load"):
        command = sub.add_parser(action)
        command.add_argument("--directory", type=Path, required=True)
        command.add_argument("--commit", required=True)
        command.add_argument("--bundle", choices=BUNDLES, default="final")
        if action == "load":
            command.add_argument("--env-file", type=Path)
        else:
            command.add_argument("--prepared", type=Path)
    cleaner = sub.add_parser("cleanup")
    cleaner.add_argument("--slice", required=True)
    args = parser.parse_args()
    {"build": build, "export": export, "load": load,
     "cleanup": lambda value: cleanup(value.slice)}[args.action](args)


if __name__ == "__main__":
    main()
