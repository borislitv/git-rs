#!/usr/bin/env python3
"""Supervise prebuilt runtime contracts in one independently bounded aggregate.

Each phase owns a fresh systemd invocation, Docker label and private directory.
Kubernetes phases also own a fresh k3s container; an existing cluster is never
reused. ExecStopPost repeats exact container and namespace cleanup independently.
"""
import argparse
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time

import blame_gate as gate
from cache_gate import contracts
from integration import PG_IMAGE, S3_IMAGE
from maintenance_fixture import cleanup as cleanup_namespace
from runtime_process import cluster_cgroups, inspect_image


ROOT = gate.ROOT
DIRECTORIES = ROOT / ".local/runtime-gates"
K3S = "rancher/k3s:v1.37.2-k3s1@sha256:21ea6e0fac4e3930c2ce8aa40ddd41a8b18cb3e9a0222c030dc005e7e88c4332"
PHASES = ("runtime", "metadata", "integration", "receive", "upload", "history", "failures", "maintenance", "cache-matrix", "leader", "kubernetes", "istio")
CLEANUP_SECONDS = 35
MAX_DIAGNOSTIC_BYTES = 16 * 1024 * 1024
CGROUPS = Path("/sys/fs/cgroup")
CLEANUP_COMMAND = "runtime_exec.py cleanup"
CONTRACT_SUITES = ("metadata", "maintenance", "maintenance_leader", "leader_dependency")


def export_contracts(args):
    revision = gate.checkout_identity(ROOT)
    gate.require(not args.destination.exists(), "compiled contract destination must be new")
    target = args.destination / "debug/deps"
    target.mkdir(parents=True, mode=0o755)
    manifest = {"format": 1, "revision": revision, "contracts": {}}
    for name in CONTRACT_SUITES:
        source = contracts(args.target_dir.resolve() / "debug", name)
        destination = target / source.name
        shutil.copyfile(source, destination)
        destination.chmod(0o555)
        manifest["contracts"][name] = {"file": source.name, "sha256": gate.digest(destination)}
    (args.destination / "contracts.json").write_text(json.dumps(manifest, indent=2) + "\n")


def verify_contracts(target, revision):
    path = target / "contracts.json"
    gate.require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 64 * 1024,
                 "compiled contract manifest missing or oversized")
    manifest = json.loads(path.read_text())
    gate.require(manifest["format"] == 1 and manifest["revision"] == revision
                 and set(manifest["contracts"]) == set(CONTRACT_SUITES), "wrong compiled contract inventory/revision")
    for name, record in manifest["contracts"].items():
        gate.require(re.fullmatch(name + r"-[0-9a-f]+", record["file"]), "invalid compiled contract path")
        binary = contracts(target / "debug", name)
        gate.require(binary.name == record["file"] and not binary.is_symlink()
                     and binary.stat().st_size <= 512 * 1024 * 1024 and gate.digest(binary) == record["sha256"],
                     "compiled contract identity differs")
        with binary.open("rb") as executable:
            gate.require(executable.read(4) == b"\x7fELF", "compiled contract is not an ELF")
    return manifest


def directory(token):
    gate.require(re.fullmatch(r"[0-9a-f]{32}", token), "systemd invocation required")
    return DIRECTORIES / token


def private(path):
    metadata = path.lstat()
    gate.require(path.is_dir() and not path.is_symlink() and metadata.st_uid == os.getuid()
                 and metadata.st_mode & 0o777 == 0o700, "private owned gate directory required")


def cluster_name(token):
    return "git-rs-runtime-k3s-" + token


def save_cluster(local, token, identity):
    path = local / "cluster.json"
    temporary = local / "cluster.partial"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump({"format": 1, "owner": token, "id": identity, "name": cluster_name(token)}, stream)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(local, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def saved_cluster(local, token):
    path = local / "cluster.json"
    if not path.exists():
        return None
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor) as stream:
        metadata = os.fstat(stream.fileno())
        gate.require(metadata.st_uid == os.getuid() and stat.S_ISREG(metadata.st_mode)
                     and metadata.st_mode & 0o777 == 0o600 and metadata.st_size <= 1024,
                     "invalid cluster ownership journal")
        encoded = stream.read(1025)
        gate.require(len(encoded) <= 1024, "cluster ownership journal exceeded its bound")
        value = json.loads(encoded)
    gate.require(value["format"] == 1 and value["owner"] == token and value["name"] == cluster_name(token)
                 and re.fullmatch(r"[0-9a-f]{64}", value["id"]), "wrong cluster ownership journal")
    return value["id"]


def populated(path):
    fields = dict(line.split() for line in (path / "cgroup.events").read_text().splitlines())
    gate.require(fields.get("populated") in {"0", "1"}, "invalid cgroup population state")
    return fields["populated"] == "1"


def aggregate_empty(group, controller):
    gate.require(re.fullmatch(r"/[A-Za-z0-9_./:-]+", group) and ".." not in Path(group).parts
                 and Path(controller).parent == Path(group), "invalid aggregate cgroup identity")
    root = CGROUPS / group.lstrip("/")
    if (root / "cgroup.procs").read_text().strip():
        return False
    children = [child for child in root.iterdir() if child.is_dir()]
    gate.require(len(children) <= 128, "aggregate cgroup inventory exceeded its bound")
    return all(child.name == Path(controller).name or not populated(child) for child in children)


def confirm_drain(local, token):
    descriptor = os.open(local / "drain-proof.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor) as source:
        value = os.fstat(source.fileno())
        gate.require(stat.S_ISREG(value.st_mode) and value.st_uid == 0
                     and stat.S_IMODE(value.st_mode) == 0o644 and value.st_size <= 4096,
                     "root-owned aggregate drain proof required")
        data = source.read(4097)
    gate.require(len(data) <= 4096, "oversized aggregate drain proof")
    proof = json.loads(data)
    gate.require(proof["owner"] == token and proof["drained"] is True
                 and re.fullmatch(gate.SLICE_PATTERN, Path(proof["group"]).name)
                 and aggregate_empty(proof["group"], proof["controller"]),
                 "actual aggregate owners have not stopped")


def cleanup(*, containers_only=False, deadline=None):
    token = gate.invocation()
    local = directory(token)
    now = time.monotonic()
    deadline = now + CLEANUP_SECONDS if deadline is None else deadline
    gate.require(math.isfinite(deadline) and 0 < deadline - now <= CLEANUP_SECONDS,
                 "invalid runtime cleanup deadline")

    def remaining(limit):
        seconds = deadline - time.monotonic()
        gate.require(seconds > 0, "runtime cleanup absolute deadline expired")
        return min(seconds, limit)

    def output(argv, limit=5):
        return gate.output(argv, timeout=remaining(limit))

    def remove(identifiers):
        if identifiers:
            gate.command(["docker", "rm", "-f", "-v", *identifiers], timeout=remaining(10))

    def owned():
        identifiers = output(["docker", "ps", "-aq", "--no-trunc", "--filter", "label=" + gate.LABEL + token]).split()
        gate.require(len(identifiers) <= 128 and len(set(identifiers)) == len(identifiers)
                     and all(re.fullmatch(r"[0-9a-f]{64}", item) for item in identifiers),
                     "invalid owned container inventory")
        if not identifiers:
            return []
        rows = json.loads(output(["docker", "inspect", *identifiers]))
        gate.require(len(rows) == len(identifiers) and {row["Id"] for row in rows} == set(identifiers),
                     "owned container inventory changed")
        for row in rows:
            gate.require((row["Config"].get("Labels") or {}).get(gate.LABEL[:-1]) == token,
                         "container ownership changed")
        return rows

    removed = None
    if local.exists():
        private(local)
        # Stop backend/controller containers before removing their cluster. Its
        # durable immutable ID permits local credential cleanup even if the API
        # or the controller's normal namespace cleanup became unavailable.
        cluster = None
        others = []
        for row in owned():
            identifier = row["Id"]
            if row["Name"] == "/" + cluster_name(token):
                gate.require(cluster is None, "ambiguous owned cluster")
                cluster = identifier
            else:
                others.append(identifier)
        remove(others)
        recorded = saved_cluster(local, token)
        gate.require(recorded is None or cluster is None or recorded == cluster, "owned cluster identity changed")
        if cluster:
            remove([cluster])
        removed = cluster or recorded
        if removed:
            gate.require(not output(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + removed]),
                         "cluster removal is unconfirmed or the cluster owner changed")
        (local / ".metadata-env").unlink(missing_ok=True)
        for inventory in (local / "inventory/ownership.json", local / "fixture-inventory/ownership.json"):
            if inventory.exists():
                gate.command([sys.executable, "-B", ROOT / "tools/check-runtime-boundaries.py", "--cleanup", inventory],
                             timeout=remaining(10))
    # This also covers creation whose daemon-side commit outlived its client.
    remove([row["Id"] for row in owned()])
    gate.require(not owned(), "owned runtime gate containers remain")
    if local.exists() and not containers_only:
        # Docker removal is not proof that CRI tasks placed in the sibling pod
        # subtree stopped. Only the root supervisor may attest that drain.
        confirm_drain(local, token)
        for journal in (local / "fixture/maintenance-fixture/maintenance-fixture.json",
                         local / "fixture/smoke-fixture.json", local / "candidate/candidate-fixture.json"):
            if journal.exists():
                remaining(1)
                gate.require(removed is not None, "namespace journal has no exact cluster-removal evidence")
                cleanup_namespace(journal, destroyed_cluster=removed)


def start_cluster(token, slice_name, cpus, pod_root, deadline, local):
    name = cluster_name(token)
    existing = gate.output(["docker", "ps", "-aq", "--filter", "name=^/" + name + "$"])
    gate.require(not existing, "refusing to reuse a cluster container")
    # With a host cgroup namespace, kubelet's explicit cgroupfs root and actual
    # CRI task observations bind pods as well as k3s to the attested slice.
    identity = gate.output(["docker", "create", "--name", name, "--pull=never", "--privileged",
        "--cgroupns=host", "--cgroup-parent", slice_name, "--cpuset-cpus", cpus, "--cpus", ".5",
        "--memory", "1024m", "--memory-swap", "1024m", "--pids-limit", "128",
        "--label", gate.LABEL + token, "--publish", "127.0.0.1::6443",
        "--tmpfs", "/run", "--tmpfs", "/var/run", "--entrypoint", "/bin/sh", K3S,
        "-ec", 'mkdir -p "$1"; shift; exec /bin/k3s "$@"', "fixture-entrypoint",
        "/sys/fs/cgroup" + pod_root, "server", "--disable=traefik", "--disable=servicelb",
        "--disable=metrics-server", "--disable=gateway-api-crd", "--disable=local-storage",
        "--tls-san=127.0.0.1", "--kubelet-arg=cgroup-driver=cgroupfs",
        "--kubelet-arg=cgroup-root=" + pod_root, "--kubelet-arg=max-pods=16"])
    gate.require(re.fullmatch(r"[0-9a-f]{64}", identity), "invalid cluster identity")
    save_cluster(local, token, identity)  # Before starting an API or creating namespaces.
    gate.command(["docker", "start", identity])
    ready_deadline = min(deadline, time.monotonic() + 120)
    while True:
        result = subprocess.run(["docker", "exec", identity, "kubectl", "get", "nodes", "--request-timeout=5s",
            "-o", "jsonpath={.items[0].status.conditions[?(@.type==\"Ready\")].status}"],
            capture_output=True, timeout=10)
        if result.returncode == 0 and result.stdout == b"True":
            break
        gate.require(time.monotonic() < ready_deadline, "owned Kubernetes readiness deadline expired")
        time.sleep(0.1)
    version = json.loads(gate.output(["docker", "exec", identity, "kubectl", "version", "-o", "json"]))
    gate.require(re.fullmatch(r"v1\.37\.[0-9]+(?:[+.-].*)?", version["serverVersion"]["gitVersion"]),
                 "runtime contracts require Kubernetes 1.37")
    cluster_cgroups(identity, pod_root)
    return identity


def serial_sql(args, local, oracle, token, cpus, deadline):
    target = args.target_dir.resolve(strict=True) / "debug/deps"
    suites = {name: contracts(args.target_dir.resolve() / "debug", name) for name in ("metadata", "maintenance", "maintenance_leader")}
    identity = gate.output(["docker", "create", "--pull=never", "--label", gate.LABEL + token,
        "--cgroup-parent", args.slice, "--cpuset-cpus", cpus, "--cpus", ".5",
        "--memory", "1024m", "--memory-swap", "1024m", "--pids-limit", "64",
        "--tmpfs", "/var/lib/postgresql:rw,size=512m", "--publish", "127.0.0.1::5432",
        "--env", "POSTGRES_PASSWORD=development-only", "--env", "POSTGRES_DB=git_rs", PG_IMAGE])
    gate.require(re.fullmatch(r"[0-9a-f]{64}", identity), "invalid PostgreSQL fixture identity")
    gate.command(["docker", "start", identity])
    row = json.loads(gate.output(["docker", "inspect", identity]))[0]
    port = int(row["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"])
    environment = local / ".metadata-env"
    descriptor = os.open(environment, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(f"HOME=/tmp\nLANG=C\nLC_ALL=C\nPGHOST=127.0.0.1\nPGPORT={port}\n"
                     "PGUSER=postgres\nPGDATABASE=git_rs\nPGPASSWORD=development-only\n"
                     "PGSSLMODE=disable\nPGCONNECT_TIMEOUT=2\nPGGSSENCMODE=disable\nGIT_RS_DEV_HTTP=1\n")
    runner = ["docker", "run", "--rm", "--pull=never", "--init", "--read-only", "--network", "host",
        "--label", gate.LABEL + token, "--cgroup-parent", args.slice, "--cpuset-cpus", cpus,
        "--cpus", "1", "--memory", "1024m", "--memory-swap", "1024m", "--pids-limit", "96",
        "--user", f"{os.getuid()}:{os.getgid()}", "--env-file", environment,
        "--mount", f"type=bind,src={target},dst=/contracts,readonly",
        "--tmpfs", "/tmp:rw,nosuid,size=64m,mode=1777"]
    try:
        ready_deadline = min(deadline, time.monotonic() + 30)
        while True:
            result = subprocess.run(list(map(str, [*runner, "--entrypoint", "psql", oracle,
                "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", "SELECT 1"])), capture_output=True, timeout=10)
            if result.returncode == 0 and result.stdout.strip() == b"1":
                break
            gate.require(time.monotonic() < ready_deadline, "PostgreSQL TCP readiness deadline expired")
            time.sleep(0.05)
        results = {}
        for suite, binary in suites.items():
            executable = "/contracts/" + binary.name
            names = gate.inventory(gate.output([*runner, "--entrypoint", executable, oracle, "--ignored", "--list"]), suite)
            log = local / (suite + ".log")
            with log.open("xb") as stream:
                for ordinal, name in enumerate(names):
                    database = f"contract_{suite}_{ordinal}"
                    sql = [*runner, "--entrypoint", "psql", oracle, "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c"]
                    gate.command([*sql, "CREATE DATABASE " + database],
                                 timeout=max(0.1, deadline - time.monotonic()), stdout=stream, stderr=subprocess.STDOUT)
                    stream.write(f"\ncontract: {name}\n".encode())
                    stream.flush()
                    start = stream.tell()
                    gate.command([*runner, "--env", "PGDATABASE=" + database, "--entrypoint", executable, oracle,
                        "--ignored", "--exact", name, "--nocapture", "--test-threads=1"],
                        timeout=max(0.1, deadline - time.monotonic()), stdout=stream, stderr=subprocess.STDOUT)
                    gate.require(log.stat().st_size <= MAX_DIAGNOSTIC_BYTES, "PostgreSQL contract log exceeded its bound")
                    with log.open("rb") as source:
                        source.seek(start)
                        text = source.read(MAX_DIAGNOSTIC_BYTES + 1).decode("utf-8")
                    gate.require("test result: ok. 1 passed; 0 failed; 0 ignored;" in text and "test " + name in text,
                                 "discovered PostgreSQL contract was not executed exactly")
                    # Queue claims and leadership are database-global. A fresh
                    # database prevents another test's legitimate queued work
                    # from being claimed by this test's synthetic leader.
                    drained = min(deadline, time.monotonic() + 5)
                    while gate.output([*sql, "SELECT count(*) FROM pg_stat_activity WHERE datname='" + database + "'"],
                                      timeout=max(0.1, drained - time.monotonic())).strip() != "0":
                        gate.require(time.monotonic() < drained, "PostgreSQL contract sessions did not stop")
                        time.sleep(0.01)
                    gate.command([*sql, "DROP DATABASE " + database],
                                 timeout=max(0.1, deadline - time.monotonic()), stdout=stream, stderr=subprocess.STDOUT)
            results[suite] = {"binary_sha256": gate.digest(binary), "inventory": names,
                              "fresh_database_per_contract": True, "executed": len(names)}
        return results
    finally:
        environment.unlink(missing_ok=True)


def finish(local, report, started):
    original, failures = sys.exception(), []
    try:
        cleanup(containers_only=True)
        report["containers_stopped"] = True
    except BaseException as error:
        failures.append(error)
        report["cleanup_errors"] = [type(error).__name__]
    report["elapsed_seconds"] = time.monotonic() - started
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=".report-", dir=local)
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(json.dumps(report, indent=2) + "\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, local / "report.json")
        parent = os.open(local, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    except BaseException as error:
        failures.append(error)
    finally:
        if temporary is not None:
            try:
                Path(temporary).unlink(missing_ok=True)
            except BaseException as error:
                failures.append(error)
    if failures:
        if original is not None:
            original.add_note("Runtime cleanup/report also failed: " + ", ".join(type(error).__name__ for error in failures))
        elif len(failures) == 1:
            raise failures[0]
        else:
            raise BaseExceptionGroup("Runtime cleanup/report failed", failures)


def run(args):
    gate.require(getattr(args, "traced", False), "run runtime contracts through the successful-exec observer")
    token = gate.verify_unit(args.unit, args.slice, cleanup_command=CLEANUP_COMMAND)
    private(DIRECTORIES)
    local = directory(token)
    private(local)
    ready = (local / "trace-ready").lstat()
    gate.require(stat.S_ISREG(ready.st_mode) and ready.st_uid == 0
                 and stat.S_IMODE(ready.st_mode) == 0o644, "process tracing must precede runtime work")
    runtime = inspect_image(args.runtime_image, args.revision, "runtime")
    fixture = inspect_image(args.fixture_image, args.revision, "runtime-fixture")
    oracle = inspect_image(args.oracle_image, args.revision, "external-client-oracle")
    gate.require(len({runtime, fixture, oracle}) == 3, "distinct prebuilt image roles required")
    if args.phase in {"metadata", "leader"}:
        verify_contracts(args.target_dir.resolve(strict=True), args.revision)
    report = {"phase": args.phase, "completed": False, "revision": args.revision,
              "images": {"runtime": runtime, "fixture": fixture, "oracle": oracle},
              "unit": args.unit, "slice": args.slice,
              "limit_scope": "aggregate controller, backend, PG/S3, k3s and CRI pods; excludes Docker daemon overhead"}
    started = time.monotonic()
    deadline = args.deadline
    gate.require(math.isfinite(deadline) and 0 < deadline - started <= gate.RUNTIME_SECONDS,
                 "invalid runtime work deadline")
    try:
        cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))))
        group = gate.output(["systemctl", "show", args.slice, "--property=ControlGroup", "--value"])
        pod_root = group + "/pods"
        cluster = None
        controller_inputs = []
        if args.phase == "istio":
            from istio_artifacts import MAX_BYTES, verify_artifacts
            inputs = args.istio_artifacts
            prepared = verify_artifacts(inputs)
            executable = shutil.which(args.helm)
            gate.require(executable is not None, "prepare the Helm executable before measurement")
            helm = Path(executable).resolve(strict=True)
            gate.require(helm.is_file() and helm.stat().st_size <= MAX_BYTES and os.access(helm, os.X_OK),
                         "bounded prebuilt Helm executable required")
            with helm.open("rb") as source:
                gate.require(source.read(4) == b"\x7fELF", "the prebuilt Helm input must be a Linux ELF")
            report["istio_inputs"] = {"artifacts_sha256": prepared, "helm_sha256": gate.digest(helm)}
            controller_inputs = ["--mount", f"type=bind,src={inputs.resolve(strict=True)},dst=/opt/git-rs/istio-inputs,readonly",
                                 "--mount", f"type=bind,src={helm},dst=/usr/local/bin/helm,readonly",
                                 "--env", "GIT_RS_TEST_HELM_SHA256=" + report["istio_inputs"]["helm_sha256"]]
        if args.phase in {"failures", "maintenance", "cache-matrix", "leader", "kubernetes", "istio"}:
            cluster = start_cluster(token, args.slice, cpus, pod_root, deadline, local)
            report["cluster_id"] = cluster
        with (local / "controller.log").open("xb") as log:
            remaining = max(0.1, deadline - time.monotonic())
            if args.phase == "runtime":
                for image, role, output in ((runtime, "runtime", "inventory"),
                                            (fixture, "runtime-fixture", "fixture-inventory")):
                    gate.command([sys.executable, "-B", ROOT / "tools/check-runtime-boundaries.py",
                        "--runtime-image", image, "--role", role, "--revision", args.revision, "--directory", local / output],
                        timeout=max(0.1, deadline - time.monotonic()), stdout=log, stderr=subprocess.STDOUT)
            elif args.phase == "metadata":
                report["contracts"] = serial_sql(args, local, oracle, token, cpus, deadline)
            elif args.phase == "leader":
                binary = contracts(args.target_dir.resolve() / "debug", "leader_dependency")
                gate.command([sys.executable, "-B", ROOT / "tests/leader_dependency.py", "--cluster-container", cluster,
                    "--test-binary", binary, "--directory", local / "candidate"],
                    timeout=remaining, stdout=log, stderr=subprocess.STDOUT)
            elif args.phase == "kubernetes":
                env = dict(os.environ, GIT_RS_TEST_POD_CGROUP_ROOT=pod_root)
                gate.command([sys.executable, "-B", ROOT / "tests/kubernetes.py", "--cluster-container", cluster,
                    "--runtime-image", runtime, "--oracle-image", oracle, "--revision", args.revision,
                    "--fixture-dir", local / "fixture", "--helm", args.helm],
                    timeout=remaining, stdout=log, stderr=subprocess.STDOUT, env=env)
            else:
                gate.command(["docker", "run", "--rm", "--pull=never", "--init", "--read-only", "--network", "host",
                    "--name", "git-rs-runtime-controller-" + token, "--label", gate.LABEL + token,
                    "--cgroup-parent", args.slice, "--cpuset-cpus", cpus, "--cpus", ".5",
                    "--memory", "1024m", "--memory-swap", "1024m", "--pids-limit", "96",
                    "--user", f"{os.getuid()}:{os.getgid()}",
                    "--group-add", str(Path("/var/run/docker.sock").stat().st_gid),
                    "--volume", "/var/run/docker.sock:/var/run/docker.sock",
                    "--volume", f"{shutil.which('docker')}:/usr/bin/docker:ro",
                    "--mount", f"type=bind,src={local},dst=/opt/git-rs/.local",
                    *controller_inputs,
                    "--tmpfs", "/tmp:rw,nosuid,nodev,size=128m,mode=1777",
                    "--env", "INVOCATION_ID=" + token, "--env", "GIT_RS_GATE_SLICE=" + args.slice,
                    "--env", "GIT_RS_TEST_OWNER_LABEL=" + gate.LABEL + token,
                    "--env", "GIT_RS_TEST_HOST_ROOT=" + str(local),
                    "--env", "GIT_RS_TEST_CONTROLLER_ROOT=/opt/git-rs/.local",
                    "--env", "GIT_RS_TEST_SERVICE_CPUS=" + cpus,
                    "--env", "GIT_RS_TEST_RUNTIME_IMAGE=" + runtime,
                    "--env", "GIT_RS_TEST_RUNTIME_FIXTURE_IMAGE=" + fixture,
                    "--env", "GIT_RS_TEST_ORACLE_IMAGE=" + oracle,
                    "--env", "GIT_RS_TEST_REVISION=" + args.revision,
                    "--env", "GIT_RS_TEST_POD_CGROUP_ROOT=" + pod_root,
                    "--entrypoint", "/opt/test-python/bin/python3", oracle, "-B", "/opt/git-rs/tests/runtime_gate.py", "inside",
                    "--phase", args.phase, *(["--cluster-container", cluster] if cluster else [])],
                    timeout=remaining, stdout=log, stderr=subprocess.STDOUT)
        if cluster:
            report["cri_tasks"] = cluster_cgroups(cluster, pod_root)
        report["work_completed"] = True
    except BaseException as error:
        report["failure"] = type(error).__name__
        raise
    finally:
        # The still-traced root helper drains actual CRI tasks and repeats
        # credential cleanup after this unprivileged child has exited.
        finish(local, report, started)


def inside(args):
    gate.invocation()
    proxy = Path("/tmp/runtime-gate-bin")
    proxy.mkdir(mode=0o700)
    executable = proxy / "docker"
    executable.write_text('#!/bin/sh\nexec /usr/bin/python3 -B /opt/git-rs/tests/blame_gate.py docker "$@"\n')
    executable.chmod(0o700)
    os.environ["PATH"] = str(proxy) + ":" + os.environ["PATH"]
    arguments = [sys.executable, "-B", "/opt/git-rs/tests/integration.py", "--provision",
                 "--binary", "/usr/local/bin/git-rs", "--fixture-dir", "/opt/git-rs/.local/fixture"]
    if args.phase == "istio":
        gate.require(args.cluster_container, "supervisor-owned Istio cluster required")
        arguments = [sys.executable, "-B", "/opt/git-rs/tests/istio.py",
            "--cluster-container", args.cluster_container, "--fixture-dir", "/opt/git-rs/.local/fixture",
            "--image", os.environ["GIT_RS_TEST_RUNTIME_IMAGE"], "--oracle-image", os.environ["GIT_RS_TEST_ORACLE_IMAGE"],
            "--revision", os.environ["GIT_RS_TEST_REVISION"], "--helm", "/usr/local/bin/helm",
            "--artifacts", "/opt/git-rs/istio-inputs"]
    elif args.phase == "integration":
        arguments += ["--large-mib", "64", "--debug-timings", "--long-operations"]
    elif args.phase == "receive":
        arguments += ["--receive-core-only"]
    elif args.phase in {"upload", "history"}:
        arguments += ["--" + args.phase + "-core-only"]
    else:
        gate.require(args.phase in {"failures", "maintenance", "cache-matrix"} and args.cluster_container, "maintenance cluster required")
        arguments[2] = "/opt/git-rs/tests/" + args.phase.replace("-", "_") + ".py"
        arguments += ["--cluster-container", args.cluster_container]
    os.execv(arguments[0], arguments)


def collect(args):
    gate.require(os.geteuid() == 0, "collect the private fixture diagnostics as root")
    gate.require(not args.destination.exists(), "diagnostic destination must be new")
    args.destination.mkdir(mode=0o755)
    paths = list(DIRECTORIES.iterdir()) if DIRECTORIES.exists() else []
    gate.require(len(paths) <= 16, "unexpected runtime gate directory count")
    for local in paths:
        gate.require(re.fullmatch(r"[0-9a-f]{32}", local.name) and not local.is_symlink(), "unexpected gate directory")
        output = args.destination / local.name
        output.mkdir(mode=0o755)
        # Explicit synthetic diagnostics only: never export kubeconfig, service
        # account files, TLS/SSH keys, env-files or arbitrary fixture directories.
        files = [local / "report.json", local / "controller.log", local / "fixture/report.json",
                   local / "metadata.log", local / "maintenance.log", local / "maintenance_leader.log", local / "candidate/report.json",
                  local / "inventory/report.json", local / "fixture-inventory/report.json",
                  local / "exec-report.json", local / "work.log"]
        gate.require(not (local / "fixture").is_symlink() and not (local / "candidate").is_symlink(), "linked fixture directory")
        files += list((local / "fixture").glob("*.log"))
        files += list((local / "candidate").glob("*.log"))
        for index, path in enumerate(files):
            if path.is_file():
                gate.require(not path.is_symlink(), "linked diagnostic file")
                target = output / f"{index}-{path.name}"
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(descriptor, "rb") as source, target.open("xb") as destination:
                    metadata = os.fstat(source.fileno())
                    gate.require(stat.S_ISREG(metadata.st_mode), "diagnostic is not a regular file")
                    source.seek(max(0, metadata.st_size - MAX_DIAGNOSTIC_BYTES))
                    remaining = MAX_DIAGNOSTIC_BYTES
                    while remaining:
                        chunk = source.read(min(64 * 1024, remaining))
                        if not chunk:
                            break
                        destination.write(chunk)
                        remaining -= len(chunk)
                target.chmod(0o644)


def arguments(parser):
    parser.add_argument("--unit", required=True)
    parser.add_argument("--slice", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--fixture-image", required=True)
    parser.add_argument("--oracle-image", required=True)
    parser.add_argument("--helm", default="helm")
    parser.add_argument("--istio-artifacts", type=Path, default=ROOT / ".local/istio-artifacts")
    parser.add_argument("--target-dir", type=Path, default=ROOT / "target")
    parser.add_argument("--phase", choices=PHASES, required=True)
    parser.add_argument("--cluster-container")


def main():
    os.environ["DOCKER_HOST"] = "unix:///var/run/docker.sock"
    os.environ.pop("DOCKER_CONTEXT", None)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    cleaning = commands.add_parser("cleanup")
    cleaning.add_argument("--containers-only", action="store_true")
    cleaning.add_argument("--deadline", type=float)
    images = commands.add_parser("images")
    images.add_argument("--phase", choices=PHASES)
    sealing = commands.add_parser("export-contracts")
    sealing.add_argument("--target-dir", type=Path, default=ROOT / "target")
    sealing.add_argument("--destination", type=Path, required=True)
    export = commands.add_parser("collect")
    export.add_argument("--destination", type=Path, required=True)
    inside_parser = commands.add_parser("inside")
    inside_parser.add_argument("--phase", choices=PHASES, required=True)
    inside_parser.add_argument("--cluster-container")
    args = parser.parse_args()
    if args.action == "inside":
        inside(args)
    elif args.action == "cleanup":
        cleanup(containers_only=args.containers_only, deadline=args.deadline)
    elif args.action == "collect":
        collect(args)
    elif args.action == "export-contracts":
        export_contracts(args)
    else:
        dependencies = [PG_IMAGE, S3_IMAGE, K3S]
        if args.phase == "istio":
            from istio_artifacts import IMAGES
            dependencies.extend(IMAGES)
        print("\n".join(dependencies))


if __name__ == "__main__":
    main()
