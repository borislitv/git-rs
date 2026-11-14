#!/usr/bin/env python3
"""Trace successful execs across the owned runtime aggregate, including short tasks.

bpftrace is an external qualification tool, never part of a server image. The
root observer shares the bounded slice; its work child drops to the image UID.
Only executable paths/cgroup IDs are recorded, never argv, environment or tokens.
"""
import argparse
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shlex
import signal
import stat
import subprocess
import sys
import tempfile
import time

import runtime_gate as runtime


UID = GID = 10001
MAX_BYTES = 16 * 1024 * 1024
MAX_CGROUPS = 1024
MAX_EXECS = 100_000
BACKEND_PROGRAMS = {"git-rs", "tini", "docker-init", "cat", "sh", "dash", "env",
                    "mkdir", "chmod", "cp", "sshd", "sshd-session", "sshd-auth",
                    "blame-http-fixture", "receive-fixture"}


def private(path, owner, mode):
    value = path.lstat()
    runtime.gate.require(not path.is_symlink() and value.st_uid == owner
                         and stat.S_IMODE(value.st_mode) == mode, "wrong trace input ownership/mode")


def read(path, *, owner=None, mode=None):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        value = os.fstat(source.fileno())
        runtime.gate.require(stat.S_ISREG(value.st_mode) and value.st_size <= MAX_BYTES,
                             "invalid or oversized process trace input")
        runtime.gate.require(owner is None or value.st_uid == owner, "process trace input owner changed")
        runtime.gate.require(mode is None or stat.S_IMODE(value.st_mode) == mode,
                             "process trace input mode changed")
        data = source.read(MAX_BYTES + 1)
    runtime.gate.require(len(data) <= MAX_BYTES, "process trace byte bound exceeded")
    return data.decode("utf-8")


def write_json(path, value, *, owner=0, mode=0o600):
    descriptor, temporary = tempfile.mkstemp(prefix=".observer-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as destination:
            os.fchown(destination.fileno(), owner, GID if owner == UID else 0)
            os.fchmod(destination.fileno(), mode)
            json.dump(value, destination, indent=2)
            destination.write("\n")
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def time_left(deadline, maximum):
    remaining = deadline - time.monotonic()
    runtime.gate.require(math.isfinite(remaining) and remaining > 0, "process observer absolute deadline expired")
    return min(remaining, maximum)


def scope(args, deadline):
    runtime.gate.require(os.geteuid() == 0, "the bounded process observer requires root")
    token = runtime.gate.verify_unit(args.unit, args.slice, cleanup_command=runtime.CLEANUP_COMMAND,
                                     deadline=deadline)
    group = runtime.gate.output(["systemctl", "show", args.slice, "--property=ControlGroup", "--value"],
                                timeout=time_left(deadline, 5))
    controller = Path("/proc/self/cgroup").read_text().strip().removeprefix("0::")
    runtime.gate.require(Path(controller).parent == Path(group), "controller must be a direct child of its owned slice")
    return {"owner": token, "group": group, "controller": controller}


def unprivileged(arguments, deadline):
    # subprocess sets all supplementary groups before dropping GID/UID. The
    # observer itself retains root only for BPF and its exact-owned cgroup drain.
    return runtime.gate.command([sys.executable, "-B", *arguments], timeout=time_left(deadline, 900),
        user=UID, group=GID, extra_groups=sorted({GID, Path("/var/run/docker.sock").stat().st_gid}))


def drain(owned, deadline):
    root = runtime.CGROUPS / owned["group"].lstrip("/")
    pods = root / "pods"
    if pods.exists():
        runtime.gate.require(pods.is_dir() and not pods.is_symlink(), "invalid owned pod cgroup")
        if runtime.populated(pods):
            (pods / "cgroup.kill").write_text("1\n")
            ending = min(deadline, time.monotonic() + 5)
            while runtime.populated(pods):
                time_left(ending, 1)
                time.sleep(0.01)
    runtime.gate.require(runtime.aggregate_empty(owned["group"], owned["controller"]),
                         "actual backend/CRI tasks remain in the aggregate")


def cleanup(args, *, deadline=None):
    deadline = min(deadline or float("inf"), time.monotonic() + runtime.CLEANUP_SECONDS)
    owned = scope(args, deadline)
    local = runtime.directory(owned["owner"])
    command = [str(runtime.ROOT / "tests/runtime_gate.py"), "cleanup", "--deadline", str(deadline)]
    unprivileged([*command, "--containers-only"], deadline)
    if local.exists():
        private(local, UID, 0o700)
        saved = json.loads(read(local / "observer/scope.json", owner=0, mode=0o600))
        runtime.gate.require(saved == owned, "aggregate ownership changed before cleanup")
        drain(owned, deadline)
        write_json(local / "drain-proof.json", {**owned, "drained": True}, mode=0o644)
    else:
        runtime.gate.require(runtime.aggregate_empty(owned["group"], owned["controller"]),
                             "unrecorded aggregate tasks require inspection")
    unprivileged(command, deadline)


def program(group, controller):
    runtime.gate.require(group.startswith("/") and controller.startswith(group + "/")
                         and len(group) < 768 and re.fullmatch(r"/[A-Za-z0-9_./:-]+", controller),
                         "invalid trace cgroup root")
    # Seed the already-created service. Every later descendant cgroup is recorded
    # in-kernel before it can emit a successful-exec event, including CRI tasks.
    return '''config = { max_strlen=1024; max_map_keys=4096; print_maps_on_exit=false; }
BEGIN {
  @owned[cgroupid("/sys/fs/cgroupGROUP")] = 1;
  printf("C\\t%llu\\tGROUP\\n", cgroupid("/sys/fs/cgroupGROUP"));
  @owned[cgroupid("/sys/fs/cgroupCONTROLLER")] = 1;
  printf("C\\t%llu\\tCONTROLLER\\n", cgroupid("/sys/fs/cgroupCONTROLLER"));
}
tracepoint:cgroup:cgroup_mkdir {
  $path = str(args.path);
  if (strncmp($path, "GROUP/", LENGTH) == 0) {
    @owned[args.id] = 1;
    printf("C\\t%llu\\t%s\\n", args.id, $path);
  }
}
tracepoint:sched:sched_process_exec /has_key(@owned, cgroup)/ {
  printf("E\\t%llu\\t%d\\t%s\\n", cgroup, pid, str(args.filename));
}
END { printf("Z\\n"); }
'''.replace("CONTROLLER", controller).replace("GROUP", group).replace("LENGTH", str(len(group) + 1))


def inspect_trace(local, group, images, phase):
    observer = local / "observer"
    private(observer, 0, 0o700)
    runtime.gate.require(not read(observer / "exec.stderr", owner=0, mode=0o600).strip(),
                         "process tracer reported warnings, lost events or errors")
    runtime.gate.require(not read(observer / "docker.stderr", owner=0, mode=0o600).strip(), "Docker event stream failed")
    owner = local.name
    clients, backends = set(), set()
    for line in read(observer / "docker.trace", owner=0, mode=0o600).splitlines():
        row = json.loads(line)
        attributes = row["Actor"]["Attributes"]
        runtime.gate.require(row["Type"] == "container" and row["Action"] == "create"
                             and attributes.get(runtime.gate.LABEL[:-1]) == owner
                             and re.fullmatch(r"[0-9a-f]{64}", row["Actor"]["ID"]),
                             "unowned Docker lifecycle event")
        identity = row["Actor"]["ID"]
        if attributes["image"] == images["oracle"]:
            clients.add(identity)
        elif attributes["image"] in {images["runtime"], images["fixture"]}:
            backends.add(identity)
    cri = local / "fixture/cri-containers.jsonl"
    if cri.exists():
        rows = read(cri, owner=UID, mode=0o600).splitlines()
        runtime.gate.require(len(rows) <= 1024, "Kubernetes image identity inventory exceeded its bound")
        for line in rows:
            value = json.loads(line)
            role = value["role"]
            expected = images["oracle"] if role == "external-client-oracle" else images["runtime"]
            runtime.gate.require(role in {"runtime", "external-client-oracle"}
                                 and value["image"]["docker_id"] == expected
                                 and value["image_id"] in {value["image"]["reference"], value["image"]["cri_id"]}
                                 and re.fullmatch(r"[0-9a-f]{64}", value["id"]),
                                 "Kubernetes process image identity differs")
            (clients if role == "external-client-oracle" else backends).add(value["id"])
    runtime.gate.require(not clients.intersection(backends), "client and backend process identities overlap")
    groups, executions, ended, padding = {}, [], 0, 0
    data = read(observer / "exec.trace", owner=0, mode=0o600)
    runtime.gate.require(data.endswith("\n"), "truncated process trace stream")
    for line in data.splitlines():
        if not line and ended == 1:
            # bpftrace emits two terminal blank lines after END, even with -q.
            padding += 1
            runtime.gate.require(padding <= 2, "excess process trace terminal spacing")
            continue
        runtime.gate.require(ended == 0, "process trace record after end marker")
        fields = line.split("\t")
        if fields[0] == "C":
            runtime.gate.require(len(fields) == 3 and fields[1].isdigit() and len(fields[2]) < 1023
                                 and (fields[2] == group or fields[2].startswith(group + "/")),
                                 "invalid process cgroup event")
            key = int(fields[1])
            runtime.gate.require(key > 0 and groups.get(key, fields[2]) == fields[2], "cgroup identity changed")
            groups[key] = fields[2]
            runtime.gate.require(len(groups) <= MAX_CGROUPS, "process cgroup count exceeded its bound")
        elif fields[0] == "E":
            runtime.gate.require(len(fields) == 4 and fields[1].isdigit() and fields[2].isdigit()
                                 and 0 < int(fields[2]) < 2**31 and 0 < len(fields[3]) < 1023,
                                 "invalid or truncated process exec event")
            executions.append((int(fields[1]), fields[3]))
            runtime.gate.require(len(executions) <= MAX_EXECS, "successful-exec count exceeded its bound")
        else:
            runtime.gate.require(fields == ["Z"], "unexpected process tracer output")
            ended += 1
    runtime.gate.require(0 < len(groups) <= MAX_CGROUPS and len(executions) <= MAX_EXECS and ended == 1,
                         "incomplete or oversized process lifecycle trace")
    observed, native_clients, backend_programs = False, 0, set()
    for identity, executable in executions:
        runtime.gate.require(identity in groups, "exec has no recorded cgroup")
        path = groups[identity]
        containers = {match[1] for part in PurePosixPath(path).parts
                      if (match := re.fullmatch(r"(?:docker-|cri-containerd-)?([0-9a-f]{64})(?:\.scope)?", part))}
        name = PurePosixPath(executable).name
        if executable == str(local / "exec-probe"):
            observed = True
        # A checkout directory such as git-rs-fixture is not an executable. Git
        # helpers are recognized by basename or their actual git-core directory.
        native = (name in {"git", "gitk", "scalar", "gitweb.cgi"}
                  or (name.startswith("git-") and name != "git-rs")
                  or "git-core" in PurePosixPath(executable).parts)
        if native:
            runtime.gate.require(bool(containers & clients) and not bool(containers & backends),
                                 "native Git executed outside a bound external client/oracle")
            native_clients += 1
        if containers & backends:
            runtime.gate.require(name in BACKEND_PROGRAMS, "unexpected backend executable: " + name)
            backend_programs.add(name)
    runtime.gate.require(observed, "successful-exec probe was not captured before work")
    if phase in {"integration", "receive", "upload", "history", "failures", "maintenance", "cache-matrix", "kubernetes", "istio"}:
        runtime.gate.require(native_clients > 0 and "git-rs" in backend_programs,
                              "runtime/native-client execution coverage missing")
    return {"status": "passed", "successful_execs": len(executions), "cgroups": len(groups),
            "external_native_execs": native_clients, "bound_backend_programs": sorted(backend_programs),
            "probe_seen": observed, "native_git_outside_external_oracle": 0}


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
    process.wait(timeout=2)


def trace(args, arguments):
    started = time.monotonic()
    deadline = started + runtime.gate.RUNTIME_SECONDS
    owned = scope(args, deadline)
    token, group, controller = (owned[key] for key in ("owner", "group", "controller"))
    private(runtime.DIRECTORIES, UID, 0o700)
    local = runtime.directory(token)
    local.mkdir(mode=0o700)
    os.chown(local, UID, GID)
    observer_files = local / "observer"
    observer_files.mkdir(mode=0o700)
    # No pre-existing sibling work may escape the cgroup-mkdir subscription.
    root = runtime.CGROUPS / group.lstrip("/")
    runtime.gate.require([child.name for child in root.iterdir() if child.is_dir()] == [Path(controller).name],
                         "the runtime aggregate must start with only its controller")
    runtime.gate.require(not (root / "cgroup.procs").read_text().strip(), "unexpected direct aggregate tasks")
    write_json(observer_files / "scope.json", owned)
    write_json(local / "unit-proof.json", {**owned, "unit": args.unit, "slice": args.slice}, mode=0o644)
    (local / "exec-probe").symlink_to("/usr/bin/true")
    script = observer_files / "exec.bt"
    script.write_text(program(group, controller))
    script.chmod(0o400)
    version = runtime.gate.output(["bpftrace", "--version"])
    environment = {key: value for key, value in os.environ.items() if not key.startswith("BPFTRACE_")}
    environment.update(BPFTRACE_COLOR="never", DOCKER_HOST="unix:///var/run/docker.sock",
                       GIT_RS_RUNTIME_DEADLINE=str(deadline))
    environment.pop("DOCKER_CONTEXT", None)
    processes, streams = [], []
    report = {"status": "failed", "observer": version}
    selector = selectors.DefaultSelector()
    try:
        events = subprocess.Popen(["docker", "events", "--since", str(int(time.time()) - 1),
            "--filter", "type=container", "--filter", "event=create", "--filter", "label=" + runtime.gate.LABEL + token,
            "--format", "{{json .}}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
        processes.append(events)
        child = [sys.executable, "-B", str(Path(__file__).resolve()), "work", *arguments]
        observer = subprocess.Popen(["bpftrace", "-q", "-k", "-B", "line", "-c", shlex.join(child), str(script)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment)
        processes.append(observer)
        for process, prefix in ((events, "docker"), (observer, "exec")):
            for pipe, suffix in ((process.stdout, ".trace"), (process.stderr, ".stderr")):
                destination = (observer_files / (prefix + suffix)).open("xb")
                os.fchmod(destination.fileno(), 0o600)
                streams.append(destination)
                selector.register(pipe, selectors.EVENT_READ, [destination, 0, bytearray(), prefix + suffix])
        ending = False
        while selector.get_map():
            runtime.gate.require(time.monotonic() < deadline, "process observer absolute deadline expired")
            if observer.poll() is not None and not ending:
                runtime.gate.require(events.poll() is None, "Docker lifecycle observer ended early")
                events.terminate()
                ending = True
            for key, _ in selector.select(timeout=0.1):
                output, size, pending, name = key.data
                chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                size += len(chunk)
                runtime.gate.require(size <= MAX_BYTES, "process observer output exceeded its bound")
                key.data[1] = size
                output.write(chunk)
                if name == "exec.trace":
                    pending.extend(chunk)
                    while b"\n" in pending:
                        line, _, tail = pending.partition(b"\n")
                        pending[:] = tail
                        if line.startswith(b"E\t") and line.split(b"\t")[-1] == os.fsencode(local / "exec-probe"):
                            ready = local / "trace-ready"
                            if not ready.exists():
                                descriptor = os.open(ready, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                                os.fchmod(descriptor, 0o644)
                                os.close(descriptor)
                    runtime.gate.require(len(pending) <= 4096, "unterminated process trace record")
        runtime.gate.require(observer.wait(timeout=2) == 0, "process tracer failed")
        # Docker may handle our SIGTERM and return its conventional 128+signal
        # status. It must still have been alive before our requested stop.
        runtime.gate.require(ending and events.wait(timeout=2) in {0, -signal.SIGTERM, 128 + signal.SIGTERM},
                             "Docker lifecycle observer ended before its requested stop")
        for stream in streams:
            stream.flush()
        work = json.loads(read(local / "report.json", owner=UID, mode=0o600))
        runtime.gate.require(work.get("work_completed") and work.get("cleanup_completed"), "runtime phase did not complete")
        report.update(inspect_trace(local, group, work["images"], args.phase))
        work["process_trace"] = report
        work["completed"] = True
        write_json(local / "report.json", work, owner=UID)
    except BaseException as error:
        report["failure"] = type(error).__name__
        raise
    finally:
        failures = []
        try:
            for process in reversed(processes):
                try:
                    stop(process)
                except BaseException as error:
                    failures.append(error)
                finally:
                    if process.stdout:
                        process.stdout.close()
                    if process.stderr:
                        process.stderr.close()
        finally:
            selector.close()
            for stream in streams:
                stream.close()
            report["elapsed_seconds"] = time.monotonic() - started
            if failures:
                report["status"] = "failed"
                report["stop_failure"] = type(failures[0]).__name__
                work_path = local / "report.json"
                if work_path.exists():
                    work = json.loads(read(work_path, owner=UID, mode=0o600))
                    work["completed"] = False
                    write_json(work_path, work, owner=UID)
            write_json(local / "exec-report.json", report)
        if failures:
            raise failures[0]


def work(args, arguments):
    local = runtime.directory(runtime.gate.invocation())
    with (local / "work.log").open("xb") as output:
        os.fchmod(output.fileno(), 0o600)
        os.fchown(output.fileno(), UID, GID)
        os.dup2(output.fileno(), 1)
        os.dup2(output.fileno(), 2)
    deadline = time.monotonic() + 30
    while not (local / "trace-ready").exists():
        runtime.gate.require(time.monotonic() < deadline, "successful-exec tracing did not become ready")
        subprocess.run([local / "exec-probe"], check=True, timeout=2)
        time.sleep(0.05)
    outer = float(os.environ["GIT_RS_RUNTIME_DEADLINE"])
    runtime.gate.require(0 < outer - time.monotonic() <= runtime.gate.RUNTIME_SECONDS,
                         "invalid process observer deadline")
    try:
        # Reserve the final 40 seconds for actual-owner drain, credentials and
        # tracer completion. The root helper remains alive (and traced) until
        # the unprivileged phase and independent cleanup both finish.
        unprivileged([str(Path(__file__).resolve()), "phase", *arguments], outer - 40)
    finally:
        cleanup(args, deadline=outer - 2)
        report_path = local / "report.json"
        report = (json.loads(read(report_path, owner=UID, mode=0o600)) if report_path.exists()
                  else {"phase": args.phase, "completed": False, "work_completed": False})
        report["cleanup_completed"] = True
        write_json(local / "report.json", report, owner=UID)


def phase(args):
    runtime.gate.require(os.geteuid() == UID and os.getegid() == GID, "the runtime work child must use the image account")
    args.traced = True
    args.deadline = float(os.environ["GIT_RS_RUNTIME_DEADLINE"]) - 40 - runtime.CLEANUP_SECONDS
    runtime.run(args)


def main():
    os.environ["DOCKER_HOST"] = "unix:///var/run/docker.sock"
    os.environ.pop("DOCKER_CONTEXT", None)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("run", "work", "phase"):
        runtime.arguments(commands.add_parser(action))
    cleaning = commands.add_parser("cleanup")
    cleaning.add_argument("--unit", required=True)
    cleaning.add_argument("--slice", required=True)
    args = parser.parse_args()
    if args.action == "run":
        trace(args, sys.argv[2:])
    elif args.action == "work":
        work(args, sys.argv[2:])
    elif args.action == "phase":
        phase(args)
    else:
        cleanup(args)


if __name__ == "__main__":
    main()
