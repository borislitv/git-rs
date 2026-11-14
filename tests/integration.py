#!/usr/bin/env python3
"""Black-box native Git/SQL/S3 tests. All data and identities are synthetic."""
import argparse
import base64
import concurrent.futures
from contextlib import nullcontext
import hashlib
import http.client
import json
import os
from pathlib import Path
import pwd
import random
import re
import select
import shlex
import shutil
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from urllib.parse import quote_from_bytes

ROOT = Path(__file__).resolve().parents[1]
PG_IMAGE = "postgres:18.6-bookworm@sha256:f1ea2a1a4fb7b68d4836be1fdcb8a6466a89b072158bf9d97663600538c294c5"
S3_IMAGE = "versity/versitygw@sha256:89cad13cc981986ee284cbafea4613f9533ce6fd5f37a2c96c4c4fb8adb3a316"


def command(args, *, env=None, cwd=None, data=None, ok=True):
    if args[0] == "docker":
        env = dict(env or os.environ, DOCKER_HOST="unix:///var/run/docker.sock")
        env.pop("DOCKER_CONTEXT", None)
    result = subprocess.run([str(a) for a in args], input=data, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env, cwd=cwd, timeout=180)
    if ok and result.returncode:
        raise AssertionError(f"{args[0:3]} failed ({result.returncode}): "
                             f"{result.stdout[-4000:]!r} {result.stderr[-4000:]!r}")
    return result


def wait_for(check, description):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out: {description}")


def process_owned(record):
    """Keep PID reuse distinct from the original worker's lifetime."""
    try:
        fields = Path(f"/proc/{record['pid']}/stat").read_text().rsplit(")", 1)[1].split()
        return int(fields[19]) == record["startTicks"]
    except (FileNotFoundError, ProcessLookupError):
        return False


def metric_value(body, series):
    prefix = series.encode() + b" "
    values = [float(line[len(prefix):]) for line in body.splitlines() if line.startswith(prefix)]
    assert len(values) == 1, f"expected exactly one metric series: {series}"
    return values[0]


def health_framing_test(f, name):
    """Real TCP framing, not an in-memory request-line parser test."""
    address = ("127.0.0.1", f.health_ports[name])
    with socket.create_connection(address, timeout=3) as peer:
        peer.sendall(b"GET /livez HTTP/1.1\r\n")
        assert not select.select([peer], [], [], 0.2)[0], "health replied before complete headers"
        peer.sendall(b"Host: fixture.invalid\r\nX-Probe: fragmented\r\nContent-Length: 0\r\n\r\n")
        reply = http.client.HTTPResponse(peer)
        reply.begin()
        body = reply.read()
        assert (reply.status, body) == (200, b"live\n")
        assert int(reply.getheader("Content-Length")) == len(body)
        assert reply.getheader("Connection") == "close"
        assert peer.recv(1) == b"", "health must close after its complete response"
    for label, headers in [
        ("missing colon", b"invalid-header\r\n\r\n"),
        ("invalid name", b"Bad Name: value\r\n\r\n"),
        ("control byte", b"X-Probe: bad\x01value\r\n\r\n"),
        ("oversized line", b"X-Probe: " + b"a" * 8192 + b"\r\n\r\n"),
        ("oversized total", (b"X-Probe: " + b"a" * 8000 + b"\r\n") * 3 + b"\r\n"),
        ("too many headers", b"X-Probe: value\r\n" * 65 + b"\r\n"),
        ("unexpected body", b"Content-Length: 1\r\n\r\nx"),
        ("transfer encoding", b"Transfer-Encoding: chunked\r\n\r\n0\r\n\r\n"),
    ]:
        with socket.create_connection(address, timeout=3) as peer:
            try:
                peer.sendall(b"GET /livez HTTP/1.1\r\n" + headers)
                peer.shutdown(socket.SHUT_WR)
                response = peer.recv(4096)
            except (BrokenPipeError, ConnectionResetError):
                response = b""
            assert not response.startswith(b"HTTP/1.1 200"), (label, response)
    with socket.create_connection(address, timeout=3) as peer:
        start = time.monotonic()
        peer.sendall(b"GET /livez HTTP/1.1\r\nHost: incomplete")
        try:
            response = peer.recv(4096)
        except ConnectionResetError:
            response = b""
        assert response == b"", "incomplete headers must not receive success"
        assert time.monotonic() - start < 2.5, "health must retain its one-second absolute interrupt"
    f.results["health_complete_bounded_headers_and_connection_close"] = True


class Balancer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, ports):
        self.ports = ports
        self.counts = [0] * len(ports)
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), Relay)


class Relay(socketserver.BaseRequestHandler):
    def handle(self):
        with self.server.lock:
            index = sum(self.server.counts) % len(self.server.ports)
            self.server.counts[index] += 1
        with socket.create_connection(("127.0.0.1", self.server.ports[index]), 30) as upstream:
            peers = {self.request: upstream, upstream: self.request}
            while peers:
                readable, _, _ = select.select(list(peers), [], [], 130)
                if not readable:
                    return
                for source in readable:
                    data = source.recv(65536)
                    target = peers[source]
                    if not data:
                        target.shutdown(socket.SHUT_WR)
                        del peers[source]
                    else:
                        target.sendall(data)


class Fixture:
    def __init__(self, base, binary, *, runtime_image=None, runtime_fixture_image=None, revision=None):
        self.base, self.binary = Path(base).resolve(), str(binary)
        self.containers, self.processes, self.logs = [], [], []
        self.daemons, self.health_ports = {}, {}
        self.results = {}
        self.blame_fixture_binary = None
        self.lease_fixture = None
        self.blame_core_resources = False
        self.identity = uuid.uuid4().hex
        self.containers_started = bool(runtime_image)
        self.env = {k: v for k, v in os.environ.items() if k in ["PATH", "LANG", "TERM"]}
        self.env.update(HOME=str(base), LC_ALL="C", GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
                        GIT_TERMINAL_PROMPT="0", GIT_AUTHOR_NAME="Test Author",
                         GIT_AUTHOR_EMAIL="author@example.invalid", GIT_COMMITTER_NAME="Test Author",
                         GIT_COMMITTER_EMAIL="author@example.invalid", GIT_RS_CACHE_BYTES="1048576",
                         GIT_RS_DEV_HTTP="1", GIT_RS_BLAME_CONCURRENCY="1", GIT_RS_BLAME_WAITERS="0",
                         GIT_RS_S3_ACCESS_KEY="test-access",
                        GIT_RS_S3_SECRET_KEY="synthetic-secret-key", GIT_RS_S3_BUCKET="git-test",
                        PGHOST="127.0.0.1", PGUSER="postgres", PGDATABASE="git_rs",
                        PGPASSWORD="development-only", PGSSLMODE="disable", PGCONNECT_TIMEOUT="2",
                        PGGSSENCMODE="disable")
        self.server = self.base / "server"
        self.server.mkdir(mode=0o700)
        self.runtime = None
        if runtime_image:
            from runtime_process import Runtime
            self.runtime = Runtime(self.server, self.identity, runtime_image, runtime_fixture_image,
                                   revision, self.containers.append)
            self.results["server_images"] = {"runtime": self.runtime.image,
                                             "fixture": self.runtime.fixture_image,
                                             "revision": revision}

    def socket(self, name):
        return self.server / f"{name}.sock"

    def spawn(self, arguments, *, log, env=None, fixture_program=None):
        if self.runtime:
            child = self.runtime.start(arguments, env or self.env, log, program=fixture_program)
        else:
            child = subprocess.Popen([fixture_program or self.binary, *map(str, arguments)],
                                     env=env or self.env, stdout=log, stderr=log)
        self.processes.append(child)
        return child

    def process_status(self, child):
        if self.runtime and hasattr(child, "process_status"):
            return child.process_status()
        return Path(f"/proc/{child.pid}/status").read_text()

    def bridge_command(self, socket, principal="alice"):
        # This Rust program is an external transport client, not a backend. In
        # particular OpenSSH need not delegate Docker privileges to its login
        # session just to forward the authenticated byte stream to this socket.
        return [self.binary, "ssh", "--socket", str(socket), "--principal", principal]

    def write_bridge(self, path, socket, principal="alice"):
        path.write_text("#!/bin/sh\nexec " + shlex.join(self.bridge_command(socket, principal)) + "\n")
        path.chmod(0o700)

    def assert_no_repository_bridge(self):
        unexpected = [str(path.relative_to(self.server)) for path in self.server.rglob("*")
                      if path.name in {"objects", ".git"} or path.suffix in {".pack", ".idx"}
                      or path.name.startswith(("instance-", "request-"))]
        assert not unexpected, ("serving created a filesystem repository bridge", unexpected)

    def configure_maintenance(self, cluster):
        from maintenance_fixture import LeaseFixture
        assert self.runtime and self.lease_fixture is None
        directory = self.base / "maintenance-fixture"
        directory.mkdir(mode=0o700)
        self.lease_fixture = LeaseFixture(cluster, directory)
        self.lease_fixture.setup()

    def cleanup(self, slug, principal="alice", *, expected="succeeded"):
        assert self.runtime and self.lease_fixture, "cleanup execution requires real named-Lease credentials"
        queued = self.cli("cleanup", slug, principal).stdout.decode().rstrip("\n").split("\t")
        assert len(queued) == 3 and queued[1:] == ["queued", ""], queued
        identifier = queued[0]
        # Other previously queued repositories/retirements remain legitimate work.
        # Bound dispatch count rather than deleting their durable queue history.
        deadline = time.monotonic() + 120
        for attempt in range(16):
            remaining = deadline - time.monotonic()
            assert remaining > 0, "maintenance dispatch exceeded its absolute fixture deadline"
            env = {**self.env, **self.lease_fixture.environment,
                   "GIT_RS_POD_UID": "fixture-" + uuid.uuid4().hex}
            result = self.runtime.execute(["maintenance", "--scratch", str(self.server / "maintenance"),
                                           "--health", "127.0.0.1:0", "--once", "--wait", "30"], env,
                                          timeout=min(90, remaining), service_account=self.lease_fixture.account)
            (self.base / f"maintenance-{identifier}-{attempt}.log").write_bytes(result.stdout + result.stderr)
            state = self.sql("SELECT state,coalesce(error_code,'') FROM git_rs.maintenance_jobs WHERE id='" + identifier + "'").split("|")
            if state[0] in {"succeeded", "retry", "dead"}:
                assert state[0] == expected, state
                return identifier, state[1]
        raise AssertionError("requested maintenance job was not dispatched within the bounded queue window")

    def setup(self, provision):
        self.check_native_git()
        if provision:
            pg = self.container(["-e", "POSTGRES_PASSWORD=development-only", "-e", "POSTGRES_DB=git_rs",
                                 "-p", "127.0.0.1::5432", PG_IMAGE])
            self.env["PGPORT"] = str(self.port(pg, "5432/tcp"))
            # The image's initdb server accepts Unix-socket connections before
            # its published TCP endpoint is ready. Wait through the real path.
            try:
                wait_for(lambda: command(["psql", "-X", "-qAt", "-c", "SELECT 1"], env=self.env,
                                         ok=False).returncode == 0, "PostgreSQL TCP endpoint")
            except AssertionError:
                print(command(["docker", "logs", pg], ok=False).stderr.decode())
                print(command(["psql", "-X", "-qAt", "-c", "SELECT 1"], env=self.env,
                              ok=False).stderr.decode())
                raise
            s3 = self.container(["-e", "ROOT_ACCESS_KEY_ID=test-access", "-e", "ROOT_SECRET_ACCESS_KEY=synthetic-secret-key",
                                 "--tmpfs", "/data:rw,size=4g", "-p", "127.0.0.1::7070", S3_IMAGE, "--quiet", "posix", "/data"])
            self.env["GIT_RS_S3_ENDPOINT"] = f"http://127.0.0.1:{self.port(s3, '7070/tcp')}"
            wait_for(lambda: self.s3("PUT", "", ok=False).returncode == 0, "S3 bucket")
        else:
            for key in ["PGPORT", "GIT_RS_S3_ENDPOINT"]:
                self.env[key] = os.environ[key]

    def check_native_git(self):
        """Fail before provisioning if PATH/native helpers are not the pinned oracle."""
        found = shutil.which("git", path=self.env["PATH"])
        assert found and Path(found).resolve() == Path("/usr/bin/git").resolve(), (
            "native Git oracle must resolve to the genuine pinned /usr/bin/git", found)
        binary = Path(found).read_bytes()
        assert binary.startswith(b"\x7fELF"), "native Git oracle must be ELF, never a version wrapper"
        for executable in ("git", "/usr/bin/git"):
            version = command([executable, "--version"], env=self.env).stdout.strip()
            assert version == b"git version 2.56.0", ("pinned Git 2.56.0 oracle required", executable, version)
        helper_path = command(["/usr/bin/git", "--exec-path"], env=self.env).stdout.strip()
        assert helper_path == b"/usr/lib/git-core", ("pinned complete Git helper install required", helper_path)
        for helper in ("git-http-backend", "git-remote-http"):
            path = Path("/usr/lib/git-core") / helper
            assert path.is_file() and os.access(path, os.X_OK), ("missing native Git helper", path)
        self.results["native_git_oracle"] = {"version": "2.56.0", "executable": str(Path(found).resolve()),
            "sha256": hashlib.sha256(binary).hexdigest(), "exec_path": helper_path.decode()}

    def container(self, args):
        self.containers_started = True
        owned = ["--label", "git-rs.integration-fixture=" + self.identity]
        if os.environ.get("GIT_RS_TEST_OWNER_LABEL"):
            owned += ["--label", os.environ["GIT_RS_TEST_OWNER_LABEL"]]
        if self.blame_core_resources or os.environ.get("GIT_RS_TEST_OWNER_LABEL"):
            pg = PG_IMAGE in args
            memory = "1024m" if pg else "512m"
            owned += ["--pull=never", "--cpuset-cpus", os.environ.get("GIT_RS_TEST_SERVICE_CPUS", ",".join(map(str, sorted(os.sched_getaffinity(0))[:2]))),
                      "--cpus", ".5" if pg else ".25", "--memory", memory,
                       "--memory-swap", memory, "--pids-limit", "64" if pg else "48"]
            if pg:
                owned += ["--tmpfs", "/var/lib/postgresql:rw,size=512m"]
        identifier = command(["docker", "run", "-d", *owned, *args]).stdout.decode().strip()
        self.containers.append(identifier)
        return identifier

    def port(self, identifier, port):
        return int(json.loads(command(["docker", "inspect", identifier]).stdout)[0]["NetworkSettings"]["Ports"][port][0]["HostPort"])

    def s3(self, method, key, *, data=None, ok=True):
        url = f"{self.env['GIT_RS_S3_ENDPOINT']}/git-test/{key}"
        config = (f'url = "{url}"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
                  'user = "test-access:synthetic-secret-key"\n')
        args = ["curl", "--disable", "--silent", "--fail", "--max-time", "5", "--config", "-"]
        if method == "HEAD":
            args += ["--head", "--output", "/dev/null", "--write-out", "%{http_code}"]
        else:
            args += ["--request", method]
        if data is not None:
            args += ["--upload-file", str(data)]
        return command(args, data=config.encode(), ok=ok)

    def cli(self, *args, ok=True, env=None, data=None):
        if self.runtime:
            return self.runtime.execute(list(map(str, args)), env or self.env, data=data, ok=ok)
        return command([self.binary, *args], env=env or self.env, data=data, ok=ok)

    def git(self, path, *args, ok=True, env=None):
        return command(["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", "-C", path, *args], env=env or self.env, ok=ok)

    def sql(self, query):
        return command(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], env=self.env, data=query.encode()).stdout.decode().strip()

    def start(self, name, principal="alice"):
        log_path = self.base / f"{name}.log"
        log = log_path.open("wb")
        self.logs.append(log)
        process = self.spawn(["serve", "--socket", str(self.socket(name)), "--health", "127.0.0.1:0",
                              "--dev-http", "127.0.0.1:0", "--dev-principal", principal], log=log)

        def ready():
            if process.poll() is not None:
                raise AssertionError(log_path.read_text())
            return re.search(r"development HTTP 127.0.0.1:(\d+)", log_path.read_text())

        wait_for(ready, "Rust daemon")
        self.daemons[name] = process
        self.health_ports[name] = int(re.search(r"health 127.0.0.1:(\d+)", log_path.read_text()).group(1))
        return process, int(ready().group(1))

    def start_blame(self, name, *, control=None, phase="source", seconds=30,
                    concurrency=2, waiters=1, cache=False, principal="alice", extra=()):
        assert self.blame_fixture_binary or self.runtime, "a prebuilt blame fixture is required"
        path = self.base / f"{name}.log"
        log = path.open("xb")
        self.logs.append(log)
        args = ["--socket", str(self.socket(name)), "--seconds", str(seconds),
                "--concurrency", str(concurrency), "--waiters", str(waiters), "--principal", principal]
        if control is not None:
            args += ["--control", str(control), "--phase", phase]
        if cache:
            args.append("--cache")
        args.extend(map(str, extra))
        program = ("/usr/local/bin/examples/blame-http-fixture" if self.runtime
                   else str(self.blame_fixture_binary))
        child = self.spawn(args, log=log, fixture_program=program)
        def ready():
            if child.poll() is not None:
                raise AssertionError(path.read_text())
            return re.search(r"development HTTP 127.0.0.1:(\d+)", path.read_text())
        wait_for(ready, "test-only real-core HTTP listener")
        self.daemons[name] = child
        self.health_ports[name] = int(re.search(r"health 127.0.0.1:(\d+)", path.read_text()).group(1))
        return child, int(ready().group(1))

    def health(self, name, path):
        connection = http.client.HTTPConnection("127.0.0.1", self.health_ports[name], timeout=3)
        connection.request("GET", path)
        response = connection.getresponse()
        status, body = response.status, response.read()
        connection.close()
        return status, body

    def stop(self, name):
        child = self.daemons.pop(name)
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)

    def stop_daemons(self):
        for name in list(self.daemons):
            self.stop(name)

    def record_memory(self, max_bytes=64 * 1024 * 1024):
        peaks = []
        for process in self.daemons.values():
            if process.poll() is None:
                status = self.process_status(process)
                peaks.append(int(re.search(r"VmHWM:\s+(\d+)", status).group(1)) * 1024)
        if peaks:
            self.results["maximum_rust_daemon_hwm_bytes"] = max(peaks)
            self.results["rust_daemon_hwm_limit_bytes"] = max_bytes
            assert max(peaks) < max_bytes, peaks

    def close(self):
        if sys.exception() is not None:
            self.results["status"] = "failed"
        failures = []

        def attempt(action):
            try:
                return action()
            except BaseException as error:
                failures.append(error)
                return None

        for process in reversed(self.processes):
            def terminate(process=process):
                if process.poll() is None:
                    process.terminate()
            attempt(terminate)
        for process in reversed(self.processes):
            def reap(process=process):
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            attempt(reap)
        for log in self.logs:
            attempt(log.close)
        identifiers = set(self.containers)
        if self.containers_started:
            recovered = attempt(lambda: command(["docker", "ps", "-aq", "--no-trunc", "--filter",
                                                 "label=git-rs.integration-fixture=" + self.identity]))
            if recovered is not None:
                identifiers.update(recovered.stdout.decode().split())
        for identifier in identifiers:
            def remove(identifier=identifier):
                assert re.fullmatch(r"[0-9a-f]{64}", identifier), "invalid integration container identity"
                inspection = command(["docker", "inspect", identifier])
                rows = json.loads(inspection.stdout)
                assert len(rows) == 1 and rows[0]["Id"] == identifier, "integration container identity changed"
                assert (rows[0]["Config"].get("Labels") or {}).get("git-rs.integration-fixture") == self.identity, \
                    "foreign integration container must be preserved"
                if self.blame_core_resources:
                    (self.base / (identifier[:12] + ".resources.json")).write_bytes(inspection.stdout)
                    diagnostic = command(["docker", "logs", "--tail", "100", identifier], ok=False)
                    (self.base / (identifier[:12] + ".log")).write_bytes(
                        diagnostic.stdout[-16384:] + diagnostic.stderr[-16384:])
                command(["docker", "rm", "-f", identifier])
            attempt(remove)
        if self.containers_started:
            def confirm():
                left = command(["docker", "ps", "-aq", "--filter",
                                "label=git-rs.integration-fixture=" + self.identity]).stdout.strip()
                assert not left, "owned integration containers remain after cleanup"
            attempt(confirm)
            if not failures:
                self.results["owned_containers_removed"] = True
        if self.lease_fixture and not failures:
            attempt(self.lease_fixture.close)
        if failures:
            self.results["status"] = "failed"
            self.results["cleanup_errors"] = [type(error).__name__ for error in failures]
            original = sys.exception()
            if original is not None:
                original.add_note("Integration cleanup also failed: " + ", ".join(self.results["cleanup_errors"]))
            else:
                raise failures[0]


def pkt(payload):
    return f"{len(payload) + 4:04x}".encode() + payload


def raw_push(port, oid, names, *, missing=False, atomic=False, pack=None, slug="team/project", headers=None):
    zero = b"0" * 40
    capabilities = b"report-status side-band-64k" + (b" atomic" if atomic else b"")
    body = b""
    for index, name in enumerate(names):
        new = b"f" * 40 if missing and index else oid
        body += pkt(zero + b" " + new + b" " + name + (b"\0" + capabilities if index == 0 else b""))
    if pack is None:
        empty_pack = b"PACK" + struct.pack(">II", 2, 0)
        pack = empty_pack + hashlib.sha1(empty_pack).digest()
    body += b"0000" + pack
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    conn.request("POST", f"/{slug}.git/git-receive-pack", body,
                 {"Content-Type": "application/x-git-receive-pack-request", **(headers or {})})
    response = conn.getresponse()
    payload = response.read()
    conn.close()
    assert response.status == 200, (response.status, payload)
    return payload


def strict_ingestion_test(f, source, port, url):
    """Native Git must validate every incoming object, including unreachable ones."""
    tip = f.git(source, "rev-parse", "HEAD").stdout.strip()
    tree = f.git(source, "rev-parse", "HEAD^{tree}").stdout.strip()
    blob = command(["git", "-C", source, "hash-object", "-w", "--stdin"], env=f.env,
                   data=b"native object type boundary\n").stdout.strip()
    identity = b"Fixture <fixture@example.invalid> 1577836800 +0000\n"
    good = (b"tree " + tree + b"\nparent " + tip + b"\nauthor " + identity
            + b"committer " + identity + b"\nvalid cross-pack commit\n")

    def packed(kind, data, *, literal=False):
        args = ["git", "-C", source, "hash-object", "-w", "-t", kind, "--stdin"]
        if literal:
            args.append("--literally")
        oid = command(args, env=f.env, data=data).stdout.strip()
        # Git owns object encoding, compression, checksums and pack framing.
        pack = command(["git", "-C", source, "pack-objects", "--stdout"], env=f.env,
                       data=oid + b"\n").stdout
        return oid, pack

    before = f.git(source, "ls-remote", url).stdout
    packs_before = f.sql("SELECT count(*) FROM git_rs.packs")
    staged_before = f.sql("SELECT count(*) FROM git_rs.staged_objects")
    for name, kind, contents in [
        ("unreachable-bad-author", "commit", good.replace(b"author " + identity, b"author invalid\n")),
        ("wrong-tree-type", "commit", good.replace(b"tree " + tree, b"tree " + blob)),
        ("missing-parent", "commit", good.replace(b"parent " + tip, b"parent " + b"f" * 40)),
        ("unreachable-bad-tag", "tag", b"object " + tip + b"\ntype invalid\ntag broken\n\ninvalid\n"),
    ]:
        malformed, pack = packed(kind, contents, literal=True)
        # The malformed semantic object need not be reachable from either valid
        # ref: strict ingestion must reject it before uploads or publication.
        report = raw_push(port, tip, [f"refs/heads/{name}-a".encode(),
                                     f"refs/heads/{name}-b".encode()], pack=pack)
        assert b"ok refs/heads/" not in report, (name, malformed, report)
        assert f.git(source, "ls-remote", url).stdout == before, name
        assert f.sql("SELECT count(*) FROM git_rs.packs") == packs_before, name
        assert f.sql("SELECT count(*) FROM git_rs.staged_objects") == staged_before, name

    valid, pack = packed("commit", good)
    name = b"refs/heads/native-cross-pack"
    report = raw_push(port, valid, [name], pack=pack)
    assert b"ok " + name in report, report
    assert valid + b"\t" + name + b"\n" in f.git(source, "ls-remote", url).stdout
    assert int(f.sql("SELECT count(*) FROM git_rs.packs")) == int(packs_before) + 1
    f.git(source, "fetch", url, name.decode())
    assert f.git(source, "rev-parse", "FETCH_HEAD").stdout.strip() == valid
    f.git(source, "push", url, ":" + name.decode())
    assert f.git(source, "ls-remote", url).stdout == before
    f.results["strict_native_ingestion_and_cross_pack_links"] = "passed"


def generate(path, mib, seed):
    rng = random.Random(seed)
    digest = hashlib.sha256()
    with path.open("wb") as output:
        remaining, ordinal = mib * 1024 * 1024, 0
        while remaining:
            block = bytearray()
            for _ in range(512):
                block.extend(f"define(`RULE_{ordinal:012d}',`{rng.getrandbits(128):032x}')dnl\n".encode())
                ordinal += 1
            block = block[:remaining]
            output.write(block)
            digest.update(block)
            remaining -= len(block)
    return digest.hexdigest()


def exercise(f, large_mib, skip_ssh):
    # Four simultaneous pushes are a success/concurrency gate, not the separate
    # fail-fast saturation contract. One CPU worker plus three admitted waiters
    # fits the existing four-request transport bound without adding workers.
    f.env["GIT_RS_BLAME_WAITERS"] = "3"
    f.results["shared_cpu_capacity"] = {"workers": 1, "waiters": 3}
    f.cli("migrate")
    print("schema and stateless Smart HTTP", flush=True)
    f.cli("create", "team/project", "alice")
    first, p1 = f.start("a")
    second, p2 = f.start("b")
    balancer = Balancer([p1, p2])
    threading.Thread(target=balancer.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{balancer.server_address[1]}/team/project.git"
    source, vanilla = f.base / "source", f.base / "vanilla.git"
    command(["git", "init", "--initial-branch=main", source], env=f.env)
    command(["git", "init", "--bare", "--initial-branch=main", vanilla], env=f.env)
    assert not f.git(source, "ls-remote", url).stdout
    (source / "hello.txt").write_text("synthetic initial content\n")
    f.git(source, "add", ".")
    f.git(source, "commit", "-m", "Initial synthetic commit")
    f.git(source, "push", url, "main")
    f.git(source, "push", vanilla, "main")
    start = time.monotonic()
    print("large file, native Git comparison, tags, notes and shallow fetch", flush=True)
    expected_digest = generate(source / "generated.rules", large_mib, 7)
    f.git(source, "add", ".")
    f.git(source, "commit", "-m", "Add generated synthetic rules")
    f.git(source, "push", url, "main")
    f.results["generated_file_mib"] = large_mib
    f.results["large_push_seconds"] = round(time.monotonic() - start, 3)
    f.git(source, "push", vanilla, "main")
    f.git(source, "tag", "-a", "release-test", "-m", "Synthetic annotated tag")
    f.git(source, "notes", "add", "-m", "Synthetic note")
    f.git(source, "push", "--atomic", url, "main", "refs/tags/release-test", "refs/notes/commits")
    f.git(source, "push", "--atomic", vanilla, "main", "refs/tags/release-test", "refs/notes/commits")
    for protocol in ["0", "1", "2"]:
        assert f.git(source, "-c", f"protocol.version={protocol}", "ls-remote", url).stdout == f.git(source, "ls-remote", vanilla).stdout
    clone = f.base / "clone"
    native_clone = f.base / "native-clone"
    start = time.monotonic()
    f.git(source, "clone", "--no-local", vanilla, native_clone)
    f.results["vanilla_local_clone_seconds"] = round(time.monotonic() - start, 3)
    start = time.monotonic()
    f.git(source, "clone", url, clone)
    f.results["warm_clone_seconds"] = round(time.monotonic() - start, 3)
    assert hashlib.sha256((clone / "generated.rules").read_bytes()).hexdigest() == expected_digest
    f.git(clone, "fsck", "--full")
    shallow = f.base / "shallow"
    f.git(source, "clone", "--depth=1", url, shallow)
    assert f.git(shallow, "rev-list", "--count", "HEAD").stdout.strip() == b"1"
    f.git(shallow, "fetch", "--unshallow")
    f.git(shallow, "fsck", "--full")
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip()
    before = f.git(source, "ls-remote", url).stdout
    print("atomic failure and concurrent publication", flush=True)
    for atomic in [False, True]:
        report = raw_push(p1, oid, [b"refs/heads/raw-valid", b"refs/heads/raw-missing"], missing=True, atomic=atomic)
        assert b"ok refs/heads/raw-valid" not in report, report
        assert f.git(source, "ls-remote", url).stdout == before
    blob_oid = f.git(source, "hash-object", "-w", "hello.txt").stdout.strip().decode()
    assert f.git(source, "push", "--atomic", url, "HEAD:refs/heads/valid", f"{blob_oid}:refs/heads/blob", ok=False).returncode != 0
    assert f.git(source, "ls-remote", url).stdout == before
    strict_ingestion_test(f, source, p1, url)
    # Delete-only and no-new-object pushes must not require an incoming pack.
    packs_before = f.sql("SELECT count(*) FROM git_rs.packs")
    f.git(source, "push", url, "HEAD:refs/heads/disposable")
    f.git(source, "push", url, ":refs/heads/disposable")
    assert f.sql("SELECT count(*) FROM git_rs.packs") == packs_before
    assert f.git(source, "push", url, ":refs/heads/main", ok=False).returncode
    assert f.git(source, "push", "--force", url, "HEAD~1:refs/heads/main", ok=False).returncode
    assert f.git(source, "ls-remote", url).stdout == before
    bridge_env = dict(f.env, SSH_ORIGINAL_COMMAND="git-upload-pack 'team/missing.git'")
    assert f.cli("ssh", "--socket", f.socket("a"), "--principal", "alice", env=bridge_env, data=b"", ok=False).returncode != 0
    assert f.cli("serve", "--socket", f.socket("a"), ok=False).returncode != 0
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda i: f.git(source, "push", url, f"HEAD:refs/heads/parallel-{i}"), range(8)))
    remote = f.git(source, "ls-remote", url).stdout
    for i in range(8):
        assert oid + f"\trefs/heads/parallel-{i}\n".encode() in remote
    f.results["eight_parallel_ref_pushes_seconds"] = round(time.monotonic() - start, 3)
    race = threading.Barrier(2)

    def racer(port):
        race.wait()
        return raw_push(port, oid, [b"refs/heads/cas-race"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        reports = list(pool.map(racer, [p1, p2]))
    assert sum(b"ok refs/heads/cas-race" in result for result in reports) == 1, reports
    f.cli("grant", "team/project", "alice", "reader", "reader", "refs/")
    print("read and ref-prefix permissions", flush=True)
    _, ro_port = f.start("read-only", "reader")
    ro_url = f"http://127.0.0.1:{ro_port}/team/project.git"
    assert f.git(source, "ls-remote", ro_url).stdout
    assert f.git(source, "push", ro_url, "HEAD:refs/heads/forbidden", ok=False).returncode != 0
    f.cli("grant", "team/project", "alice", "scoped", "writer", "refs/heads/sandbox/")
    _, scoped_port = f.start("scoped", "scoped")
    scoped_url = f"http://127.0.0.1:{scoped_port}/team/project.git"
    f.git(source, "push", scoped_url, "HEAD:refs/heads/sandbox/allowed")
    before = f.git(source, "ls-remote", url).stdout
    assert f.git(source, "push", "--atomic", scoped_url, "HEAD:refs/heads/sandbox/also-allowed", "HEAD:refs/heads/outside", ok=False).returncode
    assert f.git(source, "ls-remote", url).stdout == before
    if not skip_ssh:
        print("restricted OpenSSH transport", flush=True)
        ssh_test(f, source, before)
    backup = f.server / "repository.bundle"
    print("Rust bundle backup and native restore", flush=True)
    f.cli("backup", "team/project", "alice", "--output", str(backup))
    backup_digest = hashlib.sha256(backup.read_bytes()).digest()
    assert f.cli("backup", "team/project", "alice", "--output", str(backup), ok=False).returncode
    assert hashlib.sha256(backup.read_bytes()).digest() == backup_digest
    assert backup.stat().st_mode & 0o777 == 0o600
    restored = f.base / "restored.git"
    f.git(source, "clone", "--mirror", backup, restored)
    f.git(restored, "fsck", "--full")
    assert f.git(restored, "show-ref").stdout == f.git(source, "ls-remote", url).stdout.replace(b"\t", b" ").replace(oid + b" HEAD\n", b"").replace(f.git(source, "rev-parse", "release-test^{}").stdout.strip() + b" refs/tags/release-test^{}\n", b"")
    f.cli("create", "team/restored", "alice")
    restore_url = url.replace("project.git", "restored.git")
    f.git(restored, "push", "--mirror", restore_url)
    assert f.git(source, "ls-remote", restore_url).stdout == f.git(source, "ls-remote", url).stdout
    scrapes = [f.health(name, "/metrics")[1] for name in ["a", "b"]]
    for series in [
        'git_rs_requests_total{transport="http",outcome="success"}',
        'git_rs_phase_duration_seconds_count{phase="graph"}',
        'git_rs_phase_duration_seconds_count{phase="upload"}',
        'git_rs_phase_duration_seconds_count{phase="publish"}',
        'git_rs_dependency_duration_seconds_count{operation="sql"}',
        'git_rs_dependency_duration_seconds_count{operation="s3_get"}',
        'git_rs_dependency_duration_seconds_count{operation="s3_put"}',
        'git_rs_cache_lookups_total{result="hit"}',
        'git_rs_s3_transferred_bytes_total{direction="download"}',
        'git_rs_s3_transferred_bytes_total{direction="upload"}',
    ]:
        assert sum(metric_value(body, series) for body in scrapes) > 0, series
    for stage in [
        "sql_init", "sql_acquire", "sql_query", "sql_reset", "lease_commit", "catalog_read",
        "lease_release", "cache_fill",
    ]:
        series = f'git_rs_stage_duration_seconds_count{{stage="{stage}"}}'
        assert sum(metric_value(body, series) for body in scrapes) > 0, stage
    for body in scrapes:
        for program in ["git", "other"]:
            assert metric_value(body, f'git_rs_subprocess_duration_seconds_count{{program="{program}"}}') == 0
    # Retained metric names identify the removed native implementation. Both
    # receive and upload now run on the bounded Rust runtime.
    for stage in ["ref_validate", "tip_validate", "fast_forward", "cache_link", "object_hash",
                  "restore_refs", "hook_setup", "git_input", "git_output", "git_wait",
                  "scratch_cleanup", "process_spawn"]:
        series = f'git_rs_stage_duration_seconds_count{{stage="{stage}"}}'
        assert sum(metric_value(body, series) for body in scrapes) == 0, stage
    assert all(metric_value(body, "git_rs_timing_logs_dropped_total") == 0 for body in scrapes)
    if f.env.get("GIT_RS_DEBUG_TIMINGS") == "1":
        def correlated_requests():
            records = []
            for name in ["a", "b"]:
                for line in (f.base / f"{name}.log").read_text().splitlines():
                    if line.startswith("git_rs_timing "):
                        assert re.fullmatch(
                            r"git_rs_timing level=debug pid=\d+ operation=\d+ kind=[a-z_]+ name=[a-z0-9_]+ "
                            r"elapsed_ms=\d+\.\d{3} duration_ms=\d+\.\d{3} outcome=[a-z_]+", line), line
                        records.append(dict(field.split("=", 1) for field in line.split()[1:]))
            completed = set()
            for request in records:
                if request["kind"] != "request" or request["outcome"] != "success":
                    continue
                assert request["operation"] != "0"
                stages = {row["name"] for row in records if row["kind"] == "stage"
                          and (row["pid"], row["operation"]) == (request["pid"], request["operation"])}
                if {"lease_commit", "catalog_read", "lease_release"} <= stages:
                    completed.add(request["name"])
            return {"http"} <= completed and (skip_ssh or "ssh" in completed)
        wait_for(correlated_requests, "correlated Rust request/source/cleanup timings")
        f.results["correlated_rust_timing_logs"] = "passed"
    if not skip_ssh:
        assert metric_value(scrapes[0], 'git_rs_requests_total{transport="ssh",outcome="success"}') > 0
    for process in [first, second]:
        process.kill()
        process.wait()
    _, cold_port = f.start("a")
    print("cold restart and physical history truncation", flush=True)
    cold_url = f"http://127.0.0.1:{cold_port}/team/project.git"
    cold_clone = f.base / "cold-clone"
    start = time.monotonic()
    f.git(source, "clone", cold_url, cold_clone)
    f.results["cold_clone_seconds"] = round(time.monotonic() - start, 3)
    assert hashlib.sha256((cold_clone / "generated.rules").read_bytes()).hexdigest() == expected_digest
    f.git(cold_clone, "fsck", "--full")
    truncate_test(f, source, cold_port, large_mib)
    wait_for(lambda: f.health("a", "/readyz")[0] == 200, "primary readiness")
    assert f.health("a", "/livez") == (200, b"live\n")
    assert b"git_rs_cache_allocated_bytes " in f.health("a", "/metrics")[1]
    health_framing_test(f, "a")
    # The old 64 MiB parent-only bound excluded native receive children. Rust
    # now owns those buffers in this process. Bound its baseline plus the four
    # configured 1 GiB CpuJobs reservations; the independent cgroup additionally
    # bounds the combined parent/children/services under the qualification cap.
    slots = sum(f.results["shared_cpu_capacity"].values())
    f.record_memory(max_bytes=64 * 1024 * 1024 + slots * 1024 * 1024 * 1024)
    assert all(balancer.counts), balancer.counts
    f.results["balanced_connections"] = balancer.counts
    f.assert_no_repository_bridge()
    balancer.shutdown()
    f.stop_daemons()


def ssh_test(f, source, expected):
    for name in ["host-key", "client-key"]:
        command(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", f.base / name])
    user = pwd.getpwuid(os.getuid()).pw_name
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    bridge = f.base / "authorized-ssh-bridge"
    f.write_bridge(bridge, f.socket("a"))
    forced = shlex.quote(str(bridge)).replace("\\", "\\\\").replace('"', '\\"')
    (f.base / "authorized_keys").write_text(f'restrict,command="{forced}" ' + (f.base / "client-key.pub").read_text())
    config = f.base / "sshd_config"
    config.write_text(f"Port {port}\nListenAddress 127.0.0.1\nHostKey {f.base}/host-key\n"
                      f"PidFile {f.base}/sshd.pid\nAuthorizedKeysFile {f.base}/authorized_keys\n"
                      f"AllowUsers {user}\nPasswordAuthentication no\nKbdInteractiveAuthentication no\n"
                      "UsePAM no\nPermitRootLogin forced-commands-only\nAllowTcpForwarding no\n"
                      "PermitTTY no\nX11Forwarding no\nAcceptEnv GIT_PROTOCOL\n")
    log = (f.base / "sshd.log").open("wb")
    f.logs.append(log)
    sshd = subprocess.Popen(["/usr/sbin/sshd", "-D", "-e", "-f", config], stdout=log, stderr=log)
    f.processes.append(sshd)

    def ready():
        if sshd.poll() is not None:
            raise AssertionError((f.base / "sshd.log").read_text())
        return "Server listening" in (f.base / "sshd.log").read_text()

    wait_for(ready, "isolated OpenSSH server")
    host_key = (f.base / "host-key.pub").read_text().split()
    known = f.base / "known_hosts"
    known.write_text(f"[127.0.0.1]:{port} {host_key[0]} {host_key[1]}\n")
    env = dict(f.env, GIT_SSH_COMMAND=f"ssh -p {port} -i {f.base}/client-key -o IdentitiesOnly=yes -o IdentityAgent=none -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile={known}")
    remote = f"{user}@127.0.0.1:team/project.git"
    assert f.git(source, "ls-remote", remote, env=env).stdout == expected
    before_push = metric_value(f.health("a", "/metrics")[1], 'git_rs_subprocess_duration_seconds_count{program="git"}')
    f.git(source, "push", remote, "HEAD:refs/heads/ssh-tested", env=env)
    clone = f.base / "ssh-clone"
    f.git(source, "clone", "--depth=1", remote, clone, env=env)
    assert f.git(clone, "rev-list", "--count", "HEAD").stdout.strip() == b"1"
    f.git(clone, "fetch", "--unshallow", env=env)
    f.git(clone, "fsck", "--strict", "--full")
    assert f.git(clone, "rev-list", "--topo-order", "HEAD").stdout == f.git(source, "rev-list", "--topo-order", "HEAD").stdout
    after_push = metric_value(f.health("a", "/metrics")[1], 'git_rs_subprocess_duration_seconds_count{program="git"}')
    assert after_push == before_push, "normal SSH receive must not spawn native Git"
    tip = f.git(source, "rev-parse", "HEAD").stdout.strip()
    manifest = b"0" * 40 + b" " + tip + b" refs/heads/ssh-tested\n"
    operation = f.sql("SELECT id FROM git_rs.operations WHERE commands=decode('" + manifest.hex() + "','hex') AND repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/project')")
    assert operation and len(operation.splitlines()) == 1
    f.results["normal_ssh_rust_receive"] = {"operation_id": operation, "native_git_delta": after_push - before_push,
                                          "clone_deepen_native_fsck": True}
    f.results["ssh_authentication"] = "generated Ed25519 key, restricted forced command, strict host verification"


def truncate_test(f, source, port, mib):
    f.cli("create", "team/history", "alice")
    history = f.base / "history"
    command(["git", "init", "--initial-branch=main", history], env=f.env)
    generate(history / "obsolete.rules", mib, 12)
    f.git(history, "add", ".")
    f.git(history, "commit", "-m", "Synthetic obsolete large version")
    url = f"http://127.0.0.1:{port}/team/history.git"
    f.git(history, "push", url, "main")
    (history / "obsolete.rules").unlink()
    (history / "current.txt").write_text("current contents\n")
    for index in range(4):
        (history / "current.txt").write_text(f"current contents {index}\n")
        f.git(history, "add", "-A")
        f.git(history, "commit", "-m", f"Synthetic update {index}")
    f.git(history, "push", url, "main")
    original_tree = f.git(history, "rev-parse", "HEAD^{tree}").stdout
    old_keys = f.sql("SELECT pack_key FROM git_rs.packs WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/history') UNION ALL SELECT idx_key FROM git_rs.packs WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/history')").splitlines()
    assert all(f.s3("HEAD", key).stdout == b"200" for key in old_keys)
    old_size = int(f.sql("SELECT sum(pack_size+idx_size) FROM git_rs.packs WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/history')"))
    native_history = f.git(history, "rev-list", "--topo-order", "main").stdout
    assert f.cli("history", "team/history", "alice", "--ref", "refs/heads/main").stdout == native_history
    backup = f.server / "before-truncate.bundle"
    f.cli("truncate", "team/history", "alice", "--keep", "2", "--backup", str(backup))
    trimmed = f.base / "trimmed"
    f.git(source, "clone", url, trimmed)
    f.git(trimmed, "fsck", "--full")
    assert f.git(trimmed, "rev-list", "--count", "HEAD").stdout.strip() == b"2"
    assert f.git(trimmed, "rev-parse", "HEAD^{tree}").stdout == original_tree
    new_size = int(f.sql("SELECT sum(pack_size+idx_size) FROM git_rs.packs WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/history')"))
    assert new_size < old_size / 2, (old_size, new_size)
    # Serving/admin truncation retires identities but never deletes source bytes.
    # Physical deletion is qualified separately through the elected worker.
    for key in old_keys:
        assert f.s3("HEAD", key).stdout == b"200", key
        assert f.sql("SELECT count(*) FROM git_rs.retired_objects WHERE object_key='" + key + "'") == "1"
    cleanup = f.cli("cleanup", "team/history", "alice").stdout.decode().rstrip("\n").split("\t")
    assert len(cleanup) == 3 and cleanup[1:] == ["queued", ""], cleanup
    assert f.sql("SELECT state FROM git_rs.maintenance_jobs WHERE id='" + cleanup[0] + "'") == "queued"
    assert f.sql("SELECT count(*) FROM git_rs.operations WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/history')") == "1"
    recovered = f.base / "history-recovered"
    f.git(source, "clone", backup, recovered)
    assert f.git(recovered, "rev-list", "--count", "HEAD").stdout.strip() == b"5"
    f.git(recovered, "fsck", "--full")
    f.results["truncate"] = {"old_pack_bytes": old_size, "new_pack_bytes": new_size, "remaining_commits": 2, "native_backup_commits": 5}


def upload_core_test(f, skip_ssh):
    """Native clients cover the direct server's advertised transport inventory."""
    f.cli("migrate")
    f.cli("create", "team/project", "alice")
    source = f.base / "upload-source"
    f.git(f.base, "init", "--initial-branch=main", source)
    commits = []
    for ordinal in range(5):
        (source / "file.txt").write_text(f"synthetic content {ordinal}\n" + "retained line\n" * 256)
        f.git(source, "add", ".")
        clock = 1_600_000_100 - ordinal
        env = dict(f.env, GIT_AUTHOR_DATE=f"@{clock} +0000", GIT_COMMITTER_DATE=f"@{clock} +0000")
        f.git(source, "commit", "-qm", f"Synthetic transport {ordinal}", env=env)
        commits.append(f.git(source, "rev-parse", "HEAD").stdout.strip().decode())
    f.git(source, "checkout", "-qb", "side", commits[1])
    (source / "side.txt").write_bytes(b"merge payload\n")
    f.git(source, "add", ".")
    f.git(source, "commit", "-qm", "Synthetic side", env=env)
    f.git(source, "checkout", "-q", "main")
    f.git(source, "merge", "--no-ff", "-qm", "Synthetic merge", "side", env=env)
    f.git(source, "tag", "-a", "annotated", "-m", "Synthetic annotated tag", env=env)
    f.git(source, "tag", "lightweight")
    f.git(source, "notes", "add", "-m", "Synthetic transport note", env=env)
    _, port = f.start("a")
    url = f"http://127.0.0.1:{port}/team/project.git"
    f.git(source, "push", url, "--all")
    f.git(source, "push", url, "--tags", "refs/notes/commits")
    tip = f.git(source, "rev-parse", "HEAD").stdout.strip()
    tree = f.git(source, "rev-parse", "HEAD^{tree}").stdout.strip()
    hidden = command(["git", "-C", source, "commit-tree", tree.decode(), "-p", tip.decode()],
                     env=env, data=b"unadvertised physical commit\n").stdout.strip()
    pack = command(["git", "-C", source, "pack-objects", "--stdout"], env=f.env, data=hidden + b"\n").stdout
    assert b"ok refs/heads/carrier" in raw_push(port, tip, [b"refs/heads/carrier"], pack=pack)
    expected = f.git(source, "rev-list", "--topo-order", "main").stdout
    for protocol in ("0", "1", "2"):
        clone = f.base / ("upload-v" + protocol)
        f.git(source, "-c", "protocol.version=" + protocol, "clone", "--depth=1", url, clone)
        assert f.git(clone, "rev-list", "--count", "HEAD").stdout.strip() == b"1"
        f.git(clone, "-c", "protocol.version=" + protocol, "fetch", "--deepen=2")
        assert int(f.git(clone, "rev-list", "--count", "HEAD").stdout) > 1
        f.git(clone, "-c", "protocol.version=" + protocol, "fetch", "--unshallow", "--tags")
        assert f.git(clone, "rev-list", "--topo-order", "HEAD").stdout == expected
        assert f.git(clone, "rev-parse", "HEAD^{tree}").stdout.strip() == tree
        assert f.git(clone, "cat-file", "-e", hidden.decode(), ok=False).returncode
        assert f.git(clone, "-c", "protocol.version=" + protocol, "fetch", url, hidden.decode(), ok=False).returncode
        assert f.git(clone, "cat-file", "-e", hidden.decode(), ok=False).returncode
        f.git(clone, "fsck", "--strict", "--full")
    if not skip_ssh:
        ssh_test(f, source, f.git(source, "ls-remote", url).stdout)
    assert metric_value(f.health("a", "/metrics")[1], 'git_rs_subprocess_duration_seconds_count{program="git"}') == 0
    f.assert_no_repository_bridge()
    f.results["native_upload_protocols"] = {"versions": [0, 1, 2], "shallow_deepen_unshallow": True,
        "merge_and_tags": True, "hidden_physical_want_rejected": True, "strict_fsck": True}
    return source, port


def history_core_test(f):
    source, port = upload_core_test(f, True)
    slug = "team/project"
    url = f"http://127.0.0.1:{port}/{slug}.git"
    native = f.git(source, "rev-list", "--topo-order", "main").stdout
    assert f.cli("history", slug, "alice").stdout == native
    assert f.cli("history", slug, "alice", "--ref", "refs/heads/main", "--max-commits", "2").stdout == f.git(
        source, "rev-list", "--topo-order", "-n", "2", "main").stdout
    assert f.cli("history", slug, "alice", "--max-commits", "0", ok=False).returncode
    backup = f.server / "all-refs.bundle"
    assert f.cli("backup", slug, "unregistered", "--output", backup, ok=False).returncode
    assert not backup.exists()
    f.cli("backup", slug, "alice", "--output", backup)
    f.git(source, "bundle", "verify", backup)
    digest = hashlib.sha256(backup.read_bytes()).hexdigest()
    assert backup.stat().st_mode & 0o777 == 0o600
    assert f.cli("backup", slug, "alice", "--output", backup, ok=False).returncode
    assert hashlib.sha256(backup.read_bytes()).hexdigest() == digest
    restored = f.base / "all-refs-restored.git"
    f.git(source, "clone", "--mirror", backup, restored)
    f.git(restored, "fsck", "--strict", "--full")
    assert f.git(restored, "for-each-ref", "--format=%(objectname)%09%(refname)").stdout == f.git(source, "ls-remote", "--refs", url).stdout
    objects = lambda repository: {line.split()[0] for line in f.git(repository, "rev-list", "--objects", "--all").stdout.splitlines()}
    assert objects(restored) == objects(source)
    f.cli("grant", slug, "alice", "bob", "writer", "refs/")
    denied = f.server / "denied.bundle"
    assert f.cli("truncate", slug, "bob", "--keep", "2", "--backup", denied, "--drop-other-refs", ok=False).returncode
    assert not denied.exists()
    assert f.cli("truncate", slug, "alice", "--keep", "2", "--backup", denied, ok=False).returncode
    assert not denied.exists(), "other-ref policy must reject before publishing a backup"
    truncate_test(f, source, port, 1)
    f.results["native_history_administration"] = {"complete_ordered_history": True, "bounded_prefix": True,
        "all_refs_bundle_native_restore": True, "no_overwrite": True, "owner_and_other_ref_fences": True}


def authenticated_http_test(f):
    """Exercise native Basic challenge/retry and per-request repository ACLs."""
    print("authenticated Smart HTTP identities and ACLs", flush=True)
    # Only synthetic credentials; both server and native-client stores stay in
    # the private fixture directory. Tokens never enter argv or child env.
    actors = {"alice": "a" * 43, "bob": "b" * 43, "carol": "c" * 43}
    server_credentials = f.server / "http-server-credentials"
    server_credentials.write_text("".join(f"{actor} {token}\n" for actor, token in actors.items()))
    server_credentials.chmod(0o600)
    log_path = f.base / "authenticated-http.log"
    log = log_path.open("wb")
    f.logs.append(log)
    process = f.spawn(["serve", "--socket", str(f.socket("authenticated-http")),
                                "--http", "127.0.0.1:0", "--health", "127.0.0.1:0",
                                "--http-credentials", str(server_credentials)],
                      log=log)

    def ready():
        if process.poll() is not None:
            raise AssertionError(log_path.read_text())
        return re.search(r"^HTTP 127.0.0.1:(\d+)$", log_path.read_text(), re.MULTILINE)

    wait_for(ready, "authenticated HTTP listener")
    port = int(ready().group(1))
    f.health_ports["authenticated-http"] = int(re.search(r"health 127.0.0.1:(\d+)", log_path.read_text()).group(1))
    source = f.base / "authenticated-http-source"
    f.git(f.base, "init", "-b", "main", source)
    helpers = {}
    for actor, token in actors.items():
        # Git can clear a URL username after an initial 401 and ask its helper
        # again without one. A shared store may then return another valid actor.
        # Isolate clients so a reader attempt can never fall back to the owner.
        client_credentials = f.base / f"http-client-{actor}-credentials"
        client_credentials.write_text(f"http://{actor}:{token}@127.0.0.1:{port}\n")
        client_credentials.chmod(0o600)
        helpers[actor] = f"credential.helper=store --file={client_credentials}"
        resolved = command(["git", "-c", helpers[actor], "credential", "fill"], env=f.env, cwd=source,
                           data=f"protocol=http\nhost=127.0.0.1:{port}\n\n".encode(), ok=False)
        assert resolved.returncode == 0, ("native fixture credential helper must resolve its actor", resolved.stderr)
        fields = dict(line.split(b"=", 1) for line in resolved.stdout.splitlines() if b"=" in line)
        assert fields.get(b"username") == actor.encode(), "native fixture must use the intended principal"
        assert fields.get(b"password") == token.encode(), "native fixture must use the intended token"
    (source / "synthetic.txt").write_text("authenticated HTTP fixture\n")
    f.git(source, "add", "synthetic.txt")
    f.git(source, "commit", "-qm", "synthetic HTTP fixture")
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip()
    f.cli("create", "team/http-auth", "alice")
    f.cli("grant", "team/http-auth", "alice", "bob", "reader", "refs/")
    f.cli("create", "team/http-private", "carol")
    url = f"http://alice@127.0.0.1:{port}/team/http-auth.git"
    def native_calls():
        status, body = f.health("authenticated-http", "/metrics")
        assert status == 200
        return metric_value(body, 'git_rs_subprocess_duration_seconds_count{program="git"}')
    before_push = native_calls()
    f.git(source, "-c", helpers["alice"], "push", url, "HEAD:refs/heads/main")
    assert native_calls() == before_push, "normal authenticated receive must not spawn native Git"
    manifest = b"0" * 40 + b" " + oid + b" refs/heads/main\n"
    operation = f.sql("SELECT id FROM git_rs.operations WHERE commands=decode('" + manifest.hex() + "','hex') AND repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/http-auth')")
    assert operation and len(operation.splitlines()) == 1
    f.results["normal_authenticated_rust_receive"] = {"operation_id": operation, "native_git_delta": native_calls() - before_push}
    clone = f.base / "authenticated-http-clone"
    before_clone = native_calls()
    f.git(source, "-c", helpers["bob"], "clone", url.replace("alice@", "bob@"), clone)
    assert native_calls() == before_clone, "upload-pack must use the Rust runtime without a native fallback"
    assert f.git(clone, "rev-parse", "HEAD").stdout.strip() == oid
    assert (clone / "synthetic.txt").read_text() == "authenticated HTTP fixture\n"
    before = f.git(source, "-c", helpers["alice"], "ls-remote", url).stdout
    denied = f.git(source, "-c", helpers["bob"], "-c", "http.extraHeader=X-Remote-User: alice",
                   "push", url.replace("alice@", "bob@"), "HEAD:refs/heads/reader-denied", ok=False)
    assert denied.returncode, "reader credentials must not gain owner rights from identity headers"
    refs = f.git(source, "-c", helpers["alice"], "ls-remote", url).stdout
    assert refs == before, "rejected reader push must leave all remote refs unchanged"
    assert b"reader-denied" not in refs
    private_url = url.replace("http-auth.git", "http-private.git")
    denied = f.git(source, "-c", helpers["alice"], "ls-remote", private_url, ok=False)
    assert denied.returncode, "valid credentials must not grant access to another principal's repository"
    denied = f.git(source, "-c", "credential.helper=", "ls-remote", url, ok=False)
    assert denied.returncode, "native Git without a credential must receive an authentication challenge"
    assert not any(token in log_path.read_text() for token in actors.values())
    f.results["authenticated_http_acl_roundtrip"] = "passed"
    complete_blame_test(f, source, port, actors)


def import_historical_blame_fixture(f, source, old_oid, oid):
    """Trusted synthetic pre-cutover data, never a receive acceptance bypass.

    Incoming receive now rejects >128 MiB objects. Readers must still reject an
    oversized blob already present in historical authoritative storage, without
    invoking Git. The external native oracle builds/verifies this fixture pack.
    """
    pack = f.base / "historical-blame.pack"
    index = pack.with_suffix(".idx")
    pack.write_bytes(command(["git", "-C", source, "pack-objects", "--revs", "--stdout"],
                             env=f.env, data=oid + b"\n").stdout)
    native_hash = f.git(source, "index-pack", "--strict", "-o", index, pack).stdout.strip().decode()
    assert re.fullmatch("[0-9a-f]{40}", native_hash)
    repo = f.sql("SELECT id FROM git_rs.repositories WHERE slug='team/http-auth'")
    operation = str(uuid.uuid4())
    prefix = f"repos/{repo}/operations/{operation}/{native_hash}"
    for suffix, path in [("pack", pack), ("idx", index)]:
        f.s3("PUT", f"{prefix}.{suffix}", data=path)
        assert f.s3("HEAD", f"{prefix}.{suffix}").stdout == b"200"
    wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id='{repo}'") == "0",
             "historical fixture import excludes live readers")
    name = "convert_to('refs/heads/main','UTF8')"
    manifest = old_oid + b" " + oid + b" refs/heads/main\n"
    f.sql(f"""BEGIN;
        SELECT id FROM git_rs.repositories WHERE id='{repo}' FOR UPDATE;
        DO $$ BEGIN
          IF (SELECT oid FROM git_rs.refs WHERE repo_id='{repo}' AND name={name}) <> decode('{old_oid.decode()}','hex')
             OR EXISTS(SELECT 1 FROM git_rs.repository_leases WHERE repo_id='{repo}')
          THEN RAISE EXCEPTION 'historical fixture changed during import'; END IF;
        END $$;
        UPDATE git_rs.repositories SET generation=generation+1 WHERE id='{repo}';
        INSERT INTO git_rs.operations(id,repo_id,principal,commands,generation)
          SELECT '{operation}',id,'alice',decode('{manifest.hex()}','hex'),generation FROM git_rs.repositories WHERE id='{repo}';
        INSERT INTO git_rs.packs(repo_id,operation_id,native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size,generation)
          SELECT id,'{operation}','{native_hash}','{prefix}.pack','{prefix}.idx',
            '{hashlib.sha256(pack.read_bytes()).hexdigest()}','{hashlib.sha256(index.read_bytes()).hexdigest()}',
            {pack.stat().st_size},{index.stat().st_size},generation FROM git_rs.repositories WHERE id='{repo}';
        UPDATE git_rs.refs SET oid=decode('{oid.decode()}','hex') WHERE repo_id='{repo}' AND name={name};
        INSERT INTO git_rs.ref_updates VALUES('{operation}',1,{name},decode('{old_oid.decode()}','hex'),decode('{oid.decode()}','hex'));
        COMMIT;""")
    f.results["trusted_historical_oversized_blame_fixture"] = {"operation_id": operation, "native_hash": native_hash}


def complete_blame_test(f, source, port, actors):
    """Use a real authenticated snapshot, native annotations and unreachable objects."""
    print("complete authenticated native blame and immutable reachability", flush=True)
    path = b"folder/renamed name.txt"
    (source / "folder").mkdir()
    f.git(source, "mv", "synthetic.txt", os.fsdecode(path))
    f.git(source, "commit", "-qm", "Synthetic rename")
    (source / os.fsdecode(path)).write_bytes(b"authenticated HTTP fixture\nnew line\nunterminated")
    raw_path = b"raw\xff.txt"
    (source / os.fsdecode(raw_path)).write_bytes(b"raw bytes\x00\r\nlast\xff")
    # Highly compressible native Git data exercises the uncompressed-blob bound
    # without transferring a large fixture pack or constructing it in Python RAM.
    with (source / "oversized.bin").open("wb") as large:
        block = b"\0" * (1024 * 1024)
        for _ in range(128):
            large.write(block)
        large.write(b"x")
    with (source / "blocked-output.txt").open("wb") as large:
        block = b"x" * (1024 * 1024)
        for _ in range(32):
            large.write(block)
    f.git(source, "add", "-A")
    f.git(source, "commit", "-qm", "Synthetic annotation ownership update")
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip()

    def authorization(actor):
        value = base64.b64encode(f"{actor}:{actors[actor]}".encode()).decode()
        return {"Authorization": "Basic " + value}

    helper = f"credential.helper=store --file={f.base}/http-client-alice-credentials"
    repo = "(SELECT id FROM git_rs.repositories WHERE slug='team/http-auth')"
    old_oid = f.sql(f"SELECT encode(oid,'hex') FROM git_rs.refs WHERE repo_id={repo} AND name=convert_to('refs/heads/main','UTF8')").encode()
    before_reject = f.sql(f"SELECT generation,(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}),"
                         f"(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")
    refused = f.git(source, "-c", helper, "push", f"http://alice@127.0.0.1:{port}/team/http-auth.git", "HEAD:refs/heads/main", ok=False)
    assert refused.returncode, "incoming >128 MiB blob must not pass strict receive"
    assert before_reject == f.sql(f"SELECT generation,(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}),"
                                  f"(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")
    tree = f.git(source, "rev-parse", old_oid.decode() + "^{tree}").stdout.strip()
    identity = b"Fixture <fixture@example.invalid> 1577836800 +0000\n"
    unreachable = command(["git", "-C", source, "hash-object", "-w", "-t", "commit", "--stdin"], env=f.env,
                           data=b"tree " + tree + b"\nparent " + old_oid + b"\nauthor " + identity
                          + b"committer " + identity + b"\nvalid unreachable fixture\n").stdout.strip()
    pack = command(["git", "-C", source, "pack-objects", "--stdout"], env=f.env, data=unreachable + b"\n").stdout
    report = raw_push(port, old_oid, [b"refs/heads/blame-reachability"], pack=pack,
                      slug="team/http-auth", headers=authorization("alice"))
    assert b"ok refs/heads/blame-reachability" in report, report
    import_historical_blame_fixture(f, source, old_oid, oid)
    before = f.sql(f"SELECT generation,(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}),"
                   f"(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")

    def request(commit, filename, actor="bob", slug="team/http-auth"):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            connection.request("GET", f"/{slug}.git/blame/{commit.decode()}?path={quote_from_bytes(filename, safe='')}",
                               headers=authorization(actor) if actor else {})
            response = connection.getresponse()
            return response.status, response.read(), response.getheader("Cache-Control")
        finally:
            connection.close()
            # These are serial ACL/oracle checks, not a default 1/0 overload test.
            # Receiving the last byte precedes post-flush fencing and owner drop.
            wait_for(lambda: metric_value(f.health("authenticated-http", "/metrics")[1],
                                          "git_rs_active_requests") == 0,
                     "prior authenticated HTTP handler returned before the next serial query")

    def native_calls():
        status, body = f.health("authenticated-http", "/metrics")
        assert status == 200
        return metric_value(body, 'git_rs_subprocess_duration_seconds_count{program="git"}')

    annotation_calls = []
    for filename in [path, raw_path]:
        expected = f.git(source, "--literal-pathspecs", "blame", "--root", "--line-porcelain",
                         "--no-textconv", "--no-progress", oid.decode(), "--", os.fsdecode(filename)).stdout
        before_calls = native_calls()
        status, payload, cache_control = request(oid, filename)
        assert status == 200 and payload == expected, (status, payload, expected)
        assert cache_control == "no-store"
        calls = native_calls() - before_calls
        assert calls == 0, ("real blame must not execute native Git", calls)
        annotation_calls.append(calls)
    blob = f.git(source, "rev-parse", "HEAD:" + os.fsdecode(path)).stdout.strip()
    for commit, filename in [(unreachable, path), (blob, path), (b"f" * 40, path),
                             (oid, b"missing.txt"), (oid, b"folder")]:
        status, payload, _ = request(commit, filename)
        assert status == 404 and payload == b"", (
            "blame must reject invalid or unreachable targets", commit, filename, status, payload)
    before_calls = native_calls()
    assert request(oid, b"oversized.bin")[0:2] == (413, b""), "oversized blobs must be rejected before annotation"
    oversized_calls = native_calls() - before_calls
    assert oversized_calls == 0, ("oversized blob checks must not invoke native Git", oversized_calls)
    assert request(oid, path, actor=None)[0:2] == (401, b"")
    assert request(oid, path, actor="carol")[0:2] == (500, b"")
    after = f.sql(f"SELECT generation,(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}),"
                  f"(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")
    assert after == before, "complete annotations must not publish refs, packs or operations"
    wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "0",
             "complete-blame read hold cleanup")
    f.results["complete_authenticated_blame"] = {
        "native_line_porcelain": "byte exact", "renamed_path": "passed", "non_utf8_path_and_bytes": "passed",
        "reader_acl": "passed", "unreachable_commit_in_published_pack": "rejected",
        "blob_oid_missing_path_directory": "rejected", "publication_unchanged": True,
        "oversized_uncompressed_blob": "rejected",
        "native_calls_per_annotation": annotation_calls, "native_calls_for_oversized_blob": oversized_calls,
    }
    blame_worker_test(f, source, oid, path)
    blame_blocked_output_test(f, oid)


def blame_worker_test(f, source, oid, path):
    """The obsolete worker setting must reject startup and never execute a file."""
    marker = f.server / "obsolete-worker-ran"
    worker = f.server / "obsolete-worker"
    worker.write_text(f"#!{sys.executable}\nfrom pathlib import Path\nPath({str(marker)!r}).touch()\n")
    worker.chmod(0o700)
    rejected = f.cli("serve", "--socket", f.socket("obsolete"),
                     env=dict(f.env, GIT_RS_BLAME_WORKER=str(worker)), ok=False)
    assert rejected.returncode and b"GIT_RS_BLAME_WORKER" in rejected.stderr, rejected.stderr
    assert not marker.exists(), "production must never activate an external blame worker"
    f.results["obsolete_blame_worker_rejected"] = True


def blame_lease_state(f, repo):
    """One SQL snapshot: response EOF is not acknowledgement of reader cleanup."""
    return json.loads(f.sql(
        f"SELECT json_build_object('db_now',clock_timestamp(),"
        f"'count',(SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}),"
        f"'leases',(SELECT coalesce(json_agg(row_to_json(l)),'[]') "
        f"FROM git_rs.repository_leases l WHERE repo_id={repo}),"
        f"'operations',(SELECT coalesce(json_agg(json_build_object('id',id,'principal',principal,"
        f"'generation',generation,'committed_at',committed_at,'confirmations',confirmations)),'[]') "
        f"FROM git_rs.operations WHERE repo_id={repo}))"))


def blame_blocked_output_test(f, oid, *, slug="team/http-auth"):
    """A backpressured complete response must expire and count as an error."""
    repo = f"(SELECT id FROM git_rs.repositories WHERE slug='{slug}')"
    observations = []
    f.results["blocked_blame_output_observations"] = observations
    def prior_readers_drained():
        state = blame_lease_state(f, repo)
        observation = {"phase": "prior-cleanup", "monotonic": time.monotonic(), **state}
        # Keep the first and latest states without unbounded polling history.
        observations[1:] = [observation] if observations else []
        if not observations:
            observations.append(observation)
        return state["count"] == 0
    # A prior response can be fully received while its post-flush check / lease
    # DELETE still runs in another daemon. Exclude those owners BEFORE starting
    # this test's two-second request; never kill them or discount their leases.
    try:
        wait_for(prior_readers_drained, "prior source owners drained before blocked writer")
    except AssertionError as error:
        raise AssertionError(f"{error}: {observations}") from error
    previous = dict(f.env)
    f.env.update(GIT_RS_OPERATION_SECONDS="2", GIT_RS_BLAME_CONCURRENCY="2", GIT_RS_BLAME_WAITERS="0")
    try:
        daemon, port = f.start("blame-blocked-output")
    finally:
        f.env.clear()
        f.env.update(previous)
    peer = socket.socket()
    peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    peer.settimeout(5)
    try:
        peer.connect(("127.0.0.1", port))
        target = f"/{slug}.git/blame/{oid.decode()}?path=blocked-output.txt"
        request_started = time.monotonic()
        peer.sendall(f"GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
        headers = bytearray()
        while not headers.endswith(b"\r\n\r\n"):
            byte = peer.recv(1)
            assert byte and len(headers) < 4096, "native blame must reach complete response writes"
            headers.extend(byte)
        assert headers.startswith(b"HTTP/1.1 200 OK\r\n"), headers
        assert int(re.search(rb"Content-Length: (\d+)", headers).group(1)) > 32 * 1024 * 1024
        headers_received = time.monotonic()
        state = blame_lease_state(f, repo)
        observation = {"phase": "blocked-writer", "request_started": request_started,
                       "headers_received": headers_received, "observed": time.monotonic(),
                       "absolute_deadline_seconds": 2, **state}
        observations.append(observation)
        if state["count"] != 1:
            observation["metrics"] = f.health("blame-blocked-output", "/metrics")[1].decode()
        assert state["count"] == 1, ("blocked response must own exactly one source lease", observation)
        healthy = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        try:
            healthy.request("GET", target.split("?", 1)[0] + "?path=missing-parallel.txt")
            reply = healthy.getresponse()
            assert (reply.status, reply.read()) == (404, b""), "blocked output must not prevent healthy parallel work"
        finally:
            healthy.close()
        # Stop reading only after response writes are observable. The small receive
        # window then applies real TCP backpressure until the absolute deadline.
        wait_for(lambda: metric_value(f.health("blame-blocked-output", "/metrics")[1],
                                      "git_rs_active_requests") == 0, "blocked blame deadline")
        metrics = f.health("blame-blocked-output", "/metrics")[1]
        assert metric_value(metrics, 'git_rs_requests_total{transport="http",outcome="error"}') == 2, (
            "blocked write and healthy parallel 404 must both be counted as errors")
        assert metric_value(metrics, 'git_rs_requests_total{transport="http",outcome="success"}') == 0
        wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "0",
                 "blocked writer source ownership released only after write stop")
        observations.append({"phase": "stopped-cleanup", "monotonic": time.monotonic(),
                             **blame_lease_state(f, repo)})
        status = f.process_status(daemon)
        peak = int(re.search(r"VmHWM:\s+(\d+)", status).group(1)) * 1024
        assert peak < 128 * 1024 * 1024, "complete-response buffering must fit its separate test allowance"
        f.results["blocked_blame_output"] = {
            "absolute_deadline_seconds": 2, "failed_response_counted_as_error": True,
            "observed_daemon_hwm_bytes": peak, "test_daemon_memory_allowance_bytes": 128 * 1024 * 1024,
        }
    finally:
        peer.close()
        daemon.terminate()
        daemon.wait(timeout=5)


def blame_core_test(f):
    """Independent native byte oracle over synthetic raw objects and general DAGs."""
    slug = "team/core-oracles"
    f.cli("create", slug, "alice")
    source = f.base / "core-oracles"
    f.git(f.base, "init", "--initial-branch=main", source)
    def object_id(kind, body):
        return command(["git", "-C", source, "hash-object", "-w", "-t", kind, "--stdin"],
                       env=f.env, data=body).stdout.strip()
    def tree(files):
        groups, entries = {}, []
        for path, data in files.items():
            head, separator, tail = path.partition(b"/")
            if separator:
                groups.setdefault(head, {})[tail] = data
            else:
                mode, kind, body = data if isinstance(data, tuple) else (b"100644", b"blob", data)
                oid = body if kind == b"commit" else object_id("blob", body)
                entries.append(mode + b" " + kind + b" " + oid + b"\t" + head + b"\0")
        entries += [b"040000 tree " + tree(children) + b"\t" + name + b"\0"
                    for name, children in groups.items()]
        return command(["git", "-C", source, "mktree", "-z"], env=f.env,
                       data=b"".join(entries)).stdout.strip()
    def commit(files, parents=(), *, encoding=False, timestamp=1577836800):
        identity = (b"Auteur \xe9 <author@example.invalid> " if encoding else
                    b"Author <author@example.invalid> ") + str(timestamp).encode() + b" -0000\n"
        body = b"tree " + tree(files) + b"\n" + b"".join(b"parent " + p + b"\n" for p in parents)
        body += b"author " + identity + b"committer Committer <committer@example.invalid> 1577836800 +0230\n"
        body += (b"encoding ISO-8859-1\n" if encoding else b"")
        body += b"\nSynthetic " + (b"r\xe9sum\xe9" if encoding else b"summary") + b"\ncontinued paragraph\n\nbody\n"
        return object_id("commit", body)
    raw = b'folder/quote"\\\t\xff.txt'
    files = {b"story.txt": b"one\ntwo\nthree\n", b"nested/root.txt": b"root\n",
             b"empty": b"", b"binary": b"zero\0\xff\r\nlast\r\n", b"no-lf": b"unterminated",
             raw: b"raw\xff\0\r\nno-final-LF", b"candidate-a": b"tie\n", b"candidate-b": b"tie\n"}
    root = commit(files)
    a_files, b_files = dict(files), dict(files)
    a_files[b"story.txt"], b_files[b"story.txt"] = b"ONE\ntwo\nthree\n", b"one\ntwo\nTHREE\n"
    a, b = commit(a_files, [root], timestamp=1577836790), commit(b_files, [root])
    merged = dict(files)
    merged[b"story.txt"] = b"ONE\ntwo\nTHREE\n"
    diamond = commit(merged, [a, b], timestamp=1577836780)
    reverse = commit(merged, [b, a])
    crisscross = commit(merged, [diamond, reverse])
    third_files = dict(files)
    third_files[b"story.txt"] += b"four\n"
    third = commit(third_files, [root])
    octopus_files = dict(merged)
    octopus_files[b"story.txt"] += b"four\n"
    octopus = commit(octopus_files, [a, b, third])
    renamed = dict(files)
    renamed[b"moved/story.txt"] = renamed.pop(b"story.txt") + b"edit\n"
    rename = commit(renamed, [root])
    left_files, right_files = dict(a_files), dict(b_files)
    left_files[b"left.txt"] = left_files.pop(b"story.txt")
    right_files[b"right.txt"] = right_files.pop(b"story.txt")
    left, right = commit(left_files, [a]), commit(right_files, [b])
    both_names = dict(left_files)
    both_names[b"right.txt"] = right_files[b"right.txt"]
    merged_names = commit(both_names, [left, right])
    left_final, right_final = dict(left_files), dict(right_files)
    left_final[b"merged.txt"] = left_final.pop(b"left.txt")
    right_final[b"merged.txt"] = right_final.pop(b"right.txt")
    converging_files = dict(left_final)
    converging_files[b"merged.txt"] = merged[b"story.txt"]
    converging = commit(converging_files, [commit(left_final, [left]), commit(right_final, [right])])
    ties = dict(files)
    ties.pop(b"candidate-a")
    ties[b"tie-target"] = ties.pop(b"candidate-b")
    ties[b"copied"] = ties[b"story.txt"]
    tie = commit(ties, [root])
    unrelated_files = dict(files)
    unrelated_files[b"story.txt"] = b"unrelated\n"
    unrelated = commit(unrelated_files)
    roots = commit(unrelated_files, [root, unrelated])
    codec = commit(files, encoding=True)
    modes = dict(files)
    modes[b"executable"] = (b"100755", b"blob", b"executable\n")
    modes[b"symlink"] = (b"120000", b"blob", b"nested/root.txt")
    modes[b"gitlink"] = (b"160000", b"commit", root)
    modes.pop(b"story.txt")
    modes[b"story.txt/changed-type"] = b"directory replaces old blob\n"
    type_changes = commit(modes, [root])
    tagged = commit(dict(files), [root], timestamp=1577836750)
    tag = object_id("tag", b"object " + tagged + b"\ntype commit\ntag synthetic\n"
                    b"tagger Tagger <tagger@example.invalid> 1577836800 +0000\n\nSynthetic tag\n")
    cases = [("root", root, list(files)), ("diamond", diamond, [b"story.txt"]),
             ("crisscross", crisscross, [b"story.txt"]), ("octopus", octopus, [b"story.txt"]),
             ("rename-edit", rename, [b"moved/story.txt"]), ("copy-ties", tie, [b"tie-target", b"copied"]),
             ("merged-names", merged_names, [b"left.txt", b"right.txt"]),
             ("converging-renames", converging, [b"merged.txt"]),
             ("type-changes", type_changes, [b"executable", b"symlink", b"story.txt/changed-type"]),
             ("multiple-roots", roots, [b"story.txt"]), ("metadata-codec", codec, [raw])]
    for label, oid, _ in cases:
        f.git(source, "update-ref", "refs/heads/" + label, oid.decode())
    f.git(source, "update-ref", "refs/heads/main", octopus.decode())
    f.git(source, "update-ref", "refs/tags/synthetic", tag.decode())
    _, port = f.start("core-oracles")
    def native_calls():
        status, body = f.health("core-oracles", "/metrics")
        assert status == 200
        return metric_value(body, 'git_rs_subprocess_duration_seconds_count{program="git"}')
    url = f"http://127.0.0.1:{port}/{slug}.git"
    before_push = native_calls()
    f.git(source, "push", "--mirror", url)
    assert native_calls() == before_push, "normal mirror receive must not spawn native Git"
    clone = f.base / "core-native-boundary-clone.git"
    before_clone = native_calls()
    f.git(f.base, "clone", "--bare", url, clone)
    assert f.git(clone, "rev-parse", "refs/heads/main").stdout.strip() == octopus
    assert native_calls() == before_clone, "upload-pack must use the Rust runtime without native Git"
    dangling = commit(files, [octopus], timestamp=1577836700)
    pack = command(["git", "-C", source, "pack-objects", "--stdout"], env=f.env,
                   data=dangling + b"\n").stdout
    assert b"ok refs/heads/dangling-carrier" in raw_push(port, octopus, [b"refs/heads/dangling-carrier"],
                                                       pack=pack, slug=slug)
    def request(oid, path, service_port=port):
        peer = http.client.HTTPConnection("127.0.0.1", service_port, timeout=30)
        try:
            peer.request("GET", f"/{slug}.git/blame/{oid.decode()}?path={quote_from_bytes(path, safe='')}")
            reply = peer.getresponse()
            return reply.status, reply.read()
        finally:
            peer.close()
            # A zero-byte response can be consumed before the serving thread
            # drops its completed-response admission. These are serial oracle
            # cases, not an intentional default-capacity overload/retry test.
            service = "core-oracles" if service_port == port else "core-oracles-reader"
            wait_for(lambda: metric_value(f.health(service, "/metrics")[1], "git_rs_active_requests") == 0,
                     "prior oracle HTTP handler returned before the next serial query")
    repo = f"(SELECT id FROM git_rs.repositories WHERE slug='{slug}')"
    def publication():
        return f.sql(f"SELECT generation,storage_epoch,(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}),"
                     f"(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")
    before = publication()
    before_blame = native_calls()
    count = 0
    for label, oid, paths in [*cases, ("tag-only-reachable", tagged, [b"story.txt"])]:
        for path in paths:
            oracle = f.git(source, "--literal-pathspecs", "blame", "--root", "--line-porcelain",
                           "--no-textconv", "--no-progress", oid.decode(), "--", os.fsdecode(path)).stdout
            actual = request(oid, path)
            assert actual == (200, oracle), (label, path, actual, oracle)
            count += 1
    rejected_targets = [(dangling, b"story.txt"), (tag, b"story.txt"), (root, b"folder"), (root, b"absent"),
                        (type_changes, b"gitlink"), (type_changes, b"story.txt")]
    for oid, path in rejected_targets:
        assert request(oid, path) == (404, b""), (oid, path)
    assert native_calls() == before_blame, "Rust blame must not cross the native Git server boundary"
    f.cli("grant", slug, "alice", "bob", "reader", "refs/")
    _, reader_port = f.start("core-oracles-reader", "bob")
    assert request(root, raw, reader_port)[0] == 200
    f.cli("grant", slug, "alice", "bob", "none", "refs/")
    assert request(root, raw, reader_port) == (500, b""), "revoked source access must not reuse a response"
    # Grant mutations intentionally fence generation; blame itself never publishes.
    assert before.split("|")[1:] == publication().split("|")[1:]
    wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "0", "oracle source cleanup")
    f.results["real_core_byte_oracles"] = {"byte_exact_cases": count, "tags_and_dangling": True,
        "metadata_codec_and_raw_paths": True, "general_DAG": True, "revocation": True,
        "invalid_target_cases": len(rejected_targets), "reader_acl_checks": 2,
        "functional_target_checks": count + len(rejected_targets) + 2,
        "rust_receive_upload_boundaries": True, "blame_native_subprocesses": 0}
    return source, root, raw, slug


def blame_shared_cache_test(f, source, oid, path, slug):
    """Two actual HTTP processes/restart read verified cached artifacts, not an opaque fake."""
    from blame_concurrency import Gates
    previous = dict(f.env)
    suffix = uuid.uuid4().hex[:12]
    database, role = "blame_cache_" + suffix, "blame_cache_" + suffix
    f.sql(f"CREATE ROLE {role} LOGIN PASSWORD 'synthetic-cache-only'; CREATE DATABASE {database}; "
          f"GRANT CREATE ON DATABASE {database} TO {role};")
    f.env.update(GIT_RS_CACHE_DSN=f"host=127.0.0.1 port={f.env['PGPORT']} dbname={database} user={role} password=synthetic-cache-only sslmode=disable",
                 GIT_RS_CACHE_NAMESPACE=str(uuid.uuid4()), GIT_RS_CACHE_OWNER=str(uuid.uuid4()),
                 GIT_RS_CACHE_S3_ENDPOINT=f.env["GIT_RS_S3_ENDPOINT"], GIT_RS_CACHE_S3_BUCKET="blame-derived",
                 GIT_RS_CACHE_S3_ACCESS_KEY="test-access", GIT_RS_CACHE_S3_SECRET_KEY="synthetic-secret-key")
    config = (f'url = "{f.env["GIT_RS_S3_ENDPOINT"]}/blame-derived"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
              'user = "test-access:synthetic-secret-key"\n')
    command(["curl", "--disable", "--silent", "--fail", "--max-time", "5", "--config", "-", "--request", "PUT"], data=config.encode())
    f.cli("cache-migrate")
    admin = dict(f.env, PGDATABASE=database)
    def cache_sql(sql):
        return command(["psql", "-XqAt", "-v", "ON_ERROR_STOP=1"], env=admin, data=sql.encode()).stdout.decode().strip()
    target = f"/{slug}.git/blame/{oid.decode()}?path={quote_from_bytes(path, safe='')}"
    repo = f"(SELECT id FROM git_rs.repositories WHERE slug='{slug}')"
    source_before = f.sql(f"SELECT generation,storage_epoch,(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}),"
                          f"(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")
    expected = f.git(source, "--literal-pathspecs", "blame", "--root", "--line-porcelain",
                     "--no-textconv", "--no-progress", oid.decode(), "--", os.fsdecode(path)).stdout
    gates = Gates(f.server / "cache-blame.sock")
    def fetch(port):
        peer = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        peer.request("GET", target)
        return peer
    def finish(peer):
        reply = peer.getresponse()
        try:
            assert (reply.status, reply.read()) == (200, expected)
        finally:
            peer.close()
    def source_drained():
        wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "0",
                 "real blame source ownership drained before stopping service")
    try:
        _, producer = f.start_blame("blame-cache-producer", control=gates.path, phase="complete", cache=True)
        peer = fetch(producer)
        gate, _ = gates.entered()
        assert int(cache_sql("SELECT count(*) FROM git_rs_cache.manifest_roots")) > 0
        wait_for(lambda: cache_sql("SELECT count(*) FROM git_rs_cache.read_holds") == "0",
                 "producer transient staging holds drained")
        descriptor = cache_sql("SELECT payload FROM git_rs_cache.manifest_roots ORDER BY payload")
        gate.sendall(b"G")
        finish(peer)
        consumer_process, consumer = f.start_blame("blame-cache-consumer", control=gates.path, phase="complete", cache=True)
        for port in [consumer]:
            peer = fetch(port)
            gate, _ = gates.entered()
            assert int(cache_sql("SELECT count(*) FROM git_rs_cache.read_holds WHERE payload IN "
                                 "(SELECT payload FROM git_rs_cache.manifest_roots)")) > 0, "a verified root manifest hit must own read admission"
            assert cache_sql("SELECT payload FROM git_rs_cache.manifest_roots ORDER BY payload") == descriptor
            gate.sendall(b"G")
            finish(peer)
        # Reading Content-Length bytes does not acknowledge the server's final
        # fences/hold release. Do not SIGTERM the first consumer mid-cleanup.
        wait_for(lambda: metric_value(f.health("blame-cache-consumer", "/metrics")[1], "git_rs_active_requests") == 0,
                 "independent cache consumer HTTP handler completed")
        wait_for(lambda: cache_sql("SELECT count(*) FROM git_rs_cache.read_holds") == "0",
                 "independent cache consumer released read holds before restart")
        source_drained()
        consumer_process.terminate()
        consumer_process.wait(timeout=5)
        _, restarted = f.start_blame("blame-cache-restarted", control=gates.path, phase="complete", cache=True)
        peer = fetch(restarted)
        gate, _ = gates.entered()
        assert int(cache_sql("SELECT count(*) FROM git_rs_cache.read_holds WHERE payload IN "
                             "(SELECT payload FROM git_rs_cache.manifest_roots)")) > 0
        gate.sendall(b"G")
        finish(peer)
        wait_for(lambda: cache_sql("SELECT count(*) FROM git_rs_cache.read_holds") == "0", "cache response holds released")
        source_drained()
        f.stop("blame-cache-producer")
        f.stop("blame-cache-restarted")
        _, joined_port = f.start_blame("blame-cache-joined", control=gates.path, phase="source", cache=True)
        creator = fetch(joined_port)
        gate, _ = gates.entered()
        joined = fetch(joined_port)
        wait_for(lambda: metric_value(f.health("blame-cache-joined", "/metrics")[1], "git_rs_fixture_blame_waiters") == 2,
                 "cached same-key waiter attached")
        assert f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "1"
        assert not gates.pending(), "same-key waiter must not start a second derivation"
        creator.close()
        wait_for(lambda: metric_value(f.health("blame-cache-joined", "/metrics")[1], "git_rs_fixture_blame_waiters") == 1,
                 "cached creator cancellation detached")
        assert f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "1"
        gate.sendall(b"G")
        finish(joined)
        source_drained()
        f.stop("blame-cache-joined")
        # Fence an already verified cached body before HTTP success, not merely
        # an unauthorized cold lookup. ACL changes are intentional generation writes.
        f.cli("grant", slug, "alice", "bob", "reader", "refs/")
        _, bob_port = f.start_blame("blame-cache-revocation", control=gates.path,
                                   phase="complete", cache=True, principal="bob")
        peer = fetch(bob_port)
        gate, _ = gates.entered()
        gate.sendall(b"G")
        finish(peer)
        wait_for(lambda: cache_sql("SELECT count(*) FROM git_rs_cache.read_holds") == "0", "Bob initial cache holds drained")
        peer = fetch(bob_port)
        gate, _ = gates.entered()
        assert int(cache_sql("SELECT count(*) FROM git_rs_cache.read_holds WHERE payload IN "
                             "(SELECT payload FROM git_rs_cache.manifest_roots)")) > 0
        assert not select.select([peer.sock], [], [], 0)[0], "no HTTP success before final source fencing"
        f.cli("grant", slug, "alice", "bob", "none", "refs/")
        gate.sendall(b"G")
        try:
            reply = peer.getresponse()
            assert (reply.status, reply.read()) == (500, b""), "revoked cached response must not escape"
        finally:
            peer.close()
        source_drained()
        f.stop("blame-cache-revocation")
        # Outage only this fixture-owned cache DB, never the source DB/cluster.
        f.sql(f"REVOKE CONNECT ON DATABASE {database} FROM PUBLIC,{role}; "
              f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{database}';")
        _, dead_cache = f.start_blame("blame-cache-dead", cache=True)
        finish(fetch(dead_cache))
        source_after = f.sql(f"SELECT generation,storage_epoch,(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}),"
                             f"(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")
        old, new = source_before.split("|"), source_after.split("|")
        assert int(new[0]) == int(old[0]) + 2 and new[1:] == old[1:], (
            "only the two explicit ACL grants may change source state", old, new)
        f.results["real_blame_shared_cache"] = {"independent_process_hit_with_read_holds": True,
            "cache_scope": "complete-result cache only; this fixture does not exercise checkpoint mapping reuse or warming",
            "restart_verified_hit": True, "cache_outage_bounded_recomputation": True,
            "cancelled_creator_keeps_shared_cached_job_owned": True,
            "cached_response_revocation_before200": True, "only_explicit_ACL_changes_publish": True,
            "fixture_credential_model": "single synthetic S3 account, separate bucket and namespace"}
    finally:
        gates.close()
        f.env.clear()
        f.env.update(previous)


def blame_core_output_test(f, source, slug):
    """Real serialized output limits and real blocked writes, not fabricated stdout."""
    f.git(source, "checkout", "-f", "main")
    with (source / "blocked-output.txt").open("wb") as output:
        for _ in range(32):
            output.write(b"x" * (1024 * 1024))
    (source / "output-limit.txt").write_bytes(b"x\n" * (512 * 1024))
    f.git(source, "add", "blocked-output.txt", "output-limit.txt")
    identities = dict(f.env, GIT_AUTHOR_NAME="Synthetic" * 32, GIT_COMMITTER_NAME="Synthetic" * 32)
    f.git(source, "commit", "-qm", "Synthetic bounded serialization " + "s" * 300, env=identities)
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip()
    daemon, publish = f.start("core-output-publisher")
    f.git(source, "push", f"http://127.0.0.1:{publish}/{slug}.git", "main")
    # The known metadata alone exceeds 256 MiB for 524288 lines. The source
    # blob is only 1 MiB; this specifically exercises the complete-output cap.
    assert (2 * len(identities["GIT_AUTHOR_NAME"]) + 300) * 512 * 1024 > 256 * 1024 * 1024
    peer = http.client.HTTPConnection("127.0.0.1", publish, timeout=120)
    try:
        peer.request("GET", f"/{slug}.git/blame/{oid.decode()}?path=output-limit.txt")
        reply = peer.getresponse()
        assert (reply.status, reply.read()) == (413, b""), "oversized real serialization must never emit partial 200"
    finally:
        peer.close()
    peak = int(re.search(r"VmHWM:\s+(\d+)", f.process_status(daemon)).group(1)) * 1024
    assert peak < 512 * 1024 * 1024, "preserve the separate oversized-output memory gate"
    f.results["oversized_blame_output"] = {"real_serializer_limit_bytes": 256 * 1024 * 1024,
                                           "observed_daemon_hwm_bytes": peak}
    f.stop("core-output-publisher")
    blame_blocked_output_test(f, oid, slug=slug)


def long_operation_test(f, seconds=180):
    """Cross the real default deadline while native receive-pack awaits its pack trailer."""
    print("native publication after the default 120-second deadline", flush=True)
    slug = "team/long-operation"
    f.cli("create", slug, "alice")
    source = f.base / "long-operation-source"
    f.git(f.base, "init", "--initial-branch=main", source)
    (source / "synthetic.txt").write_text("bounded long-operation fixture\n")
    f.git(source, "add", "synthetic.txt")
    f.git(source, "commit", "-qm", "Synthetic long-operation fixture")
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip()
    pack = command(["git", "-C", source, "pack-objects", "--stdout", "--revs"],
                   env=f.env, data=oid + b"\n").stdout
    names = [b"refs/heads/main", b"refs/heads/long-copy"]
    body = b"".join(pkt(b"0" * 40 + b" " + oid + b" " + name
                        + (b"\0report-status side-band-64k" if index == 0 else b""))
                    for index, name in enumerate(names)) + b"0000" + pack
    previous = f.env.get("GIT_RS_OPERATION_SECONDS")
    f.env["GIT_RS_OPERATION_SECONDS"] = str(seconds)
    try:
        _, port = f.start("long-operation")
    finally:
        if previous is None:
            del f.env["GIT_RS_OPERATION_SECONDS"]
        else:
            f.env["GIT_RS_OPERATION_SECONDS"] = previous
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=180)
    started = time.monotonic()
    try:
        connection.putrequest("POST", f"/{slug}.git/git-receive-pack")
        connection.putheader("Content-Type", "application/x-git-receive-pack-request")
        connection.putheader("Content-Length", str(len(body)))
        connection.endheaders()
        connection.send(body[:-1])
        repo = "(SELECT id FROM git_rs.repositories WHERE slug='team/long-operation')"
        wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "1",
                 "committed long-operation hold")
        assert f.sql(f"SELECT count(*) FROM git_rs.refs WHERE repo_id={repo}") == "0"

        f.assert_no_repository_bridge()
        metrics = f.health("long-operation", "/metrics")[1]
        assert metric_value(metrics, 'git_rs_subprocess_duration_seconds_count{program="git"}') == 0
        # This is an intentional wall-clock boundary test, not a sleep used to
        # guess worker ordering: the committed hold above proves request admission.
        threading.Event().wait(max(0, 125 - (time.monotonic() - started)))
        assert f.health("long-operation", "/livez") == (200, b"live\n")
        assert f.sql(f"SELECT count(*) FROM git_rs.refs WHERE repo_id={repo}") == "0"
        try:
            connection.send(body[-1:])
            response = connection.getresponse()
            payload = response.read()
        except (OSError, http.client.HTTPException) as error:
            raise AssertionError("a configured long operation must complete native publication") from error
        assert response.status == 200 and all(b"ok " + name in payload for name in names), (
            "a configured long operation must publish both refs", response.status, payload)
    finally:
        connection.close()
    elapsed = time.monotonic() - started
    assert 125 <= elapsed < seconds, elapsed
    url = f"http://127.0.0.1:{port}/{slug}.git"
    expected = b"".join(oid + b"\t" + name + b"\n" for name in sorted(names))
    assert f.git(source, "ls-remote", "--refs", url).stdout == expected
    mirror = f.base / "long-operation-mirror.git"
    f.git(source, "clone", "--mirror", url, mirror)
    f.git(mirror, "fsck", "--strict", "--full")
    assert f.git(mirror, "show", "main:synthetic.txt").stdout == b"bounded long-operation fixture\n"
    assert f.sql(f"SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}") == "1"
    wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "0",
             "completed long-operation hold release")
    f.results["long_rust_publication"] = {"budget_seconds": seconds, "elapsed_seconds": round(elapsed, 3),
                                             "refs": len(names), "native_clone_fsck": "passed",
                                              "cpu_workers": 1, "native_server_subprocesses": 0}
    f.record_memory()


def main():
    from blame_concurrency import exercise as blame_concurrency_test

    parser = argparse.ArgumentParser()
    parser.add_argument("--provision", action="store_true", help="create and remove private local Docker PostgreSQL/S3 fixtures")
    parser.add_argument("--binary", type=Path,
                         default=os.environ.get("GIT_RS_TEST_BINARY", ROOT / "target/debug/git-rs"))
    parser.add_argument("--runtime-image", default=os.environ.get("GIT_RS_TEST_RUNTIME_IMAGE"),
                        help="prebuilt immutable Git-free runtime; full integration never starts a host backend")
    parser.add_argument("--runtime-fixture-image", default=os.environ.get("GIT_RS_TEST_RUNTIME_FIXTURE_IMAGE"),
                        help="prebuilt Git-free image containing the explicit observer/receive fixtures")
    parser.add_argument("--revision", default=os.environ.get("GIT_RS_TEST_REVISION"),
                        help="full source revision required to bind both server images")
    parser.add_argument("--large-mib", type=int, default=16)
    parser.add_argument("--skip-ssh", action="store_true")
    parser.add_argument("--debug-timings", action="store_true",
                        help="verify opt-in timing logs through native Git requests")
    parser.add_argument("--long-operations", action="store_true",
                        help="also run the 125-second native publication deadline regression")
    parser.add_argument("--blame-concurrency-only", action="store_true",
                         help="run only the tiny synthetic blame concurrency contract")
    parser.add_argument("--blame-core-only", action="store_true",
                        help="real PG/S3 Rust blame byte oracles, shared reuse, output and concurrency gates")
    parser.add_argument("--receive-core-only", action="store_true",
                         help="explicit Rust receive handler with fresh PG/S3 and native client oracles")
    parser.add_argument("--upload-core-only", action="store_true",
                        help="native protocol v0/v1/v2, shallow/deepen, hidden wants and SSH against the actual runtime")
    parser.add_argument("--history-core-only", action="store_true",
                        help="complete ordered history and native bundle/truncation contracts against the actual runtime")
    parser.add_argument("--receive-fixture-binary", type=Path,
                        help="prebuilt receive_fixture test ELF")
    parser.add_argument("--blame-fixture-binary", type=Path,
                        help="prebuilt test-only blame-http-fixture example (never a production worker)")
    parser.add_argument("--fixture-dir", type=Path, default=os.environ.get("GIT_RS_TEST_FIXTURE_DIR"),
                        help="fresh private directory to retain fixture logs/results after cleanup")
    args = parser.parse_args()
    assert sum((args.blame_concurrency_only, args.blame_core_only, args.receive_core_only,
                args.upload_core_only, args.history_core_only)) <= 1, "focused integration selectors are mutually exclusive"
    if not (args.blame_core_only or args.blame_concurrency_only or args.receive_core_only):
        assert args.runtime_image and args.runtime_fixture_image and args.revision, (
            "full integration requires --runtime-image, --runtime-fixture-image and --revision")
    if args.receive_core_only:
        from receive_integration import run
        return run(args)
    if args.blame_core_only:
        assert args.provision, "--blame-core-only requires fresh owned --provision fixtures"
    # OpenSSH StrictModes checks all parents of authorized_keys. A system /tmp
    # parent is world-writable, so keep this private fixture under the checkout.
    # The focused HTTP-only contract can use /tmp to keep Unix socket paths short.
    local = None if (args.blame_concurrency_only or args.blame_core_only) else ROOT / ".local"
    if local is not None:
        local.mkdir(mode=0o700, exist_ok=True)
    if args.fixture_dir:
        assert args.fixture_dir.parent.is_dir(), "fixture directory parent must already exist"
        args.fixture_dir.mkdir(mode=0o700)
        context = nullcontext(str(args.fixture_dir))
    else:
        context = tempfile.TemporaryDirectory(prefix="grs-", dir=local)
    if args.blame_core_only or args.blame_concurrency_only:
        def interrupted(signum, _):
            raise InterruptedError(f"owned blame integration interrupted by {signum}")
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
    with context as directory:
        fixture = None
        try:
            fixture = Fixture(Path(directory), args.binary.resolve(), runtime_image=args.runtime_image,
                              runtime_fixture_image=args.runtime_fixture_image, revision=args.revision)
            fixture.blame_fixture_binary = (args.blame_fixture_binary or
                args.binary.resolve().parent / "examples/blame-http-fixture").resolve()
            assert fixture.runtime or fixture.blame_fixture_binary.is_file(), "a prebuilt blame fixture is required"
            fixture.blame_core_resources = args.blame_core_only or args.blame_concurrency_only
            if fixture.blame_core_resources and args.provision:
                invocation = os.environ.get("INVOCATION_ID")
                assert invocation and os.environ.get("GIT_RS_TEST_OWNER_LABEL", "").endswith("=" + invocation), (
                    "focused provision requires independently bounded systemd ownership/cleanup")
            if args.debug_timings:
                fixture.env["GIT_RS_DEBUG_TIMINGS"] = "1"
            fixture.setup(args.provision)
            if args.upload_core_only:
                upload_core_test(fixture, args.skip_ssh)
            elif args.history_core_only:
                history_core_test(fixture)
            elif args.blame_core_only:
                fixture.cli("migrate")
                source, oid, path, slug = blame_core_test(fixture)
                fixture.stop_daemons()
                blame_shared_cache_test(fixture, source, oid, path, slug)
                fixture.stop_daemons()
                blame_core_output_test(fixture, source, slug)
                fixture.stop_daemons()
                blame_worker_test(fixture, source, oid, path)
            elif args.blame_concurrency_only:
                fixture.cli("migrate")
            else:
                exercise(fixture, args.large_mib, args.skip_ssh)
                authenticated_http_test(fixture)
                if args.long_operations:
                    long_operation_test(fixture)
            if not (args.upload_core_only or args.history_core_only):
                blame_concurrency_test(fixture)
            print(json.dumps({"status": "passed", **fixture.results}, indent=2))
        except Exception as error:
            if fixture:
                fixture.results["error"] = repr(error)
                for path in Path(directory).glob("*.log"):
                    print(path.name, path.read_text()[-8000:])
            raise
        finally:
            if fixture:
                try:
                    fixture.close()
                finally:
                    (Path(directory) / "report.json").write_text(json.dumps(fixture.results, indent=2) + "\n")


if __name__ == "__main__":
    main()
