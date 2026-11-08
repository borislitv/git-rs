#!/usr/bin/env python3
"""Black-box native Git/SQL/S3 tests. All data and identities are synthetic."""
import argparse
import base64
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import pwd
import random
import re
import select
import signal
import socket
import socketserver
import struct
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import quote_from_bytes

ROOT = Path(__file__).resolve().parents[1]
PG_IMAGE = "postgres:18.6-bookworm@sha256:f1ea2a1a4fb7b68d4836be1fdcb8a6466a89b072158bf9d97663600538c294c5"
S3_IMAGE = "versity/versitygw@sha256:89cad13cc981986ee284cbafea4613f9533ce6fd5f37a2c96c4c4fb8adb3a316"


def command(args, *, env=None, cwd=None, data=None, ok=True):
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


def metric_value(body, series):
    prefix = series.encode() + b" "
    values = [float(line[len(prefix):]) for line in body.splitlines() if line.startswith(prefix)]
    assert len(values) == 1, f"expected exactly one metric series: {series}"
    return values[0]


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
    def __init__(self, base, binary):
        self.base, self.binary = base, str(binary)
        self.containers, self.processes, self.logs = [], [], []
        self.daemons, self.health_ports = {}, {}
        self.results = {}
        self.env = {k: v for k, v in os.environ.items() if k in ["PATH", "LANG", "TERM"]}
        self.env.update(HOME=str(base), GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
                        GIT_TERMINAL_PROMPT="0", GIT_AUTHOR_NAME="Test Author",
                        GIT_AUTHOR_EMAIL="author@example.invalid", GIT_COMMITTER_NAME="Test Author",
                        GIT_COMMITTER_EMAIL="author@example.invalid", GIT_RS_CACHE_BYTES="1048576",
                         GIT_RS_DEV_HTTP="1", GIT_RS_PACK_THREADS="4", GIT_RS_S3_ACCESS_KEY="test-access",
                        GIT_RS_S3_SECRET_KEY="synthetic-secret-key", GIT_RS_S3_BUCKET="git-test",
                        PGHOST="127.0.0.1", PGUSER="postgres", PGDATABASE="git_rs",
                        PGPASSWORD="development-only", PGSSLMODE="disable", PGCONNECT_TIMEOUT="2",
                        PGGSSENCMODE="disable")
        (base / "scratch").mkdir(mode=0o700)

    def setup(self, provision):
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

    def container(self, args):
        identifier = command(["docker", "run", "-d", *args]).stdout.decode().strip()
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

    def cli(self, *args, ok=True):
        return command([self.binary, *args], env=self.env, ok=ok)

    def git(self, path, *args, ok=True, env=None):
        return command(["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", "-C", path, *args], env=env or self.env, ok=ok)

    def sql(self, query):
        return command(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], env=self.env, data=query.encode()).stdout.decode().strip()

    def start(self, name, principal="alice"):
        log_path = self.base / f"{name}.log"
        log = log_path.open("wb")
        self.logs.append(log)
        process = subprocess.Popen([self.binary, "serve", "--scratch", str(self.base / "scratch"),
                                   "--socket", str(self.base / f"{name}.sock"), "--health", "127.0.0.1:0",
                                   "--dev-http", "127.0.0.1:0",
                                   "--dev-principal", principal], env=self.env, stdout=log, stderr=log)
        self.processes.append(process)

        def ready():
            if process.poll() is not None:
                raise AssertionError(log_path.read_text())
            return re.search(r"development HTTP 127.0.0.1:(\d+)", log_path.read_text())

        wait_for(ready, "Rust daemon")
        self.daemons[name] = process
        self.health_ports[name] = int(re.search(r"health 127.0.0.1:(\d+)", log_path.read_text()).group(1))
        return process, int(ready().group(1))

    def health(self, name, path):
        connection = http.client.HTTPConnection("127.0.0.1", self.health_ports[name], timeout=3)
        connection.request("GET", path)
        response = connection.getresponse()
        status, body = response.status, response.read()
        connection.close()
        return status, body

    def record_memory(self):
        peaks = []
        for process in self.daemons.values():
            if process.poll() is None:
                status = Path(f"/proc/{process.pid}/status").read_text()
                peaks.append(int(re.search(r"VmHWM:\s+(\d+)", status).group(1)) * 1024)
        if peaks:
            self.results["maximum_rust_daemon_hwm_bytes"] = max(peaks)
            assert max(peaks) < 64 * 1024 * 1024, peaks

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(self.processes):
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in self.logs:
            log.close()
        for identifier in self.containers:
            command(["docker", "rm", "-f", identifier], ok=False)


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
    assert command([f.binary, "ssh", "--socket", f.base / "a.sock", "--principal", "alice"], env=bridge_env, data=b"", ok=False).returncode != 0
    assert f.cli("serve", "--scratch", str(f.base / "scratch"), "--socket", str(f.base / "a.sock"), ok=False).returncode != 0
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
    backup = f.base / "repository.bundle"
    print("native bundle backup and restore", flush=True)
    f.cli("backup", "team/project", "alice", "--scratch", str(f.base / "scratch"), "--output", str(backup))
    backup_digest = hashlib.sha256(backup.read_bytes()).digest()
    assert f.cli("backup", "team/project", "alice", "--scratch", str(f.base / "scratch"), "--output", str(backup), ok=False).returncode
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
        'git_rs_subprocess_duration_seconds_count{program="git"}',
        'git_rs_cache_lookups_total{result="hit"}',
        'git_rs_s3_transferred_bytes_total{direction="download"}',
        'git_rs_s3_transferred_bytes_total{direction="upload"}',
    ]:
        assert sum(metric_value(body, series) for body in scrapes) > 0, series
    for stage in [
        "sql_init", "sql_acquire", "sql_query", "sql_reset", "lease_commit", "catalog_read",
        "lease_release", "cache_fill", "cache_link", "object_hash", "restore_refs", "hook_setup",
        "git_input", "git_output", "git_wait", "ref_validate", "tip_validate", "fast_forward",
        "scratch_cleanup", "process_spawn",
    ]:
        series = f'git_rs_stage_duration_seconds_count{{stage="{stage}"}}'
        assert sum(metric_value(body, series) for body in scrapes) > 0, stage
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
                if {"lease_commit", "catalog_read", "restore_refs", "hook_setup", "git_input",
                    "git_output", "scratch_cleanup", "lease_release"} <= stages:
                    completed.add(request["name"])
            return {"http"} <= completed and (skip_ssh or "ssh" in completed)
        wait_for(correlated_requests, "correlated native Git request/worker/cleanup timings")
        f.results["correlated_native_timing_logs"] = "passed"
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
    f.record_memory()
    assert all(balancer.counts), balancer.counts
    f.results["balanced_connections"] = balancer.counts
    wait_for(lambda: not list((f.base / "scratch").glob("instance-*/request-*")), "request scratch cleanup")
    balancer.shutdown()


def ssh_test(f, source, expected):
    for name in ["host-key", "client-key"]:
        command(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", f.base / name])
    user = pwd.getpwuid(os.getuid()).pw_name
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    forced = f'{f.binary} ssh --socket {f.base}/a.sock --principal alice'
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
    f.git(source, "push", remote, "HEAD:refs/heads/ssh-tested", env=env)
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
    backup = f.base / "before-truncate.bundle"
    f.cli("truncate", "team/history", "alice", "--keep", "2", "--scratch", str(f.base / "scratch"), "--backup", str(backup))
    trimmed = f.base / "trimmed"
    f.git(source, "clone", url, trimmed)
    f.git(trimmed, "fsck", "--full")
    assert f.git(trimmed, "rev-list", "--count", "HEAD").stdout.strip() == b"2"
    assert f.git(trimmed, "rev-parse", "HEAD^{tree}").stdout == original_tree
    new_size = int(f.sql("SELECT sum(pack_size+idx_size) FROM git_rs.packs WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/history')"))
    assert new_size < old_size / 2, (old_size, new_size)
    for key in old_keys:
        deleted = f.s3("HEAD", key, ok=False)
        assert deleted.returncode == 22 and deleted.stdout == b"404", (key, deleted.stdout, deleted.stderr)
    assert f.sql("SELECT count(*) FROM git_rs.retired_objects") == "0"
    assert f.sql("SELECT count(*) FROM git_rs.operations WHERE repo_id=(SELECT id FROM git_rs.repositories WHERE slug='team/history')") == "1"
    recovered = f.base / "history-recovered"
    f.git(source, "clone", backup, recovered)
    assert f.git(recovered, "rev-list", "--count", "HEAD").stdout.strip() == b"5"
    f.git(recovered, "fsck", "--full")
    f.results["truncate"] = {"old_pack_bytes": old_size, "new_pack_bytes": new_size, "remaining_commits": 2, "native_backup_commits": 5}


def authenticated_http_test(f):
    """Exercise native Basic challenge/retry and per-request repository ACLs."""
    print("authenticated Smart HTTP identities and ACLs", flush=True)
    # Only synthetic credentials; both server and native-client stores stay in
    # the private fixture directory. Tokens never enter argv or child env.
    actors = {"alice": "a" * 43, "bob": "b" * 43, "carol": "c" * 43}
    server_credentials = f.base / "http-server-credentials"
    server_credentials.write_text("".join(f"{actor} {token}\n" for actor, token in actors.items()))
    server_credentials.chmod(0o600)
    log_path = f.base / "authenticated-http.log"
    log = log_path.open("wb")
    f.logs.append(log)
    process = subprocess.Popen([f.binary, "serve", "--scratch", str(f.base / "scratch"),
                                "--socket", str(f.base / "authenticated-http.sock"),
                                "--http", "127.0.0.1:0", "--health", "127.0.0.1:0",
                                "--http-credentials", str(server_credentials)],
                               env=f.env, stdout=log, stderr=log)
    f.processes.append(process)

    def ready():
        if process.poll() is not None:
            raise AssertionError(log_path.read_text())
        return re.search(r"^HTTP 127.0.0.1:(\d+)$", log_path.read_text(), re.MULTILINE)

    wait_for(ready, "authenticated HTTP listener")
    port = int(ready().group(1))
    f.health_ports["authenticated-http"] = int(re.search(r"health 127.0.0.1:(\d+)", log_path.read_text()).group(1))
    helpers = {}
    for actor, token in actors.items():
        # Git can clear a URL username after an initial 401 and ask its helper
        # again without one. A shared store may then return another valid actor.
        # Isolate clients so a reader attempt can never fall back to the owner.
        client_credentials = f.base / f"http-client-{actor}-credentials"
        client_credentials.write_text(f"http://{actor}:{token}@127.0.0.1:{port}\n")
        client_credentials.chmod(0o600)
        helpers[actor] = f"credential.helper=store --file={client_credentials}"
        resolved = command(["git", "-c", helpers[actor], "credential", "fill"], env=f.env,
                           data=f"protocol=http\nhost=127.0.0.1:{port}\n\n".encode(), ok=False)
        assert resolved.returncode == 0, "native fixture credential helper must resolve its actor"
        fields = dict(line.split(b"=", 1) for line in resolved.stdout.splitlines() if b"=" in line)
        assert fields.get(b"username") == actor.encode(), "native fixture must use the intended principal"
        assert fields.get(b"password") == token.encode(), "native fixture must use the intended token"
    source = f.base / "authenticated-http-source"
    f.git(f.base, "init", "-b", "main", source)
    (source / "synthetic.txt").write_text("authenticated HTTP fixture\n")
    f.git(source, "add", "synthetic.txt")
    f.git(source, "commit", "-qm", "synthetic HTTP fixture")
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip()
    f.cli("create", "team/http-auth", "alice")
    f.cli("grant", "team/http-auth", "alice", "bob", "reader", "refs/")
    f.cli("create", "team/http-private", "carol")
    url = f"http://alice@127.0.0.1:{port}/team/http-auth.git"
    f.git(source, "-c", helpers["alice"], "push", url, "HEAD:refs/heads/main")
    clone = f.base / "authenticated-http-clone"
    f.git(source, "-c", helpers["bob"], "clone", url.replace("alice@", "bob@"), clone)
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
    f.git(source, "-c", helper, "push", f"http://alice@127.0.0.1:{port}/team/http-auth.git", "HEAD:refs/heads/main")
    tree = f.git(source, "rev-parse", "HEAD^{tree}").stdout.strip()
    identity = b"Fixture <fixture@example.invalid> 1577836800 +0000\n"
    unreachable = command(["git", "-C", source, "hash-object", "-w", "-t", "commit", "--stdin"], env=f.env,
                          data=b"tree " + tree + b"\nparent " + oid + b"\nauthor " + identity
                          + b"committer " + identity + b"\nvalid unreachable fixture\n").stdout.strip()
    pack = command(["git", "-C", source, "pack-objects", "--stdout"], env=f.env, data=unreachable + b"\n").stdout
    report = raw_push(port, oid, [b"refs/heads/blame-reachability"], pack=pack,
                      slug="team/http-auth", headers=authorization("alice"))
    assert b"ok refs/heads/blame-reachability" in report, report
    repo = "(SELECT id FROM git_rs.repositories WHERE slug='team/http-auth')"
    before = f.sql(f"SELECT generation,(SELECT count(*) FROM git_rs.operations WHERE repo_id={repo}),"
                   f"(SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}) FROM git_rs.repositories WHERE id={repo}")

    def request(commit, filename, actor="bob", slug="team/http-auth"):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        connection.request("GET", f"/{slug}.git/blame/{commit.decode()}?path={quote_from_bytes(filename, safe='')}",
                           headers=authorization(actor) if actor else {})
        response = connection.getresponse()
        result = response.status, response.read(), response.getheader("Cache-Control")
        connection.close()
        return result

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
        assert calls <= 3, (
            "blame type/size preflight must use library headers; at most reachability, literal path and annotation remain native",
            calls)
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
    assert oversized_calls <= 2, ("oversized blob header checks must not invoke cat-file or an annotation worker", oversized_calls)
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
    """Exercise executable selection, buffered failure, busy admission and group cleanup."""
    print("complete-blame worker admission, disconnect and absolute cancellation", flush=True)
    state_file = f.base / "blame-worker-state.json"
    worker_source = f'''#!{sys.executable}
import json,os,signal,subprocess,sys
from pathlib import Path
assert not any(key.startswith(("PG", "AWS", "GIT_RS_S3")) for key in os.environ)
assert 0 < int(os.environ["GIT_RS_BLAME_REMAINING_MILLIS"]) <= 30000
mode = MODE
if mode == "native":
    os.execv("/usr/bin/git", ["git", "--literal-pathspecs", "-C", sys.argv[1], "blame", "--root", "--line-porcelain", "--no-textconv", "--no-progress", sys.argv[2], "--", sys.argv[3]])
if mode == "failure":
    sys.stdout.buffer.write(b"incomplete annotation must never escape\\n")
    sys.stdout.buffer.flush()
    sys.exit(7)
if mode == "oversized-output":
    block = b"x" * (1024 * 1024)
    for _ in range(256):
        sys.stdout.buffer.write(block)
    sys.stdout.buffer.write(b"x")
    sys.stdout.buffer.flush()
    sys.exit(0)
child = subprocess.Popen([sys.executable, "-c", "import signal; signal.pause()"],
                         stdout=subprocess.DEVNULL if mode == "closed-output" else None)
def identity(pid):
    fields=Path("/proc/"+str(pid)+"/stat").read_text().rsplit(")",1)[1].split()
    return {{"pid":pid,"startTicks":int(fields[19])}}
sys.stdout.buffer.write(b"not a completed annotation\\n")
sys.stdout.buffer.flush()
if mode == "closed-output":
    os.close(sys.stdout.fileno())
ready=Path({str(state_file)!r})
pending=ready.with_suffix(".pending")
pending.write_text(json.dumps({{"leader":identity(os.getpgrp()),"parent":identity(os.getpid()),"child":identity(child.pid)}}))
pending.replace(ready)
signal.pause()
'''

    def start(name, mode, seconds):
        worker = f.base / ("selected-" + name)
        worker.write_text(worker_source.replace("mode = MODE", "mode = " + repr(mode)))
        worker.chmod(0o700)
        previous = dict(f.env)
        f.env.update(GIT_RS_BLAME_WORKER=str(worker), GIT_RS_OPERATION_SECONDS=str(seconds))
        try:
            return f.start(name)
        finally:
            f.env.clear()
            f.env.update(previous)

    target = f"/team/http-auth.git/blame/{oid.decode()}?path={quote_from_bytes(path, safe='')}"
    _, native_port = start("selected-blame-native", "native", 30)
    connection = http.client.HTTPConnection("127.0.0.1", native_port, timeout=5)
    connection.request("GET", target)
    response = connection.getresponse()
    body = response.read()
    connection.close()
    expected = f.git(source, "--literal-pathspecs", "blame", "--root", "--line-porcelain",
                     "--no-textconv", "--no-progress", oid.decode(), "--", os.fsdecode(path)).stdout
    assert response.status == 200 and body == expected, "selected workers must return the complete native annotation"
    _, failed_port = start("selected-blame-failure", "failure", 30)
    connection = http.client.HTTPConnection("127.0.0.1", failed_port, timeout=5)
    connection.request("GET", target)
    response = connection.getresponse()
    assert response.status == 500 and response.read() == b"", "failed workers must not return partial annotations"
    connection.close()
    oversized_daemon, oversized_port = start("selected-blame-oversized-output", "oversized-output", 30)
    connection = http.client.HTTPConnection("127.0.0.1", oversized_port, timeout=15)
    try:
        connection.request("GET", target)
        response = connection.getresponse()
        assert response.status == 500 and response.read() == b"", "oversized output must not become a partial success"
    finally:
        connection.close()
    status = Path(f"/proc/{oversized_daemon.pid}/status").read_text()
    oversized_hwm = int(re.search(r"VmHWM:\s+(\d+)", status).group(1)) * 1024
    assert oversized_hwm < 512 * 1024 * 1024, "bounded annotation output must fit its separate daemon memory allowance"
    f.results["oversized_blame_output"] = {
        "rejected_payload_bytes": 256 * 1024 * 1024 + 1,
        "observed_daemon_hwm_bytes": oversized_hwm,
        "test_daemon_memory_allowance_bytes": 512 * 1024 * 1024,
    }
    # This case deliberately fills a large annotation buffer. Qualify its HWM
    # separately, then release it before the ordinary streaming Git memory tests.
    oversized_daemon.terminate()
    oversized_daemon.wait(timeout=5)

    def owned(record):
        try:
            fields = Path(f"/proc/{record['pid']}/stat").read_text().rsplit(")", 1)[1].split()
            return int(fields[19]) == record["startTicks"]
        except FileNotFoundError:
            return False

    def require_reaped(records):
        deadline = time.monotonic() + 5
        while any(owned(record) for record in records.values()):
            assert time.monotonic() < deadline, "blame cancellation must reap the complete owned group promptly"
            threading.Event().wait(0.02)

    for label, seconds, disconnect, mode in [
        ("disconnect", 30, True, "slow"),
        ("deadline", 2, False, "slow"),
        ("closed-output-disconnect", 30, True, "closed-output"),
    ]:
        if state_file.exists():
            state_file.unlink()
        _, port = start("blame-" + label, mode, seconds)
        peer = socket.create_connection(("127.0.0.1", port), timeout=5)
        records = None
        try:
            peer.sendall(f"GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
            wait_for(state_file.exists, "owned blocking blame worker and descendant")
            records = json.loads(state_file.read_text())
            assert not select.select([peer], [], [], 0)[0], "annotations must be complete before success headers or body"
            assert f.health("blame-" + label, "/livez") == (200, b"live\n")
            if disconnect:
                busy = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                busy.request("GET", target)
                response = busy.getresponse()
                assert response.status == 503 and response.read() == b"", "only one blame worker may execute per daemon"
                busy.close()
                peer.close()
            else:
                peer.settimeout(5)
                try:
                    payload = peer.recv(4096)
                except ConnectionResetError:
                    payload = b""
                assert not payload.startswith(b"HTTP/1.1 200"), "expired queries cannot return successful annotations"
            require_reaped(records)
            repo = "(SELECT id FROM git_rs.repositories WHERE slug='team/http-auth')"
            wait_for(lambda: f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}") == "0",
                     "cancelled blame hold release")
            instance = f.base / "scratch"
            assert not list(instance.glob("instance-*/request-*")), "cancelled request scratch must be removed"
            followup = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            followup.request("GET", target.split("?", 1)[0] + "?path=definitely-missing.txt")
            response = followup.getresponse()
            assert response.status == 404 and response.read() == b"", "cancellation must return blame admission"
            followup.close()
        finally:
            peer.close()
            if records and owned(records["leader"]):
                os.killpg(records["leader"]["pid"], signal.SIGKILL)
    f.results["complete_blame_worker_lifecycle"] = {
        "server_owned_executable": "passed", "credentials_not_inherited": True,
        "failed_output_not_returned": True, "one_executing_query": True,
        "disconnect_and_deadline_reap_descendants": True, "read_holds_and_scratch_released": True,
        "closed_output_wait_is_cancellable": True, "cancelled_capacity_reusable": True,
        "oversized_output_not_returned": True,
    }


def blame_blocked_output_test(f, oid):
    """A backpressured complete response must expire and count as an error."""
    previous = f.env.get("GIT_RS_OPERATION_SECONDS")
    f.env["GIT_RS_OPERATION_SECONDS"] = "2"
    try:
        daemon, port = f.start("blame-blocked-output")
    finally:
        if previous is None:
            del f.env["GIT_RS_OPERATION_SECONDS"]
        else:
            f.env["GIT_RS_OPERATION_SECONDS"] = previous
    peer = socket.socket()
    peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    peer.settimeout(5)
    try:
        peer.connect(("127.0.0.1", port))
        target = f"/team/http-auth.git/blame/{oid.decode()}?path=blocked-output.txt"
        peer.sendall(f"GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
        headers = bytearray()
        while not headers.endswith(b"\r\n\r\n"):
            byte = peer.recv(1)
            assert byte and len(headers) < 4096, "native blame must reach complete response writes"
            headers.extend(byte)
        assert headers.startswith(b"HTTP/1.1 200 OK\r\n"), headers
        assert int(re.search(rb"Content-Length: (\d+)", headers).group(1)) > 32 * 1024 * 1024
        # Stop reading only after response writes are observable. The small receive
        # window then applies real TCP backpressure until the absolute deadline.
        wait_for(lambda: metric_value(f.health("blame-blocked-output", "/metrics")[1],
                                      "git_rs_active_requests") == 0, "blocked blame deadline")
        metrics = f.health("blame-blocked-output", "/metrics")[1]
        assert metric_value(metrics, 'git_rs_requests_total{transport="http",outcome="error"}') == 1, (
            "blocked blame writes must be counted as errors")
        assert metric_value(metrics, 'git_rs_requests_total{transport="http",outcome="success"}') == 0
        status = Path(f"/proc/{daemon.pid}/status").read_text()
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
    previous_requests = set((f.base / "scratch").glob("instance-*/request-*"))
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

        def configured_requests():
            return [path for path in (f.base / "scratch").glob("instance-*/request-*")
                    if path not in previous_requests and (path / "config").is_file()
                    and b"[pack]\n" in (path / "config").read_bytes()]

        wait_for(configured_requests, "native request configuration")
        requests = configured_requests()
        assert len(requests) == 1, requests
        assert f.git(requests[0], "config", "--int", "--get", "pack.threads").stdout == b"4\n", (
            "native receive-pack must use the configured worker budget")
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
    f.results["long_native_publication"] = {"budget_seconds": seconds, "elapsed_seconds": round(elapsed, 3),
                                             "refs": len(names), "native_clone_fsck": "passed",
                                             "native_pack_threads": 4}
    f.record_memory()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--provision", action="store_true", help="create and remove private local Docker PostgreSQL/S3 fixtures")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/git-rs")
    parser.add_argument("--large-mib", type=int, default=16)
    parser.add_argument("--skip-ssh", action="store_true")
    parser.add_argument("--debug-timings", action="store_true",
                        help="verify opt-in timing logs through native Git requests")
    parser.add_argument("--long-operations", action="store_true",
                        help="also run the 125-second native publication deadline regression")
    args = parser.parse_args()
    # OpenSSH StrictModes checks all parents of authorized_keys. A system /tmp
    # parent is world-writable, so keep this private fixture under the checkout.
    local = ROOT / ".local"
    local.mkdir(mode=0o700, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="grs-", dir=local) as directory:
        fixture = None
        try:
            fixture = Fixture(Path(directory), args.binary.resolve())
            if args.debug_timings:
                fixture.env["GIT_RS_DEBUG_TIMINGS"] = "1"
            fixture.setup(args.provision)
            exercise(fixture, args.large_mib, args.skip_ssh)
            authenticated_http_test(fixture)
            if args.long_operations:
                long_operation_test(fixture)
            print(json.dumps({"status": "passed", **fixture.results}, indent=2))
        except Exception:
            if fixture:
                for path in Path(directory).glob("*.log"):
                    print(path.name, path.read_text()[-8000:])
            raise
        finally:
            if fixture:
                fixture.close()


if __name__ == "__main__":
    main()
