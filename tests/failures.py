#!/usr/bin/env python3
"""Dependency-failure regressions against real local PostgreSQL and S3 fixtures."""
import argparse
import concurrent.futures
import http.client
import http.server
import json
from pathlib import Path
import select
import socket
import socketserver
import tempfile
import threading
import time
from urllib.parse import urlsplit

from integration import Fixture, ROOT, command, wait_for


class FaultGateway(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, upstream):
        self.upstream = urlsplit(upstream)
        self.mode = None
        self.reached = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        super().__init__(("127.0.0.1", 0), FaultHandler)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server_address[1]}"

    def arm(self, mode):
        self.reached.clear()
        self.release.clear()
        self.finished.clear()
        self.mode = mode

    def reset(self):
        self.mode = None
        self.release.set()

    def close(self):
        self.reset()
        self.shutdown()
        self.server_close()


class FaultHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def forward(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        mode = self.server.mode
        pack_get = self.command == "GET" and self.path.endswith(".pack")
        if mode == "version-enabled" and "?versioning" in self.path:
            self.reply(200, b'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>')
            return
        if (mode == "pause-get" and pack_get) or (mode == "pause-put" and self.command == "PUT"):
            self.server.reached.set()
            if not self.server.release.wait(30):
                self.reply(504, b"test gate timed out\n")
                return
        if mode == "delete-fail" and self.command == "DELETE":
            self.reply(503, b"injected DELETE failure\n")
            return
        connection = http.client.HTTPConnection(self.server.upstream.hostname,
                                                self.server.upstream.port, timeout=30)
        try:
            # Keep the signed Host header; the gateway verifies the original
            # request signature rather than the address of this forwarding hop.
            connection.request(self.command, self.path, body=body, headers=dict(self.headers))
            response = connection.getresponse()
            data, status = response.read(), response.status
        finally:
            connection.close()
        if mode == "unknown-idx-put" and self.command == "PUT" and self.path.endswith(".idx") and status < 300:
            self.reply(503, b"injected lost successful PUT response\n")
        elif mode == "corrupt-get" and pack_get and status == 200:
            self.reply(status, data[:-1] + bytes([data[-1] ^ 1]))
        elif mode == "oversize-get" and pack_get and status == 200:
            self.reply(status, data + b"unexpected excess bytes")
        else:
            self.reply(status, data)
        self.server.finished.set()

    do_GET = do_HEAD = do_PUT = do_DELETE = forward


def receive_exact(stream, length):
    chunks = bytearray()
    while len(chunks) < length:
        data = stream.recv(length - len(chunks))
        if not data:
            raise EOFError
        chunks.extend(data)
    return bytes(chunks)


class CommitGateway(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, port):
        self.upstream_port = port
        self.mode = None
        self.lock = threading.Lock()
        self.dropped = threading.Event()
        super().__init__(("127.0.0.1", 0), CommitRelay)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def arm(self, mode):
        self.dropped.clear()
        self.mode = mode

    def cut(self, phase):
        with self.lock:
            if self.mode == phase:
                self.mode = None
                self.dropped.set()
                return True
        return False

    def close(self):
        self.shutdown()
        self.server_close()


class CommitRelay(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            with socket.create_connection(("127.0.0.1", self.server.upstream_port), 10) as upstream:
                self.request.settimeout(10)
                size = receive_exact(self.request, 4)
                upstream.sendall(size + receive_exact(self.request, int.from_bytes(size, "big") - 4))
                publishing = False
                while True:
                    readable, _, _ = select.select([self.request, upstream], [], [], 15)
                    if not readable:
                        return
                    for source in readable:
                        header = receive_exact(source, 5)
                        payload = receive_exact(source, int.from_bytes(header[1:], "big") - 4)
                        if source is self.request:
                            if header[:1] == b"Q" and b"SELECT publish(" in payload:
                                publishing = True
                            if publishing and header[:1] == b"Q" and payload.strip().startswith(b"COMMIT") and self.server.cut("before"):
                                return
                            upstream.sendall(header + payload)
                        else:
                            if publishing and header[:1] == b"C" and payload == b"COMMIT\0" and self.server.cut("after"):
                                return
                            self.request.sendall(header + payload)
        except (EOFError, OSError):
            return


def exercise(f):
    f.cli("migrate")
    print("repeatable, concurrent schema initialization", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: f.cli("migrate"), range(2)))
    f.cli("create", "team/project", "alice")
    _, port = f.start("normal")
    url = f"http://127.0.0.1:{port}/team/project.git"
    source = f.base / "source"
    command(["git", "init", "--initial-branch=main", source], env=f.env)
    (source / "data.txt").write_text("initial synthetic contents\n")
    f.git(source, "add", ".")
    f.git(source, "commit", "-m", "Initial failure-test fixture")
    f.git(source, "push", url, "main")
    before = f.git(source, "ls-remote", url).stdout
    gateway = FaultGateway(f.env["GIT_RS_S3_ENDPOINT"])
    try:
        f.env["GIT_RS_S3_ENDPOINT"] = gateway.endpoint
        f.env["GIT_RS_CACHE_BYTES"] = "0"
        _, faulty_port = f.start("faults")
        faulty_url = f"http://127.0.0.1:{faulty_port}/team/project.git"
        print("unknown successful PUT outcome and orphan cleanup", flush=True)
        (source / "data.txt").write_text("new unpublished synthetic contents\n")
        f.git(source, "commit", "-am", "Unpublished dependency failure")
        gateway.arm("unknown-idx-put")
        result = f.git(source, "push", faulty_url, "main", ok=False)
        assert result.returncode, result.stdout
        gateway.reset()
        assert f.git(source, "ls-remote", url).stdout == before
        staged = f.sql("SELECT object_key FROM git_rs.staged_objects ORDER BY object_key").splitlines()
        assert len(staged) == 2, staged
        assert all(f.s3("HEAD", key).stdout == b"200" for key in staged)
        f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"))
        for key in staged:
            missing = f.s3("HEAD", key, ok=False)
            assert missing.returncode == 22 and missing.stdout == b"404", (key, missing.stdout)
        f.results["unknown_put_orphans_reclaimed"] = len(staged)
        uncertain = f.sql("SELECT object_key FROM git_rs.staged_objects WHERE state='planned'").splitlines()
        assert len(uncertain) == 1, uncertain
        # A provider may finish an abandoned PUT even after the client timed out.
        # Durable tombstones must permit another explicit cleanup of that key.
        f.s3("PUT", uncertain[0], data=source / "data.txt")
        assert f.s3("HEAD", uncertain[0]).stdout == b"200"
        f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"))
        assert f.s3("HEAD", uncertain[0], ok=False).stdout == b"404"

        print("integrity, size limits and recovery from failed downloads", flush=True)
        for mode in ["corrupt-get", "oversize-get"]:
            gateway.arm(mode)
            assert f.git(source, "ls-remote", faulty_url, ok=False).returncode, mode
            gateway.reset()
            assert f.git(source, "ls-remote", faulty_url).stdout == before
        f.results["invalid_downloads_rejected"] = ["digest mismatch", "excess content length"]

        print("durable reader fencing and failed physical DELETE", flush=True)
        epoch = f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'")
        gateway.arm("pause-get")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(f.git, source, "ls-remote", faulty_url)
            assert gateway.reached.wait(10), "reader did not reach the S3 gate"
            try:
                assert int(f.sql("SELECT count(*) FROM git_rs.repository_leases WHERE NOT exclusive")) >= 1
                # No database connection needs to remain alive for this hold.
                refused = f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"), ok=False)
                assert refused.returncode and b"repository busy" in refused.stderr, refused.stderr
                assert f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'") == epoch
            finally:
                gateway.reset()
            assert reader.result().stdout == before
        wait_for(lambda: f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0", "reader lease release")
        gateway.arm("delete-fail")
        refused = f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"), ok=False)
        assert refused.returncode
        assert int(f.sql("SELECT count(*) FROM git_rs.retired_objects")) >= 1
        gateway.reset()
        f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"))
        assert f.sql("SELECT count(*) FROM git_rs.retired_objects") == "0"
        assert f.git(source, "ls-remote", url).stdout == before
        f.results["reader_fencing_and_delete_retry"] = "passed"

        print("deadline cancellation during a pending PUT", flush=True)
        f.env["GIT_RS_OPERATION_SECONDS"] = "4"
        _, deadline_port = f.start("deadline")
        del f.env["GIT_RS_OPERATION_SECONDS"]
        deadline_url = f"http://127.0.0.1:{deadline_port}/team/project.git"
        gateway.arm("pause-put")
        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            writer = pool.submit(f.git, source, "push", deadline_url, "HEAD:refs/heads/deadline-attempt", ok=False)
            assert gateway.reached.wait(10), "writer did not reach the S3 gate"
            try:
                assert writer.result(timeout=12).returncode
                assert time.monotonic() - start < 12
                assert f.git(source, "ls-remote", url).stdout == before
            finally:
                gateway.reset()
        assert gateway.finished.wait(5), "pending PUT did not finish at the provider"
        wait_for(lambda: f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0", "cancelled request lease release")
        f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"))
        assert f.git(source, "ls-remote", url).stdout == before
        f.results["deadline_put_no_publication"] = "passed"

        print("fail-closed bucket policy and metadata outage", flush=True)
        gateway.arm("version-enabled")
        epoch = f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'")
        assert f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"), ok=False).returncode
        gateway.reset()
        assert f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'") == epoch
        saved_port = f.env["PGPORT"]
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            f.env["PGPORT"] = str(reservation.getsockname()[1])
            _, unavailable_port = f.start("unavailable")
            assert f.health("unavailable", "/livez") == (200, b"live\n")
            assert f.health("unavailable", "/readyz")[0] == 503
            unavailable_url = f"http://127.0.0.1:{unavailable_port}/team/project.git"
            assert f.git(source, "ls-remote", unavailable_url, ok=False).returncode
        f.env["PGPORT"] = saved_port
        f.results["dependency_fail_closed"] = "passed"

        print("PostgreSQL transport loss immediately before and after COMMIT", flush=True)
        commits = CommitGateway(int(saved_port))
        try:
            f.env["PGPORT"] = str(commits.server_address[1])
            f.env["PGGSSENCMODE"] = "disable"
            _, commit_port = f.start("commit-faults")
            f.env["PGPORT"] = saved_port
            commit_url = f"http://127.0.0.1:{commit_port}/team/project.git"
            count = int(f.sql("SELECT count(*) FROM git_rs.operations"))
            commits.arm("after")
            f.git(source, "push", commit_url, "main")
            assert commits.dropped.is_set(), "COMMIT response was not intercepted"
            assert int(f.sql("SELECT count(*) FROM git_rs.operations")) == count + 1
            assert int(f.sql("SELECT confirmations FROM git_rs.operations ORDER BY committed_at DESC LIMIT 1")) >= 1
            before = f.git(source, "ls-remote", url).stdout
            (source / "data.txt").write_text("transaction rolled back on transport loss\n")
            f.git(source, "commit", "-am", "Synthetic pre-COMMIT disconnect")
            commits.arm("before")
            assert f.git(source, "push", commit_url, "main", ok=False).returncode
            assert commits.dropped.is_set(), "pre-COMMIT request was not intercepted"
            assert int(f.sql("SELECT count(*) FROM git_rs.operations")) == count + 1
            assert f.git(source, "ls-remote", url).stdout == before
            f.cli("cleanup", "team/project", "alice", "--scratch", str(f.base / "scratch"))
        finally:
            commits.close()
        f.results["unknown_commit_confirmation_and_rollback"] = "passed"
        f.record_memory()
    finally:
        gateway.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provision", action="store_true")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/git-rs")
    args = parser.parse_args()
    local = ROOT / ".local"
    local.mkdir(mode=0o700, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="failures-", dir=local) as directory:
        fixture = Fixture(Path(directory), args.binary.resolve())
        try:
            fixture.setup(args.provision)
            exercise(fixture)
            print(json.dumps({"status": "passed", **fixture.results}, indent=2))
        except Exception:
            for path in Path(directory).glob("*.log"):
                print(path.name, path.read_text()[-8000:])
            raise
        finally:
            fixture.close()


if __name__ == "__main__":
    main()
