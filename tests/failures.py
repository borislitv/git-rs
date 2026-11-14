#!/usr/bin/env python3
"""Dependency-failure regressions against real local PostgreSQL and S3 fixtures."""
import argparse
import concurrent.futures
from contextlib import contextmanager, nullcontext
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import select
import socket
import socketserver
import sys
import tempfile
import threading
import time
from urllib.parse import parse_qs, urlsplit

from integration import Fixture, ROOT, command, metric_value, wait_for


class OwnedGateway(socketserver.ThreadingMixIn):
    """Retain accepted sockets, upstream sockets and threads until finite cleanup."""
    daemon_threads = True
    block_on_close = False

    def __init__(self, address, handler):
        self.stopping = threading.Event()
        self.release = threading.Event()
        self.owners = threading.Lock()
        self.streams = set()
        self.workers = set()
        self.errors = []
        super().__init__(address, handler)
        self.thread = threading.Thread(target=self.serve_forever,
            kwargs={"poll_interval": .05},
            name=f"{type(self).__name__}-{self.server_address[1]}", daemon=True)
        try:
            self.thread.start()
        except BaseException:
            self.server_close()
            raise

    def process_request(self, request, client_address):
        with self.owners:
            stopped = self.stopping.is_set()
            if not stopped:
                request.settimeout(30)
                # Keep completed owners until another accept or close observes
                # their actual exit; a handler cannot join itself in its finally.
                self.workers = {worker for worker in self.workers if worker.is_alive()}
                worker = threading.Thread(target=self.process_request_thread,
                    args=(request, client_address), name=self.thread.name + "-request",
                    daemon=True)
                self.streams.add(request)
                self.workers.add(worker)
                try:
                    worker.start()
                except BaseException:
                    self.streams.discard(request)
                    self.workers.discard(worker)
                    raise
        if stopped:
            self.shutdown_request(request)

    def shutdown_request(self, request):
        try:
            super().shutdown_request(request)
        finally:
            with self.owners:
                self.streams.discard(request)

    @contextmanager
    def connect(self, address, *, timeout):
        # Connection establishment has no socket owner to interrupt until it
        # returns. Its three-second bound therefore fits inside close's five.
        with socket.create_connection(address, timeout=3) as stream:
            stream.settimeout(timeout)
            with self.owners:
                if self.stopping.is_set():
                    raise EOFError
                self.streams.add(stream)
            try:
                yield stream
            finally:
                with self.owners:
                    self.streams.discard(stream)

    def handle_error(self, request, client_address):
        error = sys.exception()
        if not self.stopping.is_set() and not isinstance(
                error, (EOFError, BrokenPipeError, ConnectionResetError)):
            with self.owners:
                if len(self.errors) < 16:
                    self.errors.append(repr(error)[:1024])

    def close(self):
        original, failures = sys.exception(), []
        self.stopping.set()
        self.release.set()
        try:
            if self.thread.is_alive():
                self.shutdown()
        except BaseException as error:
            failures.append(error)
        with self.owners:
            streams, workers = list(self.streams), list(self.workers)
        for stream in streams:
            try:
                stream.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # A peer may already have closed its half of the connection.
        try:
            self.server_close()
        except BaseException as error:
            failures.append(error)
        deadline = time.monotonic() + 5
        for thread in (self.thread, *workers):
            try:
                thread.join(timeout=max(0, deadline - time.monotonic()))
                if thread.is_alive():
                    failures.append(AssertionError("gateway thread did not stop: " + thread.name))
            except BaseException as error:
                failures.append(error)
        with self.owners:
            if self.errors:
                failures.append(AssertionError(f"gateway forwarding failed: {self.errors}"))
        if failures:
            if original is not None:
                original.add_note("Gateway cleanup also failed: " + "; ".join(
                    str(error)[:1024] for error in failures))
            elif len(failures) == 1:
                raise failures[0]
            else:
                raise BaseExceptionGroup("Gateway cleanup failed", failures)


def close_gateways(*gateways):
    original, failures = sys.exception(), []
    for gateway in gateways:
        if gateway is not None:
            try:
                gateway.close()
            except BaseException as error:
                failures.append(error)
    if failures:
        if original is not None:
            original.add_note("Gateway cleanup also failed: " + "; ".join(
                str(error)[:1024] for error in failures))
        elif len(failures) == 1:
            raise failures[0]
        else:
            raise BaseExceptionGroup("Gateway cleanup failed", failures)


class FaultGateway(OwnedGateway, http.server.HTTPServer):
    BODY_LIMIT = 16 * 1024 * 1024

    def __init__(self, upstream):
        self.upstream = urlsplit(upstream)
        if (self.upstream.scheme != "http" or not self.upstream.hostname
                or self.upstream.username is not None or self.upstream.password is not None
                or self.upstream.path not in ("", "/") or self.upstream.query or self.upstream.fragment):
            raise ValueError("fault gateway requires a plain HTTP origin without credentials or a path")
        self.mode = None
        self.pack_gets = 0
        self.lock = threading.Lock()
        self.reached = threading.Event()
        self.finished = threading.Event()
        super().__init__(("127.0.0.1", 0), FaultHandler)

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server_address[1]}"

    def arm(self, mode):
        self.reached.clear()
        self.release.clear()
        self.finished.clear()
        with self.lock:
            self.pack_gets = 0
            self.mode = mode

    def reset(self):
        with self.lock:
            self.mode = None
        self.release.set()


class FaultHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def reply(self, status, body, headers=None):
        self.send_response(status)
        # HEAD describes the upstream object's body without transmitting it.
        # Replacing its length with the empty forwarded body changes metadata.
        length = headers.get("Content-Length", str(len(body))) if headers and self.command == "HEAD" else str(len(body))
        self.send_header("Content-Length", length)
        self.send_header("Connection", "close")
        for key in ("Content-Range", "ETag", "Content-Type", "Content-Encoding"):
            if headers and key in headers:
                self.send_header(key, headers[key])
        self.end_headers()
        self.close_connection = True
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def forward(self):
        self.gated = False
        try:
            self.forward_request()
        finally:
            if self.gated:
                self.server.finished.set()

    def forward_request(self):
        if self.server.stopping.is_set():
            return
        length = int(self.headers.get("Content-Length", "0"))
        if self.headers.get("Transfer-Encoding") or not 0 <= length <= self.server.BODY_LIMIT:
            self.reply(413, b"fixture request body exceeds its bound\n")
            return
        body = self.rfile.read(length)
        if len(body) != length:
            raise EOFError
        with self.server.lock:
            mode = self.server.mode
        target = urlsplit(self.path)
        query = parse_qs(target.query, keep_blank_values=True)
        complete = self.command == "POST" and "uploadId" in query
        pack_get = self.command == "GET" and self.path.endswith(".pack")
        second_pack = False
        if pack_get:
            with self.server.lock:
                self.server.pack_gets += 1
                second_pack = mode == "pause-second-pack" and self.server.pack_gets == 2
        if mode == "version-enabled" and "?versioning" in self.path:
            self.reply(200, b'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>')
            return
        if second_pack or (mode == "pause-get" and pack_get) or (mode == "pause-put" and self.command == "PUT"):
            self.gated = True
            self.server.reached.set()
            if not self.server.release.wait(30):
                self.reply(504, b"test gate timed out\n")
                return
        if self.server.stopping.is_set():
            return
        if mode == "delete-fail" and self.command == "DELETE":
            self.reply(503, b"injected DELETE failure\n")
            return
        if mode == "multipart-complete-error" and complete:
            self.server.reached.set()
            self.reply(200, b"<Error><Code>InternalError</Code></Error>")
            return
        with self.server.connect((self.server.upstream.hostname,
                                  self.server.upstream.port or 80), timeout=30) as stream:
            connection = http.client.HTTPConnection(self.server.upstream.hostname,
                                                    self.server.upstream.port, timeout=30)
            connection.sock = stream
            try:
                # Keep the signed Host header; the gateway verifies the original
                # request signature rather than the address of this forwarding hop.
                connection.request(self.command, self.path, body=body, headers=dict(self.headers))
                response = connection.getresponse()
                data, status = response.read(self.server.BODY_LIMIT + 1), response.status
                if len(data) > self.server.BODY_LIMIT:
                    raise ValueError("fixture upstream body exceeds its bound")
                headers = response.headers  # HTTP field names are case-insensitive.
            finally:
                connection.close()
        if mode == "unknown-multipart-init" and self.command == "POST" and "uploads" in query and status < 300:
            self.server.reached.set()
            self.reply(503, b"injected lost successful multipart initiation response\n")
        elif mode == "unknown-idx-complete" and complete and target.path.endswith(".idx") and status < 300:
            self.server.reached.set()
            self.reply(503, b"injected lost successful multipart completion response\n")
        elif mode == "unknown-idx-put" and self.command == "PUT" and self.path.endswith(".idx") and status < 300:
            self.reply(503, b"injected lost successful PUT response\n")
        elif mode == "corrupt-get" and pack_get and status in (200, 206):
            self.reply(status, data[:-1] + bytes([data[-1] ^ 1]), headers)
        elif mode == "oversize-get" and pack_get and status in (200, 206):
            self.reply(status, data + b"unexpected excess bytes", headers)
        elif mode == "short-get" and pack_get and status in (200, 206):
            self.reply(status, data[:-1], headers)
        else:
            self.reply(status, data, headers)

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward


def receive_exact(stream, length):
    if not 0 <= length <= 16 * 1024 * 1024:
        raise ValueError("fixture PostgreSQL frame exceeds its bound")
    chunks = bytearray()
    while len(chunks) < length:
        data = stream.recv(length - len(chunks))
        if not data:
            raise EOFError
        chunks.extend(data)
    return bytes(chunks)


class CommitGateway(OwnedGateway, socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, port):
        self.upstream_port = port
        self.operation = b"SELECT publish("
        self.mode = None
        self.lock = threading.Lock()
        self.dropped = threading.Event()
        super().__init__(("127.0.0.1", 0), CommitRelay)

    def arm(self, mode, *, operation=b"SELECT publish("):
        self.dropped.clear()
        self.release.clear()
        self.operation = operation
        self.mode = mode

    def cut(self, phase):
        with self.lock:
            if self.mode == phase:
                self.mode = None
                self.dropped.set()
                return True
        return False

class CommitRelay(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            with self.server.connect(("127.0.0.1", self.server.upstream_port), timeout=10) as upstream:
                self.request.settimeout(10)
                size = receive_exact(self.request, 4)
                upstream.sendall(size + receive_exact(self.request, int.from_bytes(size, "big") - 4))
                publishing = False
                cut_before_commit = False
                while not self.server.stopping.is_set():
                    readable, _, _ = select.select([self.request, upstream], [], [], 15)
                    if not readable:
                        return
                    for source in readable:
                        header = receive_exact(source, 5)
                        payload = receive_exact(source, int.from_bytes(header[1:], "big") - 4)
                        if source is self.request:
                            if header[:1] == b"Q" and self.server.operation in payload:
                                publishing = True
                                # The native driver sends a transaction batch in
                                # one Query message. Execute publication, but
                                # withhold its final COMMIT until disconnect.
                                if self.server.mode == "before" and b"COMMIT;" in payload:
                                    payload = payload.rsplit(b"COMMIT;", 1)[0] + b"\0"
                                    header = b"Q" + (len(payload) + 4).to_bytes(4, "big")
                                    cut_before_commit = True
                            if publishing and header[:1] == b"Q" and payload.strip().startswith(b"COMMIT") and self.server.cut("before"):
                                return
                            upstream.sendall(header + payload)
                        else:
                            if publishing and header[:1] == b"Z":
                                if cut_before_commit and self.server.cut("before"):
                                    return
                                publishing = False
                            if publishing and header[:1] == b"C" and payload == b"COMMIT\0" and self.server.cut("after"):
                                return
                            if publishing and header[:1] == b"C" and payload == b"COMMIT\0" and self.server.cut("pause-after"):
                                if not self.server.release.wait(20):
                                    return
                                if self.server.stopping.is_set():
                                    return
                            self.request.sendall(header + payload)
        except (EOFError, OSError):
            return


def cache_lifecycle_test(f, gateway, normal_url):
    """Range-cache bytes are reused and evicted without filesystem repositories."""
    print("bounded RAM range-cache reuse and pressure", flush=True)
    slug = "team/cache-lifecycle"
    f.cli("create", slug, "alice")
    source = f.base / "cache-source"
    f.git(f.base, "init", "--initial-branch=main", source)
    normal = normal_url.replace("team/project.git", slug + ".git")
    for name, contents in [("initial.txt", "first synthetic object\n"),
                           ("next.txt", "second synthetic object\n")]:
        (source / name).write_text(contents)
        f.git(source, "add", name)
        f.git(source, "commit", "-m", "Add " + name)
        f.git(source, "push", normal, "main")
    assert f.sql("SELECT count(*) FROM git_rs.packs WHERE repo_id="
                 "(SELECT id FROM git_rs.repositories WHERE slug='team/cache-lifecycle')") == "2"
    expected = f.git(source, "ls-remote", normal).stdout
    f.env["GIT_RS_CACHE_BYTES"] = "8192"
    _, port = f.start("cache-pins")
    f.env["GIT_RS_CACHE_BYTES"] = "0"
    url = f"http://127.0.0.1:{port}/{slug}.git"
    gateway.arm("pause-second-pack")
    clone = f.base / "cache-clone"
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        reader = pool.submit(f.git, source, "clone", url, clone)
        try:
            assert gateway.reached.wait(10), "reader did not reach its second pack"
            assert int(f.sql("SELECT count(*) FROM git_rs.repository_leases WHERE repo_id="
                             "(SELECT id FROM git_rs.repositories WHERE slug='team/cache-lifecycle')")) >= 1
            assert 0 < metric_value(f.health("cache-pins", "/metrics")[1], "git_rs_cache_allocated_bytes") <= 8192
            f.assert_no_repository_bridge()
        finally:
            gateway.reset()
        assert reader.result(timeout=15).returncode == 0
    wait_for(lambda: metric_value(f.health("cache-pins", "/metrics")[1],
                                  "git_rs_active_requests") == 0, "completed cache reader")
    assert 0 < metric_value(f.health("cache-pins", "/metrics")[1], "git_rs_cache_allocated_bytes") <= 8192
    downloads = gateway.pack_gets
    hits = metric_value(f.health("cache-pins", "/metrics")[1],
                         'git_rs_cache_lookups_total{result="hit"}')
    f.git(clone, "fsck", "--strict", "--full")
    assert (clone / "initial.txt").read_text() == "first synthetic object\n"
    assert (clone / "next.txt").read_text() == "second synthetic object\n"
    repeated = f.base / "cache-repeated-clone"
    f.git(source, "clone", url, repeated)
    f.git(repeated, "fsck", "--strict", "--full")
    assert f.git(repeated, "rev-parse", "HEAD").stdout == f.git(clone, "rev-parse", "HEAD").stdout
    assert gateway.pack_gets == downloads, "tiny immutable source ranges must be reused"
    assert metric_value(f.health("cache-pins", "/metrics")[1],
                         'git_rs_cache_lookups_total{result="hit"}') > hits

    # Incompressible source bytes plus their index exceed the cache's remaining
    # capacity. They must evict unpinned entries or use admitted private ranges.
    f.cli("create", "team/cache-pressure", "alice")
    pressure = f.base / "cache-pressure-source"
    f.git(f.base, "init", "--initial-branch=main", pressure)
    import random
    payload = random.Random(23).randbytes(6000)
    (pressure / "pressure.bin").write_bytes(payload)
    f.git(pressure, "add", ".")
    f.git(pressure, "commit", "-m", "Synthetic cache pressure")
    pressure_normal = normal_url.replace("team/project.git", "team/cache-pressure.git")
    f.git(pressure, "push", pressure_normal, "main")
    pressure_url = url.replace("cache-lifecycle.git", "cache-pressure.git")
    pressure_clone = f.base / "cache-pressure-clone"
    f.git(pressure, "clone", pressure_url, pressure_clone)
    assert (pressure_clone / "pressure.bin").read_bytes() == payload
    f.git(pressure_clone, "fsck", "--strict", "--full")
    wait_for(lambda: metric_value(f.health("cache-pins", "/metrics")[1],
                                  "git_rs_active_requests") == 0, "completed pressure reader")
    assert metric_value(f.health("cache-pins", "/metrics")[1], "git_rs_cache_allocated_bytes") <= 8192
    downloads = gateway.pack_gets
    after_pressure = f.base / "cache-after-pressure"
    f.git(source, "clone", url, after_pressure)
    assert f.git(after_pressure, "rev-parse", "HEAD").stdout == f.git(clone, "rev-parse", "HEAD").stdout
    assert gateway.pack_gets > downloads, "evicted immutable ranges must be fetched again"
    f.assert_no_repository_bridge()

    # Independent advertisements must not leak another repository's default ref.
    branch = b"refs/heads/\xff"
    f.git(source, "push", normal, "HEAD:" + branch.decode(errors="surrogateescape"))
    f.sql("UPDATE git_rs.repositories SET default_ref=decode('" + branch.hex() + "','hex') "
          "WHERE slug='team/cache-lifecycle'")
    head = f.git(source, "ls-remote", "--symref", url, "HEAD").stdout
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip()
    assert head == b"ref: " + branch + b"\tHEAD\n" + oid + b"\tHEAD\n", head
    f.cli("create", "team/cache-empty", "alice")
    f.sql("UPDATE git_rs.repositories SET default_ref=decode('" + branch.hex() + "','hex') "
          "WHERE slug='team/cache-empty'")
    empty_clone = f.base / "unborn-cache-clone"
    f.git(source, "clone", url.replace("cache-lifecycle.git", "cache-empty.git"), empty_clone)
    assert (empty_clone / ".git/HEAD").read_bytes() == b"ref: " + branch + b"\n"
    original = f.git(source, "ls-remote", "--symref", normal_url, "HEAD").stdout
    assert original.startswith(b"ref: refs/heads/main\tHEAD\n")
    assert f.git(source, "ls-remote", "--symref", url.replace("cache-lifecycle.git", "project.git"), "HEAD").stdout == original
    f.results["shared_range_cache_reuse_eviction_and_byte_head"] = "passed"


def truncation_lease_test(f, source, url):
    print("truncation rechecks its exclusive lease after catalog attachment", flush=True)
    before = f.git(source, "ls-remote", url).stdout
    head = f.git(source, "ls-remote", url, "refs/heads/main").stdout.split()[0]
    catalog_query = ("SELECT generation,storage_epoch,"
        "(SELECT string_agg(pack_key||':'||idx_key,',' ORDER BY native_hash) FROM git_rs.packs WHERE repo_id=r.id),"
        "(SELECT count(*) FROM git_rs.operations WHERE repo_id=r.id),"
        "(SELECT count(*) FROM git_rs.retired_objects WHERE repo_id=r.id),"
        "(SELECT count(*) FROM git_rs.maintenance_jobs WHERE repo_id=r.id) "
        "FROM git_rs.repositories r WHERE slug='team/project'")
    catalog_before = f.sql(catalog_query)
    backup = f.server / "rejected-truncation.bundle"
    f.sql("CREATE FUNCTION git_rs.expire_truncation_lease() RETURNS trigger LANGUAGE plpgsql AS $$ "
          "BEGIN UPDATE git_rs.repository_leases SET expires_at=clock_timestamp()-interval '1 second' "
          "WHERE repo_id=NEW.repo_id AND exclusive; RETURN NEW; END $$; "
          "CREATE TRIGGER expire_truncation_lease AFTER INSERT ON git_rs.packs "
          "FOR EACH ROW EXECUTE FUNCTION git_rs.expire_truncation_lease();")
    try:
        rejected = f.cli("truncate", "team/project", "alice", "--keep", "1", "--backup", backup, ok=False)
        assert rejected.returncode and b"truncation rejected or outcome unknown" in rejected.stderr, rejected.stderr
        assert f.sql(catalog_query) == catalog_before, "expired ownership must roll back publication and retirement"
        assert f.git(source, "ls-remote", url).stdout == before
        restored = f.base / "rejected-truncation-restore"
        f.git(source, "clone", "--mirror", backup, restored)
        f.git(restored, "fsck", "--strict", "--full")
        assert f.git(restored, "rev-parse", "HEAD").stdout.strip() == head
    finally:
        f.sql("DROP TRIGGER expire_truncation_lease ON git_rs.packs; DROP FUNCTION git_rs.expire_truncation_lease();")
    f.results["truncation_late_lease_expiry_preserves_publication_and_backup"] = "passed"


def backup_snapshot_test(f, gateway, source, normal_url):
    print("backup retains its held snapshot across an ordinary concurrent push", flush=True)
    before = f.git(source, "ls-remote", normal_url).stdout
    before_refs = f.git(source, "ls-remote", "--refs", normal_url).stdout
    tip = f.git(source, "ls-remote", normal_url, "refs/heads/main").stdout.split()[0].decode()
    epoch = f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'")
    destination = f.server / "concurrent-snapshot.bundle"
    gateway.arm("pause-get")
    published = False
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            backup = pool.submit(f.cli, "backup", "team/project", "alice", "--output", destination, ok=False)
            try:
                assert gateway.reached.wait(10), "backup did not reach its held source GET"
                # The original daemon has a direct S3 endpoint and a different
                # process-local CPU pool; this is a real concurrent Git publish.
                f.git(source, "push", normal_url, tip + ":refs/heads/concurrent-backup")
                published = True
                assert f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'") == epoch
            finally:
                gateway.reset()
            result = backup.result(timeout=15)
            assert result.returncode == 0, result.stderr
        restored = f.base / "concurrent-snapshot-restore"
        f.git(source, "clone", "--mirror", destination, restored)
        f.git(restored, "fsck", "--strict", "--full")
        assert f.git(restored, "for-each-ref", "--format=%(objectname)\t%(refname)").stdout == before_refs
        assert f.git(restored, "show-ref", "--verify", "refs/heads/concurrent-backup", ok=False).returncode
        assert f.git(restored, "rev-parse", "HEAD").stdout.strip().decode() == tip
        assert not list(f.server.glob(".bundle-*"))
    finally:
        if published:
            f.git(source, "push", normal_url, ":refs/heads/concurrent-backup")
    assert f.git(source, "ls-remote", normal_url).stdout == before
    f.results["backup_survives_concurrent_ref_publication"] = "passed"


def supported_ref_catalog_test(f, source, normal_url):
    print("supported ref catalogs remain readable and correctable after crossing shared limits", flush=True)
    slug = "team/ref-catalog"
    f.cli("create", slug, "alice")
    url = normal_url.replace("team/project.git", slug + ".git")
    f.git(source, "push", url, "HEAD:refs/heads/main")
    f.git(source, "push", url, *[f"HEAD:refs/heads/b{number:05}" for number in range(4095)])
    f.git(source, "push", url, "HEAD:refs/heads/b04095")
    assert len(f.git(source, "ls-remote", url).stdout.splitlines()) == 4098
    f.git(source, "push", url, *[f"HEAD:refs/heads/b{number:05}" for number in range(4096, 9999)])
    advertised = f.git(source, "ls-remote", url).stdout
    assert len(advertised.splitlines()) == 10_001
    bundle = f.server / "large-ref-catalog.bundle"
    f.cli("backup", slug, "alice", "--output", bundle)
    restored = f.base / "large-ref-catalog-restore"
    f.git(source, "clone", "--mirror", bundle, restored)
    f.git(restored, "fsck", "--strict", "--full")
    assert len(f.git(restored, "for-each-ref").stdout.splitlines()) == 10_000
    f.git(source, "push", url, ":refs/heads/b04095")
    assert len(f.git(source, "ls-remote", url).stdout.splitlines()) == 10_000
    clone = f.base / "large-ref-catalog-clone"
    f.git(source, "clone", "--mirror", url, clone)
    f.git(clone, "fsck", "--strict", "--full")
    assert len(f.git(clone, "for-each-ref").stdout.splitlines()) == 9999
    f.results["supported_ref_catalog_crossing_fetch_backup_and_corrective_delete"] = "passed"


def supported_long_ref_catalog_test(f, source, normal_url):
    print("full-size ref names remain supported by source, bundle and truncate projections", flush=True)
    slug = "team/long-ref-catalog"
    f.cli("create", slug, "alice")
    url = normal_url.replace("team/project.git", slug + ".git")
    f.git(source, "push", url, "HEAD:refs/heads/main")
    prefix = "refs/heads/" + "/".join(["n" * 200] * 5) + "/"
    names = [prefix + f"b{number:07}" for number in range(9999)]
    assert all(len(name.encode()) == 1024 for name in names)
    # Keep argv below the operating system bound while exercising the supported
    # aggregate catalog, rather than inventing one oversized native invocation.
    for offset in range(0, len(names), 1000):
        f.git(source, "push", url, *["HEAD:" + name for name in names[offset:offset + 1000]])
    before = f.git(source, "ls-remote", "--refs", url).stdout
    assert len(before.splitlines()) == 10_000
    backup = f.server / "long-ref-catalog.bundle"
    f.cli("truncate", slug, "alice", "--keep", "1", "--backup", backup)
    restored = f.base / "long-ref-catalog-restore"
    f.git(source, "clone", "--mirror", backup, restored)
    f.git(restored, "fsck", "--strict", "--full")
    assert f.git(restored, "for-each-ref", "--format=%(objectname)\t%(refname)").stdout == before
    after = f.git(source, "ls-remote", "--refs", url).stdout
    assert [line.split(b"\t", 1)[1] for line in after.splitlines()] == [line.split(b"\t", 1)[1] for line in before.splitlines()]
    clone = f.base / "long-ref-catalog-clone"
    f.git(source, "clone", "--mirror", url, clone)
    f.git(clone, "fsck", "--strict", "--full")
    assert f.git(clone, "rev-list", "--count", "--all").stdout.strip() == b"1"
    f.results["full_size_ref_catalog_truncate_projection_and_verified_backup"] = "passed"


def backup_authority_test(f, gateway):
    print("backup rechecks source permission before publishing its private file", flush=True)
    f.cli("grant", "team/project", "alice", "backup-reader", "reader", "refs/")
    destination = f.server / "revoked-reader.bundle"
    gateway.arm("pause-get")
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        backup = pool.submit(f.cli, "backup", "team/project", "backup-reader",
                             "--output", destination, ok=False)
        try:
            assert gateway.reached.wait(10), "backup did not reach its physical source GET"
            f.sql("DELETE FROM git_rs.permissions WHERE principal='backup-reader'")
        finally:
            gateway.reset()
        result = backup.result(timeout=15)
        assert result.returncode, "revoked backup reader published a successful bundle"
        assert b"source authority changed" in result.stderr, result.stderr
    assert not destination.exists(), "authorization must be rechecked before linking the private backup"
    assert not list(f.server.glob(".bundle-*")), "failed backup retained its private temporary file"
    f.results["backup_revocation_before_file_publication"] = "passed"


def standalone_orphan_race_test(f, gateway, source, normal_url, faulty_url):
    print("standalone orphan inspection fences an expired staged publisher without compaction", flush=True)
    repo = f.sql("SELECT id FROM git_rs.repositories WHERE slug='team/project'")
    catalog = f.sql(f"SELECT pack_key,idx_key FROM git_rs.packs WHERE repo_id='{repo}' ORDER BY native_hash")
    before = f.git(source, "ls-remote", normal_url).stdout
    epoch = int(f.sql(f"SELECT storage_epoch FROM git_rs.repositories WHERE id='{repo}'"))
    gateway.arm("pause-put")
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        writer = pool.submit(f.git, source, "push", faulty_url, "HEAD:refs/heads/orphan-race", ok=False)
        try:
            assert gateway.reached.wait(10), "publisher did not reach its staged PUT"
            staged = f.sql(f"SELECT object_key FROM git_rs.staged_objects WHERE repo_id='{repo}' AND retired_at IS NULL ORDER BY object_key").splitlines()
            assert len(staged) == 2
            assert int(f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id='{repo}' AND NOT exclusive")) == 1
            # Model expiry while the network request is still in flight. The
            # explicit inspector must fence this publisher independently of age.
            f.sql(f"UPDATE git_rs.repository_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE repo_id='{repo}' AND NOT exclusive")
            f.cleanup("team/project")
            assert not gateway.finished.is_set(), "the provider PUT escaped its actual gate before retirement"
            assert int(f.sql(f"SELECT storage_epoch FROM git_rs.repositories WHERE id='{repo}'")) == epoch + 1
            assert f.sql(f"SELECT count(*) FROM git_rs.maintenance_operations o JOIN git_rs.maintenance_jobs j ON j.id=o.job_id WHERE j.repo_id='{repo}' AND j.kind='compact'") == "0"
            assert f.sql(f"SELECT pack_key,idx_key FROM git_rs.packs WHERE repo_id='{repo}' ORDER BY native_hash") == catalog
            assert all(f.s3("HEAD", key, ok=False).stdout == b"404" for key in staged)
        finally:
            gateway.reset()
        assert writer.result(timeout=15).returncode, "an expired staged publisher must not attach retired keys"
    assert gateway.finished.wait(5), "late PUT did not finish at the provider"
    assert any(f.s3("HEAD", key, ok=False).stdout == b"200" for key in staged), "late PUT must really exist after the first DELETE"
    assert f.git(source, "ls-remote", normal_url).stdout == before
    f.sql(f"UPDATE git_rs.retired_objects SET delete_after=clock_timestamp() WHERE repo_id='{repo}'")
    f.cleanup("team/project")
    assert all(f.s3("HEAD", key, ok=False).stdout == b"404" for key in staged)
    assert all(f.s3("HEAD", key).stdout == b"200" for row in catalog.splitlines() for key in row.split("|"))
    f.results["standalone_orphan_inspection_fences_late_put"] = {"retired_planned_keys": len(staged),
        "advanced_epoch": True, "compaction_required": False, "late_put_redeleted": True}


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
        f.cleanup("team/project")
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
        f.sql("UPDATE git_rs.retired_objects SET delete_after=clock_timestamp()")
        f.cleanup("team/project")
        assert f.s3("HEAD", uncertain[0], ok=False).stdout == b"404"
        standalone_orphan_race_test(f, gateway, source, url, faulty_url)

        print("integrity, size limits and recovery from failed downloads", flush=True)
        for cache_bytes in ["0", "8192"]:
            for mode in ["corrupt-get", "oversize-get", "short-get"]:
                f.env["GIT_RS_CACHE_BYTES"] = cache_bytes
                _, download_port = f.start(f"download-{cache_bytes}-{mode}")
                f.env["GIT_RS_CACHE_BYTES"] = "0"
                download_url = f"http://127.0.0.1:{download_port}/team/project.git"
                gateway.arm(mode)
                failed = f.base / f"invalid-download-{cache_bytes}-{mode}"
                assert f.git(source, "clone", download_url, failed, ok=False).returncode, (cache_bytes, mode)
                gateway.reset()
                recovered = f.base / f"recovered-download-{cache_bytes}-{mode}"
                f.git(source, "clone", download_url, recovered)
                f.git(recovered, "fsck", "--strict", "--full")
                assert f.git(recovered, "rev-parse", "HEAD").stdout.strip() + b"\trefs/heads/main\n" in before
                # Each variant has completed both its failure and recovery
                # assertions. Do not retain six idle CPU/SQL pools for later cases.
                f.stop(f"download-{cache_bytes}-{mode}")
        f.results["invalid_downloads_rejected"] = {
            "paths": ["admitted private ranges", "RAM cache fills"],
            "faults": ["digest mismatch", "excess content length", "short body"],
        }
        backup_snapshot_test(f, gateway, source, url)
        backup_authority_test(f, gateway)
        supported_ref_catalog_test(f, source, url)
        supported_long_ref_catalog_test(f, source, url)
        cache_lifecycle_test(f, gateway, url)
        f.stop("cache-pins")

        print("durable reader fencing and failed physical DELETE", flush=True)
        epoch = f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'")
        gateway.arm("pause-get")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(f.git, source, "ls-remote", faulty_url)
            assert gateway.reached.wait(10), "reader did not reach the S3 gate"
            try:
                assert int(f.sql("SELECT count(*) FROM git_rs.repository_leases WHERE NOT exclusive")) >= 1
                # No database connection needs to remain alive for this hold.
                _, code = f.cleanup("team/project", expected="retry")
                assert code == "worker_failed", code
                assert f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'") == epoch
            finally:
                gateway.reset()
            assert reader.result().stdout == before
        wait_for(lambda: f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0", "reader lease release")
        gateway.arm("delete-fail")
        f.sql("UPDATE git_rs.retired_objects SET delete_after=clock_timestamp()")
        _, code = f.cleanup("team/project", expected="retry")
        assert code == "worker_failed", code
        assert int(f.sql("SELECT count(*) FROM git_rs.retired_objects")) >= 1
        gateway.reset()
        f.cleanup("team/project")
        assert int(f.sql("SELECT count(*) FROM git_rs.retired_objects")) >= 2, "retired identities remain permanent"
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
        f.cleanup("team/project")
        assert f.git(source, "ls-remote", url).stdout == before
        f.results["deadline_put_no_publication"] = "passed"

        print("fail-closed bucket policy and metadata outage", flush=True)
        gateway.arm("version-enabled")
        epoch = f.sql("SELECT storage_epoch FROM git_rs.repositories WHERE slug='team/project'")
        _, code = f.cleanup("team/project", expected="dead")
        assert code == "bucket_versioning_unsupported", code
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
            f.cleanup("team/project")
        finally:
            commits.close()
        f.results["unknown_commit_confirmation_and_rollback"] = "passed"
        truncation_lease_test(f, source, url)
        f.record_memory()
    finally:
        gateway.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provision", action="store_true")
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/git-rs")
    parser.add_argument("--runtime-image", default=os.environ.get("GIT_RS_TEST_RUNTIME_IMAGE"), required=False)
    parser.add_argument("--revision", default=os.environ.get("GIT_RS_TEST_REVISION"))
    parser.add_argument("--cluster-container", required=True, help="existing disposable Kubernetes 1.37 cluster")
    parser.add_argument("--fixture-dir", type=Path, default=os.environ.get("GIT_RS_TEST_FIXTURE_DIR"),
                        help="fresh private directory to retain fixture logs/results after cleanup")
    args = parser.parse_args()
    assert args.runtime_image and args.revision, "failure contracts require an exact prebuilt Git-free runtime"
    local = ROOT / ".local"
    local.mkdir(mode=0o700, exist_ok=True)
    if args.fixture_dir:
        assert args.fixture_dir.parent.is_dir(), "fixture directory parent must already exist"
        args.fixture_dir.mkdir(mode=0o700)
        context = nullcontext(str(args.fixture_dir))
    else:
        context = tempfile.TemporaryDirectory(prefix="failures-", dir=local)
    with context as directory:
        fixture = Fixture(Path(directory), args.binary.resolve(), runtime_image=args.runtime_image, revision=args.revision)
        try:
            fixture.configure_maintenance(args.cluster_container)
            fixture.setup(args.provision)
            exercise(fixture)
            fixture.results["status"] = "passed"
            print(json.dumps({"status": "passed", **fixture.results}, indent=2))
        except Exception:
            for path in Path(directory).glob("*.log"):
                print(path.name, path.read_text()[-8000:])
            raise
        finally:
            try:
                fixture.close()
            finally:
                (Path(directory) / "report.json").write_text(json.dumps(fixture.results, indent=2) + "\n")


if __name__ == "__main__":
    main()
