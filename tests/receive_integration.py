"""Real explicit Rust receive fixture. Native Git is an external client only."""
import gzip
import concurrent.futures
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import signal
import socket
import socketserver
import struct
import subprocess
import time
import uuid
import zlib

from integration import Fixture, command, metric_value, pkt, raw_push, wait_for


def packets(data):
    offset = 0
    while offset < len(data):
        size = int(data[offset:offset + 4], 16)
        assert size == 0 or 4 <= size <= 65520
        end = offset + (size or 4)
        assert end <= len(data)
        yield None if not size else data[offset + 4:end]
        offset = end


def pack_objects(objects):
    result = b"PACK" + struct.pack(">II", 2, len(objects))
    for kind, data in objects:
        size = len(data)
        first = (kind << 4) | (size & 15)
        size >>= 4
        header = bytearray([first | (128 if size else 0)])
        while size:
            byte = size & 127
            size >>= 7
            header.append(byte | (128 if size else 0))
        result += bytes(header) + zlib.compress(data)
    return result + hashlib.sha1(result).digest()


def post(port, body, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
    try:
        conn.request("POST", "/team/project.git/git-receive-pack", body,
                     {"Content-Type": "application/x-git-receive-pack-request", **(headers or {})})
        response = conn.getresponse()
        return response.status, response.read()
    except (http.client.RemoteDisconnected, ConnectionResetError):
        return 0, b""
    finally:
        conn.close()


def exercise(f, helper, gateway, commit_gateway, cache_outage):
    f.cli("migrate")
    f.cli("create", "team/project", "alice")
    log_path = f.base / "receive.log"
    log = log_path.open("xb")
    f.logs.append(log)
    env = {**f.env, "GIT_RS_RECEIVE_FIXTURE": str(f.server)}
    program = "/usr/local/bin/tests/receive-fixture" if f.runtime else helper
    child = f.spawn(["--ignored", "--exact", "receive_fixture_process", "--nocapture"],
                    env=env, log=log, fixture_program=program)
    def ready():
        assert child.poll() is None, log_path.read_text()
        return re.search(r"development HTTP 127.0.0.1:(\d+)", log_path.read_text())
    wait_for(ready, "explicit receive fixture")
    assert "optional-cache-outage confirmed" in log_path.read_text()
    initial_cache_connections = cache_outage.connections
    assert initial_cache_connections > 0
    port = int(ready().group(1))
    health_port = int(re.search(r"health 127.0.0.1:(\d+)", log_path.read_text()).group(1))
    def metrics():
        connection = http.client.HTTPConnection("127.0.0.1", health_port, timeout=5)
        try:
            connection.request("GET", "/metrics")
            return connection.getresponse().read()
        finally:
            connection.close()
    url = f"http://127.0.0.1:{port}/team/project.git"
    f.env["GIT_TRACE_PACKET"] = "1"
    source = f.base / "source"
    f.git(f.base, "init", "-b", "main", source)
    (source / "file").write_text("first\n")
    f.git(source, "add", ".")
    f.git(source, "commit", "-m", "first")
    tip = f.git(source, "rev-parse", "HEAD").stdout.strip()
    tree = f.git(source, "rev-parse", "HEAD^{tree}").stdout.strip()
    # No native server operation is allowed even transiently. Broken server Git
    # configuration is distinct from the genuine external client oracle PATH.
    initial_pack = command(["git", "-C", source, "pack-objects", "--revs", "--stdout"],
                           env=f.env, data=tip + b"\n").stdout
    report = raw_push(port, tip, [b"refs/heads/main"], pack=initial_pack)
    assert b"ok refs/heads/main" in report, report
    f.results["raw_rust_initial_publication"] = True
    names = [b"refs/heads/order-z", b"refs/heads/order-a-\xff"]
    report = raw_push(port, tip, names)
    outer = list(packets(report))
    assert outer[-1] is None and all(record[:1] == b"\x01" for record in outer[:-1])
    nested = b"".join(record[1:] for record in outer[:-1])
    manifest = b"".join(b"0" * 40 + b" " + tip + b" " + name + b"\n" for name in names)
    operation = "(SELECT id FROM git_rs.operations WHERE commands=decode('" + manifest.hex() + "','hex'))"
    assert f.sql("SELECT count(*) FROM git_rs.operations WHERE id=" + operation) == "1"
    assert f.sql("SELECT encode(name,'hex') FROM git_rs.ref_updates WHERE operation_id=" + operation +
                 " ORDER BY ordinal").splitlines() == [name.hex() for name in names]
    assert list(packets(nested)) == [b"unpack ok\n", *(b"ok " + name + b"\n" for name in names), None]
    f.results["exact_ordered_byte_preserved_sql_manifest_and_status"] = True
    pack_cases(f, port, tip)

    def rejected(label, body, headers=None):
        before = state(f)
        status, report = post(port, body, headers)
        assert b"ok refs/" not in report, (label, status, report)
        assert state(f) == before, (label, "partial publication or staged upload")
        f.results.setdefault("rejected_cases", []).append(label)
        return status, report

    zero = b"0" * 40
    def commands(name=b"refs/heads/carrier", target=tip):
        return pkt(zero + b" " + target + b" " + name + b"\0report-status side-band-64k") + b"0000"
    identity = b"Fixture <fixture@example.invalid> 1577836800 +0000\n"
    good = b"tree " + tree + b"\nparent " + tip + b"\nauthor " + identity + b"committer " + identity + b"\ngood\n"
    for label, kind, data in [
        ("unreachable-commit", 1, good.replace(b"author " + identity, b"author invalid\n")),
        ("unreachable-tag", 4, b"object " + tip + b"\ntype invalid\ntag bad\n\nbad\n"),
        ("unreachable-tree", 2, b"100644 bad/name\0" + bytes.fromhex(tip.decode())),
        ("missing-typed-parent", 1, good.replace(b"parent " + tip, b"parent " + b"f" * 40)),
        ("wrong-tree-kind", 1, good.replace(b"tree " + tree, b"tree " + tip)),
    ]:
        status, report = rejected(label, commands() + pack_objects([(kind, data)]))
        assert status == 200 and b"ng refs/heads/carrier" in report, (label, status, report)
    malformed = good.replace(b"author " + identity, b"author invalid\n")
    rejected("duplicate-physical-unreachable", commands() + pack_objects([(1, malformed), (1, malformed)]))
    bad_module = b'[submodule "x"]\n\tpath = -evil\n\turl = https://example.invalid/x\n'
    bad_module_tree = b"100644 .gitmodules\0" + bytes.fromhex("f7c64f494ac830a8bd41d82d4f3d18b7d5445854")
    bad_module_commit = b"tree 176b92700401d61c379c2d2f25301830b54c50f1\nauthor A <a@example.invalid> 1 +0000\ncommitter A <a@example.invalid> 1 +0000\n\nfixture\n"
    rejected("unreachable-gitmodules-path", commands() + pack_objects([
        (3, bad_module), (2, bad_module_tree), (1, bad_module_commit)]))
    def physical_id(kind, data):
        return hashlib.sha1(kind + b" " + str(len(data)).encode() + b"\0" + data).digest()
    tab_module = b'[submodule "x"]\npath = sub\nurl = "https://example.invalid/a\\tb"\n'
    tab_tree = b"100644 .gitmodules\0" + physical_id(b"blob", tab_module)
    tab_commit = (b"tree " + physical_id(b"tree", tab_tree).hex().encode() + b"\nauthor " + identity +
                  b"committer " + identity + b"\nnative-compatible config\n")
    tab_tip = physical_id(b"commit", tab_commit).hex().encode()
    status, report = post(port, commands(b"refs/heads/modules-tab", tab_tip) + pack_objects([
        (3, tab_module), (2, tab_tree), (1, tab_commit)]))
    assert status == 200 and b"ok refs/heads/modules-tab" in report, (status, report)
    assert f.sql("SELECT encode(oid,'hex') FROM git_rs.refs WHERE name=convert_to('refs/heads/modules-tab','UTF8')") == tab_tip.decode()
    f.results["maintained_gitmodules_policy_real_publication"] = True
    batch = (pkt(zero + b" " + tip + b" refs/heads/first\0report-status") +
             pkt(zero + b" " + b"f" * 40 + b" refs/heads/second") + b"0000" + pack_objects([]))
    rejected("whole-batch-without-atomic", batch)
    valid = commands() + pack_objects([])
    for label, body in [("bad-crc", gzip.compress(valid)[:-8] + b"\0" * 8),
                        ("truncated-gzip", gzip.compress(valid)[:-1]),
                        ("trailing-gzip", gzip.compress(valid) + b"trailing")]:
        rejected(label, body, {"Content-Encoding": "gzip"})
    for label, body in [("trailing-pack", valid + b"tail"), ("bad-pack-hash", valid[:-1] + bytes([valid[-1] ^ 1])),
                        ("truncated-pack", valid[:-1]), ("delimiter", b"0001"),
                        ("response-end", b"0002"), ("bad-length", b"zzzz")]:
        rejected(label, body)
    # Pause before framed EOF. No PUT, COMMIT or positive ACK may escape.
    before = state(f)
    with socket.create_connection(("127.0.0.1", port), 10) as peer:
        peer.settimeout(.2)
        peer.sendall(("POST /team/project.git/git-receive-pack HTTP/1.1\r\nHost: localhost\r\n"
                      "Content-Type: application/x-git-receive-pack-request\r\n"
                      f"Content-Length: {len(valid) + 1}\r\n\r\n").encode() + valid)
        try:
            early = peer.recv(4096)
        except TimeoutError:
            early = b""
        assert not early, ("publication before framed EOF", early)
        assert state(f) == before
        peer.shutdown(socket.SHUT_WR)
        try:
            tail = peer.recv(4096)
        except ConnectionResetError:
            tail = b""
        assert b"ok refs/" not in tail
    wait_for(lambda: f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0", "receive source cleanup")
    assert state(f) == before, "short Content-Length must not publish after premature socket EOF"
    # Malformed HTTP framing must not bypass the exact CL/chunk completion owner.
    for label, headers, body in [
        ("short-content-length", f"Content-Length: {len(valid)+1}\r\n", valid),
        ("bad-chunk-crlf", "Transfer-Encoding: chunked\r\n", f"{len(valid):x}\r\n".encode() + valid + b"XX0\r\n\r\n"),
        ("bad-chunk-size", "Transfer-Encoding: chunked\r\n", b"zz\r\n"),
        ("chunk-trailer", "Transfer-Encoding: chunked\r\n", f"{len(valid):x}\r\n".encode() + valid + b"\r\n0\r\nUnexpected: tail\r\n\r\n"),
        ("duplicate-cl", "Content-Length: 0\r\nContent-Length: 0\r\n", b""),
        ("cl-and-te", "Content-Length: 0\r\nTransfer-Encoding: chunked\r\n", b"0\r\n\r\n"),
    ]:
        before = state(f)
        with socket.create_connection(("127.0.0.1", port), 10) as peer:
            peer.settimeout(20)
            peer.sendall(("POST /team/project.git/git-receive-pack HTTP/1.1\r\nHost: localhost\r\n"
                          "Content-Type: application/x-git-receive-pack-request\r\n" + headers + "\r\n").encode() + body)
            peer.shutdown(socket.SHUT_WR)
            try:
                reply = peer.recv(4096)
            except ConnectionResetError:
                reply = b""
            assert b"ok refs/" not in reply, (label, reply)
        wait_for(lambda: f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0", label + " cleanup")
        assert state(f) == before, label
        f.results.setdefault("rejected_cases", []).append(label)

    # Accepted gzip and genuinely pack-less deletion exercise both positive
    # transport boundaries; no empty catalog pack is manufactured.
    counts = f.sql("SELECT count(*) FROM git_rs.packs")
    status, report = post(port, gzip.compress(commands(b"refs/heads/gzip") + pack_objects([])),
                          {"Content-Encoding": "gzip"})
    assert status == 200 and b"ok refs/heads/gzip" in report, (status, report)
    delete = pkt(tip + b" " + zero + b" refs/heads/gzip\0report-status side-band-64k") + b"0000"
    status, report = post(port, delete)
    assert status == 200 and b"ok refs/heads/gzip" in report, (status, report)
    assert f.sql("SELECT count(*) FROM git_rs.packs") == counts
    f.results["gzip_and_packless_delete"] = True
    for body, headers in [(b"0000", {}), (gzip.compress(b"0000"), {"Content-Encoding": "gzip"})]:
        before = state(f)
        assert post(port, body, headers) == (200, b"")
        assert state(f) == before, "HTTP typed no-op must not create a publication transaction"
    rejected("noop-trailing-payload", b"0000unexpected")
    rejected("noop-truncated-gzip", gzip.compress(b"0000")[:-1], {"Content-Encoding": "gzip"})
    f.results["typed_http_noop_completion_no_publication"] = True
    fault_cases(f, port, tip, tree, commands, gateway, commit_gateway)
    raw_ssh_case(f, tip)
    deadline_cases(f, port, tip, metrics)
    output_backpressure_case(f, port, tip, metrics)
    native_upload_cases(f, tip)
    bridge = f.base / "bridge"
    f.write_bridge(bridge, f.socket("receive"))
    ssh_env = {**f.env, "GIT_SSH_COMMAND": str(bridge), "GIT_SSH_VARIANT": "simple",
               "SSH_ORIGINAL_COMMAND": "git-receive-pack 'team/project.git'"}
    # Characterize both interactive empty-command and single-pack clients even
    # when one fails; defer the aggregate assertion, never mark a failed gate green.
    native = {}
    def native_case(label, *args, env=None):
        result = f.git(source, *args, env=env, ok=False)
        native[label] = {"returncode": result.returncode, "stderr": result.stderr.decode(errors="replace")[-8000:]}
        (f.base / (label + ".stderr")).write_bytes(result.stderr)
        return result
    f.cli("create", "team/native-empty", "alice")
    native_case("http-first-push", "push", "--atomic", url.replace("team/project.git", "team/native-empty.git"), "main")
    before_noop = state(f)
    native_case("ssh-noop", "push", "ssh://fixture/team/project.git", "main", env=ssh_env)
    assert state(f) == before_noop, "SSH no-op must not publish"
    created = native_case("ssh-create", "push", "ssh://fixture/team/project.git", "main:refs/heads/ssh", env=ssh_env)
    if created.returncode == 0:
        assert f.sql("SELECT count(*) FROM git_rs.refs WHERE name=convert_to('refs/heads/ssh','UTF8')") == "1"
    f.git(source, "branch", "other")
    f.git(source, "tag", "-a", "v1", "-m", "tag")
    native_case("http-atomic", "push", "--atomic", url, "other")
    native_case("http-mirror", "push", "--mirror", url)
    before_noop = state(f)
    noop = native_case("http-noop", "push", url, "main")
    assert b"Everything up-to-date" in noop.stderr, noop.stderr
    assert state(f) == before_noop, "native no-op must not publish"
    native_case("http-delete", "push", url, ":other")
    f.results["native_clients"] = native
    wait_for(lambda: metric_value(metrics(), "git_rs_fixture_receive_admitted") == 0, "actual receive permit cleanup")
    native_count = metric_value(metrics(), 'git_rs_subprocess_duration_seconds_count{program="git"}')
    assert native_count == 0, "receive must not invoke any native server subprocess or fallback"
    f.results["rust_handler_native_subprocess_count"] = native_count
    assert cache_outage.connections == initial_cache_connections, "receive must not contact the unavailable optional cache"
    f.results["optional_cache_outage_no_receive_dependency"] = True
    assert all(case["returncode"] == 0 for case in native.values()), native
    assert f.sql("SELECT count(*) FROM git_rs.refs WHERE name=convert_to('refs/heads/other','UTF8')") == "0"
    f.results["native_http_push_mirror_noop_delete"] = True
    # A real native SSH client keeps stdin open awaiting status. This must finish
    # on the single-pack boundary, never a socket-wide EOF probe.
    assert f.sql("SELECT count(*) FROM git_rs.refs WHERE name=convert_to('refs/heads/ssh','UTF8')") == "0", "native mirror must remove a remote-only branch"
    assert "rust-receive-ssh result=Ok(())" in log_path.read_text()
    f.results["native_interactive_ssh"] = True


def state(f):
    return f.sql("SELECT (SELECT jsonb_agg(r ORDER BY repo_id,name) FROM git_rs.refs r),"
                 "(SELECT count(*) FROM git_rs.packs),(SELECT count(*) FROM git_rs.operations),"
                 "(SELECT count(*) FROM git_rs.staged_objects),"
                 "(SELECT generation FROM git_rs.repositories WHERE slug='team/project')")


def authority(f):
    return f.sql("SELECT (SELECT jsonb_agg(r ORDER BY repo_id,name) FROM git_rs.refs r),"
                 "(SELECT count(*) FROM git_rs.packs),(SELECT count(*) FROM git_rs.operations),"
                 "(SELECT generation FROM git_rs.repositories WHERE slug='team/project')")


def pack_cases(f, port, tip):
    def oid(data):
        return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).digest()
    def finish(entries):
        data = b"PACK" + struct.pack(">II", 2, len(entries)) + b"".join(entries)
        return data + hashlib.sha1(data).digest()
    thin = b"\x7a" + oid(b"first\n") + zlib.compress(b"\x06\x07\x07second\n")
    forward = [b"\x76" + oid(b"abcd") + zlib.compress(b"\x04\x05\x90\x04\x01e"),
               b"\x76" + oid(b"abc") + zlib.compress(b"\x03\x04\x90\x03\x01d"),
               b"\x33" + zlib.compress(b"abc")]
    base = b"\x33" + zlib.compress(b"abc")
    assert len(base) < 128
    ofs = [base, b"\x66" + bytes([len(base)]) + zlib.compress(b"\x03\x04\x90\x03\x01d")]
    for label, entries, expected in [
        ("thin-cross-pack", [thin], [b"second\n"]),
        ("forward-chain", forward, [b"abc", b"abcd", b"abcde"]),
        ("ofs-delta", ofs, [b"abc", b"abcd"]),
    ]:
        pack = finish(entries)
        report = raw_push(port, tip, [b"refs/heads/" + label.encode()], pack=pack)
        assert b"ok refs/heads/" + label.encode() in report, (label, report)
        key = f.sql("SELECT pack_key FROM git_rs.packs ORDER BY generation DESC LIMIT 1")
        path = f.base / (label + ".pack")
        path.write_bytes(f.s3("GET", key).stdout)
        # External pinned Git checks the exact immutable S3 bytes, not a mocked
        # adapter output. The normalized pack must be physically self-contained.
        f.git(f.base / "source", "index-pack", "--strict", path)
        manifest = f.git(f.base / "source", "verify-pack", "-v", path.with_suffix(".idx")).stdout
        ids = {line.split()[0] for line in manifest.splitlines()
               if re.fullmatch(b"[0-9a-f]{40}", line.split()[0])}
        assert ids == {oid(data).hex().encode() for data in expected}, (label, manifest)
    f.results["real_s3_normalized_thin_forward_ofs_native_manifests"] = 3
    zero = b"0" * 40
    prefix = pkt(zero + b" " + tip + b" refs/heads/broken-delta\0report-status") + b"0000"
    for label, entries in [
        ("cycle", [b"\x7a" + oid(b"cycle-b") + zlib.compress(b"\x07\x07\x07cycle-a"),
                   b"\x7a" + oid(b"cycle-a") + zlib.compress(b"\x07\x07\x07cycle-b")]),
        ("missing-base", [b"\x76" + b"f" * 20 + zlib.compress(b"\x03\x04\x90\x03\x01d")]),
        ("zero-delta-opcode", [b"\x73" + oid(b"first\n") + zlib.compress(b"\x06\x07\0")]),
        ("delta-bomb", [b"\x77" + oid(b"first\n") + zlib.compress(b"\x06\x81\x80\x80\x40\x01x")]),
    ]:
        before = state(f)
        status, report = post(port, prefix + finish(entries))
        assert b"ok refs/" not in report and state(f) == before, (label, status, report)
        f.results.setdefault("rejected_cases", []).append(label)


def deadline_cases(f, port, tip, metrics):
    # Keep both peers open. Only the server's absolute deadline can terminate the
    # stalled framing/pack reads; neither case can borrow client EOF as its stop.
    for transport in ("http", "ssh"):
        before = state(f)
        if transport == "http":
            peer = socket.create_connection(("127.0.0.1", port), 10)
            peer.sendall(b"POST /team/project.git/git-receive-pack HTTP/1.1\r\nHost: localhost\r\n"
                         b"Content-Type: application/x-git-receive-pack-request\r\nContent-Length: 1000\r\n\r\n")
        else:
            peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            peer.settimeout(10)
            peer.connect(str(f.socket("receive")))
            peer.sendall(b"".join(pkt(field) for field in
                         (b"git-rs-ssh-1", b"alice", b"team/project", b"git-receive-pack", b"")))
            peer.sendall(pkt(b"0" * 40 + b" " + tip + b" refs/heads/stalled\0report-status") +
                         b"0000PACK" + struct.pack(">II", 2, 1))
        try:
            wait_for(lambda: metric_value(metrics(), "git_rs_fixture_receive_admitted") == 1,
                     transport + " admitted source owner")
            wait_for(lambda: f.sql("SELECT count(*) FROM git_rs.repository_leases") == "1",
                     transport + " live source hold")
            assert state(f) == before
            # A second real request uses the remaining shared slot and completes.
            report = raw_push(port, tip, [b"refs/heads/foreign-" + transport.encode()])
            assert b"ok refs/heads/foreign-" + transport.encode() in report, report
            foreign = state(f)
            peer.settimeout(20)
            output = bytearray()
            while True:
                try:
                    data = peer.recv(4096)
                except ConnectionResetError:
                    break
                if not data:
                    break
                output.extend(data)
            assert b"ok refs/heads/stalled" not in output
            wait_for(lambda: metric_value(metrics(), "git_rs_fixture_receive_admitted") == 0,
                     transport + " actual stop and admission cleanup")
            assert f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0"
            assert state(f) == foreign, "deadline must not publish late or remove foreign work"
        finally:
            peer.close()
    f.results["http_ssh_absolute_deadline_and_foreign_work"] = True


def raw_ssh_case(f, tip):
    from failures import receive_exact
    payload = bytearray()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as peer:
        peer.settimeout(20)
        peer.connect(str(f.socket("receive")))
        peer.sendall(b"".join(pkt(field) for field in
                     (b"git-rs-ssh-1", b"alice", b"team/project", b"git-receive-pack", b"")))
        peer.sendall(pkt(b"0" * 40 + b" " + tip + b" refs/heads/ssh-raw\0report-status side-band-64k") +
                     b"0000" + pack_objects([(3, b"ssh-body")]))
        # Keep the write half open until the backend's actual framed terminal
        # outcome. This exercises Engine, not only the lower adapter's test API.
        while True:
            size = int.from_bytes(receive_exact(peer, 4), "big")
            if not size:
                assert receive_exact(peer, 1) == b"\0"
                break
            assert size <= 65536 and len(payload) + size <= 256 * 1024
            payload.extend(receive_exact(peer, size))
    records = iter(packets(payload))
    for record in records:
        if record is None:
            break
    nested = bytearray()
    for record in records:
        if record is None:
            break
        assert record[:1] == b"\x01", record
        nested.extend(record[1:])
    assert list(records) == [], "unexpected bytes after outer report-status flush"
    assert list(packets(nested)) == [b"unpack ok\n", b"ok refs/heads/ssh-raw\n", None]
    assert f.sql("SELECT count(*) FROM git_rs.refs WHERE name=convert_to('refs/heads/ssh-raw','UTF8')") == "1"
    f.results["engine_ssh_open_stdin_exact_nested_status"] = True


def output_backpressure_case(f, port, tip, metrics):
    prefix = "refs/heads/output-gate/"
    f.sql("INSERT INTO git_rs.refs(repo_id,name,oid) SELECT id,convert_to('" + prefix +
          "'||lpad(n::text,6,'0')||repeat('x',950),'UTF8'),decode('" + tip.decode() +
          "','hex') FROM git_rs.repositories CROSS JOIN generate_series(1,6000) n WHERE slug='team/project'")
    peer = socket.socket()
    peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    peer.settimeout(20)
    try:
        peer.connect(("127.0.0.1", port))
        started = time.monotonic()
        peer.sendall(b"GET /team/project.git/info/refs?service=git-receive-pack HTTP/1.1\r\nHost: localhost\r\n\r\n")
        # Peek without draining: output has started, but more than the bounded
        # socket buffers remains. Keep this peer alive and unread through deadline.
        assert peer.recv(1, socket.MSG_PEEK) == b"H"
        assert metric_value(metrics(), "git_rs_fixture_receive_admitted") == 1
        assert f.sql("SELECT count(*) FROM git_rs.repository_leases") == "1"
        wait_for(lambda: metric_value(metrics(), "git_rs_fixture_receive_admitted") == 0,
                 "absolute output deadline releases actual source and admission")
        elapsed = time.monotonic() - started
        assert elapsed >= 12, ("advertisement did not actually encounter output backpressure", elapsed)
        assert f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0"
        f.results["real_http_output_backpressure_seconds"] = elapsed
    finally:
        peer.close()
        f.sql("DELETE FROM git_rs.refs WHERE substring(name from 1 for " + str(len(prefix)) +
              ")=convert_to('" + prefix + "','UTF8')")


def native_upload_cases(f, tip):
    _, upload_port = f.start("native-upload")
    http_clone = f.base / "http-clone.git"
    f.git(f.base, "clone", "--bare", f"http://127.0.0.1:{upload_port}/team/project.git", http_clone)
    assert f.git(http_clone, "rev-parse", "refs/heads/main").stdout.strip() == tip
    assert f.git(http_clone, "show", "main:file").stdout == b"first\n"
    f.git(http_clone, "fsck", "--strict")
    bridge = f.base / "upload-bridge"
    f.write_bridge(bridge, f.socket("native-upload"))
    env = {**f.env, "GIT_SSH_COMMAND": str(bridge), "GIT_SSH_VARIANT": "simple",
           "SSH_ORIGINAL_COMMAND": "git-upload-pack 'team/project.git'"}
    ssh_clone = f.base / "ssh-clone.git"
    f.git(f.base, "clone", "--bare", "ssh://fixture/team/project.git", ssh_clone, env=env)
    assert f.git(ssh_clone, "rev-parse", "refs/heads/main").stdout.strip() == tip
    f.git(ssh_clone, "fsck", "--strict")
    f.stop("native-upload")
    f.results["native_upload_http_and_cancel_joined_ssh_bridge"] = True


def fault_cases(f, port, tip, tree, commands, gateway, commit_gateway):
    identity = b"Fixture <fixture@example.invalid> 1577836800 +0000\n"
    def incoming(label):
        data = b"tree " + tree + b"\nparent " + tip + b"\nauthor " + identity + b"committer " + identity + b"\n" + label.encode() + b"\n"
        oid = hashlib.sha1(b"commit " + str(len(data)).encode() + b"\0" + data).hexdigest().encode()
        return commands(b"refs/heads/" + label.encode(), oid) + pack_objects([(1, data)])
    for mode in ("before", "after"):
        before = authority(f)
        commit_gateway.arm(mode)
        status, report = post(port, incoming("commit-" + mode))
        assert commit_gateway.dropped.wait(2), (mode, "COMMIT gate not exercised")
        if mode == "before":
            assert b"ok refs/" not in report and authority(f) == before, (mode, status, report)
        else:
            assert status == 200 and b"ok refs/heads/commit-after" in report, (status, report)
            assert authority(f) != before
            # Exactly one operation survives the lost reply and fresh WAL-producing
            # exact-manifest confirmation; never publish an inferred success.
            assert f.sql("SELECT count(*) FROM git_rs.operations WHERE position(convert_to('commit-after','UTF8') in commands)>0 AND confirmations>0") == "1"
    f.results["lost_commit_before_after_confirmation"] = True
    # COMMIT is already durable while the SQL reply is held. Losing the client
    # output cannot turn that committed publication into a rejected transaction.
    body = incoming("lost-output")
    commit_gateway.arm("pause-after")
    peer = socket.create_connection(("127.0.0.1", port), 10)
    try:
        peer.sendall(("POST /team/project.git/git-receive-pack HTTP/1.1\r\nHost: localhost\r\n"
                      "Content-Type: application/x-git-receive-pack-request\r\n"
                      f"Content-Length: {len(body)}\r\n\r\n").encode() + body)
        assert commit_gateway.dropped.wait(5), "after-COMMIT output gate not reached"
        committed = authority(f)
        assert f.sql("SELECT count(*) FROM git_rs.refs WHERE name=convert_to('refs/heads/lost-output','UTF8')") == "1"
        peer.shutdown(socket.SHUT_RDWR)
    finally:
        peer.close()
        commit_gateway.release.set()
    wait_for(lambda: f.sql("SELECT count(*) FROM git_rs.repository_leases") == "0", "lost-output cleanup")
    assert authority(f) == committed
    f.results["lost_output_keeps_confirmed_publication"] = True
    before = authority(f)
    prior_staged = set(f.sql("SELECT object_key FROM git_rs.staged_objects").splitlines())
    gateway.arm("unknown-idx-put")
    status, report = post(port, incoming("unknown-put"))
    gateway.reset()
    assert b"ok refs/" not in report and authority(f) == before, (status, report)
    uncertain = set(f.sql("SELECT object_key FROM git_rs.staged_objects").splitlines()) - prior_staged
    assert len(uncertain) == 2, uncertain
    for key in uncertain:
        assert f.s3("HEAD", key).stdout == b"200", ("successful PUT must remain recoverable after lost reply", key)
    states = f.sql("SELECT state FROM git_rs.staged_objects WHERE object_key IN (" +
                   ",".join("'" + key + "'" for key in uncertain) + ") ORDER BY state").splitlines()
    assert states == ["planned", "uploaded"], states
    f.results["lost_put_journal"] = True

    # The first actual PUT is gated after both journal rows are durable. A
    # disjoint ref-only publication then changes generation under the first held
    # snapshot: old-OID CAS, not snapshot-generation equality, must decide it.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        before = authority(f)
        prior_staged = set(f.sql("SELECT object_key FROM git_rs.staged_objects").splitlines())
        gateway.arm("pause-put")
        future = pool.submit(post, port, incoming("disjoint-a"))
        try:
            assert gateway.reached.wait(5), "before-PUT gate not reached"
            assert authority(f) == before and not future.done()
            planned = set(f.sql("SELECT object_key FROM git_rs.staged_objects").splitlines()) - prior_staged
            assert len(planned) == 2, planned
            rows = f.sql("SELECT operation_id::text||'|'||state FROM git_rs.staged_objects WHERE object_key IN (" +
                         ",".join("'" + key + "'" for key in planned) + ")").splitlines()
            assert len(rows) == 2 and rows[0] == rows[1] and rows[0].endswith("|planned"), rows
            for key in planned:
                reply = f.s3("HEAD", key, ok=False)
                assert reply.returncode == 22 and reply.stdout == b"404", ("journal must precede actual PUT", key, reply.stdout)
            report = raw_push(port, tip, [b"refs/heads/disjoint-b"])
            assert b"ok refs/heads/disjoint-b" in report, report
        finally:
            gateway.reset()
        status, report = future.result(timeout=20)
        assert status == 200 and b"ok refs/heads/disjoint-a" in report, (status, report)
    f.results["staged_before_put_and_disjoint_generation"] = True
    for fence in ("old-oid", "acl", "epoch", "lease"):
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            gateway.arm("pause-put")
            future = pool.submit(post, port, incoming("fence-" + fence))
            try:
                assert gateway.reached.wait(5), (fence, "PUT not reached")
                if fence == "old-oid":
                    report = raw_push(port, tip, [b"refs/heads/fence-old-oid"])
                    assert b"ok refs/heads/fence-old-oid" in report, report
                elif fence == "acl":
                    f.sql("UPDATE git_rs.permissions SET role='reader' WHERE principal='alice'")
                elif fence == "epoch":
                    f.sql("UPDATE git_rs.repositories SET storage_epoch=storage_epoch+1 WHERE slug='team/project'")
                else:
                    f.sql("UPDATE git_rs.repository_leases SET expires_at=clock_timestamp()-interval '1 second'")
                before_release = authority(f)
            finally:
                gateway.reset()
            status, report = future.result(timeout=20)
            assert b"ok refs/" not in report and authority(f) == before_release, (fence, status, report)
            if fence == "acl":
                f.sql("UPDATE git_rs.permissions SET role='owner' WHERE principal='alice'")
    f.results["publication_old_oid_acl_epoch_live_lease_fences"] = True


def run(args):
    assert args.provision and args.fixture_dir
    assert args.receive_fixture_binary or args.runtime_fixture_image, "a prebuilt receive fixture is required"
    base = args.fixture_dir
    assert base.parent.is_dir()
    base.mkdir(mode=0o700)
    def interrupted(signum, _):
        raise InterruptedError(f"receive fixture interrupted by {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    f = Fixture(base, args.binary.resolve(), runtime_image=args.runtime_image,
                runtime_fixture_image=args.runtime_fixture_image, revision=args.revision)
    f.blame_core_resources = True  # reuse existing bounded disposable PG/S3 contract
    try:
        f.setup(True)
        from failures import CommitGateway, FaultGateway, OwnedGateway, close_gateways
        class CacheUnavailable(socketserver.BaseRequestHandler):
            def handle(self):
                self.server.connections += 1
                self.request.shutdown(socket.SHUT_RDWR)
        class CacheOutage(OwnedGateway, socketserver.TCPServer):
            connections = 0
        gateway = commit_gateway = cache_outage = None
        try:
            gateway = FaultGateway(f.env["GIT_RS_S3_ENDPOINT"])
            commit_gateway = CommitGateway(int(f.env["PGPORT"]))
            cache_outage = CacheOutage(("127.0.0.1", 0), CacheUnavailable)
            f.env["GIT_RS_S3_ENDPOINT"] = gateway.endpoint
            f.env["PGPORT"] = str(commit_gateway.server_address[1])
            f.env.update(GIT_RS_CACHE_NAMESPACE=str(uuid.uuid4()), GIT_RS_CACHE_OWNER=str(uuid.uuid4()),
                         GIT_RS_CACHE_PGPORT=str(cache_outage.server_address[1]))
            helper = str(args.receive_fixture_binary.resolve()) if args.receive_fixture_binary else None
            exercise(f, helper, gateway, commit_gateway, cache_outage)
            f.results["status"] = "passed"
        finally:
            close_gateways(cache_outage, commit_gateway, gateway)
    except Exception as error:
        f.results["error"] = repr(error)
        raise
    finally:
        try:
            f.close()
        finally:
            (base / "report.json").write_text(json.dumps(f.results, indent=2) + "\n")
    print(json.dumps(f.results, indent=2))
