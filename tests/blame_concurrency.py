#!/usr/bin/env python3
"""Synthetic HTTP concurrency contract; requires disposable PostgreSQL/S3 endpoints."""
import argparse
import concurrent.futures
import http.client
from pathlib import Path
import select
import socket
import sys
import tempfile

from integration import Fixture, command, metric_value, wait_for


def exercise(f):
    f.cli("create", "team/blame-concurrency", "alice")
    source = f.base / "concurrency-source"
    command(["git", "init", "--initial-branch=main", source], env=f.env)
    for name in ["alpha", "beta", "gamma", "delta"]:
        (source / name).write_text(name + "\n")
    f.git(source, "add", ".")
    f.git(source, "commit", "-m", "Synthetic independent annotations")
    _, publish_port = f.start("concurrency-publisher")
    f.git(source, "push", f"http://127.0.0.1:{publish_port}/team/blame-concurrency.git", "main")
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip().decode()
    expected = {name: f.git(source, "blame", "--root", "--line-porcelain", oid, "--", name).stdout
                for name in ["alpha", "beta", "gamma", "delta"]}
    control = f.base / "concurrency-control.sock"
    worker = f.base / "concurrency-worker"
    worker.write_text(f'''#!{sys.executable}
import os,socket,sys
with socket.socket(socket.AF_UNIX) as control:
    control.connect({str(control)!r})
    control.sendall(os.fsencode(sys.argv[3]) + b"\\n")
    result = control.recv(1)
if result != b"1":
    sys.stdout.buffer.write(b"partial output must not escape\\n")
    sys.exit(7)
os.execv("/usr/bin/git", ["git", "--literal-pathspecs", "-C", sys.argv[1], "blame", "--root", "--line-porcelain", "--no-textconv", "--no-progress", sys.argv[2], "--", sys.argv[3]])
''')
    worker.chmod(0o700)
    previous = dict(f.env)

    def start(name, seconds=30):
        f.env.update(GIT_RS_BLAME_WORKER=str(worker), GIT_RS_BLAME_CONCURRENCY="2",
                     GIT_RS_BLAME_WAITERS="1", GIT_RS_OPERATION_SECONDS=str(seconds))
        try:
            return f.start(name)[1]
        finally:
            f.env.clear()
            f.env.update(previous)

    def request(port, name):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        connection.request("GET", f"/team/blame-concurrency.git/blame/{oid}?path={name}")
        return connection

    def response(connection):
        try:
            result = connection.getresponse()
            return result.status, result.read()
        finally:
            connection.close()

    repo = "(SELECT id FROM git_rs.repositories WHERE slug='team/blame-concurrency')"

    def leases():
        return int(f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}"))

    def active(name):
        return metric_value(f.health(name, "/metrics")[1], "git_rs_active_requests")

    # A worker reports only after all real authorization, snapshot, cache and
    # reachability work. The test controls completion, not the admission gate.
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(str(control))
        listener.listen(4)
        listener.settimeout(8)
        peers = []

        def entered(name, pending=None):
            try:
                peer, _ = listener.accept()
            except TimeoutError as error:
                result = pending.result(timeout=1) if pending and pending.done() else "still waiting"
                raise AssertionError(f"independent worker {name} did not execute: {result}") from error
            peers.append(peer)
            peer.settimeout(8)
            assert peer.recv(4096) == name.encode() + b"\n", "wrong independent worker"
            return peer

        try:
            port = start("concurrent-blame")
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                first = pool.submit(response, request(port, "alpha"))
                alpha = entered("alpha")
                second = pool.submit(response, request(port, "beta"))
                beta = entered("beta", second)
                assert not first.done() and not second.done(), "two workers must overlap"
                assert leases() == 2, "independent readers must retain independent holds"
                third = pool.submit(response, request(port, "gamma"))
                wait_for(lambda: active("concurrent-blame") == 3, "third admitted request")
                assert response(request(port, "delta")) == (503, b""), "bounded queue must reject overflow"
                assert not third.done() and not select.select([listener], [], [], 0)[0], "queued work must not execute"
                assert leases() == 2, "waiting must not acquire a lease or materialize"
                assert len(list((f.base / "scratch").glob("instance-*/request-*"))) == 2
                alpha.sendall(b"1")
                gamma = entered("gamma")
                assert first.result(timeout=8) == (200, expected["alpha"])
                assert not second.done(), "one completion must not cancel another reader"
                beta.sendall(b"0")
                assert second.result(timeout=8) == (500, b""), "worker errors must not expose partial output"
                # Failure releases the slot while gamma remains live.
                fourth = pool.submit(response, request(port, "delta"))
                delta = entered("delta")
                gamma.sendall(b"1")
                delta.sendall(b"1")
                assert third.result(timeout=8) == (200, expected["gamma"])
                assert fourth.result(timeout=8) == (200, expected["delta"])
            wait_for(lambda: leases() == 0 and active("concurrent-blame") == 0, "success/error cleanup")

            # A queued disconnect returns admission without ever creating a worker.
            first = request(port, "alpha")
            alpha = entered("alpha")
            second = request(port, "beta")
            beta = entered("beta")
            cancelled = request(port, "gamma")
            wait_for(lambda: active("concurrent-blame") == 3, "queued cancellation admission")
            cancelled.close()
            wait_for(lambda: active("concurrent-blame") == 2, "queued cancellation release")
            assert leases() == 2 and not select.select([listener], [], [], 0)[0]
            replacement = request(port, "delta")
            wait_for(lambda: active("concurrent-blame") == 3, "reusable waiting admission")
            first.close()
            delta = entered("delta")
            beta.sendall(b"1")
            delta.sendall(b"1")
            assert response(second) == (200, expected["beta"])
            assert response(replacement) == (200, expected["delta"])
            wait_for(lambda: leases() == 0 and active("concurrent-blame") == 0, "disconnect cleanup")

            # Consume part of the waiter's absolute budget in HTTP headers,
            # then occupy both workers with newer requests. Its deadline must
            # expire while both holders are still alive, not restart at queuing.
            deadline_port = start("deadline-blame", seconds=3)
            queued = socket.create_connection(("127.0.0.1", deadline_port), timeout=8)
            queued.sendall(f"GET /team/blame-concurrency.git/blame/{oid}?path=gamma HTTP/1.1\r\nHost: localhost\r\n".encode())
            wait_for(lambda: active("deadline-blame") == 1, "incomplete-header admission")
            assert not select.select([queued], [], [], 1)[0], "incomplete headers must wait within their budget"
            expired = request(deadline_port, "alpha")
            alpha = entered("alpha")
            expired_second = request(deadline_port, "beta")
            beta = entered("beta")
            queued.sendall(b"\r\n")
            wait_for(lambda: active("deadline-blame") == 3, "deadline waiter admission")
            try:
                payload = queued.recv(4096)
                assert not payload.startswith(b"HTTP/1.1 200"), "expired waiter cannot succeed"
            except ConnectionResetError:
                pass
            finally:
                queued.close()
            wait_for(lambda: active("deadline-blame") == 2, "waiting deadline returns admission")
            assert leases() == 2 and not select.select([listener], [], [], 0)[0], "expired waiter must never launch"
            assert not select.select([alpha, beta], [], [], 0)[0], "holders must still execute at waiter expiry"
            alpha.sendall(b"1")
            beta.sendall(b"1")
            assert response(expired) == (200, expected["alpha"])
            assert response(expired_second) == (200, expected["beta"])
            wait_for(lambda: active("deadline-blame") == 0, "absolute execution/wait deadlines")
            wait_for(lambda: leases() == 0, "all reader holds released")
            assert not list((f.base / "scratch").glob("instance-*/request-*"))
            followup = request(deadline_port, "delta")
            delta = entered("delta")
            delta.sendall(b"1")
            assert response(followup) == (200, expected["delta"]), "deadline must release execution and queue permits"
        finally:
            for peer in peers:
                peer.close()
    f.results["bounded_concurrent_blame"] = "passed"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="git-rs-concurrency-") as directory:
        fixture = Fixture(Path(directory), args.binary.resolve())
        try:
            fixture.setup(False)
            wait_for(lambda: fixture.s3("HEAD", "", ok=False).stdout == b"200"
                     or fixture.s3("PUT", "", ok=False).returncode == 0, "synthetic S3 bucket")
            fixture.cli("migrate")
            exercise(fixture)
            print(fixture.results)
        except Exception:
            for path in fixture.base.glob("*.log"):
                print(path.name, path.read_text()[-4000:])
            raise
        finally:
            fixture.close()
