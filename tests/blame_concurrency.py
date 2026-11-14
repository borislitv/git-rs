#!/usr/bin/env python3
"""Real root HTTP/core admission contracts; the observer never replaces attribution."""
import concurrent.futures
import http.client
import json
import select
import socket

from integration import metric_value, wait_for


class Gates:
    def __init__(self, path):
        self.path = path
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(path))
        self.listener.listen(16)
        self.listener.settimeout(8)
        self.peers = []

    def entered(self):
        peer, _ = self.listener.accept()
        peer.settimeout(8)
        reader = peer.makefile("rb")
        event = json.loads(reader.readline())
        assert event["phase"] in ("SourceAcquired", "Complete", "Preparing", "BodyBuildClaimed"), event
        self.peers.append((peer, reader))
        return peer, reader

    def pending(self):
        return bool(select.select([self.listener], [], [], 0)[0])

    def close(self):
        for peer, reader in self.peers:
            peer.close()
            reader.close()
        self.listener.close()


def exercise(f):
    f.cli("create", "team/blame-concurrency", "alice")
    source = f.base / "concurrency-source"
    f.git(f.base, "init", "--initial-branch=main", source)
    for name in ("alpha", "beta", "gamma", "delta"):
        (source / name).write_text(name + "\n")
    f.git(source, "add", ".")
    f.git(source, "commit", "-m", "Synthetic independent annotations")
    _, publish = f.start("concurrency-publisher")
    f.git(source, "push", f"http://127.0.0.1:{publish}/team/blame-concurrency.git", "main")
    oid = f.git(source, "rev-parse", "HEAD").stdout.strip().decode()
    expected = {name: f.git(source, "--literal-pathspecs", "blame", "--root", "--line-porcelain",
                           "--no-textconv", "--no-progress", oid, "--", name).stdout
                for name in ("alpha", "beta", "gamma", "delta")}
    f.stop("concurrency-publisher")
    repo = "(SELECT id FROM git_rs.repositories WHERE slug='team/blame-concurrency')"
    def leases():
        return int(f.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id={repo}"))
    def active(name):
        return metric_value(f.health(name, "/metrics")[1], "git_rs_active_requests")
    def admitted(name):
        return metric_value(f.health(name, "/metrics")[1], "git_rs_fixture_blame_admitted")
    def waiters(name):
        return metric_value(f.health(name, "/metrics")[1], "git_rs_fixture_blame_waiters")
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
    gates = Gates(f.server / "core-gates.sock")
    try:
        _, port = f.start_blame("concurrent-blame", control=gates.path)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            a = pool.submit(response, request(port, "alpha"))
            alpha, _ = gates.entered()
            b = pool.submit(response, request(port, "beta"))
            beta, _ = gates.entered()
            assert not a.done() and not b.done() and leases() == 2
            c = pool.submit(response, request(port, "gamma"))
            wait_for(lambda: admitted("concurrent-blame") == 3, "distinct queued waiter")
            assert response(request(port, "delta")) == (503, b"")
            assert not gates.pending() and leases() == 2
            f.assert_no_repository_bridge()
            alpha.sendall(b"G")
            gamma, _ = gates.entered()
            assert a.result(timeout=8) == (200, expected["alpha"])
            beta.sendall(b"E")
            assert b.result(timeout=8) == (500, b""), "no partial success after dependency failure"
            d = pool.submit(response, request(port, "delta"))
            delta, _ = gates.entered()
            gamma.sendall(b"G")
            delta.sendall(b"P")
            assert c.result(timeout=8) == (200, expected["gamma"])
            assert d.result(timeout=8) == (500, b""), "panic must not publish partial 200"
        wait_for(lambda: leases() == 0 and active("concurrent-blame") == 0, "success/error/panic cleanup")

        first = request(port, "alpha")
        alpha, _ = gates.entered()
        second = request(port, "beta")
        beta, _ = gates.entered()
        cancelled = request(port, "gamma")
        wait_for(lambda: admitted("concurrent-blame") == 3, "queued cancellation admitted")
        cancelled.close()
        observations = f.results["cancelled_queue_observations"] = []
        def waiting_admission_released():
            status, body = f.health("concurrent-blame", "/metrics")
            assert status == 200
            state = {"active": metric_value(body, "git_rs_active_requests"),
                     "admitted": metric_value(body, "git_rs_fixture_blame_admitted"),
                     "waiters": metric_value(body, "git_rs_fixture_blame_waiters")}
            if not observations or state != observations[-1]:
                observations.append(state)
            return state["active"] == 2 and state["admitted"] == 2
        wait_for(waiting_admission_released,
                 "queued cancellation detached and waiting admission released")
        assert leases() == 2 and not gates.pending()
        replacement = request(port, "delta")
        wait_for(lambda: admitted("concurrent-blame") == 3, "reusable waiting admission")
        first.close()
        wait_for(lambda: active("concurrent-blame") == 2, "active waiter detached")
        assert leases() == 2 and not gates.pending(), "disconnect must not release actual running work"
        alpha.sendall(b"G")
        delta, _ = gates.entered()
        beta.sendall(b"G")
        delta.sendall(b"G")
        assert response(second) == (200, expected["beta"])
        assert response(replacement) == (200, expected["delta"])
        wait_for(lambda: leases() == 0, "cancelled queued source never acquired")

        # Same-key waiter cancellation leaves the one real source/build alive.
        creator = request(port, "alpha")
        alpha, _ = gates.entered()
        joined = request(port, "alpha")
        wait_for(lambda: waiters("concurrent-blame") == 2, "same-key waiter attached to real job")
        assert leases() == 1 and not gates.pending()
        creator.close()
        wait_for(lambda: waiters("concurrent-blame") == 1, "creator detached from real job")
        assert leases() == 1
        alpha.sendall(b"G")
        assert response(joined) == (200, expected["alpha"])
        wait_for(lambda: leases() == 0, "shared waiter result consumed")
        f.stop("concurrent-blame")

        _, deadline_port = f.start_blame("deadline-blame", control=gates.path, seconds=3)
        queued = socket.create_connection(("127.0.0.1", deadline_port), timeout=8)
        queued.sendall(f"GET /team/blame-concurrency.git/blame/{oid}?path=gamma HTTP/1.1\r\nHost: localhost\r\n".encode())
        wait_for(lambda: active("deadline-blame") == 1, "incomplete-header admission")
        assert not select.select([queued], [], [], 1)[0]
        first = request(deadline_port, "alpha")
        alpha, _ = gates.entered()
        second = request(deadline_port, "beta")
        beta, _ = gates.entered()
        queued.sendall(b"\r\n")
        wait_for(lambda: admitted("deadline-blame") == 3, "original deadline queued")
        try:
            assert not queued.recv(4096).startswith(b"HTTP/1.1 200")
        except ConnectionResetError:
            pass
        finally:
            queued.close()
        wait_for(lambda: active("deadline-blame") == 2, "original queued deadline expired")
        assert leases() == 2 and not gates.pending()
        assert not select.select([alpha, beta], [], [], 0)[0], "newer holders must still run"
        alpha.sendall(b"G")
        beta.sendall(b"G")
        assert response(first) == (200, expected["alpha"])
        assert response(second) == (200, expected["beta"])
        wait_for(lambda: leases() == 0, "deadline case source cleanup")
        f.stop("deadline-blame")

        # Last active waiter leaves; source/pins survive actual Control stop until
        # the observer acknowledges it, rather than being freed by HTTP teardown.
        _, stop_port = f.start_blame("stop-ack-blame", control=gates.path, seconds=2)
        abandoned = request(stop_port, "alpha")
        alpha, reader = gates.entered()
        abandoned.close()
        wait_for(lambda: waiters("stop-ack-blame") == 0, "last waiter disconnected from real job")
        assert leases() == 1
        assert reader.readline() == b"stop-requested\n"
        assert leases() == 1 and admitted("stop-ack-blame") == 1
        f.assert_no_repository_bridge()
        alpha.sendall(b"A")
        wait_for(lambda: leases() == 0, "actual stopped job released source")
        healthy = request(stop_port, "delta")
        delta, _ = gates.entered()
        delta.sendall(b"G")
        assert response(healthy) == (200, expected["delta"])
        wait_for(lambda: leases() == 0, "reusable execution after stop acknowledgment")
        f.stop("stop-ack-blame")
    finally:
        gates.close()
    f.results["bounded_concurrent_blame"] = {"implementation": "real in-process root/core",
        "two_running_one_unmaterialized_waiter": True, "overflow503": True,
        "queued_and_active_disconnect": True, "shared_waiter_ownership": True,
        "original_queued_deadline": True, "error_and_panic_no_partial200": True,
        "source_pins_until_stop_ack": True}


if __name__ == "__main__":
    import sys
    from integration import main
    sys.argv.insert(1, "--blame-concurrency-only")
    main()
