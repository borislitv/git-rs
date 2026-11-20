#!/usr/bin/env python3
"""Owned large-v1 body I/O regression with a delayed, real VersityGW exchange."""
import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import select
import signal
import socket
import socketserver
import sys
import threading
import time
from urllib.parse import urlsplit

from warm_reconcile_runtime import Fixture, eventually, http, require, stop


@contextmanager
def delay_first_exchange(env, check_renewal=None):
    """Forward bytes unchanged (including signed Host); delay only cache traffic."""
    key = "GIT_RS_CACHE_S3_ENDPOINT"
    gateway = urlsplit(env[key]).port
    claimed, forwarded = threading.Event(), threading.Event()
    lock = threading.Lock()
    errors = []

    class Relay(socketserver.BaseRequestHandler):
        def handle(self):
            try:
                self.request.settimeout(5)
                first = self.request.recv(65536)
                if not first:
                    return
                with lock:
                    delay = not claimed.is_set()
                    claimed.set()
                if delay:
                    started = time.monotonic()
                    if check_renewal:
                        check_renewal()
                    time.sleep(max(0, 2.20 - (time.monotonic() - started)))
                with socket.create_connection(("127.0.0.1", gateway), timeout=5) as upstream:
                    upstream.sendall(first)
                    if delay:
                        forwarded.set()
                    while ready := select.select([self.request, upstream], [], [], 5)[0]:
                        for source in ready:
                            data = source.recv(65536)
                            if not data:
                                return
                            (upstream if source is self.request else self.request).sendall(data)
            except OSError:
                return  # The actual publication/hit assertions detect premature close.
            except RuntimeError as failure:
                errors.append(str(failure))

    with socketserver.ThreadingTCPServer(("127.0.0.1", 0), Relay) as server:
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield dict(env, **{key: f"http://127.0.0.1:{server.server_address[1]}"})
            require(forwarded.is_set() and not errors, f"delayed cache exchange did not complete: {errors}")
        finally:
            server.shutdown()
            thread.join(timeout=5)


class LargeBody(Fixture):
    def assert_renewal(self):
        # Only the ordinary complete-body claim is eligible. Ignore the opaque
        # payload lease and optional map/seed rows.
        def expiry():
            return self.cache_sql("SELECT extract(epoch FROM max(lease_until))::numeric(20,6) "
                                  f"FROM git_rs_cache.blame_builds WHERE namespace='{self.namespace}' "
                                  "AND position(convert_to('complete-blame-v2','UTF8') in canonical)>0;")

        first = expiry()
        require(bool(first), "no signed body build at first chunk exchange")
        eventually(lambda: expiry() > first, "ordinary build lease did not renew during chunk I/O", seconds=3)

    def exercise(self):
        self.setup()
        settings = dict(GIT_RS_BLAME_RESOURCE_PROFILE="large-v1", GIT_RS_CACHE_MEMORY_BYTES=str(384 << 20))
        self.warmer.update(settings)
        self.foreground.update(settings)
        self.grant("alice", "reader")
        self.checkout.mkdir()
        self.native("init", "--quiet", "--initial-branch=main")
        args = [self.cli, "serve", "--socket", self.directory / "receive.sock", "--health", "127.0.0.1:0",
                "--dev-http", "127.0.0.1:0", "--dev-principal", "writer"]
        with self.service("push", args, self.writer) as (health, listener):
            eventually(lambda: http(health, "/readyz")[0] == 200, "receive metadata not ready")
            # 20 MiB + porcelain metadata; one commit, no imported evidence.
            lines = (b"x" * 4095 + b"\n") * 5120
            tip = self.push(listener, lines, "large body")
            self.drain()
        require(not self.bodies(), "cache must be cold before worker execution")
        with delay_first_exchange(self.warmer, self.assert_renewal) as self.warmer:
            self.once()
        warmed = self.bodies()
        require(set(warmed) == {("alice", tip)}, "delayed warm pass did not confirm the body")
        root = warmed["alice", tip][0]
        require(int(self.cache_sql(f"SELECT children FROM git_rs_cache.manifest_roots WHERE payload='{root}'")) >= 5,
                "fixture did not exercise a multi-chunk manifest")
        with delay_first_exchange(self.foreground) as self.foreground:
            self.verify("delayed-exact-hit", "alice", tip, True, maximum=32 << 20)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "workdir", "git", "binary", "observer"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--owned-loopback", required=True, choices=["synthetic-only"])
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, stop)
    LargeBody(args).exercise()
    print("large signed-body loopback contract passed: delayed staging, renewal and exact hit")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, KeyError) as error:
        sys.exit(f"large signed-body contract failed: {error}")
