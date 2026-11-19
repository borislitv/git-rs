#!/usr/bin/env python3
"""Opt-in multi-quantum loopback contract using the real scoped warm CLI."""
import argparse
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from warm_reconcile_runtime import Fixture, eventually, http, require, stop

CHUNK_BYTES = 4 * 1024**2


class Initial(Fixture):
    def audit(self):
        # Fixture-only instrumentation: a committed deletion of a scoped hold
        # witnesses the boundary without adding a production test gate or lease.
        self.admin("CREATE TABLE git_rs.fixture_warm_quantum (released timestamptz NOT NULL);"
                   "CREATE FUNCTION git_rs.fixture_warm_release() RETURNS trigger LANGUAGE plpgsql AS $$ "
                   "BEGIN INSERT INTO git_rs.fixture_warm_quantum VALUES(clock_timestamp()); RETURN OLD; END $$;"
                   "CREATE TRIGGER fixture_warm_release AFTER DELETE ON git_rs.warm_reader_holds "
                   "FOR EACH ROW EXECUTE FUNCTION git_rs.fixture_warm_release();")

    def count(self):
        return int(self.admin("SELECT count(*) FROM git_rs.fixture_warm_quantum;"))

    @contextmanager
    def child(self):
        with subprocess.Popen([self.cli, "warm-reconcile", "--once"], env=self.warmer,
                              stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, start_new_session=True) as child:
            try:
                yield child
            finally:
                if child.poll() is None:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)

    def between(self, child, expected):
        def released():
            require(child.poll() is None, "worker exited before the intended quantum boundary")
            return self.count() == expected

        eventually(released, "quantum did not release scoped hold")
        require(self.admin(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id='{self.repo}'") == "0",
                "worker retained a source hold between quanta")
        require(not self.bodies(), "a partial quantum published a body")

    def compressed_parts(self, body, expected):
        root = self.cache_sql("SELECT (p.state='live' AND p.object_key IS NULL AND p.body IS NOT NULL "
                              "AND octet_length(p.body)=COALESCE(p.stored_size,p.size) "
                              "AND sha256(p.body)=COALESCE(p.stored_digest,p.digest)) IS TRUE "
                              "FROM git_rs_cache.payloads p "
                              f"WHERE p.namespace='{self.namespace}' AND p.id='{body}';")
        require(root == "t", "signed body descriptor must be stored inline with its stored digest")
        rows = self.cache_sql("SELECT c.ordinal,c.size,encode(c.digest,'hex'),p.stored_size, "
                              "(p.state='live' AND p.object_key IS NULL AND p.body IS NOT NULL "
                              "AND p.encoding='zstd-v1' AND p.stored_size<p.size "
                              "AND octet_length(p.body)=p.stored_size AND sha256(p.body)=p.stored_digest "
                              "AND p.size=c.size AND p.digest=c.digest) IS TRUE "
                              "FROM git_rs_cache.manifest_children c LEFT JOIN git_rs_cache.payloads p "
                              "ON p.namespace=c.namespace AND p.id=c.child "
                              f"WHERE c.namespace='{self.namespace}' AND c.parent='{body}' ORDER BY c.ordinal;").splitlines()
        require(2 <= len(rows) == (len(expected) + CHUNK_BYTES - 1) // CHUNK_BYTES,
                "expected multipart body with the same logical length as Git")
        stored_bytes = 0
        for index, row in enumerate(rows):
            ordinal, size, digest, stored, valid = row.split("|")
            part = expected[index * CHUNK_BYTES:(index + 1) * CHUNK_BYTES]
            require(valid == "t" and int(ordinal) == index and int(size) == len(part)
                    and digest == hashlib.sha256(part).hexdigest(),
                    f"body part {index} must be smaller zstd inline with exact logical and stored digests")
            stored_bytes += int(stored)
        print(f"PostgreSQL cached {len(rows)} compressed body parts: {len(expected)} logical, {stored_bytes} stored bytes")

    def exercise(self):
        self.setup()
        self.grant("alice", "reader")
        self.checkout.mkdir()
        self.native("init", "--quiet", "--initial-branch=main")
        args = [self.cli, "serve", "--socket", self.directory / "receive.sock", "--health", "127.0.0.1:0",
                "--dev-http", "127.0.0.1:0", "--dev-principal", "writer"]
        with self.service("push", args, self.writer) as (health, listener):
            eventually(lambda: http(health, "/readyz")[0] == 200, "receive metadata not ready")
            for index in range(7):
                # A changed line at each edge, plus one stable since root.
                contents = f"stable\nline {index}\nlast\n".encode()
                if self.all_pg:
                    # Repeated porcelain metadata makes this a multipart body.
                    contents += b"stable synthetic PostgreSQL cache line\n" * 32_768
                tip = self.push(listener, contents, f"change {index}")
            self.drain()

        self.audit()
        self.warmer.update(GIT_RS_BLAME_RESOURCE_PROFILE="large-v1", GIT_RS_WARM_JOB_SECONDS="4",
                           GIT_RS_WARM_INITIAL_SECONDS="60", GIT_RS_WARM_SEGMENT_EDGES="2",
                           GIT_RS_WARM_INITIAL_SEGMENTS="8", GIT_RS_WARM_QUANTUM_PAUSE_MS="5000")
        self.foreground.update(GIT_RS_BLAME_RESOURCE_PROFILE="large-v1")

        with self.child() as child:
            self.between(child, 1)
            self.grant("alice", "none")
            require(child.wait(timeout=12) != 0, "revocation did not stop the aggregate run")
        self.drain()
        require(self.count() == 1 and not self.bodies(), "revocation continued or published a stale prefix")
        self.grant("alice", "reader")

        with self.child() as child:
            self.between(child, 2)
            child.terminate()
            require(child.wait(timeout=6) == -signal.SIGTERM, "worker did not terminate between quanta")
        self.drain()
        require(not self.bodies(), "cancelled partial run published")

        self.warmer["GIT_RS_WARM_INITIAL_MIB"] = "64"
        with self.child() as child:
            require(child.wait(timeout=10) != 0, "worker bypassed initial memory admission")
        require(self.count() == 2 and not self.bodies(), "admission failure acquired source or published")
        del self.warmer["GIT_RS_WARM_INITIAL_MIB"]

        self.warmer["GIT_RS_WARM_INITIAL_SEGMENTS"] = "1"
        with self.child() as child:
            require(child.wait(timeout=12) != 0, "segment exhaustion was reported as success")
        self.drain()
        require(self.count() == 3 and not self.bodies(), "segment limit did not stop before publication")
        self.warmer["GIT_RS_WARM_INITIAL_SEGMENTS"] = "8"

        started = time.monotonic()
        self.once()
        # Three 2-edge windows (the last reaches root), then finalization; each pause
        # exceeds the original piece's deadline and owns no source lease.
        require(self.count() == 7 and time.monotonic() - started >= 15,
                "worker did not finish in independent renewed quanta")
        bodies = self.bodies()
        require(set(bodies) == {("alice", tip)}, "local run did not confirm the ordinary body")
        require(not self.maps(), "initial worker issued unrelated cache material")
        expected = self.oracle(tip)
        if self.all_pg:
            self.compressed_parts(bodies[("alice", tip)][0], expected)
        self.verify("initial-exact-hit", "alice", tip, True,
                    maximum=16 * 1024**2 if self.all_pg else 256 * 1024, expected=expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "workdir", "git", "binary", "observer"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--owned-loopback", required=True, choices=["synthetic-only"])
    parser.add_argument("--cache-placement", choices=["pg-s3", "all-pg"], default="pg-s3")
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, stop)
    Initial(args).exercise()
    print(f"initial warm contract passed ({args.cache_placement}): scoped renewal, cancellation, admission, exact native hit")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, KeyError, subprocess.TimeoutExpired) as error:
        sys.exit(f"initial warm contract failed: {error}")
