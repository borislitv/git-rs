#!/usr/bin/env python3
"""Owned PostgreSQL/S3 and independent-process contracts for opaque derived artifacts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import signal
import socket
import subprocess
import threading
import time
import uuid

from integration import PG_IMAGE, S3_IMAGE
from support.cache_faults import ObjectProxy, PgProxy

ROOT = Path(__file__).resolve().parents[1]

PG_CONTRACTS = {
    "real_source_revocation_and_lease_expiry_reject_cached_bytes",
    "payload_and_namespace_quotas_leave_source_operations_usable",
    "binary_payload_reuses_across_independent_pools_and_never_bypasses_authority",
    "simultaneous_claims_have_one_live_owner_and_expired_tokens_cannot_return",
    "corrupt_inline_payload_is_a_miss_and_replacement_is_verified",
    "retirement_preserves_an_admitted_reader_and_expired_hold_cannot_consume",
    "cancelled_waiter_keeps_actual_work_admitted_and_second_waiter_receives_result",
    "cached_lookup_isolates_all_key_dimensions_and_stale_invalidation_cannot_remove_replacement",
    "last_cancelled_waiter_retains_source_until_cooperative_cpu_deadline_and_cleanup",
    "panic_oversized_result_and_revoked_source_do_not_publish_or_poison_admission",
}


def run(args, *, env=None, data=None, timeout=30, ok=True):
    result = subprocess.run([str(arg) for arg in args], input=data, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if ok and result.returncode:
        raise AssertionError(f"{args[:2]} failed ({result.returncode}): {result.stdout[-6000:]!r} {result.stderr[-6000:]!r}")
    return result


def wait_until(check, message, seconds=20):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = check()
        if value:
            return value
        time.sleep(.02)
    raise AssertionError(f"deadline: {message}")


class Process:
    def __init__(self, fixture, env, name):
        self.log = (fixture.base / f"{name}.stderr").open("xb")
        self.child = subprocess.Popen([fixture.process_binary], env=env, stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=self.log, start_new_session=True)
        self.lines = queue.Queue()
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()
        fixture.processes.append(self)
        assert self.receive() == "ready"

    def read(self):
        for line in self.child.stdout:
            self.lines.put(line.decode().strip())
        self.lines.put(None)

    def receive(self, seconds=25):
        line = self.lines.get(timeout=seconds)
        assert line is not None, f"fixture exited: {self.child.poll()}"
        return line

    def send(self, command):
        self.child.stdin.write((command + "\n").encode())
        self.child.stdin.flush()

    def ask(self, command):
        self.send(command)
        return self.receive()

    def close(self):
        if self.child.poll() is None:
            self.child.terminate()
            try:
                self.child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.child.kill()
        self.child.wait(timeout=5)
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        self.child.stdin.close()
        self.child.stdout.close()
        self.log.close()


class Fixture:
    def __init__(self, args):
        self.identity = str(uuid.uuid4())
        parent = ROOT / ".local"
        parent.mkdir(exist_ok=True)
        self.base = parent / ("derived-cache-" + self.identity)
        self.base.mkdir(mode=0o700)
        self.binary = str(args.binary.resolve())
        self.process_binary = str(args.process_binary.resolve())
        self.contract_binary = args.contract_binary
        self.contract_filter = getattr(args, "contract_filter", None)
        self.extra_contracts = [("cache_coordinator", getattr(args, "coordinator_contract_binary", None)),
                                ("cache_budget", getattr(args, "budget_contract_binary", None))]
        self.containers, self.processes = [], []
        self.results = {"completed": False, "fixture": self.identity, "cases": {}}
        self.cpus = ",".join(map(str, sorted(os.sched_getaffinity(0))[:2]))
        self.env = {key: value for key, value in os.environ.items() if key in ("PATH", "LANG", "CARGO_HOME", "CARGO_TARGET_DIR", "CARGO_NET_OFFLINE")}
        self.env.update(PGHOST="127.0.0.1", PGUSER="postgres", PGDATABASE="git_rs",
                        PGPASSWORD="development-only", PGSSLMODE="disable", PGCONNECT_TIMEOUT="2",
                        GIT_RS_DEV_HTTP="1", GIT_RS_CACHE_PGUSER="git_rs_cache",
                        GIT_RS_CACHE_PGPASSWORD="synthetic-cache-only")

    def container(self, memory, cpus, pids, arguments):
        labels = ["--label", "git-rs.cache-fixture=" + self.identity]
        if os.environ.get("GIT_RS_TEST_OWNER_LABEL"):
            labels += ["--label", os.environ["GIT_RS_TEST_OWNER_LABEL"]]
        cid = run(["docker", "run", "-d", *labels, "--cpuset-cpus", self.cpus,
                   "--cpus", str(cpus), "--memory", memory, "--memory-swap", memory,
                   "--pids-limit", str(pids), *arguments]).stdout.decode().strip()
        self.containers.append(cid)
        return cid

    @staticmethod
    def port(cid, port):
        return json.loads(run(["docker", "inspect", cid]).stdout)[0]["NetworkSettings"]["Ports"][port][0]["HostPort"]

    def sql(self, sql, env=None, ok=True):
        return run(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], env=env or self.env,
                   data=sql.encode(), ok=ok).stdout.decode().strip()

    def setup(self):
        self.pg = self.container("1024m", .5, 64, ["--tmpfs", "/var/lib/postgresql:rw,size=512m",
            "-e", "POSTGRES_PASSWORD=development-only", "-e", "POSTGRES_DB=git_rs", "-p", "127.0.0.1::5432", PG_IMAGE])
        self.env["PGPORT"] = self.port(self.pg, "5432/tcp")
        wait_until(lambda: self.sql("SELECT 1", ok=False) == "1", "primary PostgreSQL")
        self.sql("CREATE ROLE git_rs_cache LOGIN PASSWORD 'synthetic-cache-only'; CREATE DATABASE cache_other; GRANT CREATE ON DATABASE git_rs,cache_other TO git_rs_cache;")
        run([self.binary, "migrate"], env=self.env)
        run([self.binary, "create", "synthetic/cache-source", "alice"], env=self.env)
        self.pg2 = self.container("512m", .25, 48, ["--tmpfs", "/var/lib/postgresql:rw,size=256m",
            "-e", "POSTGRES_PASSWORD=development-only", "-e", "POSTGRES_DB=git_rs", "-p", "127.0.0.1::5432", PG_IMAGE])
        self.other = dict(self.env, PGPORT=self.port(self.pg2, "5432/tcp"))
        wait_until(lambda: self.sql("SELECT 1", self.other, ok=False) == "1", "independent PostgreSQL cluster")
        self.sql("CREATE ROLE git_rs_cache LOGIN PASSWORD 'synthetic-cache-only'; GRANT CREATE ON DATABASE git_rs TO git_rs_cache;", self.other)
        self.s3 = self.container("256m", .25, 32, ["--tmpfs", "/data:rw,size=128m", "-p", "127.0.0.1::7070",
            "-e", "ROOT_ACCESS_KEY_ID=test-cache-access", "-e", "ROOT_SECRET_ACCESS_KEY=synthetic-cache-secret",
            S3_IMAGE, "--quiet", "posix", "/data"])
        self.endpoint = "http://127.0.0.1:" + self.port(self.s3, "7070/tcp")
        wait_until(lambda: self.object_request("PUT", "derived-test", "", ok=False).returncode == 0, "cache S3 bucket")
        self.object_request("PUT", "source-test", "")
        marker = self.base / "source-marker"
        marker.write_bytes(b"authoritative source sentinel\n")
        self.object_request("PUT", "source-test", "repos/protected/sentinel", data=marker)
        self.env.update(GIT_RS_CACHE_S3_ENDPOINT=self.endpoint, GIT_RS_CACHE_S3_BUCKET="derived-test",
                        GIT_RS_CACHE_S3_ACCESS_KEY="test-cache-access", GIT_RS_CACHE_S3_SECRET_KEY="synthetic-cache-secret")

    def object_request(self, method, bucket, key, *, data=None, ok=True):
        config = (f'url = "{self.endpoint}/{bucket}/{key}"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
                  'user = "test-cache-access:synthetic-cache-secret"\n')
        args = ["curl", "--disable", "--silent", "--show-error", "--fail", "--max-time", "5", "--config", "-", "--request", method]
        if data is not None:
            args += ["--upload-file", str(data)]
        return run(args, data=config.encode(), ok=ok)

    def environment(self, mode="same", **settings):
        env = dict(self.env, GIT_RS_CACHE_NAMESPACE=str(uuid.uuid4()), GIT_RS_CACHE_OWNER=str(uuid.uuid4()))
        if mode != "same":
            port = self.other["PGPORT"] if mode == "cluster" else self.env["PGPORT"]
            database = "git_rs" if mode == "cluster" else "cache_other"
            env["GIT_RS_CACHE_DSN"] = f"host=127.0.0.1 port={port} user=git_rs_cache password=synthetic-cache-only dbname={database}"
        env.update(settings)
        run([self.binary, "cache-migrate"], env=env)
        return env

    def process(self, env, name):
        return Process(self, env, name)

    def run_contracts(self):
        env = self.environment()
        env["GIT_RS_TEST_CACHE_ADMIN_DSN"] = f"host=127.0.0.1 port={env['PGPORT']} user=postgres password=development-only dbname=git_rs"
        suites = [("cache_contract", self.contract_binary), *self.extra_contracts]
        if self.contract_filter:
            suites = suites[:1]
        for name, binary in suites:
            command = [str(binary)] if binary else ["cargo", "test", "--locked", "--test", name, "--"]
            if self.contract_filter:
                command.append(self.contract_filter)
            listed = run([*command, "--ignored", "--list"], env=env, timeout=30).stdout.decode()
            names = {line.removesuffix(": test") for line in listed.splitlines() if line.endswith(": test")}
            assert names, f"{name} contains no fixture contracts"
            if name == "cache_contract":
                expected_names = {name for name in PG_CONTRACTS if not self.contract_filter or self.contract_filter in name}
                assert names == expected_names, f"contract inventory changed: missing={expected_names - names}, extra={names - expected_names}"
            result = run([*command, "--ignored", "--test-threads=1"], env=env, timeout=180, ok=False)
            (self.base / (name + ".log")).write_bytes(result.stdout + result.stderr)
            assert result.returncode == 0, (result.stdout + result.stderr).decode()
            expected = f"{len(names)} passed; 0 failed; 0 ignored".encode()
            assert expected in result.stdout, f"{name} fixture contracts did not all execute"
            self.results.setdefault("contract_inventory", {})[name] = sorted(names)
        self.results["cases"]["public_api_pg_contracts"] = "passed"

    @staticmethod
    def assert_hit(reply, size, byte):
        parts = reply.split()
        assert parts[:3] == ["hit", str(size), hashlib.sha256(bytes([byte]) * size).hexdigest()], reply
        return parts[3]

    def cross_process(self):
        for mode in ("same", "database", "cluster"):
            env = self.environment(mode)
            a, b = self.process(env, mode + "-a"), self.process(env, mode + "-b")
            a.send("claim shared")
            b.send("claim shared")
            result = [a.receive(), b.receive()]
            assert sorted(result) == ["claimed", "pending"], result
            owner, reader = (a, b) if result[0] == "claimed" else (b, a)
            assert owner.ask("publish shared 65536 173") == "true"
            self.assert_hit(reader.ask("get shared"), 65536, 173)
            owner.close()
            replacement = self.process(env, mode + "-restart")
            self.assert_hit(replacement.ask("get shared"), 65536, 173)
            assert reader.ask("deny") == "ok"
            assert reader.ask("get shared").startswith("error fixture authorization")
            assert replacement.ask("get other-input") == "miss"
            replacement.close()
            reader.close()
            self.results["cases"]["independent_process_" + mode] = "passed"

    def stale_owner(self):
        env = self.environment(GIT_RS_CACHE_LEASE_SECONDS="1", GIT_RS_CACHE_JOB_SECONDS="4")
        a, b = self.process(env, "stale-a"), self.process(env, "stale-b")
        assert a.ask("claim stale") == "claimed"
        os.kill(a.child.pid, signal.SIGSTOP)
        wait_until(lambda: self.sql(f"SELECT count(*) FROM git_rs_cache.jobs WHERE namespace='{env['GIT_RS_CACHE_NAMESPACE']}' AND lease_until>clock_timestamp()") == "0", "old owner DB-clock expiry")
        assert b.ask("claim stale") == "claimed"
        assert b.ask("publish stale 16384 42") == "true"
        os.kill(a.child.pid, signal.SIGCONT)
        assert a.ask("renew stale") == "false"
        assert a.ask("publish stale 16384 99") == "false"
        self.assert_hit(b.ask("get stale"), 16384, 42)
        assert a.ask("claim killed") == "claimed"
        a.child.kill()
        a.child.wait(timeout=5)
        wait_until(lambda: self.sql(f"SELECT count(*) FROM git_rs_cache.jobs WHERE namespace='{env['GIT_RS_CACHE_NAMESPACE']}' AND state='computing' AND lease_until>clock_timestamp()") == "0", "dead owner expiry")
        assert b.ask("claim killed") == "claimed"
        assert b.ask("publish killed 32768 17") == "true"
        self.assert_hit(b.ask("get killed"), 32768, 17)
        self.results["cases"]["stale_and_killed_owner"] = "passed"
        a.close()
        b.close()

    def expiry_after_lock_wait(self):
        env = self.environment(GIT_RS_CACHE_LEASE_SECONDS="1", GIT_RS_CACHE_JOB_SECONDS="4")
        ns = env["GIT_RS_CACHE_NAMESPACE"]
        process = self.process(env, "lock-expiry")
        assert process.ask("claim locked") == "claimed"
        locker = subprocess.Popen(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], env=self.env,
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            locker.stdin.write((f"UPDATE git_rs_cache.jobs SET lease_until=clock_timestamp()+interval '500 milliseconds' WHERE namespace='{ns}';\n"
                                f"BEGIN; SELECT 'locked' FROM git_rs_cache.jobs WHERE namespace='{ns}' FOR UPDATE;\n").encode())
            locker.stdin.flush()
            assert locker.stdout.readline().strip() == b"locked"
            process.send("renew locked")
            wait_until(lambda: int(self.sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='git-rs-cache' AND wait_event_type='Lock'")) > 0,
                       "actual cache job row-lock wait", seconds=1)
            wait_until(lambda: self.sql(f"SELECT lease_until<=clock_timestamp() FROM git_rs_cache.jobs WHERE namespace='{ns}'") == "t",
                       "lease expiry while its renewal waits for lock", seconds=1)
            locker.stdin.write(b"COMMIT;\n\\q\n")
            locker.stdin.flush()
            assert process.receive() == "false", "renewal must check DB time after acquiring its row lock"
            assert locker.wait(timeout=5) == 0
            self.results["cases"]["expiry_after_lock_wait"] = "passed"
        finally:
            if locker.poll() is None:
                locker.kill()
            locker.wait(timeout=5)
            for stream in (locker.stdin, locker.stdout, locker.stderr):
                stream.close()
            process.close()

    def s3_reader_and_gc(self):
        env = self.environment(GIT_RS_CACHE_GRACE_SECONDS="1")
        ns = env["GIT_RS_CACHE_NAMESPACE"]
        a, b = self.process(env, "s3-reader"), self.process(env, "s3-gc")
        assert a.ask("claim large") == "claimed"
        assert a.ask("publish large 524288 81") == "true"
        payload = self.assert_hit(a.ask("hold large"), 524288, 81)
        assert a.ask("invalidate large") == "true"
        assert b.ask("maintenance") == "owned"
        assert b.ask("retire") == "1"
        self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{ns}'")
        assert b.ask("delete") == "0"
        self.assert_hit(a.ask("consume large"), 524288, 81)
        assert b.ask("get large") == "miss"
        assert a.ask("release large") == "ok"
        assert b.ask("delete") == "1"
        assert self.object_request("GET", "derived-test", f"cache/{ns}/{payload}", ok=False).returncode != 0
        assert self.object_request("GET", "source-test", "repos/protected/sentinel").stdout == b"authoritative source sentinel\n"
        fresh = self.environment("database")
        c = self.process(fresh, "cutover")
        assert c.ask("claim current") == "claimed"
        assert c.ask("publish current 524288 82") == "true"
        self.sql(f"UPDATE git_rs_cache.payloads SET last_delete_at=clock_timestamp()-interval '31 seconds' WHERE namespace='{ns}'")
        assert b.ask("delete") == "1"
        self.assert_hit(c.ask("get current"), 524288, 82)
        self.results["cases"]["reader_retirement_and_namespace_cutover"] = "passed"
        for process in (a, b, c):
            process.close()

    def coalesced_processes(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(10)
        env = self.environment(GIT_RS_CACHE_LEASE_SECONDS="1", GIT_RS_CACHE_JOB_SECONDS="5")
        producer_env = dict(env, GIT_RS_TEST_COMPUTE_GATE=f"127.0.0.1:{listener.getsockname()[1]}")
        a, b = self.process(producer_env, "compute-a"), self.process(env, "compute-b")
        try:
            a.send("compute derived 65536 33")
            assert a.receive() == "computing"
            gate, _ = listener.accept()
            with gate:
                b.send("compute derived 65536 33")
                # Observe actual heartbeat while computation is paused. This
                # demonstrates there is no held SQL connection/transaction.
                old_expiry = self.sql(f"SELECT lease_until FROM git_rs_cache.jobs WHERE namespace='{env['GIT_RS_CACHE_NAMESPACE']}'")
                wait_until(lambda: self.sql(f"SELECT lease_until FROM git_rs_cache.jobs WHERE namespace='{env['GIT_RS_CACHE_NAMESPACE']}'") != old_expiry, "job heartbeat")
                gate.sendall(b"G")
            self.assert_hit(a.receive(), 65536, 33)
            self.assert_hit(b.receive(), 65536, 33)
            self.results["cases"]["shared_compute_and_heartbeat"] = "passed"
        finally:
            listener.close()
            a.close()
            b.close()

    def binary_faults(self):
        proxy = PgProxy(self.env["PGPORT"])
        try:
            env = self.environment(GIT_RS_CACHE_PGPORT=str(proxy.server_address[1]))
            process = self.process(env, "binary-faults")
            proxy.arm("git_rs_cache.claim(", "commit")
            assert process.ask("claim ambiguous") == "claimed"
            assert proxy.hit.wait(timeout=5), "claim COMMIT fault did not fire"
            proxy.arm("git_rs_cache.prepare_payload(", "commit")
            assert process.ask("publish ambiguous 65536 73") == "true"
            assert proxy.hit.wait(timeout=5), "intent COMMIT fault did not fire"
            assert process.ask("claim publication") == "claimed"
            proxy.arm("git_rs_cache.publish(", "commit")
            assert process.ask("publish publication 65536 74") == "true"
            assert proxy.hit.wait(timeout=5), "publication COMMIT fault did not fire"
            self.assert_hit(process.ask("get publication"), 65536, 74)
            proxy.arm("SELECT p.body FROM", "truncate")
            assert process.ask("get publication") == "miss", "partial binary payload must not escape"
            assert proxy.hit.wait(timeout=5), "binary response fault did not fire"
            assert process.ask("claim publication") == "claimed"
            assert process.ask("publish publication 65536 75") == "true"
            for _ in range(8):
                self.assert_hit(process.ask("get publication"), 65536, 75)
            process.close()
            self.results["cases"]["binary_commit_confirmation_and_interrupted_session"] = "passed"
        finally:
            proxy.close()

    def binary_deadline_and_pool_recovery(self):
        proxy = PgProxy(self.env["PGPORT"])
        try:
            env = self.environment(GIT_RS_CACHE_PGPORT=str(proxy.server_address[1]), GIT_RS_CACHE_CONNECTIONS="1")
            writer = self.process(env, "binary-deadline-writer")
            assert writer.ask("claim deadline") == "claimed"
            assert writer.ask("publish deadline 65536 76") == "true"
            reader = self.process(dict(env, GIT_RS_TEST_OPERATION_SECONDS="1"), "binary-deadline-reader")
            proxy.arm("SELECT p.body FROM", "stall")
            reader.send("get deadline")
            assert proxy.hit.wait(timeout=5), "binary payload deadline boundary was not reached"
            started = time.monotonic()
            assert reader.receive(seconds=4).startswith("error "), "a stalled binary payload must respect the request deadline"
            assert time.monotonic() - started < 4
            proxy.release.set()
            for _ in range(3):
                self.assert_hit(reader.ask("get deadline"), 65536, 76)
            writer.close()
            reader.close()
            self.results["cases"]["binary_payload_deadline_and_single_connection_pool_recovery"] = "passed"
        finally:
            proxy.close()

    def s3_body_faults_and_read_expiry(self):
        env = self.environment()
        process = self.process(env, "s3-body-faults")
        ns = env["GIT_RS_CACHE_NAMESPACE"]
        for name in ("missing", "corrupt", "oversized"):
            assert process.ask(f"claim {name}") == "claimed"
            assert process.ask(f"publish {name} 524288 83") == "true"
            payload = self.assert_hit(process.ask(f"get {name}"), 524288, 83)
            object_key = f"cache/{ns}/{payload}"
            if name == "missing":
                self.object_request("DELETE", "derived-test", object_key)
            else:
                body = self.base / ("body-" + name)
                body.write_bytes(bytes([84]) * (524289 if name == "oversized" else 524288))
                self.object_request("PUT", "derived-test", object_key, data=body)
            assert process.ask(f"get {name}") == "miss", f"{name} S3 bytes must never escape verification"
            assert process.ask(f"claim {name}") == "claimed"
            assert process.ask(f"publish {name} 524288 85") == "true"
            new_payload = self.assert_hit(process.ask(f"get {name}"), 524288, 85)
            assert new_payload != payload, "corrupt object identities must never be reused"
        process.close()
        proxy = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.environment(GIT_RS_CACHE_READ_SECONDS="1", GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{proxy.server_address[1]}")
            ns = env["GIT_RS_CACHE_NAMESPACE"]
            process = self.process(env, "s3-expired-reader")
            assert process.ask("claim expiring") == "claimed"
            assert process.ask("publish expiring 524288 86") == "true"
            proxy.arm("GET", f"/derived-test/cache/{ns}/")
            process.send("get expiring")
            assert proxy.hit.wait(timeout=5)
            wait_until(lambda: self.sql(f"SELECT count(*) FROM git_rs_cache.read_holds WHERE namespace='{ns}' AND lease_until>clock_timestamp()") == "0", "read hold expiry during actual S3 I/O")
            proxy.release.set()
            reply = process.receive()
            assert reply == "miss", f"expired read hold must be a cache miss before consumption, got {reply!r}"
            process.close()
            self.results["cases"]["s3_missing_corrupt_oversized_and_read_expiry"] = "passed"
        finally:
            proxy.close()

    def delete_permission_failure_is_retryable(self):
        proxy = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.environment(GIT_RS_CACHE_GRACE_SECONDS="1", GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{proxy.server_address[1]}")
            ns = env["GIT_RS_CACHE_NAMESPACE"]
            process = self.process(env, "gc-permissions")
            assert process.ask("claim retired") == "claimed"
            assert process.ask("publish retired 524288 87") == "true"
            payload = self.assert_hit(process.ask("hold retired"), 524288, 87)
            assert process.ask("invalidate retired") == "true"
            assert process.ask("release retired") == "ok"
            assert process.ask("maintenance") == "owned"
            assert process.ask("retire") == "1"
            self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{ns}'")
            proxy.arm("DELETE", f"/derived-test/cache/{ns}/", "forbidden")
            assert process.ask("delete").startswith("error ")
            assert proxy.hit.wait(timeout=5), "cache DELETE permission fault was not exercised"
            assert self.sql(f"SELECT last_delete_at IS NULL FROM git_rs_cache.payloads WHERE namespace='{ns}'") == "t", "failed deletion must retain retry evidence"
            assert self.object_request("GET", "derived-test", f"cache/{ns}/{payload}").stdout == bytes([87]) * 524288
            for versioning in ("Enabled", "Suspended"):
                proxy.arm("GET", "/derived-test?versioning", versioning)
                reply = process.ask("delete")
                assert "unversioned bucket" in reply, f"{versioning} policy must reject non-version-aware GC: {reply}"
                assert proxy.hit.wait(timeout=5), "bucket versioning policy boundary was not exercised"
                assert self.object_request("GET", "derived-test", f"cache/{ns}/{payload}").stdout == bytes([87]) * 524288
                assert self.sql(f"SELECT last_delete_at IS NULL FROM git_rs_cache.payloads WHERE namespace='{ns}'") == "t"
            assert process.ask("delete") == "1"
            assert self.object_request("GET", "derived-test", f"cache/{ns}/{payload}", ok=False).returncode != 0
            assert self.object_request("GET", "source-test", "repos/protected/sentinel").stdout == b"authoritative source sentinel\n"
            process.close()
            self.results["cases"]["delete_permissions_and_versioning_preserve_retry_evidence"] = "passed"
        finally:
            proxy.close()

    def late_upload(self):
        proxy = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.environment(GIT_RS_CACHE_GRACE_SECONDS="1", GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{proxy.server_address[1]}")
            ns = env["GIT_RS_CACHE_NAMESPACE"]
            uploader, gc = self.process(env, "late-uploader"), self.process(env, "late-gc")
            assert uploader.ask("claim late") == "claimed"
            proxy.arm("PUT", f"/derived-test/cache/{ns}/")
            uploader.send("publish late 524288 111")
            assert proxy.hit.wait(timeout=5), "PUT never reached the controlled store boundary"
            object_key = self.sql(f"SELECT object_key FROM git_rs_cache.payloads WHERE namespace='{ns}'")
            assert object_key.startswith(f"cache/{ns}/")
            uploader.child.kill()
            uploader.child.wait(timeout=5)
            self.sql(f"UPDATE git_rs_cache.jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{ns}'")
            assert gc.ask("maintenance") == "owned"
            assert gc.ask("reconcile") == "1"
            self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{ns}'")
            assert gc.ask("delete") == "1"
            proxy.release.set()
            assert proxy.finished.wait(timeout=5)
            assert self.object_request("GET", "derived-test", object_key).stdout == bytes([111]) * 524288
            self.sql(f"UPDATE git_rs_cache.payloads SET last_delete_at=clock_timestamp()-interval '31 seconds' WHERE namespace='{ns}'")
            assert gc.ask("delete") == "1", "late PUT must remain rediscoverable"
            assert self.object_request("GET", "derived-test", object_key, ok=False).returncode != 0
            assert self.sql(f"SELECT count(*) FROM git_rs_cache.upload_intents WHERE namespace='{ns}' AND state='retired'") == "1"
            assert self.object_request("GET", "source-test", "repos/protected/sentinel").stdout == b"authoritative source sentinel\n"
            uploader.close()
            gc.close()
            self.results["cases"]["intent_before_put_and_late_put_redeletion"] = "passed"
        finally:
            proxy.close()

    def slow_publication_keeps_ownership(self):
        proxy = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.environment(GIT_RS_CACHE_LEASE_SECONDS="1", GIT_RS_CACHE_JOB_SECONDS="10",
                                   GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{proxy.server_address[1]}")
            ns = env["GIT_RS_CACHE_NAMESPACE"]
            producer, waiter = self.process(env, "slow-publisher"), self.process(env, "slow-waiter")
            proxy.arm("PUT", f"/derived-test/cache/{ns}/")
            producer.send("compute slow 524288 91")
            assert producer.receive() == "computing"
            assert proxy.hit.wait(timeout=5)
            old_expiry = self.sql(f"SELECT lease_until FROM git_rs_cache.jobs WHERE namespace='{ns}'")
            wait_until(lambda: self.sql(f"SELECT clock_timestamp()>'{old_expiry}'::timestamptz") == "t", "initial lease expiry during slow publication")
            assert self.sql(f"SELECT lease_until>clock_timestamp() FROM git_rs_cache.jobs WHERE namespace='{ns}'") == "t", \
                "healthy computation must retain ownership through slow S3 publication"
            assert waiter.ask("claim slow") == "pending"
            proxy.release.set()
            self.assert_hit(producer.receive(), 524288, 91)
            self.assert_hit(waiter.ask("get slow"), 524288, 91)
            producer.close()
            waiter.close()
            self.results["cases"]["heartbeat_through_slow_publication"] = "passed"
        finally:
            proxy.close()

    def killed_after_put_and_committed_descriptor(self):
        proxy = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.environment(GIT_RS_CACHE_GRACE_SECONDS="1", GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{proxy.server_address[1]}")
            ns = env["GIT_RS_CACHE_NAMESPACE"]
            publisher, gc = self.process(env, "put-crash"), self.process(env, "put-crash-gc")
            assert publisher.ask("claim accepted") == "claimed"
            proxy.arm("PUT", f"/derived-test/cache/{ns}/", "response")
            publisher.send("publish accepted 524288 88")
            assert proxy.hit.wait(timeout=5), "real PUT did not commit before the response gate"
            object_key = self.sql(f"SELECT object_key FROM git_rs_cache.payloads WHERE namespace='{ns}'")
            assert self.object_request("GET", "derived-test", object_key).stdout == bytes([88]) * 524288
            publisher.child.kill()
            publisher.child.wait(timeout=5)
            self.sql(f"UPDATE git_rs_cache.jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{ns}'")
            assert gc.ask("maintenance") == "owned"
            assert gc.ask("reconcile") == "1", "accepted PUT without acknowledgment must remain discoverable"
            self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{ns}'")
            assert gc.ask("delete") == "1"
            assert self.object_request("GET", "derived-test", object_key, ok=False).returncode != 0
            assert self.sql(f"SELECT state FROM git_rs_cache.upload_intents WHERE namespace='{ns}'") == "retired"
            proxy.release.set()
            assert proxy.finished.wait(timeout=5)
            publisher.close()
            gc.close()
        finally:
            proxy.close()
        proxy = PgProxy(self.env["PGPORT"])
        try:
            env = self.environment(GIT_RS_CACHE_PGPORT=str(proxy.server_address[1]))
            ns = env["GIT_RS_CACHE_NAMESPACE"]
            publisher = self.process(env, "descriptor-crash")
            assert publisher.ask("claim committed") == "claimed"
            proxy.arm("git_rs_cache.publish(", "commit-gate")
            publisher.send("publish committed 65536 89")
            assert proxy.hit.wait(timeout=5), "descriptor COMMIT gate did not observe ReadyForQuery"
            assert self.sql(f"SELECT state FROM git_rs_cache.directory WHERE namespace='{ns}'") == "ready"
            publisher.child.kill()
            publisher.child.wait(timeout=5)
            proxy.release.set()
            restarted = self.process(env, "descriptor-restart")
            self.assert_hit(restarted.ask("get committed"), 65536, 89)
            assert restarted.ask("source") == "source 0 0"
            publisher.close()
            restarted.close()
            self.results["cases"]["crash_after_accepted_put_and_descriptor_commit"] = "passed"
        finally:
            proxy.close()

    def cache_outage(self):
        env = self.environment("cluster")
        proxy = PgProxy(self.other["PGPORT"])
        try:
            env["GIT_RS_CACHE_DSN"] = env["GIT_RS_CACHE_DSN"].replace(f"port={self.other['PGPORT']}", f"port={proxy.server_address[1]}")
            process = self.process(env, "cache-outage")
            assert process.ask("source") == "source 0 0"
            with proxy.lock:
                proxy.reject = True
            run(["docker", "stop", "--time", "2", self.pg2])
            before = time.monotonic()
            process.send("compute unavailable 16384 36")
            assert process.receive() == "computing"
            self.assert_hit(process.receive(), 16384, 36)
            assert time.monotonic() - before < 10, "cache outage must permit bounded recomputation"
            breaker_started = time.monotonic()
            with proxy.lock:
                attempts = proxy.accepted
            assert attempts > 0, "initial failure must actually attempt the unavailable cache"
            for index in range(3):
                process.send(f"compute repeated-outage-{index} 16384 37")
                assert process.receive() == "computing"
                self.assert_hit(process.receive(), 16384, 37)
            assert time.monotonic() - breaker_started < .8, "fixture exceeded the one-second circuit-breaker observation window"
            with proxy.lock:
                assert proxy.accepted == attempts, "open circuit must disable repeated cache connections, not merely tolerate their errors"
            assert process.ask("source") == "source 0 0", "cache cluster cannot become a metadata dependency"
            process.close()
            self.results["cases"]["cache_outage_bounded_recompute_source_independence"] = "passed"
        finally:
            proxy.close()

    def close(self):
        errors = []
        for process in self.processes:
            try:
                process.close()
            except Exception as error:
                errors.append(str(error))
        for cid in self.containers:
            diagnostic = run(["docker", "logs", "--tail", "100", cid], ok=False)
            (self.base / (cid[:12] + ".log")).write_bytes(diagnostic.stdout + diagnostic.stderr)
        # The label also recovers a container whose creation response was lost.
        ids = run(["docker", "ps", "-aq", "--filter", "label=git-rs.cache-fixture=" + self.identity]).stdout.decode().split()
        for cid in ids:
            run(["docker", "rm", "-f", cid])
        assert not run(["docker", "ps", "-aq", "--filter", "label=git-rs.cache-fixture=" + self.identity]).stdout.strip()
        self.results["cleanup_errors"] = errors
        self.results["owned_containers_removed"] = True
        (self.base / "report.json").write_text(json.dumps(self.results, indent=2) + "\n")
        assert not errors, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provision", action="store_true", required=True)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/git-rs")
    parser.add_argument("--process-binary", type=Path, default=ROOT / "target/debug/examples/cache-process")
    parser.add_argument("--contract-binary", type=Path)
    parser.add_argument("--coordinator-contract-binary", type=Path)
    parser.add_argument("--budget-contract-binary", type=Path)
    parser.add_argument("--contract-filter", help="run matching primary PG contracts only (for fault qualification)")
    phases = ("contracts", "cross-process", "stale-owner", "lock-expiry", "gc", "coalesced", "binary", "binary-deadline", "s3-bodies", "gc-permissions", "late-upload", "slow-publication", "committed-crashes", "outage")
    parser.add_argument("--phase", choices=phases, action="append", help="run selected phases; default runs the entire gate")
    args = parser.parse_args()
    if not args.process_binary.exists():
        run(["cargo", "build", "--locked", "--example", "cache-process"], timeout=600)
    fixture = Fixture(args)
    def stop(signum, _frame):
        raise InterruptedError(f"fixture interrupted by {signum}")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        fixture.setup()
        actions = (fixture.run_contracts, fixture.cross_process, fixture.stale_owner,
                   fixture.expiry_after_lock_wait, fixture.s3_reader_and_gc,
                   fixture.coalesced_processes, fixture.binary_faults,
                   fixture.binary_deadline_and_pool_recovery, fixture.s3_body_faults_and_read_expiry,
                   fixture.delete_permission_failure_is_retryable, fixture.late_upload,
                   fixture.slow_publication_keeps_ownership,
                   fixture.killed_after_put_and_committed_descriptor, fixture.cache_outage)
        for phase, action in zip(phases, actions, strict=True):
            if args.phase is None or phase in args.phase:
                action()
        fixture.results["completed"] = True
    except BaseException as error:
        fixture.results["error"] = str(error)
        raise
    finally:
        fixture.close()
        print(json.dumps(fixture.results, indent=2), flush=True)


if __name__ == "__main__":
    main()
