#!/usr/bin/env python3
"""Owned PostgreSQL/S3 qualification for finite builds and complete artifacts.

Requires already-built binaries. Run under a bounded controller with an independent
ExecStopPost cleanup for --owner-label; this script never builds or repairs fixtures.
All data is synthetic. A failed assertion is a failed case, not a retry request.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import time
import uuid

from derived_cache import Fixture as OpaqueFixture, Process as LegacyProcess, run, wait_until
from support.blame_artifact_faults import ArtifactProcess, ObjectProxy, PgProxy, SqlLocker, disconnect_and_reject


V2_MIGRATION_CONTRACT = "v2_index_migration_cancellation_and_failure_preserve_retryable_state"


def record_contracts(results, suite, names, result):
    """Credit only a successful, fully executed selection; merge fixture phases."""
    assert result.returncode == 0, (result.stdout + result.stderr).decode()
    summary = rf"^test result: ok\. {len(names)} passed; 0 failed; 0 ignored;"
    assert re.search(summary.encode(), result.stdout, re.MULTILINE), "selected contracts did not all execute"
    inventory = results.setdefault("contract_inventory", {})
    inventory[suite] = sorted(set(inventory.get(suite, [])) | set(names))


class Fixture(OpaqueFixture):
    def __init__(self, args):
        args.process_binary = args.legacy_process_binary or args.binary
        args.contract_binary = args.opaque_contract_binary
        super().__init__(args)
        self.extra_contracts = [(name, binary) for name, binary in self.extra_contracts if binary]
        self.args = args
        self.artifact_binary = args.artifact_contract_binary.resolve()

    def artifact_process(self, env, name):
        return ArtifactProcess(self, env, name)

    def test_environment(self, **settings):
        env = self.environment(**settings)
        env["GIT_RS_TEST_CACHE_ADMIN_DSN"] = self.admin_dsn(self.env)
        return env

    @staticmethod
    def admin_dsn(env):
        return f"host=127.0.0.1 port={env['PGPORT']} user=postgres password=development-only dbname={env['PGDATABASE']}"

    def rust_contracts(self):
        env = self.test_environment()
        for name, binary in (("blame_build", self.args.build_contract_binary),
                             ("blame_artifacts", self.artifact_binary),
                             ("blame_checkpoints", self.args.checkpoint_contract_binary)):
            if binary is None:
                continue
            listed = run([binary, "--ignored", "--list"], env=env).stdout.decode()
            names = {line.removesuffix(": test") for line in listed.splitlines() if line.endswith(": test")}
            names.discard("artifact_process")
            names.discard(V2_MIGRATION_CONTRACT)
            selected = self.args.contract_filter
            if selected:
                if selected not in names:
                    continue
                names = {selected}
            assert names, f"no {name} contracts discovered"
            command = [binary, "--ignored", "--test-threads=1", "--skip", "artifact_process",
                       "--skip", V2_MIGRATION_CONTRACT]
            if selected:
                command += ["--exact", selected]
            result = run(command, env=env, timeout=300, ok=False)
            (self.base / (name + ".log")).write_bytes(result.stdout + result.stderr)
            record_contracts(self.results, name, names, result)
        if self.args.contract_filter:
            assert self.results.get("contract_inventory"), "unknown exact contract filter"
        self.results["cases"]["new_api_contracts"] = "passed"

    @staticmethod
    def assert_complete(reply, pieces):
        digest = hashlib.sha256()
        total = 0
        for size, byte in pieces:
            digest.update(bytes([byte]) * size)
            total += size
        assert reply == f"complete {total} {digest.hexdigest()}", reply

    def ambiguous_commits(self):
        proxy = PgProxy(self.env["PGPORT"])
        try:
            env = self.test_environment(GIT_RS_CACHE_PGPORT=str(proxy.server_address[1]))
            namespace = env["GIT_RS_CACHE_NAMESPACE"]
            process = self.artifact_process(env, "commit-owner")
            for function, command, expected, table in (
                ("claim_blame_build", "build confirmed", "claimed", "blame_builds"),
                ("claim_blame_payload", "stage confirmed 524288 73", "staged", "jobs"),
            ):
                proxy.arm("git_rs_cache." + function + "(", "commit-gate")
                process.send(command)
                assert proxy.hit.wait(timeout=5), f"{function} COMMIT boundary not reached"
                before = self.sql(f"SELECT token::text||':'||generation::text||':'||hard_until::text FROM git_rs_cache.{table} WHERE namespace='{namespace}'")
                assert before and ":1:" in before
                proxy.release.set()
                assert process.receive() == expected
                after = self.sql(f"SELECT token::text||':'||generation::text||':'||hard_until::text FROM git_rs_cache.{table} WHERE namespace='{namespace}'")
                assert after == before, f"{function} confirmation reset its attempt/deadline"

            proxy.arm("git_rs_cache.publish_manifest(", "commit-gate")
            process.send("publish confirmed")
            assert proxy.hit.wait(timeout=5), "manifest COMMIT boundary not reached"
            assert self.sql(f"SELECT count(*) FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'") == "1"
            assert self.sql(f"SELECT count(*) FROM git_rs_cache.manifest_children WHERE namespace='{namespace}'") == "1"
            proxy.release.set()
            assert process.receive() == "true", "unknown manifest COMMIT must confirm one complete descriptor"
            self.assert_complete(process.ask("get confirmed"), [(524288, 73)])

            assert process.ask("build crash") == "claimed"
            assert process.ask("stage crash 65536 74") == "staged"
            proxy.arm("git_rs_cache.publish_manifest(", "commit-gate")
            process.send("publish crash")
            assert proxy.hit.wait(timeout=5)
            process.kill()
            proxy.release.set()
            replacement = self.artifact_process(env, "committed-restart")
            self.assert_complete(replacement.ask("get crash"), [(65536, 74)])
            assert self.sql(f"SELECT count(*) FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'") == "2"
            assert self.sql(f"SELECT count(*) FROM git_rs_cache.manifest_children WHERE namespace='{namespace}'") == "2"
            process.close()
            replacement.close()
            self.results["cases"]["new_function_commit_confirmation_and_crash_restart"] = "passed"
        finally:
            proxy.close()

    def child_get_gc_races(self):
        proxy = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            for expired in (False, True):
                env = self.test_environment(GIT_RS_CACHE_READ_SECONDS="10", GIT_RS_CACHE_GRACE_SECONDS="1",
                    GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{proxy.server_address[1]}")
                namespace = env["GIT_RS_CACHE_NAMESPACE"]
                prefix = "expired" if expired else "live"
                publisher = self.artifact_process(env, prefix + "-publisher")
                reader = self.artifact_process(env, prefix + "-reader")
                gc = self.artifact_process(env, prefix + "-gc")
                assert publisher.ask("build body") == "claimed"
                assert publisher.ask("stage body 524288 81") == "staged"
                assert publisher.ask("stage body 524288 82") == "staged"
                assert publisher.ask("publish body") == "true"
                assert publisher.ask("drop-builds") == "ok"
                root = self.sql(f"SELECT payload::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'")
                child_key = self.sql(f"SELECT p.object_key FROM git_rs_cache.manifest_children e JOIN git_rs_cache.payloads p ON p.namespace=e.namespace AND p.id=e.child WHERE e.namespace='{namespace}' AND e.ordinal=0")
                assert child_key.startswith(f"cache/{namespace}/")
                self.sql(f"UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'; UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}' AND payload<>'{root}'")
                assert gc.ask("maintenance") == "owned"
                assert gc.ask("retire") == "0", "live root directory must retain both children without build pins"
                assert reader.ask("consumers") == "0"
                proxy.arm("GET", "/derived-test/" + child_key)
                reader.send("get body")
                assert proxy.hit.wait(timeout=5), "actual child S3 GET gate was not reached"
                assert self.sql(f"SELECT count(*) FROM git_rs_cache.read_holds WHERE namespace='{namespace}' AND payload='{root}' AND lease_until>clock_timestamp()") == "1"
                self.sql(f"UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'")
                assert gc.ask("retire") == "1", "a live root read hold must retain all children during gated S3 I/O"
                if expired:
                    self.sql(f"UPDATE git_rs_cache.read_holds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'")
                    assert gc.ask("retire") == "2"
                    self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'")
                    assert gc.ask("delete") == "3"
                else:
                    self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}' AND state='retired'")
                    assert gc.ask("delete") == "0"
                proxy.release.set()
                assert proxy.finished.wait(timeout=5)
                reply = reader.receive()
                if expired:
                    assert reply == "miss" or reply.startswith("error "), reply
                    assert reader.ask("consumers") == "0", "expired root must never invoke a successful complete consumer"
                else:
                    self.assert_complete(reply, [(524288, 81), (524288, 82)])
                    assert reader.ask("consumers") == "1"
                    assert gc.ask("retire") == "2", "children are reclaimable only after the root reader drains"
                    self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'")
                    assert gc.ask("delete") == "3"
                fresh = self.test_environment()
                current = self.artifact_process(fresh, prefix + "-current-namespace")
                assert current.ask("build current") == "claimed"
                assert current.ask("stage current 524288 83") == "staged"
                assert current.ask("publish current") == "true"
                self.sql(f"UPDATE git_rs_cache.payloads SET last_delete_at=clock_timestamp()-interval '31 seconds' WHERE namespace='{namespace}'")
                assert gc.ask("delete") == "2", "old GC re-deletes only its permanently retired external children"
                self.assert_complete(current.ask("get current"), [(524288, 83)])
                current.close()
                assert self.object_request("GET", "source-test", "repos/protected/sentinel").stdout == b"authoritative source sentinel\n"
                for process in (publisher, reader, gc):
                    process.close()
                self.results["cases"][prefix + "_root_hold_gated_child_get_gc"] = "passed"
        finally:
            proxy.close()

    def single_pool_outage(self):
        pg = PgProxy(self.env["PGPORT"])
        objects = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.test_environment(GIT_RS_CACHE_CONNECTIONS="1", GIT_RS_CACHE_LEASE_SECONDS="1",
                GIT_RS_CACHE_JOB_SECONDS="8", GIT_RS_CACHE_PGPORT=str(pg.server_address[1]),
                GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{objects.server_address[1]}")
            env.update(GIT_RS_TEST_BUILD_LEASE_SECONDS="1", GIT_RS_TEST_BUILD_HARD_SECONDS="15")
            namespace = env["GIT_RS_CACHE_NAMESPACE"]
            process = self.artifact_process(env, "one-pool")
            assert process.ask("source") == "source 0 0"
            objects.arm("PUT", f"/derived-test/cache/{namespace}/")
            process.send("derive owned 524288 91")
            working = process.receive().split()
            assert working[0] == "working" and len(working) == 2
            repo = working[1]
            assert objects.hit.wait(timeout=5)
            initial = self.sql(f"SELECT lease_until::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'")
            child_initial = self.sql(f"SELECT lease_until::text FROM git_rs_cache.jobs WHERE namespace='{namespace}' AND state='computing'")
            wait_until(lambda: self.sql(f"SELECT b.lease_until>'{initial}'::timestamptz AND clock_timestamp()>greatest('{initial}'::timestamptz,'{child_initial}'::timestamptz) AND b.lease_until>clock_timestamp() AND EXISTS(SELECT 1 FROM git_rs_cache.jobs j WHERE j.namespace=b.namespace AND j.state='computing' AND j.lease_until>'{child_initial}'::timestamptz AND j.lease_until>clock_timestamp()) FROM git_rs_cache.blame_builds b WHERE b.namespace='{namespace}'") == "t", "both heartbeats with one cache connection", seconds=4)
            assert self.sql(f"SELECT count(*) FROM git_rs_cache.jobs WHERE namespace='{namespace}' AND state='computing' AND lease_until>clock_timestamp()") == "1", "payload heartbeat must retain actual upload authority too"
            assert self.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id='{repo}' AND expires_at>clock_timestamp()") == "1"
            before = time.monotonic()
            disconnect_and_reject(pg)
            objects.release.set()
            expected = hashlib.sha256(bytes([91]) * 524288).hexdigest()
            assert process.receive() == f"local 524288 {expected}", "optional cache outage must preserve the complete local result"
            assert time.monotonic() - before < 10, "cache failure/fallback must be finite"
            assert process.ask("active") == "0", "actual work releases admission only after completion"
            assert process.ask("source") == "source 0 0", "cache pool outage must not occupy authoritative sessions"
            assert self.sql(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id='{repo}'") == "0"
            with pg.lock:
                pg.reject = False
            assert process.ask("build healthy") == "claimed"
            assert process.ask("stage healthy 1024 92") == "staged"
            assert process.ask("publish healthy") == "true"
            self.assert_complete(process.ask("get healthy"), [(1024, 92)])
            process.close()
            self.results["cases"]["one_connection_heartbeats_outage_local_result_and_pool_recovery"] = "passed"
        finally:
            objects.close()
            pg.close()

    def narrow_caller_and_late_put(self):
        objects = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.test_environment(GIT_RS_CACHE_GRACE_SECONDS="1",
                GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{objects.server_address[1]}")
            namespace = env["GIT_RS_CACHE_NAMESPACE"]
            process = self.artifact_process(env, "narrow-caller")
            gc = self.artifact_process(env, "narrow-caller-gc")
            assert process.ask("build finite") == "claimed"
            objects.arm("PUT", f"/derived-test/cache/{namespace}/")
            before = time.monotonic()
            process.send("stage-short finite 524288 97")
            assert objects.hit.wait(timeout=5), "actual PUT must reach the bounded caller gate"
            reply = process.receive(seconds=4)
            assert reply.startswith("error "), reply
            assert time.monotonic() - before < 4, "a long build lease must not widen a one-second caller"
            assert self.sql(f"SELECT count(*) FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'") == "0"
            assert self.sql(f"SELECT state FROM git_rs_cache.upload_intents WHERE namespace='{namespace}'") == "unknown"
            object_key = self.sql(f"SELECT object_key FROM git_rs_cache.payloads WHERE namespace='{namespace}'")
            self.sql(f"UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'; UPDATE git_rs_cache.jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'")
            assert gc.ask("maintenance") == "owned"
            assert gc.ask("retire") == "1"
            self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'")
            assert gc.ask("delete") == "1"
            objects.release.set()
            assert objects.finished.wait(timeout=5)
            assert self.object_request("GET", "derived-test", object_key).stdout == bytes([97]) * 524288
            self.sql(f"UPDATE git_rs_cache.payloads SET last_delete_at=clock_timestamp()-interval '31 seconds' WHERE namespace='{namespace}'")
            assert gc.ask("delete") == "1", "a late PUT from a bound build must remain discoverable/re-deletable"
            assert self.object_request("GET", "derived-test", object_key, ok=False).returncode != 0
            process.close()
            gc.close()
            self.results["cases"]["narrow_caller_deadline_and_bound_build_late_put"] = "passed"
        finally:
            objects.close()

    def checkpoint_roll(self):
        proxy = PgProxy(self.env["PGPORT"])
        try:
            env = self.test_environment(GIT_RS_CACHE_PGPORT=str(proxy.server_address[1]),
                GIT_RS_CACHE_READ_SECONDS="60", GIT_RS_TEST_BUILD_HARD_SECONDS="60",
                GIT_RS_TEST_BUILD_LEASE_SECONDS="60")
            namespace = env["GIT_RS_CACHE_NAMESPACE"]
            writer = self.artifact_process(env, "checkpoint-writer")
            reader = self.artifact_process(env, "checkpoint-old-reader")
            assert writer.ask("build work") == "claimed"
            assert writer.ask("stage work 524288 101") == "staged"
            first = writer.ask("checkpoint work").split()
            assert first[0] == "checkpoint", first
            self.assert_complete(reader.ask("checkpoint-hold work"), [(524288, 101)])
            for function, byte in (("claim_checkpoint", 102), ("publish_checkpoint", 103)):
                assert writer.ask(f"stage work 524288 {byte}") == "staged"
                proxy.arm("git_rs_cache." + function + "(", "commit-gate")
                writer.send("checkpoint work")
                assert proxy.hit.wait(timeout=5), f"{function} COMMIT gate was not reached"
                attempt = self.sql(f"SELECT token::text||':'||generation::text||':'||hard_until::text FROM git_rs_cache.jobs WHERE namespace='{namespace}' AND checkpoint_write")
                root = self.sql(f"SELECT payload::text FROM git_rs_cache.directory d JOIN git_rs_cache.jobs j USING(namespace,key_hash) WHERE d.namespace='{namespace}' AND j.checkpoint_write")
                if function == "claim_checkpoint":
                    assert root == first[1], "replacement claim must not hide the previous ready root"
                else:
                    assert root != first[1]
                    assert self.sql(f"SELECT count(*) FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'") == "0"
                self.assert_complete(reader.ask("checkpoint-consume work"), [(524288, 101)])
                proxy.release.set()
                reply = writer.receive().split()
                assert reply[0] == "checkpoint", reply
                assert self.sql(f"SELECT token::text||':'||generation::text||':'||hard_until::text FROM git_rs_cache.jobs WHERE namespace='{namespace}' AND checkpoint_write") == attempt
                assert self.sql(f"SELECT payload::text FROM git_rs_cache.directory d JOIN git_rs_cache.jobs j USING(namespace,key_hash) WHERE d.namespace='{namespace}' AND j.checkpoint_write") == reply[1]
                assert self.sql(f"SELECT encode(digest,'hex') FROM git_rs_cache.payloads WHERE namespace='{namespace}' AND id='{reply[1]}'") == reply[3]
                if function == "publish_checkpoint":
                    assert reply[1] == root, "unknown COMMIT must confirm the exact new root, not a later retry"
                self.assert_complete(writer.ask("checkpoint-get work"), [(524288, byte)])
            # A's successful COMMIT is hidden, then B supersedes it using the
            # same complete bytes and parent. A must never confirm B's UUID just
            # because the digest matches. Both writes use real API/SQL sessions.
            assert writer.ask("stage work 524288 103") == "staged"
            proxy.arm("git_rs_cache.publish_checkpoint(", "commit-gate")
            assert writer.ask("checkpoint-start work") == "started"
            assert proxy.hit.wait(timeout=5)
            root_a = self.sql(f"SELECT d.payload::text FROM git_rs_cache.directory d JOIN git_rs_cache.jobs j USING(namespace,key_hash) WHERE d.namespace='{namespace}' AND j.checkpoint_write")
            self.assert_complete(writer.ask("checkpoint-get work"), [(524288, 103)])
            assert writer.ask("stage work 524288 103") == "staged"
            root_b = writer.ask("checkpoint work").split()
            assert root_b[0] == "checkpoint" and root_b[1] != root_a, root_b
            assert self.sql(f"SELECT count(DISTINCT encode(digest,'hex')) FROM git_rs_cache.payloads WHERE namespace='{namespace}' AND id IN ('{root_a}','{root_b[1]}')") == "1"
            proxy.release.set()
            late_a = writer.ask("checkpoint-await").split()
            assert late_a == ["error", "PostgreSQL", "transport", "failed"], "superseded A cannot confirm its own UUID from the directory; same-digest B must not turn its lost ACK into success"
            self.results["superseded_commit_confirmation"] = {"a": root_a, "b": root_b[1], "a_result": " ".join(late_a)}
            assert self.sql(f"SELECT d.payload::text FROM git_rs_cache.directory d JOIN git_rs_cache.jobs j USING(namespace,key_hash) WHERE d.namespace='{namespace}' AND j.checkpoint_write") == root_b[1]
            self.results["cases"]["same_digest_superseded_commit_never_confirms_later_uuid"] = "passed"
            assert writer.ask("stage work 524288 104") == "staged"
            proxy.arm("git_rs_cache.publish_checkpoint(", "commit-gate")
            writer.send("checkpoint work")
            assert proxy.hit.wait(timeout=5)
            committed = self.sql(f"SELECT payload::text FROM git_rs_cache.directory d JOIN git_rs_cache.jobs j USING(namespace,key_hash) WHERE d.namespace='{namespace}' AND j.checkpoint_write")
            writer.kill()
            proxy.release.set()
            restarted = self.artifact_process(env, "checkpoint-cold-restart")
            self.assert_complete(restarted.ask("checkpoint-get work"), [(524288, 104)])
            assert self.sql(f"SELECT payload::text FROM git_rs_cache.directory d JOIN git_rs_cache.jobs j USING(namespace,key_hash) WHERE d.namespace='{namespace}' AND j.checkpoint_write") == committed
            self.assert_complete(reader.ask("checkpoint-consume work"), [(524288, 101)])
            assert restarted.ask("source") == "source 0 0"
            reader.close()
            writer.close()
            restarted.close()
            self.results["cases"]["rolling_checkpoint_exact_commit_confirmation_and_cold_restart"] = "passed"
        finally:
            proxy.close()

    def checkpoint_gc_race(self):
        objects = ObjectProxy(self.port(self.s3, "7070/tcp"))
        try:
            env = self.test_environment(GIT_RS_CACHE_READ_SECONDS="60", GIT_RS_CACHE_GRACE_SECONDS="1",
                GIT_RS_TEST_BUILD_HARD_SECONDS="60", GIT_RS_TEST_BUILD_LEASE_SECONDS="60",
                GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{objects.server_address[1]}")
            namespace = env["GIT_RS_CACHE_NAMESPACE"]
            writer = self.artifact_process(env, "checkpoint-race-writer")
            reader = self.artifact_process(env, "checkpoint-race-reader")
            gc = self.artifact_process(env, "checkpoint-race-gc")
            other_env = self.test_environment(GIT_RS_CACHE_READ_SECONDS="60")
            other = self.artifact_process(other_env, "checkpoint-race-other-namespace")
            assert other.ask("build work") == "claimed"
            assert other.ask("stage work 524288 114") == "staged"
            assert other.ask("checkpoint work").startswith("checkpoint ")
            assert writer.ask("build work") == "claimed"
            for byte in (111, 112):
                assert writer.ask(f"stage work 524288 {byte}") == "staged"
            first = writer.ask("checkpoint work").split()
            assert first[0] == "checkpoint", first
            child = self.sql(f"SELECT p.object_key FROM git_rs_cache.manifest_children e JOIN git_rs_cache.payloads p ON p.namespace=e.namespace AND p.id=e.child WHERE e.namespace='{namespace}' AND e.parent='{first[1]}' AND e.ordinal=0")
            objects.arm("GET", "/derived-test/" + child)
            reader.send("checkpoint-hold work")
            assert objects.hit.wait(timeout=5), "old-root reader must reach its actual child GET"
            assert writer.ask("stage work 524288 113") == "staged"
            second = writer.ask("checkpoint work").split()
            assert second[0] == "checkpoint", second
            self.sql(f"UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}' AND payload<>'{second[1]}'")
            assert gc.ask("maintenance") == "owned"
            assert gc.ask("retire") == "1", "old reader hold must retain both children after the root swap and pin release"
            objects.release.set()
            self.assert_complete(reader.receive(), [(524288, 111), (524288, 112)])
            self.assert_complete(reader.ask("checkpoint-consume work"), [(524288, 111), (524288, 112)])
            assert reader.ask("checkpoint-release work") == "ok"
            assert gc.ask("retire") == "2"
            self.sql(f"UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}' AND state='retired'")
            assert gc.ask("delete") == "3"
            self.assert_complete(writer.ask("checkpoint-get work"), [(524288, 113)])
            self.assert_complete(other.ask("checkpoint-get work"), [(524288, 114)])
            assert writer.ask("source") == "source 0 0"
            assert self.object_request("GET", "source-test", "repos/protected/sentinel").stdout == b"authoritative source sentinel\n"
            writer.close()
            reader.close()
            gc.close()
            other.close()
            self.results["cases"]["rolling_checkpoint_old_reader_s3_gc_race"] = "passed"
        finally:
            objects.close()

    def source_state(self, admin):
        # Canonical rows, not row counts: bytea, permissions, generations and
        # operation confirmations must survive cache DDL exactly. Request leases
        # are deliberately excluded; they are ephemeral, not publication state.
        tables = ("schema_version", "repositories", "permissions", "refs", "packs",
                  "operations", "ref_updates", "retired_objects", "staged_objects")
        entries = ",".join(
            f"'{table}',(SELECT coalesce(jsonb_agg(row ORDER BY row::text),'[]'::jsonb) "
            f"FROM (SELECT to_jsonb(t) AS row FROM git_rs.{table} t) rows)"
            for table in tables)
        return self.sql("SELECT jsonb_build_object(" + entries + ")::text", admin)

    def prepare_source_proof(self, admin, name):
        run([self.binary, "migrate"], env=admin)
        run([self.binary, "create", "synthetic/" + name, "alice"], env=admin)
        # Synthetic metadata exercises every durable source table. This is a
        # migration non-interference proof, not a real pack or attribution proof.
        self.sql("""
            INSERT INTO git_rs.permissions
                SELECT id,'reader','reader',decode('726566732f','hex') FROM git_rs.repositories;
            UPDATE git_rs.repositories SET generation=7, storage_epoch=3;
            INSERT INTO git_rs.refs
                SELECT id,convert_to('refs/heads/main','UTF8'),decode(repeat('11',20),'hex')
                FROM git_rs.repositories;
            INSERT INTO git_rs.operations
                SELECT '00000000-0000-0000-0000-000000000007',id,'alice',decode('00ff','hex'),
                    7,2,'2026-01-01T00:00:00Z' FROM git_rs.repositories;
            INSERT INTO git_rs.ref_updates VALUES
                ('00000000-0000-0000-0000-000000000007',1,convert_to('refs/heads/main','UTF8'),
                 NULL,decode(repeat('11',20),'hex'));
            INSERT INTO git_rs.packs
                SELECT id,'00000000-0000-0000-0000-000000000007',repeat('11',20),
                    'synthetic/pack','synthetic/idx',repeat('22',32),repeat('33',32),17,19,7
                FROM git_rs.repositories;
            INSERT INTO git_rs.retired_objects
                SELECT id,'synthetic/retired',2 FROM git_rs.repositories;
            INSERT INTO git_rs.staged_objects
                SELECT id,'00000000-0000-0000-0000-000000000007','synthetic/staged',
                    'uploaded','2026-01-01T00:00:00Z' FROM git_rs.repositories;
        """, admin)
        proof = self.source_state(admin)
        (self.base / (name + "-source-before.json")).write_text(proof + "\n")
        return proof

    def assert_source_proof(self, admin, name, before):
        after = self.source_state(admin)
        (self.base / (name + "-source-after.json")).write_text(after + "\n")
        assert after == before, "cache migration changed authoritative source metadata"
        assert self.object_request("GET", "source-test", "repos/protected/sentinel").stdout == b"authoritative source sentinel\n"
        self.results.setdefault("source_preservation", {})[name] = {
            "sha256": hashlib.sha256(after.encode()).hexdigest(),
            "scope": "synthetic durable metadata rows and source S3 sentinel; not attribution",
        }

    def migrations(self):
        for path, expected in ((self.args.legacy_binary, self.args.legacy_sha256),
                               (self.args.legacy_process_binary, self.args.legacy_process_sha256)):
            assert path and expected, "migration qualification requires explicitly pinned v1 binaries"
            assert hashlib.sha256(path.read_bytes()).hexdigest() == expected, "legacy binary hash mismatch"
        db = "blame_v1_" + uuid.uuid4().hex
        self.sql(f"CREATE DATABASE {db}; GRANT CREATE ON DATABASE {db} TO git_rs_cache;")
        admin = dict(self.env, PGDATABASE=db)
        source_before = self.prepare_source_proof(admin, "v1-upgrade")
        env = dict(self.env, GIT_RS_CACHE_NAMESPACE=str(uuid.uuid4()), GIT_RS_CACHE_OWNER=str(uuid.uuid4()),
            GIT_RS_CACHE_DSN=f"host=127.0.0.1 port={self.env['PGPORT']} user=git_rs_cache password=synthetic-cache-only dbname={db}")
        run([self.args.legacy_binary, "cache-migrate"], env=env)
        self.assert_source_proof(admin, "v1-installed", source_before)
        assert self.sql("SELECT version FROM git_rs_cache.schema_version", admin) == "1"
        legacy = LegacyProcess(self, env, "live-v1-runtime")
        assert legacy.ask("claim retained") == "claimed"
        assert legacy.ask("publish retained 65536 37") == "true"
        self.assert_hit(legacy.ask("hold retained"), 65536, 37)
        upgrade_env = dict(env, GIT_RS_TEST_CACHE_ADMIN_DSN=self.admin_dsn(admin), GIT_RS_TEST_CACHE_SCHEMA_EXPECTED="1")
        locker = SqlLocker(self, admin, "v1-upgrade-lock", "LOCK TABLE git_rs_cache.jobs IN ACCESS SHARE MODE")
        with ThreadPoolExecutor(max_workers=1) as pool:
            upgrade = pool.submit(run, [self.args.build_contract_binary, "--ignored", "--exact",
                "migration_preserves_existing_opaque_bytes_and_is_idempotent", "--nocapture"], env=upgrade_env, timeout=60)
            try:
                wait_until(lambda: self.sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid WHERE a.datname=current_database() AND a.application_name='git-rs-cache' AND a.wait_event_type='Lock' AND NOT l.granted AND l.relation='git_rs_cache.jobs'::regclass)", admin) == "t", "actual v2 ALTER wait while v1 reader remains live", seconds=.9)
                self.assert_hit(legacy.ask("consume retained"), 65536, 37)
            finally:
                locker.close()
            result = upgrade.result(timeout=65)
        (self.base / "v1-upgrade.log").write_bytes(result.stdout + result.stderr)
        assert b"cache schema before upgrade: 1" in result.stdout
        self.assert_source_proof(admin, "v1-upgraded", source_before)
        self.assert_hit(legacy.ask("consume retained"), 65536, 37)
        functions = "SELECT md5(string_agg(pg_get_functiondef(p.oid),'' ORDER BY p.oid)) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='git_rs_cache'"
        before = self.sql(functions, admin)
        old = run([self.args.legacy_binary, "cache-migrate"], env=env, ok=False)
        assert old.returncode != 0, "old migrator must reject v3 rather than replace its functions"
        assert self.sql(functions, admin) == before
        outcomes = self.concurrent_migrators(env, admin, "v2-migrators")
        run([self.binary, "cache-migrate"], env=env)
        self.assert_hit(legacy.ask("consume retained"), 65536, 37)
        assert legacy.ask("invalidate retained") == "true"
        assert legacy.ask("maintenance") == "owned"
        assert legacy.ask("retire") == "1"
        assert legacy.ask("consume retained").startswith("hit ")
        assert legacy.ask("release retained") == "ok"
        self.sql("UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second'", admin)
        assert legacy.ask("delete") == "1", "old runtime maintenance must remain safe across v3 upgrade"
        legacy.close()
        self.assert_source_proof(admin, "v2-idempotent", source_before)

        fresh = "blame_fresh_" + uuid.uuid4().hex
        self.sql(f"CREATE DATABASE {fresh}; GRANT CREATE ON DATABASE {fresh} TO git_rs_cache;")
        fresh_admin = dict(self.env, PGDATABASE=fresh)
        fresh_source_before = self.prepare_source_proof(fresh_admin, "fresh-upgrade")
        fresh_env = dict(env, GIT_RS_CACHE_NAMESPACE=str(uuid.uuid4()), GIT_RS_CACHE_OWNER=str(uuid.uuid4()),
            GIT_RS_CACHE_DSN=f"host=127.0.0.1 port={self.env['PGPORT']} user=git_rs_cache password=synthetic-cache-only dbname={fresh}")
        assert self.sql("SELECT to_regclass('git_rs_cache.schema_version') IS NULL", fresh_admin) == "t"
        outcomes = self.concurrent_migrators(fresh_env, fresh_admin, "fresh-migrators")
        run([self.binary, "cache-migrate"], env=fresh_env)
        assert self.sql("SELECT version FROM git_rs_cache.schema_version", fresh_admin) == "3"
        assert self.sql("SELECT count(*) FROM git_rs_cache.namespaces", fresh_admin) == "1"
        self.assert_source_proof(fresh_admin, "fresh-v2", fresh_source_before)
        self.results["migration_outcomes"] = [result.returncode for result in outcomes]
        # Install the exact released v2 state; editing 002 alone cannot upgrade it.
        v2 = "blame_v2_" + uuid.uuid4().hex
        self.sql(f"CREATE DATABASE {v2}; GRANT CREATE ON DATABASE {v2} TO git_rs_cache;")
        v2_admin = dict(self.env, PGDATABASE=v2)
        v2_source = self.prepare_source_proof(v2_admin, "v2-upgrade")
        for migration in ("001", "002"):
            self.sql("SET ROLE git_rs_cache; " + (Path(__file__).resolve().parents[1] / "migrations" / "cache" / (migration + ".sql")).read_text(), v2_admin)
        v2_env = dict(env, GIT_RS_TEST_CACHE_ADMIN_DSN=self.admin_dsn(v2_admin),
            GIT_RS_CACHE_DSN=f"host=127.0.0.1 port={self.env['PGPORT']} user=git_rs_cache password=synthetic-cache-only dbname={v2}")
        result = run([self.args.build_contract_binary, "--ignored", "--exact",
            V2_MIGRATION_CONTRACT, "--nocapture"], env=v2_env, timeout=60)
        (self.base / "v2-upgrade.log").write_bytes(result.stdout + result.stderr)
        record_contracts(self.results, "blame_build", {V2_MIGRATION_CONTRACT}, result)
        self.assert_source_proof(v2_admin, "v2-upgraded", v2_source)
        if self.args.opaque_contract_binary:
            self.run_contracts()
        else:
            raise AssertionError("migration gate requires existing opaque contracts against v3")
        self.results["cases"]["fresh_v1_v2_v3_concurrent_migrators_and_live_old_runtime"] = "passed"

    def concurrent_migrators(self, env, admin, name):
        locker = SqlLocker(self, admin, name + "-lock", "DO $$ BEGIN PERFORM pg_advisory_xact_lock(91079170002); END $$")
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(run, [self.binary, "cache-migrate"], env=env, ok=False) for _ in range(2)]
            try:
                wait_until(lambda: self.sql("SELECT count(*)=2 FROM pg_stat_activity WHERE datname=current_database() AND application_name='git-rs-cache' AND wait_event_type='Lock' AND query LIKE '%pg_advisory_lock%'", admin) == "t", "two actual independent migrators waiting on migration ownership", seconds=.9)
            finally:
                locker.close()
            outcomes = [future.result(timeout=35) for future in futures]
        assert any(result.returncode == 0 for result in outcomes)
        for result in outcomes:
            assert result.returncode == 0 or b"55P03" in result.stderr, "only a documented bounded lock timeout is a retryable concurrency outcome"
        self.results.setdefault("concurrent_migrators", {})[name] = [result.returncode for result in outcomes]
        return outcomes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provision", action="store_true", required=True)
    parser.add_argument("--owner-label", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--build-contract-binary", type=Path, required=True)
    parser.add_argument("--artifact-contract-binary", type=Path, required=True)
    parser.add_argument("--checkpoint-contract-binary", type=Path)
    parser.add_argument("--opaque-contract-binary", type=Path)
    parser.add_argument("--coordinator-contract-binary", type=Path)
    parser.add_argument("--budget-contract-binary", type=Path)
    parser.add_argument("--legacy-binary", type=Path)
    parser.add_argument("--legacy-process-binary", type=Path)
    parser.add_argument("--legacy-sha256")
    parser.add_argument("--legacy-process-sha256")
    parser.add_argument("--phase", choices=("contracts", "faults", "migration", "all"), default="all")
    parser.add_argument("--case", choices=("ambiguous_commits", "child_get_gc_races", "single_pool_outage", "narrow_caller_and_late_put", "checkpoint_roll", "checkpoint_gc_race"))
    parser.add_argument("--contract-filter")
    args = parser.parse_args()
    assert re.fullmatch(r"git-rs\.blame-fixture-owner=[a-zA-Z0-9_-]{8,80}", args.owner_label), "use an independently owned cleanup label"
    for binary in (args.binary, args.build_contract_binary, args.artifact_contract_binary):
        assert binary.is_file(), f"prebuild required: {binary}"
    os.environ["GIT_RS_TEST_OWNER_LABEL"] = args.owner_label
    fixture = Fixture(args)
    def stop(signum, _frame):
        raise InterruptedError(f"fixture interrupted by {signum}")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        fixture.setup()
        if args.phase in ("contracts", "all"):
            fixture.rust_contracts()
        if args.phase in ("faults", "all"):
            cases = [args.case] if args.case else ["ambiguous_commits", "child_get_gc_races", "single_pool_outage", "narrow_caller_and_late_put", "checkpoint_roll", "checkpoint_gc_race"]
            for case in cases:
                getattr(fixture, case)()
        if args.phase in ("migration", "all"):
            fixture.migrations()
        fixture.results["completed"] = True
    except BaseException as error:
        fixture.results["error"] = str(error)
        raise
    finally:
        fixture.close()
        print(json.dumps(fixture.results, indent=2), flush=True)


if __name__ == "__main__":
    main()
