#!/usr/bin/env python3
"""Stage-2 synthetic cache cost gate; requires prebuilt test binaries and owned services.

No L1 retention: each get materializes a verified RAM payload then releases it.
Repeated-process reads are shared-cache reuse, not an application RAM hit. The cold
read is process-cold, not an OS/PG buffer-cache flush. Input scratch is test-only.
Use an externally bounded owned systemd unit with label-scoped container cleanup.
"""
import argparse
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import signal
import socket
import socketserver
import threading
import time
import uuid

from derived_cache import Fixture, ROOT, run, wait_until
from integration import PG_IMAGE, S3_IMAGE


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def distribution(values):
    require(bool(values), "empty latency sample")
    ordered = sorted(values)
    return {f"p{p}": ordered[max(0, math.ceil(len(ordered) * p / 100) - 1)]
            for p in (50, 95, 99)} | {"count": len(values), "max": max(values)}


class WireProxy:
    """Counts plaintext TCP bytes forwarded, including SQL/HTTP protocol overhead.

    Not NIC packets/retransmits and not a measurement of kernel/driver/TLS copies.
    Only cache connections traverse these proxies; monitoring/source do not.
    """
    def __init__(self, host, port):
        self.target = (host, int(port))
        self.lock = threading.Lock()
        self.counts = [0, 0]
        self.sockets = set()
        self.errors = []
        proxy = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    with socket.create_connection(proxy.target, timeout=3) as upstream:
                        upstream.settimeout(1)
                        self.request.settimeout(1)
                        # Avoid proxy-induced Nagle/delayed-ACK round trips on
                        # the persistent SQL stream; still count actual bytes.
                        upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        with proxy.lock:
                            proxy.sockets.update((upstream, self.request))

                        def pump(source, destination, direction):
                            try:
                                while True:
                                    try:
                                        data = source.recv(65536)
                                    except socket.timeout:
                                        continue
                                    if not data:
                                        break
                                    destination.sendall(data)
                                    with proxy.lock:
                                        proxy.counts[direction] += len(data)
                            except OSError:
                                pass  # Peer close/reset is normal at process teardown.
                            finally:
                                try:
                                    destination.shutdown(socket.SHUT_WR)
                                except OSError:
                                    pass

                        thread = threading.Thread(target=pump,
                                                  args=(self.request, upstream, 0))
                        thread.start()
                        pump(upstream, self.request, 1)
                        thread.join(timeout=5)
                        require(not thread.is_alive(), "wire forwarding did not stop")
                        with proxy.lock:
                            proxy.sockets.difference_update((upstream, self.request))
                except Exception as error:
                    with proxy.lock:
                        proxy.errors.append(str(error))

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            block_on_close = False

        self.server = Server(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return list(self.counts)

    def close(self):
        self.server.shutdown()
        with self.lock:
            for stream in self.sockets:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        self.server.server_close()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive(), "wire listener did not stop")
        require(not self.errors, f"wire proxy failed: {self.errors}")


class SourceProbe:
    def __init__(self, process, expected):
        self.process, self.expected = process, expected
        self.samples, self.errors = [], []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.work)

    def work(self):
        while not self.stop.is_set():
            start = time.monotonic()
            try:
                require(self.process.ask("source") == self.expected, "source snapshot changed")
                self.samples.append((time.monotonic() - start) * 1000)
            except Exception as error:
                self.errors.append(str(error))
                break
            self.stop.wait(.01)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=30)
        require(not self.thread.is_alive(), "source probe did not stop")


class ResourceProbe:
    def __init__(self, fixture, result):
        self.fixture, self.result = fixture, result
        self.stop = threading.Event()
        self.errors = []
        self.thread = threading.Thread(target=self.work)

    def work(self):
        try:
            while not self.stop.is_set():
                self.result["lag_bytes"].append(self.fixture.lag())
                self.result["primary_resources"].append(self.fixture.resource_sample())
                self.stop.wait(.1)
        except Exception as error:
            self.errors.append(str(error))

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=15)
        require(not self.thread.is_alive() and not self.errors,
                f"resource probe failed: {self.errors}")


class CostFixture(Fixture):
    def __init__(self, args):
        super().__init__(args)
        if args.service_cpus:
            self.cpus = args.service_cpus
        self.budget = json.loads(args.budget.read_text())
        self.proxies = []
        self.results["provenance"] = {
            "git_head": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).stdout.decode().strip(),
            "binary_sha256": hashlib.sha256(Path(self.binary).read_bytes()).hexdigest(),
            "process_binary_sha256": hashlib.sha256(Path(self.process_binary).read_bytes()).hexdigest(),
            "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "example_source_sha256": hashlib.sha256((ROOT / "tests/support/cache_process.rs").read_bytes()).hexdigest(),
            "cargo_lock_sha256": hashlib.sha256((ROOT / "Cargo.lock").read_bytes()).hexdigest(),
            "postgres_image": PG_IMAGE, "object_image": S3_IMAGE,
            "container_envelope": {"cpus": 1, "memory_mib": 1792, "pids": 144,
                                   "cpu_affinity": self.cpus},
            "controller_envelope_required": "at most 1 CPU, 768 MiB, 64 tasks; independent RuntimeMax/ExecStopPost",
        }
        self.results.update(budget=self.budget, budget_sha256=hashlib.sha256(
            args.budget.read_bytes()).hexdigest(), cells=[], coverage={
                "local_hot_policy": "No retained L1; first read of each key, repeated process shared-cache reads, restarted reader; PG/OS buffers not flushed",
                "wire": "Actual forwarded plaintext cache TCP bytes including protocol framing; excludes source, monitoring, replication, NIC retransmits and TLS",
                "copies": "Exact fixture file bytes materialized and digest input bytes; production SQL/S3/allocator/kernel copies not instrumented",
                "replication": "Real physical streaming standby of fresh source/cache primary; sampled byte lag plus final replay fence, not failover/HA",
                "source": "Real metadata::snapshot concurrent with cache IO; synthetic callback authorization only; serving authorization deferred",
                "storage": "Fresh cache database per cell, logged tables, default TOAST policy; no cache maintenance deletion during measurement",
            })

    def setup_standby(self):
        # The independent-cluster correctness fixture is not used by this harness.
        run(["docker", "stop", "--time", "5", self.pg2])
        hba = self.base / "replication-hba.conf"
        hba.write_text("local all all trust\nhost all all 0.0.0.0/0 scram-sha-256\n"
                       "host replication postgres 0.0.0.0/0 scram-sha-256\n")
        # hba_file is postmaster-only; replace only this disposable primary's
        # existing file so pg_reload_conf applies its contents without a restart.
        # docker cp cannot see this daemon's live tmpfs mount. Write through exec
        # as its existing owner, rather than touching a host PostgreSQL directory.
        run(["docker", "exec", "-i", "--user", "postgres", self.pg,
             "tee", self.sql("SHOW hba_file")], data=hba.read_bytes())
        self.sql("SELECT pg_reload_conf();")
        primary = json.loads(run(["docker", "inspect", self.pg]).stdout)[0]
        address = primary["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
        uid = run(["docker", "exec", self.pg, "id", "-u", "postgres"]).stdout.decode().strip()
        gid = run(["docker", "exec", self.pg, "id", "-g", "postgres"]).stdout.decode().strip()
        self.standby = self.container("512m", .25, 48, [
            "--tmpfs", f"/standby:rw,size=384m,uid={uid},gid={gid}", "--user", "postgres",
            "-e", "PGHOST=" + address, "-e", "PGUSER=postgres",
            "-e", "PGPASSWORD=development-only", "-e", "PGAPPNAME=cache-cost-standby",
            "--entrypoint", "sh", PG_IMAGE, "-ec",
            "pg_basebackup -D /standby/data -R -X stream --checkpoint=fast; "
            "exec postgres -D /standby/data -c shared_buffers=16MB "
            "-c listen_addresses='' -c hba_file=/standby/data/pg_hba.conf"])
        wait_until(lambda: self.sql("SELECT count(*) FROM pg_stat_replication WHERE "
                                   "application_name='cache-cost-standby' AND state='streaming'") == "1",
                   "actual streaming standby", seconds=45)
        self.results["postgres_version"] = self.sql("SHOW server_version")

    def source_state(self):
        return [self.sql(f"SELECT COALESCE(json_agg(t ORDER BY row_to_json(t)::text),'[]'::json) "
                         f"FROM git_rs.{table} t")
                for table in ("repositories", "permissions", "refs", "packs")]

    def standby_sql(self, sql):
        return run(["docker", "exec", "--user", "postgres", "-e", "PGHOST=/var/run/postgresql",
                    "-e", "PGDATABASE=postgres", self.standby, "psql", "-XqAt",
                    "-v", "ON_ERROR_STOP=1", "-c", sql]).stdout.decode().strip()

    def replica_fault_check(self):
        """A real paused replay must fail the same finite acceptance fence."""
        env = self.environment()
        publisher = self.process(env, "replica-fault-publisher")
        self.standby_sql("SELECT pg_wal_replay_pause()")
        try:
            wait_until(lambda: self.standby_sql("SELECT pg_get_wal_replay_pause_state()") == "paused",
                       "standby replay actually paused")
            require(publisher.ask("claim paused-replay") == "claimed", "replica fault claim failed")
            path = self.base / "replica-fault.bin"
            path.write_bytes(hashlib.shake_256(b"synthetic-replica-fault-v1").digest(16384))
            require(publisher.ask(f"publish-file paused-replay {path}") == "true",
                    "replica fault publication failed")
            backlog = self.lag()
            require(backlog > 0, "paused standby must have actual WAL backlog")
            start = time.monotonic()
            try:
                self.replay_fence()
            except AssertionError as error:
                require("deadline: standby final replay fence" in str(error),
                        "replica fault failed for an unrelated reason")
            else:
                raise AssertionError("paused replay incorrectly passed the acceptance fence")
            self.results["replica_failure_sensitivity"] = {
                "paused_lag_bytes": backlog, "rejected_after_seconds": time.monotonic() - start,
                "expected_rejection": "deadline: standby final replay fence",
            }
        finally:
            self.standby_sql("SELECT pg_wal_replay_resume()")
            publisher.close()
        self.results["replica_failure_sensitivity"]["restored_catchup_seconds"] = self.replay_fence()
        require(self.source_state() == self.initial_source_state, "replica fault changed source state")
        self.results["cases"]["real_paused_replica_rejected_then_restored"] = "passed"

    def database_metrics(self, env):
        query = """
SELECT json_build_object(
 'relation_bytes', COALESCE((SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c
 JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='git_rs_cache' AND c.relkind='r'),0),
 'tables', COALESCE((SELECT json_agg(json_build_object('table',relname,
 'heap_bytes',pg_relation_size(relid),'indexes_bytes',pg_indexes_size(relid),
 'toast_total_bytes',(SELECT CASE WHEN reltoastrelid=0 THEN 0 ELSE pg_total_relation_size(reltoastrelid) END FROM pg_class WHERE oid=relid),
 'total_bytes',pg_total_relation_size(relid),'estimated_live',n_live_tup,
 'estimated_dead',n_dead_tup,'vacuum_count',vacuum_count,'autovacuum_count',autovacuum_count))
 FROM pg_stat_user_tables WHERE schemaname='git_rs_cache'),'[]'::json),
 'database_io', (SELECT row_to_json(d) FROM (SELECT blks_read,blks_hit,tup_returned,
 tup_fetched,tup_inserted,tup_updated,tup_deleted,temp_bytes,deadlocks
 FROM pg_stat_database WHERE datname=current_database()) d));
"""
        return json.loads(self.sql(query, env))

    def primary_metrics(self):
        return json.loads(self.sql("SELECT json_build_object('io',"
            "(SELECT json_agg(i) FROM pg_stat_io i),'source_database',"
            "(SELECT row_to_json(d) FROM pg_stat_database d WHERE datname='git_rs'))"))

    def lag(self):
        raw = self.sql("SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(),replay_lsn),-1)::bigint "
                       "FROM pg_stat_replication WHERE application_name='cache-cost-standby' "
                       "AND state='streaming'")
        require(bool(raw) and int(raw) >= 0, "standby not streaming or replay LSN missing")
        return int(raw)

    def resource_sample(self):
        # Raw API memory usage includes the charged page cache; the CLI's displayed
        # working set subtracts inactive pages and is too optimistic for headroom.
        connection = http.client.HTTPConnection("localhost", timeout=10)
        stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stream.settimeout(10)
        try:
            stream.connect("/var/run/docker.sock")
            connection.sock = stream
            connection.request("GET", f"/containers/{self.pg}/stats?stream=false")
            response = connection.getresponse()
            require(response.status == 200, "Docker resource statistics unavailable")
            stats = json.loads(response.read())
        finally:
            connection.close()
            stream.close()
        memory = stats["memory_stats"]
        cpu, previous = stats["cpu_stats"], stats["precpu_stats"]
        system_delta = cpu["system_cpu_usage"] - previous.get("system_cpu_usage", 0)
        used_delta = cpu["cpu_usage"]["total_usage"] - previous.get("cpu_usage", {}).get("total_usage", 0)
        cores = used_delta / system_delta * cpu["online_cpus"] if system_delta > 0 else None
        return {"memory_usage_bytes": memory["usage"], "memory_limit_bytes": memory["limit"],
                "memory_headroom_fraction": 1 - memory["usage"] / memory["limit"],
                "cpu_cores": cores, "cpu_limit_cores": .5,
                "cpu_headroom_fraction": None if cores is None else 1 - cores / .5,
                "cpu": cpu, "block_io": stats["blkio_stats"], "pids": stats["pids_stats"]}

    def replay_fence(self):
        lsn = self.sql("SELECT pg_current_wal_lsn()")
        start = time.monotonic()
        wait_until(lambda: self.sql("SELECT count(*) FROM pg_stat_replication WHERE "
                                   "application_name='cache-cost-standby' AND state='streaming' "
                                   f"AND replay_lsn>='{lsn}'::pg_lsn") == "1",
                   "standby final replay fence", self.budget["replica_catchup_seconds_max"])
        return time.monotonic() - start

    def cell(self, mode, profile, size, baseline):
        b = self.budget
        tag = f"{mode}-{profile}-{size}"
        database = "cache_cost_" + uuid.uuid4().hex
        self.sql(f'CREATE DATABASE "{database}"; GRANT CREATE ON DATABASE "{database}" TO git_rs_cache;')
        pg = WireProxy("127.0.0.1", self.env["PGPORT"])
        s3 = WireProxy("127.0.0.1", self.port(self.s3, "7070/tcp"))
        self.proxies.extend((pg, s3))
        env = dict(self.env, GIT_RS_CACHE_NAMESPACE=str(uuid.uuid4()),
                   GIT_RS_CACHE_OWNER=str(uuid.uuid4()), GIT_RS_CACHE_CONNECTIONS="2",
                   GIT_RS_CACHE_INLINE_BYTES=str(8 * 1024 * 1024 if mode == "all-pg" else 0),
                   GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{s3.port}",
                   GIT_RS_CACHE_DSN=f"host=127.0.0.1 port={pg.port} user=git_rs_cache "
                   f"password=synthetic-cache-only dbname={database} sslmode=disable")
        run([self.binary, "cache-migrate"], env=env)
        admin = dict(self.env, PGDATABASE=database)
        before = self.database_metrics(admin)
        writer = self.process(env, tag + "-writer")
        reader = self.process(env, tag + "-reader")
        source = self.process(env, tag + "-source")
        wire_before = [pg.snapshot(), s3.snapshot()]
        lsn = self.sql("SELECT pg_current_wal_lsn()")
        result = {"mode": mode, "profile": profile, "size": size,
                  "latency_ms": {}, "lag_bytes": [], "primary_resources": [],
                  "primary_before": self.primary_metrics()}
        self.results["cells"].append(result)
        writes, cold, reuse, restart, sustained = [], [], [], [], []
        expected = "source 0 0"
        start = time.monotonic()
        with ResourceProbe(self, result), SourceProbe(source, expected) as probe:
            for index in range(b["writes_per_cell"]):
                # Stable synthetic PRNG stream, independent of mode; no source corpus.
                seed = f"cache-cost-v1/{profile}/{size}/{index}".encode()
                block = hashlib.shake_256(seed).digest(size if profile == "random" else 128)
                data = block if profile == "random" else (block * math.ceil(size / 128))[:size]
                path = self.base / f"input-{profile}-{size}-{index}.bin"
                path.write_bytes(data)
                digest = hashlib.sha256(data).hexdigest()
                name = "artifact-" + str(index)
                t = time.monotonic()
                require(writer.ask("claim " + name) == "claimed", "fresh namespace must claim")
                require(writer.ask(f"publish-file {name} {path}") == "true", "cold publication failed")
                writes.append((time.monotonic() - t) * 1000)
                for n in range(1 + b["reuse_reads_per_artifact"]):
                    t = time.monotonic()
                    reply = reader.ask("get " + name).split()
                    require(reply[:3] == ["hit", str(size), digest], "independent digest mismatch")
                    (cold if n == 0 else reuse).append((time.monotonic() - t) * 1000)
                result["lag_bytes"].append(self.lag())
            # A cache hit must not invoke the file loader (path deliberately absent).
            first_hit = ["hit", str(size), hashlib.sha256(
                (self.base / f"input-{profile}-{size}-0.bin").read_bytes()).hexdigest()]
            require(reader.ask("compute-file artifact-0 /nonexistent-synthetic-input").split()[:3]
                    == first_hit, "reuse recomputed or corrupted")
            require(reader.ask("file-stats") == "files 0 0", "reuse loaded input")
            require(writer.ask("file-stats") == f"files {b['writes_per_cell']} {b['writes_per_cell'] * size}",
                    "file materialization accounting mismatch")
            fresh = self.process(env, tag + "-restart")
            t = time.monotonic()
            require(fresh.ask("get artifact-0").split()[:3] == first_hit,
                    "restarted process lost artifact")
            restart.append((time.monotonic() - t) * 1000)
            fresh.close()
            # Identical fixed extra stress in both modes, not a duration-driven
            # loop that would silently compare different numbers of cache hits.
            for _ in range(512):
                t = time.monotonic()
                require(reader.ask("get artifact-0").split()[:3] == first_hit,
                        "sustained reuse lost or corrupted an artifact")
                sustained.append((time.monotonic() - t) * 1000)
            result["active_io_seconds"] = time.monotonic() - start
            probe.stop.set()
            # Resource samples may extend to the declared minimum window, but
            # idle padding is never included in authoritative-source quantiles.
            remaining = b["minimum_cell_seconds"] - (time.monotonic() - start)
            if remaining > 0:
                time.sleep(remaining)
        result["source_latency_ms"] = distribution(probe.samples)
        result["primary_after"] = self.primary_metrics()
        result["source_errors"] = probe.errors
        result["elapsed_seconds"] = time.monotonic() - start
        result["replica_catchup_seconds"] = self.replay_fence()
        result["wal_bytes"] = int(self.sql(f"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'{lsn}'::pg_lsn)::bigint"))
        for name, samples in (("cold_write", writes), ("cold_read", cold),
                              ("reuse_read", reuse), ("restart_read", restart)):
            result["latency_ms"][name] = distribution(samples)
        result["sustained_reuse_reads"] = len(sustained)
        if sustained:
            result["latency_ms"]["sustained_reuse_read"] = distribution(sustained)
        # Close processes before SQL stats snapshot, so final backend counters flush.
        writer.close()
        reader.close()
        source.close()
        after = self.database_metrics(admin)
        result["payload_layout"] = json.loads(self.sql("SELECT json_build_object("
            "'rows',count(*),'inline_bytes',COALESCE(sum(octet_length(body)),0),"
            "'object_rows',count(object_key),'declared_bytes',COALESCE(sum(size),0)) "
            "FROM git_rs_cache.payloads WHERE state='live'", admin))
        result["storage_before"], result["storage_after"] = before, after
        result["relation_growth_bytes"] = after["relation_bytes"] - before["relation_bytes"]
        self.sql("VACUUM (ANALYZE);", admin)
        result["storage_after_vacuum"] = self.database_metrics(admin)
        result["wire_bytes"] = {name: {"request": proxy.snapshot()[0] - old[0],
                                        "response": proxy.snapshot()[1] - old[1]}
                                for name, proxy, old in zip(("pg", "s3"), (pg, s3), wire_before)}
        result["fixture_materialized_bytes"] = size * b["writes_per_cell"]
        result["verified_digest_input_bytes"] = size * (b["writes_per_cell"] *
                                                        (1 + b["reuse_reads_per_artifact"]) + 2 + len(sustained))
        result["source_unchanged"] = self.source_state() == self.initial_source_state
        self.score(result, baseline)
        result["passed"] = True
        # Do not retain one listener thread per completed cell: the task budget
        # covers the whole fixture, not an ever-growing set of idle proxies.
        for proxy in (pg, s3):
            proxy.close()
            self.proxies.remove(proxy)

    def score(self, row, baseline):
        b = self.budget
        size = row["size"] * b["writes_per_cell"]
        require(row["source_unchanged"], "authoritative repository set changed")
        layout = row["payload_layout"]
        require(layout["rows"] == b["writes_per_cell"] and layout["declared_bytes"] == size,
                "payload set incomplete or duplicated")
        require((layout["inline_bytes"], layout["object_rows"]) ==
                ((size, 0) if row["mode"] == "all-pg" else (0, b["writes_per_cell"])),
                "payload storage policy not exercised")
        require(len(row["source_errors"]) <= b["source_errors_max"], "source request failed")
        require(row["source_latency_ms"]["count"] >= b["minimum_source_samples"], "too few source samples")
        require(row["source_latency_ms"]["p99"] <= min(b["source_p99_ms_max"],
                baseline["p99"] * b["source_p99_baseline_factor"] + b["source_p99_baseline_allowance_ms"]),
                "authoritative source tail exceeded predetermined budget")
        require(all(x["max"] <= b["operation_ms_max"] for x in row["latency_ms"].values()),
                "cache operation exceeded budget")
        require(max(row["lag_bytes"]) <= b["replica_lag_bytes_max"], "replica lag exceeded budget")
        require(row["replica_catchup_seconds"] <= b["replica_catchup_seconds_max"],
                "replica final catchup exceeded budget")
        require(row["wal_bytes"] <= size * b["wal_per_artifact_byte_max"] + b["wal_fixed_allowance_bytes"],
                "WAL amplification exceeded budget")
        require(row["relation_growth_bytes"] <= size * b["relation_per_artifact_byte_max"] +
                b["relation_fixed_allowance_bytes"], "relation amplification exceeded budget")
        wire = sum(sum(d.values()) for d in row["wire_bytes"].values())
        logical = size + row["verified_digest_input_bytes"]
        require(wire <= logical * b["wire_per_logical_byte_max"] + b["wire_fixed_allowance_bytes"],
                "wire amplification exceeded budget")
        require(sum(t["estimated_dead"] for t in row["storage_after"]["tables"]) <=
                b["estimated_dead_tuples_max"], "estimated vacuum debt exceeded budget")
        require(min(s["memory_headroom_fraction"] for s in row["primary_resources"]) >=
                b["primary_memory_headroom_fraction_min"], "primary memory headroom exhausted")
        pg, s3 = row["wire_bytes"]["pg"], row["wire_bytes"]["s3"]
        if row["mode"] == "all-pg":
            require(sum(s3.values()) == 0 and pg["request"] >= size,
                    "all-PG transport did not carry the payloads exclusively")
        else:
            require(s3["request"] >= size and s3["response"] >= row["verified_digest_input_bytes"],
                    "S3 transport missing measured payload IO")

    def close(self):
        try:
            super().close()
        finally:
            for proxy in self.proxies:
                proxy.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provision", action="store_true", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--process-binary", type=Path, required=True)
    parser.add_argument("--budget", type=Path, default=ROOT / "tests/fixtures/cache-storage-budget.json")
    parser.add_argument("--deadline-seconds", type=int, default=900)
    parser.add_argument("--service-cpus", help="Docker CPU affinity; defaults to controller affinity")
    args = parser.parse_args()
    args.contract_binary = None
    require(args.binary.is_file() and args.process_binary.is_file(), "prebuilt binaries required; no implicit build")
    require(bool(os.environ.get("GIT_RS_TEST_OWNER_LABEL")), "independent owner cleanup label required")
    invocation = os.environ.get("INVOCATION_ID")
    require(bool(invocation) and os.environ["GIT_RS_TEST_OWNER_LABEL"].endswith("=" + invocation),
            "run in an independently bounded systemd unit; owner label must match INVOCATION_ID")
    require(1 <= args.deadline_seconds <= 1200, "invalid finite fixture deadline")
    fixture = CostFixture(args)

    def interrupted(signum, _):
        raise InterruptedError(f"cost fixture interrupted by {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    signal.alarm(args.deadline_seconds)
    try:
        fixture.setup()
        fixture.setup_standby()
        fixture.initial_source_state = fixture.source_state()
        fixture.replica_fault_check()
        env = fixture.environment()
        source = fixture.process(env, "baseline-source")
        with SourceProbe(source, "source 0 0") as baseline:
            baseline.stop.wait(fixture.budget["minimum_cell_seconds"])
        require(not baseline.errors, "source-only baseline failed")
        fixture.results["source_baseline_ms"] = distribution(baseline.samples)
        source.close()
        for profile in fixture.budget["profiles"]:
            for size in fixture.budget["sizes"]:
                for mode in ("all-pg", "pg-s3"):
                    fixture.cell(mode, profile, size, fixture.results["source_baseline_ms"])
        fixture.results["completed"] = True
    except BaseException as error:
        fixture.results["error"] = str(error)
        raise
    finally:
        signal.alarm(0)
        fixture.close()
        print(json.dumps(fixture.results, indent=2), flush=True)


if __name__ == "__main__":
    main()
