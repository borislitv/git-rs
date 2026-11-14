#!/usr/bin/env python3
"""Versioned synthetic serving/reuse matrix over prebuilt Git-free containers.

The outer runtime supervisor owns aggregate limits, lifecycle tracing and cleanup.
Cells rotate placements and use fresh source/cache databases and namespaces. OS/PG
buffers are not flushed. This bounded smoke is not full-corpus or GitLab scoring.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from urllib.parse import quote

from cache_storage_cost import WireProxy
from blame_concurrency import Gates
from failures import FaultGateway
from integration import Fixture, PG_IMAGE, ROOT, command
from maintenance import stop, until, worker


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def load_config(path):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 16384,
            "bounded regular matrix configuration required")
    value = json.loads(path.read_text())
    require(isinstance(value, dict) and set(value) == {"format", "scope", "repetitions", "placements", "profiles", "fixture", "paths", "limits"}
            and type(value["format"]) is int and value["format"] == 1 and value["scope"] == "ci-smoke", "unsupported matrix configuration")
    require(value["placements"] == ["disabled", "all-pg", "pg-s3"]
            and value["profiles"] == ["derivation", "body"], "incomplete or reordered matrix inventory")
    require(type(value["repetitions"]) is int and 2 <= value["repetitions"] <= 4, "invalid repetition count")
    fixture = value["fixture"]
    require(isinstance(fixture, dict) and set(fixture) == {"commits", "small_files", "large_lines", "branches"}, "unknown synthetic shape")
    for name, lower, upper in (("commits", 8, 64), ("small_files", 8, 64),
                                ("large_lines", 256, 4096), ("branches", 2, 8)):
        require(type(fixture[name]) is int and lower <= fixture[name] <= upper, "invalid synthetic " + name)
    require(value["paths"] == ["large.txt", "small/00.txt", "renamed.txt", "sparse/deep.txt"],
            "complete query inventory required")
    bounds = {"cell_seconds": (30, 60), "request_seconds": (3, 10), "response_bytes": (1, 2 * 1024 * 1024),
              "minimum_source_samples": (3, 30), "source_p99_ms": (1, 1000),
              "source_baseline_factor": (1, 5), "source_baseline_allowance_ms": (0, 100),
              "replica_catchup_seconds": (1, 10), "daemon_rss_bytes": (1, 900 * 1024 * 1024)}
    require(isinstance(value["limits"], dict) and set(value["limits"]) == set(bounds), "unknown or missing acceptance budget")
    for name, (lower, upper) in bounds.items():
        limit = value["limits"][name]
        require(type(limit) is int and lower <= limit <= upper, "invalid acceptance budget " + name)
    return value


def distribution(values):
    require(values, "missing latency samples")
    ordered = sorted(values)
    return {"count": len(values), **{f"p{p}": ordered[math.ceil(len(values) * p / 100) - 1]
                                    for p in (50, 95, 99)}, "max": ordered[-1]}


def cleanup_all(actions, record):
    failures = []
    for action in actions:
        try:
            action()
        except BaseException as error:
            failures.append(error)
    record["cleanup_errors"] = [type(error).__name__ for error in failures]
    if failures:
        original = sys.exception()
        if original is not None:
            original.add_note("Matrix cleanup also failed: " + ", ".join(record["cleanup_errors"]))
        else:
            raise failures[0]


def write_report(f):
    encoded = (json.dumps(f.results, indent=2) + "\n").encode()
    require(len(encoded) <= 16 * 1024 * 1024, "matrix report exceeded its diagnostic bound")
    descriptor, temporary = tempfile.mkstemp(prefix=".report-", dir=f.base)
    try:
        with os.fdopen(descriptor, "wb") as output:
            os.fchmod(output.fileno(), 0o600)
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, f.base / "report.json")
        parent = os.open(f.base, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def native(f, directory, arguments, seconds):
    child = subprocess.Popen(["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", "-C", str(directory), *arguments],
                             env=f.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        stdout, stderr = child.communicate(timeout=seconds)
    except BaseException:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.communicate(timeout=5)
        raise
    require(child.returncode == 0, "mixed native client failed: " + repr(stderr[-1024:]))
    return {"seconds_budget": seconds, "stdout_bytes": len(stdout)}


def sql(f, query, environment=None):
    environment = dict(environment or f.env, PGOPTIONS="-c statement_timeout=2000")
    result = subprocess.run(["psql", "-XqAt", "-v", "ON_ERROR_STOP=1"], input=query.encode(),
                            env=environment, capture_output=True, timeout=4)
    require(result.returncode == 0, "matrix monitoring SQL failed")
    require(len(result.stdout) <= 1024 * 1024, "matrix SQL observation exceeded its bound")
    return result.stdout.decode().strip()


def http(port, path, seconds, maximum):
    peer = http.client.HTTPConnection("127.0.0.1", port, timeout=seconds)
    try:
        peer.request("GET", path)
        response = peer.getresponse()
        body = response.read(maximum + 1)
        require(len(body) <= maximum, "matrix response exceeded its bound")
        return response.status, body
    finally:
        peer.close()


def metrics(f, name):
    status, body = f.health(name, "/metrics")
    require(status == 200 and len(body) <= 256 * 1024, "fixture metrics unavailable")
    return {line.rsplit(" ", 1)[0]: float(line.rsplit(" ", 1)[1])
            for line in body.decode().splitlines() if line and not line.startswith("#")}


def resource(identifier):
    connection = http.client.HTTPConnection("localhost", timeout=3)
    stream = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stream.settimeout(3)
    try:
        stream.connect("/var/run/docker.sock")
        connection.sock = stream
        connection.request("GET", f"/containers/{identifier}/stats?stream=false&one-shot=true")
        response = connection.getresponse()
        require(response.status == 200, "actual runtime resource statistics unavailable")
        encoded = response.read(1024 * 1024 + 1)
        require(len(encoded) <= 1024 * 1024, "resource observation exceeded its bound")
        value = json.loads(encoded)
        require(value["id"] == identifier, "runtime statistics identity changed")
        return {"cpu_ns": value["cpu_stats"]["cpu_usage"]["total_usage"],
                "memory_bytes": value["memory_stats"]["usage"], "pids": value["pids_stats"]["current"],
                "block_io": value["blkio_stats"]}
    finally:
        connection.close()
        stream.close()


class Replica:
    def __init__(self, f):
        self.f = f
        primary = f.containers[0]
        info = json.loads(command(["docker", "inspect", primary]).stdout)[0]
        address = info["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
        hba = sql(f, "SHOW hba_file")
        command(["docker", "exec", "-i", "--user", "postgres", primary, "tee", hba],
                data=b"local all all trust\nhost all all 0.0.0.0/0 scram-sha-256\n"
                     b"host replication postgres 0.0.0.0/0 scram-sha-256\n")
        sql(f, "SELECT pg_reload_conf()")
        uid = command(["docker", "exec", primary, "id", "-u", "postgres"]).stdout.decode().strip()
        gid = command(["docker", "exec", primary, "id", "-g", "postgres"]).stdout.decode().strip()
        self.identifier = f.container(["--user", "postgres", "--tmpfs", f"/replica:rw,size=384m,uid={uid},gid={gid}",
            "-e", "PGHOST=" + address, "-e", "PGPORT=5432", "-e", "PGUSER=postgres",
            "-e", "PGPASSWORD=development-only", "-e", "PGAPPNAME=matrix-standby", "--entrypoint", "sh", PG_IMAGE,
            "-ec", "pg_basebackup -D /replica/data -R -X stream --checkpoint=fast; "
            "exec postgres -D /replica/data -c shared_buffers=16MB -c listen_addresses='' "])
        until(lambda: sql(f, "SELECT count(*) FROM pg_stat_replication WHERE application_name='matrix-standby' "
                             "AND state='streaming' AND replay_lsn IS NOT NULL") == "1", "actual streaming replay", 30)

    def lag(self):
        answer = sql(self.f, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),replay_lsn)::bigint "
                            "FROM pg_stat_replication WHERE application_name='matrix-standby' AND state='streaming'")
        require(answer and int(answer) >= 0, "real streaming replica disappeared")
        return int(answer)

    def fence(self, seconds):
        lsn = sql(self.f, "SELECT pg_current_wal_lsn()")
        started = time.monotonic()
        until(lambda: sql(self.f, "SELECT count(*) FROM pg_stat_replication WHERE application_name='matrix-standby' "
                         f"AND state='streaming' AND replay_lsn>='{lsn}'::pg_lsn") == "1", "final replay fence", seconds)
        return time.monotonic() - started


class Probe:
    """Actual Rust metadata::snapshot latency while foreground work is in flight."""
    def __init__(self, f, port, replica, row, deadline):
        self.f, self.port, self.replica, self.row, self.deadline = f, port, replica, row, deadline
        self.done = threading.Event()
        self.samples, self.errors, self.lags = [], [], []
        self.resources = []
        self.thread = threading.Thread(target=self.run)

    def run(self):
        expected = None
        try:
            while not self.done.is_set():
                require(time.monotonic() < self.deadline, "matrix cell absolute deadline expired")
                require(len(self.samples) < 1024, "source observation count exceeded its bound")
                status, body = http(self.port, "/source-probe", 4, 1024)
                fields = body.split()
                require(status == 200 and len(fields) == 6 and fields[0] == b"snapshot", "source probe failed")
                require(all(field.isdigit() for field in fields[1:]), "invalid source observation")
                identity = fields[2:]
                expected = identity if expected is None else expected
                require(identity == expected, "control repository changed during cache work")
                self.samples.append(int(fields[1]) / 1_000_000)
                if self.done.is_set():
                    break
                self.lags.append(self.replica.lag())
                sample = {}
                for name, identity in (("primary", self.f.containers[0]), ("replica", self.replica.identifier)):
                    if self.done.is_set():
                        break
                    sample[name] = resource(identity)
                self.resources.append(sample)
                self.done.wait(.1)
        except Exception as error:
            self.errors.append(type(error).__name__ + ": " + str(error)[:256])

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, error_type, *_):
        self.done.set()
        self.thread.join(timeout=9)
        self.row.update(source_probe_errors=self.errors, source_sample_count=len(self.samples),
                        replica_lag_bytes=self.lags, database_resources=self.resources)
        if self.samples:
            self.row["source_snapshot_ms"] = distribution(self.samples)
        require(not self.thread.is_alive(), "actual source probe did not stop")
        if error_type is None:
            require(not self.errors, "source latency observation failed")


def seed(f, settings):
    directory = f.base / "synthetic.git-worktree"
    f.git(f.base, "init", "--initial-branch=main", directory)
    (directory / "small").mkdir()
    (directory / "sparse").mkdir()
    for index in range(settings["small_files"]):
        (directory / "small" / f"{index:02}.txt").write_text(f"small synthetic file {index}\n")
    lines = [f"stable synthetic line {index:04}\n" for index in range(settings["large_lines"])]
    (directory / "other-large.txt").write_text("other unchanged payload\n" * settings["large_lines"])
    (directory / "rename-source.txt").write_text("byte-preserving rename target\n")
    commits = []
    for ordinal in range(settings["commits"]):
        lines[ordinal] = f"changed synthetic line {ordinal:04}\n"
        (directory / "large.txt").write_text("".join(lines))
        if ordinal % 4 == 0:
            (directory / "sparse/deep.txt").write_text(f"sparse change {ordinal}\n")
        f.git(directory, "add", ".")
        clock = 1_600_000_000 + (ordinal // 2 if ordinal % 2 else 1000 - ordinal)
        environment = dict(f.env, GIT_AUTHOR_DATE=f"@{clock} +0000", GIT_COMMITTER_DATE=f"@{clock} +0000")
        f.git(directory, "commit", "-qm", f"Synthetic history {ordinal}", env=environment)
        commits.append(f.git(directory, "rev-parse", "HEAD").stdout.strip().decode())
    for ordinal in range(settings["branches"]):
        branch = f"side-{ordinal}"
        f.git(directory, "checkout", "-qb", branch, commits[ordinal])
        (directory / f"wide-{ordinal}.txt").write_text(f"wide synthetic branch {ordinal}\n")
        f.git(directory, "add", ".")
        environment = dict(f.env, GIT_AUTHOR_DATE="@1600000100 +0000", GIT_COMMITTER_DATE="@1600000100 +0000")
        f.git(directory, "commit", "-qm", "Synthetic wide branch", env=environment)
        f.git(directory, "checkout", "-q", "main")
        f.git(directory, "merge", "--no-ff", "-qm", "Synthetic merge", branch, env=environment)
    f.git(directory, "mv", "rename-source.txt", "renamed.txt")
    f.git(directory, "commit", "-qm", "Synthetic rename", env=environment)
    f.git(directory, "tag", "-a", "synthetic-tag", "-m", "Synthetic annotated tag", env=environment)
    tip = f.git(directory, "rev-parse", "HEAD").stdout.strip().decode()
    return directory, commits, tip


def storage(f, database):
    environment = dict(f.env, PGDATABASE=database)
    return json.loads(sql(f, "SELECT json_build_object("
        "'relation_bytes',(SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='git_rs_cache' AND c.relkind='r'),"
        "'payloads',(SELECT json_build_object('rows',count(*),'logical_bytes',coalesce(sum(size),0),"
        "'stored_bytes',coalesce(sum(coalesce(stored_size,size)),0),'s3_rows',count(object_key),"
        "'inline_bytes',coalesce(sum(octet_length(body)),0)) FROM git_rs_cache.payloads WHERE state='live'),"
        "'build_claims',(SELECT count(*) FROM git_rs_cache.blame_builds),"
        "'build_attempts',(SELECT coalesce(sum(generation),0) FROM git_rs_cache.blame_builds),"
        "'database',(SELECT row_to_json(d) FROM (SELECT blks_read,blks_hit,temp_bytes,tup_inserted,tup_updated,tup_deleted "
        "FROM pg_stat_database WHERE datname=current_database()) d),"
        "'vacuum',(SELECT coalesce(json_agg(t),'[]') FROM (SELECT relname,n_live_tup,n_dead_tup,vacuum_count,autovacuum_count "
        "FROM pg_stat_user_tables WHERE schemaname='git_rs_cache') t))", environment))


def compaction_state(f):
    return json.loads(sql(f, "SELECT json_build_object('generation',r.generation,'epoch',r.storage_epoch,"
        "'packs',(SELECT count(*) FROM git_rs.packs WHERE repo_id=r.id),"
        "'operations',(SELECT coalesce(json_agg(json_build_object('operation',o.operation_id,'job',o.job_id,"
        "'attempt',o.claim_generation,'current_attempt',j.claim_generation,'state',j.state,"
        "'input_generation',o.input_generation,'input_epoch',o.input_epoch,"
        "'result_generation',o.result_generation,'result_epoch',o.result_epoch)),'[]') "
        "FROM git_rs.maintenance_operations o JOIN git_rs.maintenance_jobs j ON j.id=o.job_id "
        "WHERE j.repo_id=r.id AND j.kind='compact')) FROM git_rs.repositories r WHERE slug='matrix/mutable'"))


def require_compaction(before, after):
    require(not before["operations"] and before["packs"] >= 8, "mixed input is not a fresh compaction candidate")
    require(len(after["operations"]) == 1 and after["packs"] == 1,
            "mixed work has no exact physical compaction publication")
    operation = after["operations"][0]
    require(operation["state"] == "succeeded" and operation["attempt"] == operation["current_attempt"]
            and operation["input_generation"] == before["generation"] + 1
            and operation["result_generation"] == after["generation"] == before["generation"] + 2
            and operation["input_epoch"] == before["epoch"]
            and operation["result_epoch"] == after["epoch"] == before["epoch"] + 1,
            "mixed compaction did not confirm the post-receive catalog")


def drop_database(f, database, admin):
    # Do not force-disconnect a pool whose actual process has not stopped.
    until(lambda: sql(f, f"SELECT count(*) FROM pg_stat_activity WHERE datname='{database}'", admin) == "0",
          "matrix database owners drained", 5)
    sql(f, "DROP DATABASE " + database, admin)


class Matrix:
    def __init__(self, fixture, config):
        self.f, self.config, self.limits = fixture, config, config["limits"]
        fixture.results.update(config=config, cells=[], scope={
            "kind": "bounded synthetic smoke; not full corpus, deployed HA or GitLab scoring",
            "cold": "fresh source/cache databases, namespace and process; OS/PG buffers are not flushed",
            "source_latency": "actual Rust metadata::snapshot wall time, excluding HTTP/probe client startup",
            "copies": "verified response bytes plus actual object/mapping copies; kernel/allocator copies are not instrumented",
            "wal": "whole disposable primary WAL during foreground/probes/mixed source work, not cache-only attribution",
            "replica": "real physical streaming replay and final fence; not failover durability",
            "credentials": "distinct synthetic PG role/database and S3 bucket; one synthetic S3 account",
            "aggregate": "outer supervisor: <=4 GiB, <=2 CPUs, <=256 tasks, no extra swap"})

    def fetch(self, name, port, target, row, label):
        require(time.monotonic() < self.deadline, "matrix cell absolute deadline expired")
        process = self.f.daemons[name]
        before, cpu = metrics(self.f, name), resource(process.identifier)
        started = time.monotonic()
        status, body = http(port, target["url"], min(self.limits["request_seconds"], self.deadline - started),
                            self.limits["response_bytes"])
        elapsed = time.monotonic() - started
        event = {"phase": label, "path": target["path"], "status": status, "seconds": elapsed,
                 "response_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
        row["requests"].append(event)
        require(status == 200 and body == target["expected"], "complete native blame bytes differ")
        until(lambda: metrics(self.f, name)["git_rs_active_requests"] == 0, "actual response cleanup", 5)
        after, consumed = metrics(self.f, name), resource(process.identifier)
        fields = ("commits", "mapping_edges", "mapping_hits", "mapping_computations", "mapping_completed",
                  "mapping_rejected", "objects_reused", "imported_objects_reused", "imported_mapping_loads", "seconds")
        completed = after['git_rs_fixture_work_completions{kind="foreground"}'] - before['git_rs_fixture_work_completions{kind="foreground"}']
        require(completed in (0, 1), "unaccounted duplicate foreground completion")
        event.update(cpu_ns=consumed["cpu_ns"] - cpu["cpu_ns"], memory_bytes=consumed["memory_bytes"],
                     work_completions=completed,
                     body_hits=after["git_rs_fixture_body_hits_total"] - before["git_rs_fixture_body_hits_total"],
                     body_build_claims={outcome: after[f'git_rs_fixture_body_build_claims_total{{outcome="{outcome}"}}'] -
                                       before[f'git_rs_fixture_body_build_claims_total{{outcome="{outcome}"}}']
                                       for outcome in ("owned", "pending", "unavailable")},
                     source_cache_allocated_bytes=after["git_rs_cache_allocated_bytes"],
                     work={field: after[f'git_rs_fixture_work_last_{field}{{kind="foreground"}}'] for field in fields} if completed else {},
                     dependency_deltas={key: after[key] - before[key] for key in before if key.startswith((
                         "git_rs_dependency_duration_seconds_count", "git_rs_s3_transferred_bytes_total", "git_rs_cache_lookups_total"))})
        require(event["work_completions"] + event["body_hits"] == 1, "neither real derivation nor verified body reuse observed")
        require(elapsed <= self.limits["request_seconds"], "foreground request deadline exceeded")
        status = self.f.process_status(process)
        event["rss_hwm_bytes"] = int(re.search(r"VmHWM:\s+(\d+)", status)[1]) * 1024
        require(event["rss_hwm_bytes"] <= self.limits["daemon_rss_bytes"], "daemon RSS exceeded the versioned budget")
        return event

    def start(self, name, profile, enabled, **options):
        extra = ["--source-probe", "matrix/control"]
        if enabled:
            extra.append("--checkpoint-fixture-key")
        if profile == "derivation":
            extra.append("--measure-work")
        return self.f.start_blame(name, cache=enabled, concurrency=1, waiters=0,
                                  seconds=self.limits["request_seconds"], extra=extra, **options)

    def contention(self, row, target):
        f = self.f
        gates = Gates(f.server / ("claim-" + row["id"] + ".sock"))
        first, second = "claim-owner-" + row["id"], "claim-peer-" + row["id"]
        peer = None
        # This scope is new even after the ordinary cell warmed its other repo.
        target = {**target, "url": target["url"].replace("/matrix/history.git/", "/matrix/contended.git/")}
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            _, owner_port = self.start(first, "derivation", True, control=gates.path, phase="body-claimed")
            _, contender_port = self.start(second, "derivation", True)
            owner = pool.submit(self.fetch, first, owner_port, target, row, "contention-owner")
            peer, _ = gates.entered()
            other = self.fetch(second, contender_port, target, row, "contention-peer")
            require(other["body_build_claims"]["pending"] == 1, "contender did not observe the actual live build claim")
            peer.sendall(b"G")
            peer = None
            result = owner.result(timeout=10)
            require(result["body_build_claims"]["owned"] == 1, "first process did not own the exact build claim")
            row["contention"] = {"owned_claims": 1, "pending_claims": 1,
                                 "actual_derivations": result["work_completions"] + other["work_completions"],
                                 "fresh_mapping_computations": result["work"].get("mapping_computations", 0) +
                                                               other["work"].get("mapping_computations", 0)}
        finally:
            actions = ([lambda: peer.sendall(b"E")] if peer is not None else [])
            actions += [lambda: pool.shutdown(wait=True, cancel_futures=True), gates.close]
            actions += [lambda name=name: f.stop(name) for name in (first, second) if name in f.daemons]
            cleanup_all(actions, row.setdefault("contention_cleanup", {}))

    def mixed(self, row, target, name, port, mutable, tip, fetch_url, publisher):
        f = self.f
        gateway = FaultGateway(f.env["GIT_RS_S3_ENDPOINT"])
        active = None
        try:
            gateway.arm("pause-get")
            active, _ = worker(f, "matrix-worker-" + row["id"], GIT_RS_S3_ENDPOINT=gateway.endpoint)
            require(gateway.reached.wait(10), "mixed maintenance did not reach its actual source GET")
            require(sql(f, "SELECT count(*) FROM git_rs.maintenance_jobs j JOIN git_rs.repositories r ON r.id=j.repo_id "
                           "WHERE j.state='running' AND j.kind='compact' AND r.slug='matrix/mutable'") == "1",
                    "mixed worker has no live compaction on the mutable repository")
            before = compaction_state(f)
            with ThreadPoolExecutor(max_workers=2) as pool:
                push = pool.submit(native, f, mutable, ["push", publisher + "/matrix/mutable.git", tip + ":refs/heads/mixed"], self.limits["request_seconds"])
                fetch = pool.submit(native, f, mutable, ["fetch", fetch_url, "main"], self.limits["request_seconds"])
                self.fetch(name, port, target, row, "mixed")
                push.result(timeout=10)
                fetch.result(timeout=10)
            gateway.reset()
            def published():
                state = compaction_state(f)
                return state if state["operations"] else None
            after = until(published, "mixed physical compaction after concurrent publication", 20)
            require_compaction(before, after)
            row["mixed"] = {"receive": True, "fetch": True, "blame": True, "compaction": True,
                             "before": before, "after": after,
                             "overlap": "live maintenance claim/source GET held during foreground operations on separate repositories"}
        finally:
            gateway.reset()
            try:
                if active is not None:
                    stop(active)
            finally:
                gateway.close()

    def cell(self, ordinal, repetition, placement, profile, seed, commits, tip, expected, replica):
        f = self.f
        previous = dict(f.env)
        source_db, cache_db, role = ("matrix_" + uuid.uuid4().hex for _ in range(3))
        admin = dict(f.env)
        row = {"id": str(ordinal), "repetition": repetition, "placement": placement, "profile": profile,
               "status": "preparing", "requests": []}
        f.results["cells"].append(row)
        write_report(f)
        proxies = []
        created = []
        role_created = False
        try:
            for database in (source_db, cache_db):
                sql(f, "CREATE DATABASE " + database, admin)
                created.append(database)
            sql(f, f"CREATE ROLE {role} LOGIN PASSWORD 'synthetic-cache-only'", admin)
            role_created = True
            sql(f, f"GRANT CREATE ON DATABASE {cache_db} TO {role}", admin)
            f.env["PGDATABASE"] = source_db
            f.cli("migrate")
            if placement != "disabled":
                pg = WireProxy("127.0.0.1", f.env["PGPORT"])
                proxies.append(pg)
                s3 = WireProxy("127.0.0.1", int(f.env["GIT_RS_S3_ENDPOINT"].rsplit(":", 1)[1]))
                proxies.append(s3)
                f.env.update(GIT_RS_CACHE_DSN=f"host=127.0.0.1 port={pg.port} user={role} password=synthetic-cache-only dbname={cache_db} sslmode=disable",
                    GIT_RS_CACHE_NAMESPACE=str(uuid.uuid4()), GIT_RS_CACHE_OWNER=str(uuid.uuid4()),
                    GIT_RS_CACHE_INLINE_BYTES=str(8 * 1024 * 1024 if placement == "all-pg" else 0),
                    GIT_RS_CACHE_S3_ENDPOINT=f"http://127.0.0.1:{s3.port}", GIT_RS_CACHE_S3_BUCKET="matrix-derived",
                    GIT_RS_CACHE_S3_ACCESS_KEY="test-access", GIT_RS_CACHE_S3_SECRET_KEY="synthetic-secret-key")
                f.cli("cache-migrate")
            for slug in ("matrix/control", "matrix/history", "matrix/mutable", "matrix/contended"):
                f.cli("create", slug, "alice")
            _, publish = f.start("publisher-" + str(ordinal))
            publisher = f"http://127.0.0.1:{publish}"
            f.git(seed, "push", publisher + "/matrix/control.git", commits[0] + ":refs/heads/main")
            f.git(seed, "push", publisher + "/matrix/history.git", "main", "--tags")
            f.git(seed, "push", publisher + "/matrix/contended.git", "main")
            for commit in commits[:7] + [tip]:
                f.git(seed, "push", publisher + "/matrix/mutable.git", commit + ":refs/heads/main")
            targets = [{"path": path, "url": f"/matrix/history.git/blame/{tip}?path={quote(path, safe='')}", "expected": body}
                       for path, body in expected.items()]
            row["artifact_identities"] = [{"path": target["path"], "bytes": len(target["expected"]),
                                          "sha256": hashlib.sha256(target["expected"]).hexdigest()} for target in targets]
            first, second = "first-" + str(ordinal), "second-" + str(ordinal)
            _, port = self.start(first, profile, placement != "disabled")
            self.deadline = time.monotonic() + self.limits["cell_seconds"]
            started = time.monotonic()
            row["status"] = "running"
            lsn = sql(f, "SELECT pg_current_wal_lsn()")
            wire = [proxy.snapshot() for proxy in proxies]
            if proxies:
                row["storage_before"] = storage(f, cache_db)
            with Probe(f, f.health_ports[first], replica, row, self.deadline) as probe:
                for phase in ("cold", "local-repeat"):
                    for target in targets:
                        self.fetch(first, port, target, row, phase)
                _, other = self.start(second, profile, placement != "disabled")
                for target in targets:
                    self.fetch(second, other, target, row, "cross-process")
                f.stop(second)
                _, other = self.start(second + "-restart", profile, placement != "disabled")
                for target in targets:
                    self.fetch(second + "-restart", other, target, row, "restart")
                f.stop(second + "-restart")
                if proxies:
                    self.contention(row, targets[0])
                    sql(f, f"REVOKE CONNECT ON DATABASE {cache_db} FROM PUBLIC,{role}; "
                           f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{cache_db}'", admin)
                    _, outage = self.start(second + "-outage", profile, True)
                    for target in targets:
                        self.fetch(second + "-outage", outage, target, row, "cache-outage")
                    f.stop(second + "-outage")
                    sql(f, f"GRANT CONNECT ON DATABASE {cache_db} TO {role}", admin)
                self.mixed(row, targets[0], first, port, seed, tip, publisher + "/matrix/history.git", publisher)
                until(lambda: len(probe.samples) >= self.limits["minimum_source_samples"] or probe.errors,
                      "minimum actual source sample inventory", 5)
            row["replica_catchup_seconds"] = replica.fence(self.limits["replica_catchup_seconds"])
            row["wal_bytes"] = int(sql(f, f"SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'{lsn}'::pg_lsn)::bigint"))
            row["wire_bytes"] = {name: [now - old for now, old in zip(proxy.snapshot(), before)]
                                 for name, proxy, before in zip(("cache_pg", "cache_s3"), proxies, wire)}
            f.stop_daemons()
            if proxies:
                row["storage_after"] = storage(f, cache_db)
                layout = row["storage_after"]["payloads"]
                require(layout["rows"] > 0, "matrix failed to publish real derived payloads")
                require((placement == "all-pg" and layout["s3_rows"] == 0 and layout["inline_bytes"] == layout["stored_bytes"])
                        or (placement == "pg-s3" and layout["s3_rows"] == layout["rows"] and layout["inline_bytes"] == 0),
                        "declared physical placement was not exercised")
            row["elapsed_seconds"] = time.monotonic() - started
            require(row["elapsed_seconds"] <= self.limits["cell_seconds"], "matrix cell exceeded its absolute budget")
            self.score(row)
            row["status"] = "work-passed"
        except BaseException as error:
            row.update(status="failed", failure=type(error).__name__)
            raise
        finally:
            def restore_environment():
                f.env.clear()
                f.env.update(previous)

            actions = [lambda name=name: f.stop(name) for name in list(f.daemons)]
            actions += [proxy.close for proxy in reversed(proxies)]
            actions.append(restore_environment)
            actions += [lambda database=database: drop_database(f, database, admin) for database in created]
            if role_created:
                actions.append(lambda: sql(f, "DROP ROLE " + role, admin))
            try:
                cleanup_all(actions, row)
                if row["status"] == "work-passed" and not row["cleanup_errors"]:
                    row["status"] = "passed"
            finally:
                if row["cleanup_errors"]:
                    row["status"] = "failed"
                write_report(f)

    def score(self, row):
        require(not row["source_probe_errors"] and row["source_sample_count"] >= self.limits["minimum_source_samples"],
                "incomplete source observations")
        require(row["source_snapshot_ms"]["p99"] <= self.limits["source_p99_ms"], "source tail exceeded the frozen budget")
        requests = row["requests"]
        inventory = {phase: len(self.config["paths"]) for phase in ("cold", "local-repeat", "cross-process", "restart")}
        inventory["mixed"] = 1
        if row["placement"] != "disabled":
            inventory.update({"cache-outage": len(self.config["paths"]), "contention-owner": 1, "contention-peer": 1})
        require({phase: sum(event["phase"] == phase for event in requests) for phase in {event["phase"] for event in requests}} == inventory,
                "incomplete or duplicated measured request inventory")
        for phase in ("cold", "local-repeat", "cross-process", "restart"):
            require({event["path"] for event in requests if event["phase"] == phase} == set(self.config["paths"]),
                    "changed query set within a cell")
        if row["placement"] == "disabled":
            require(all(event["work_completions"] == 1 and event["body_hits"] == 0 for event in requests),
                    "disabled cache unexpectedly reused a final body")
        elif row["profile"] == "body":
            require(all(event["body_hits"] == 1 for event in requests if event["phase"] in {"local-repeat", "cross-process", "restart"}),
                    "complete-body cache did not survive cross-process restart")
        else:
            require(all(event["body_hits"] == 0 for event in requests), "ready body contaminated derivation measurements")
            cold = sum(event["work"].get("mapping_computations", 0) for event in requests if event["phase"] == "cold")
            for phase in ("local-repeat", "cross-process", "restart"):
                events = [event for event in requests if event["phase"] == phase]
                require(sum(event["work"].get("mapping_hits", 0) for event in events) > 0
                        and sum(event["work"].get("imported_mapping_loads", 0) for event in events) > 0
                        and sum(event["work"].get("mapping_computations", 0) for event in events) < cold,
                        "shared checkpoints did not avoid actual fresh derivation")
        if row["placement"] != "disabled":
            require(all(event["work_completions"] == 1 and event["body_hits"] == 0 for event in requests if event["phase"] == "cache-outage"),
                     "cache outage did not recompute a complete correct body")
        require_compaction(row["mixed"]["before"], row["mixed"]["after"])
        row["latency_seconds"] = {phase: distribution([event["seconds"] for event in requests if event["phase"] == phase])
                                  for phase in sorted({event["phase"] for event in requests})}
        row["build_plus_reads_seconds"] = sum(event["seconds"] for event in requests
                                              if event["phase"] in {"cold", "local-repeat", "cross-process", "restart"})
        row["daemon_cpu_ns_by_phase"] = {phase: sum(event["cpu_ns"] for event in requests if event["phase"] == phase)
                                        for phase in row["latency_seconds"]}

    def run(self):
        f = self.f
        directory, commits, tip = seed(f, self.config["fixture"])
        expected = {}
        controls = []
        for path in self.config["paths"]:
            started = time.monotonic()
            expected[path] = f.git(directory, "--literal-pathspecs", "blame", "--root", "--line-porcelain",
                                   "--no-textconv", "--no-progress", tip, "--", path).stdout
            require(len(expected[path]) <= self.limits["response_bytes"], "native control exceeds fixture output budget")
            controls.append({"path": path, "seconds": time.monotonic() - started,
                             "bytes": len(expected[path]), "sha256": hashlib.sha256(expected[path]).hexdigest()})
        f.results["native_controls"] = controls
        config = (f'url = "{f.env["GIT_RS_S3_ENDPOINT"]}/matrix-derived"\naws-sigv4 = "aws:amz:us-east-1:s3"\n'
                  'user = "test-access:synthetic-secret-key"\n')
        command(["curl", "--disable", "--silent", "--fail", "--max-time", "5", "--config", "-", "--request", "PUT"], data=config.encode())
        replica = Replica(f)
        ordinal = 0
        for repetition in range(self.config["repetitions"]):
            placements = self.config["placements"]
            placements = placements[repetition % len(placements):] + placements[:repetition % len(placements)]
            for profile in self.config["profiles"]:
                for placement in placements:
                    self.cell(ordinal, repetition, placement, profile, directory, commits, tip, expected, replica)
                    ordinal += 1
        for row in f.results["cells"]:
            baseline = next(other for other in f.results["cells"] if other["placement"] == "disabled"
                            and other["repetition"] == row["repetition"] and other["profile"] == row["profile"])
            require(row["artifact_identities"] == baseline["artifact_identities"], "rotated cells used different artifacts")
            bound = min(self.limits["source_p99_ms"], baseline["source_snapshot_ms"]["p99"] *
                        self.limits["source_baseline_factor"] + self.limits["source_baseline_allowance_ms"])
            require(row["source_snapshot_ms"]["p99"] <= bound, "source latency exceeded the matched baseline budget")
        require(len(f.results["cells"]) == self.config["repetitions"] * len(self.config["placements"]) * len(self.config["profiles"])
                and all(row["status"] == "passed" for row in f.results["cells"]), "incomplete matrix execution or cleanup")
        f.results["status"] = "passed"
        write_report(f)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "tests/fixtures/cache-matrix.json")
    parser.add_argument("--provision", action="store_true")
    parser.add_argument("--runtime-image", default=os.environ.get("GIT_RS_TEST_RUNTIME_IMAGE"))
    parser.add_argument("--runtime-fixture-image", default=os.environ.get("GIT_RS_TEST_RUNTIME_FIXTURE_IMAGE"))
    parser.add_argument("--revision", default=os.environ.get("GIT_RS_TEST_REVISION"))
    parser.add_argument("--cluster-container", required=True)
    parser.add_argument("--fixture-dir", type=Path)
    args = parser.parse_args()
    require(args.provision and args.runtime_image and args.runtime_fixture_image and args.revision, "prebuilt supervised matrix inputs required")
    config = load_config(args.config)
    if args.fixture_dir:
        args.fixture_dir.mkdir(mode=0o700)
        context = nullcontext(str(args.fixture_dir))
    else:
        local = ROOT / ".local"
        local.mkdir(mode=0o700, exist_ok=True)
        context = tempfile.TemporaryDirectory(prefix="cache-matrix-", dir=local)
    with context as directory:
        f = Fixture(Path(directory), "/usr/local/bin/git-rs", runtime_image=args.runtime_image,
                    runtime_fixture_image=args.runtime_fixture_image, revision=args.revision)
        f.results["config_sha256"] = hashlib.sha256(args.config.read_bytes()).hexdigest()
        try:
            f.configure_maintenance(args.cluster_container)
            f.setup(True)
            Matrix(f, config).run()
        finally:
            try:
                f.close()
            finally:
                write_report(f)


if __name__ == "__main__":
    main()
