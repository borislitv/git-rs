#!/usr/bin/env python3
"""Owned loopback contract; see docs/warm-reconcile-runtime.md. No provisioning of daemons."""
import argparse
from contextlib import contextmanager
import hashlib
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
from urllib.parse import quote_from_bytes
from uuid import UUID


ROOT = Path(__file__).resolve().parents[1]
SLUG = "synthetic/warm"
BRANCH = "refs/heads/main"
PATH = b"fixture-\xff.txt"
WARM_API = ("warm_slots()", "warm_acquire(integer,uuid,integer)", "warm_identity(uuid)",
            "warm_snapshot(uuid)", "warm_check(uuid,uuid,bigint,bigint,bytea)", "warm_release(uuid)")
KEY_HEADER = b"git-rs-artifact-key\0\x01"
BODY_KIND = "complete-blame-v2"
MAP_KIND = "blame-completed-attribution-v1"
SEED_KIND = "blame-replacement-seed-v1"


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def eventually(probe, message, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(0.1)
    raise RuntimeError(message)


def run(args, env, label, data=None, seconds=20, failure=None):
    with subprocess.Popen(args, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, start_new_session=True) as child:
        try:
            out, err = child.communicate(data, timeout=seconds)
        except BaseException:
            os.killpg(child.pid, signal.SIGKILL)
            child.communicate(timeout=5)
            raise
        # Do not echo environment, SQL, or subprocess diagnostics containing credentials.
        if failure is None:
            require(child.returncode == 0, f"{label} exited {child.returncode}")
        else:
            require(child.returncode > 0 and failure in err, f"{label}: expected diagnostic failure")
        return out


def private_config(path):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                and not info.st_mode & 0o077 and info.st_size <= 8192,
                "use an owner-private regular configuration file")
        return json.loads(stream.read(8193))


def uuid(value):
    require(str(UUID(value)) == value, "use a canonical fixture UUID")
    return value


def parse_key(encoded, include_optional=False):
    wire = bytes.fromhex(encoded)
    require(wire.startswith(KEY_HEADER), "unexpected complete-body key framing")
    offset = len(KEY_HEADER)

    def number():
        nonlocal offset
        require(offset + 4 <= len(wire), "short key framing")
        result = int.from_bytes(wire[offset:offset + 4], "big")
        offset += 4
        return result

    def field():
        nonlocal offset
        size = number()
        require(offset + size <= len(wire), "short key field")
        result = wire[offset:offset + size]
        offset += size
        return result

    fields = [field() for _ in range(7)]
    # The chunk-manifest kind is shared by bodies, seeds, WorkSets, and maps.
    if not include_optional and tuple(fields[3:5]) in {
        (b"blame-completed-attribution-v1", b"native-root-completed-map-v2"),
        (b"blame-replacement-seed-v1", b"fixed-coordinate-complete-attribution-v1"),
        (b"complete-blame-workset", b"authenticated-complete-units-v1"),
    }:
        return None
    require(number() == 3, "unexpected complete-body input arity")
    inputs = [field() for _ in range(3)]
    require(offset == len(wire), "unexpected complete-body key suffix")
    return hashlib.sha256(wire).hexdigest(), fields, inputs


def http(port, path, maximum=256 * 1024):
    connection = HTTPConnection("127.0.0.1", port, timeout=20)
    try:
        connection.request("GET", path, headers={"Connection": "close"})
        response = connection.getresponse()
        body = response.read(maximum + 1)
        require(len(body) <= maximum, "fixture response exceeds limit")
        return response.status, body
    finally:
        connection.close()


class Fixture:
    def __init__(self, args):
        cfg = private_config(args.config)
        self.repo, self.namespace, self.owner = (uuid(cfg[k]) for k in ("repository", "namespace", "owner"))
        src, cache, warm = (cfg[k] for k in ("source_db", "cache_db", "warm_role"))
        for value, prefix in [(src["database"], "warm_src_"), (cache["database"], "warm_cache_"),
                              (warm["user"], "warm_reader_")]:
            require(re.fullmatch(prefix + r"[a-z0-9_]{1,32}", value), "require disposable fixture names")
        require(len({src["user"], cache["user"], warm["user"]}) == 3, "PG identities must be distinct")
        require(len({cfg[k]["access"] for k in ("source_s3", "warm_source_s3", "cache_s3")}) == 3,
                "S3 identities must be distinct")
        require(cfg["source_s3"]["bucket"] != cfg["cache_s3"]["bucket"], "use separate S3 buckets")
        self.directory = args.workdir.resolve(strict=True)
        require(self.directory.stat().st_uid == os.getuid() and self.directory.stat().st_mode & 0o777 == 0o700
                and not any(self.directory.iterdir()), "use an empty owner-private 0700 work directory")
        require(len(os.fsencode(self.directory)) <= 80, "use a short work directory for Unix sockets")
        for parent in (self.directory, *self.directory.parents):
            info = parent.stat()
            require(info.st_uid in (0, os.getuid()) and not info.st_mode & 0o022,
                    "signing-key ancestry must be protected; do not use /tmp")
        self.git, self.cli, self.observer = (str(getattr(args, k).resolve(strict=True))
                                            for k in ("git", "binary", "observer"))
        key = self.directory / "signing-key"
        key.write_bytes(secrets.token_bytes(32))  # main sets umask before any file is created
        self.base = dict(PATH=os.defpath, HOME=str(self.directory), XDG_CONFIG_HOME=str(self.directory),
                         LANG="C", LC_ALL="C", TZ="UTC", GIT_CONFIG_NOSYSTEM="1",
                         GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0", GIT_PROTOCOL_FROM_USER="0",
                         GIT_ALLOW_PROTOCOL="http", GIT_RS_DEV_HTTP="1", GIT_RS_OPERATION_SECONDS="30")
        pgport, s3port = (cfg[k] for k in ("pg_port", "s3_port"))
        require(all(type(p) is int and 1024 <= p <= 65535 for p in (pgport, s3port)), "invalid loopback port")
        pg = dict(PGHOST="127.0.0.1", PGPORT=str(pgport), PGSSLMODE="disable", PGCONNECT_TIMEOUT="3")
        self.source = dict(self.base, **pg, PGDATABASE=src["database"], PGUSER=src["user"], PGPASSWORD=src["password"])
        self.cache = dict(self.base, **pg, PGDATABASE=cache["database"], PGUSER=cache["user"], PGPASSWORD=cache["password"])
        self.reader = dict(self.source, PGUSER=warm["user"], PGPASSWORD=warm["password"])
        self.login = warm["user"]
        self.owner_tag = f"git-rs-warm-runtime:{self.repo}"

        def s3(prefix, data, bucket):
            require(re.fullmatch(r"warm-[a-z0-9-]{1,48}", bucket), "require disposable S3 bucket names")
            return {prefix + k: v for k, v in dict(ENDPOINT=f"http://127.0.0.1:{s3port}", BUCKET=bucket,
                                                  ACCESS_KEY=data["access"], SECRET_KEY=data["secret"]).items()}

        source_bucket = cfg["source_s3"]["bucket"]
        self.writer = dict(self.source, **s3("GIT_RS_S3_", cfg["source_s3"], source_bucket))
        derived = {"GIT_RS_CACHE_" + k: self.cache[k] for k in pg if k != "PGCONNECT_TIMEOUT"}
        derived.update({"GIT_RS_CACHE_" + k: self.cache[k] for k in ("PGDATABASE", "PGUSER", "PGPASSWORD")})
        derived.update(s3("GIT_RS_CACHE_S3_", cfg["cache_s3"], cfg["cache_s3"]["bucket"]))
        derived.update(GIT_RS_CACHE_NAMESPACE=self.namespace, GIT_RS_CACHE_OWNER=self.owner,
                       GIT_RS_CACHE_INLINE_BYTES="0", GIT_RS_BLAME_CHECKPOINT_KEY_FILE=str(key))
        self.warmer = dict(self.reader, **s3("GIT_RS_S3_", cfg["warm_source_s3"], source_bucket), **derived,
                           GIT_RS_WARM_JOB_SECONDS="30")
        self.foreground = dict(self.writer, **derived)
        self.checkout = self.directory / "client"

    def sql(self, env, text):
        return run(["psql", "-XqAt", "-v", "ON_ERROR_STOP=1"],
                   dict(env, PGOPTIONS="-c statement_timeout=5000"), "fixture SQL",
                   data=text.encode(), seconds=8).decode().strip()

    def admin(self, text):
        return self.sql(self.source, text)

    def cache_sql(self, text):
        return self.sql(self.cache, text)

    def setup(self):
        require(run([self.git, "--version"], self.base, "Git oracle version").strip() == b"git version 2.56.0",
                "exact native Git 2.56.0 oracle required")
        marker = "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database();"
        require(all(self.sql(env, marker) == self.owner_tag for env in (self.source, self.cache)),
                "fixture database ownership marker missing")
        require(self.admin("SELECT to_regnamespace('git_rs') IS NULL;") == "t"
                and self.cache_sql("SELECT to_regnamespace('git_rs_cache') IS NULL;") == "t",
                "require fresh disposable schemas; do not run over an existing installation")
        old = ["001.sql", "002_maintenance.sql", "current_functions.sql"]
        self.admin("BEGIN;\n" + "\n".join((ROOT / "migrations" / p).read_text() for p in old) + "\nCOMMIT;")
        self.admin(f"INSERT INTO git_rs.repositories(id,slug) VALUES ('{self.repo}','{SLUG}');"
                   f"INSERT INTO git_rs.permissions(repo_id,principal,role) VALUES ('{self.repo}','writer','owner');")
        before = self.source_state()
        require(self.admin("SELECT version FROM git_rs.schema_version") == "2"
                and self.admin("SELECT to_regclass('git_rs.warm_targets') IS NULL") == "t", "missing pre-warm v2 fixture")
        run([self.cli, "migrate"], self.source, "upgrade metadata v2")
        grants = ",".join("git_rs." + name for name in WARM_API)
        self.admin(f'GRANT USAGE ON SCHEMA git_rs TO "{self.login}";'
                   f'GRANT EXECUTE ON FUNCTION {grants} TO "{self.login}";')
        run([self.cli, "migrate"], self.source, "reapply metadata migration with grants")
        require(before == self.source_state(), "migration changed existing repository")
        self.rebind("alice", insert=True)
        require(self.sql(self.reader, f"SELECT session_user || '|' || git_rs.warm_slots();") == self.login + "|1",
                "scoped worker login or grants missing")
        require(self.sql(self.reader, "SELECT NOT rolbypassrls AND NOT rolsuper AND NOT rolcreaterole "
                         "AND NOT rolcreatedb AND NOT rolinherit AND NOT EXISTS "
                         "(SELECT FROM pg_auth_members m WHERE m.member=r.oid) "
                         "FROM pg_roles r WHERE rolname=session_user;") == "t", "worker login is too broad")
        require(self.sql(self.reader, "SELECT NOT EXISTS (SELECT FROM pg_class c JOIN pg_namespace n "
                         "ON n.oid=c.relnamespace WHERE n.nspname='git_rs' AND c.relkind IN ('r','v') "
                         "AND has_table_privilege(c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE'));") == "t",
                "worker has direct source table privileges")
        run([self.cli, "cache-migrate"], self.foreground, "migrate independent cache")
        require(self.cache_sql(f"SELECT count(*) FROM git_rs_cache.namespaces WHERE id='{self.namespace}' "
                               f"AND owner='{self.owner}' AND owner_role=current_user;") == "1", "cache ownership mismatch")

    def rebind(self, principal, insert=False):
        if insert:
            self.admin(f"INSERT INTO git_rs.warm_targets VALUES ('{self.login}',1,'{self.repo}',"
                       f"'{principal}',convert_to('{BRANCH}','UTF8'),decode('{PATH.hex()}','hex'));")
        else:
            self.admin(f"UPDATE git_rs.warm_targets SET principal='{principal}' WHERE worker='{self.login}' AND slot=1;")

    def grant(self, principal, role):
        run([self.cli, "grant", SLUG, "writer", principal, role, "refs/"], self.writer, "change fixture ACL")

    def source_state(self):
        return self.admin(f"SELECT r.generation,r.storage_epoch,(SELECT count(*) FROM git_rs.operations "
                          f"WHERE repo_id=r.id),(SELECT count(*) FROM git_rs.packs WHERE repo_id=r.id),"
                          f"(SELECT encode(oid,'hex') FROM git_rs.refs WHERE repo_id=r.id "
                          f"AND name=convert_to('{BRANCH}','UTF8')) FROM git_rs.repositories r WHERE r.id='{self.repo}';")

    def drain(self):
        eventually(lambda: self.admin(f"SELECT count(*) FROM git_rs.repository_leases WHERE repo_id='{self.repo}'") == "0"
                   and self.cache_sql(f"SELECT count(*) FROM git_rs_cache.read_holds WHERE namespace='{self.namespace}'") == "0",
                   "source/cache read holds did not drain")

    def once(self, failure=None):
        before = self.source_state()
        run([self.cli, "warm-reconcile", "--once"], self.warmer, "dedicated warm pass", seconds=40,
            failure=failure)
        self.drain()
        require(before == self.source_state(), "warm worker mutated source publication")

    @contextmanager
    def service(self, label, args, env):
        log = self.directory / (label + ".log")
        with log.open("xb") as output, subprocess.Popen(args, env=env, stdin=subprocess.DEVNULL,
                stdout=output, stderr=subprocess.STDOUT, start_new_session=True) as child:
            def ports():
                require(child.poll() is None, f"{label} exited; inspect private log")
                with log.open("rb") as stream:
                    lines = stream.read(65536)
                health = re.search(rb"(?m)^health 127\.0\.0\.1:(\d+)$", lines)
                listener = re.search(rb"(?m)^development HTTP 127\.0\.0\.1:(\d+)$", lines)
                return (int(health[1]), int(listener[1])) if health and listener else None

            try:
                yield eventually(ports, f"{label} did not bind both loopback listeners")
            finally:
                if child.poll() is None:
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait(timeout=5)

    def native(self, *args):
        return run([self.git, "-c", "user.name=Synthetic", "-c", "user.email=warm@example.invalid",
                    "-c", "commit.gpgsign=false", "-C", self.checkout, *args], self.base, "native Git fixture")

    def push(self, port, lines, label):
        (self.checkout / os.fsdecode(PATH)).write_bytes(lines)
        self.native("add", "--", os.fsdecode(PATH))
        self.native("commit", "--quiet", "-m", label)
        tip = self.native("rev-parse", "HEAD").decode().strip()
        self.native("push", "--quiet", f"http://127.0.0.1:{port}/{SLUG}.git", f"HEAD:{BRANCH}")
        require(self.source_state().split("|")[-1] == tip, "native push did not publish expected tip")
        return tip

    def bodies(self):
        rows = self.cache_sql("SELECT encode(j.canonical,'hex'),encode(d.key_hash,'hex'),d.payload,"
                              "EXISTS(SELECT FROM git_rs_cache.manifest_children c JOIN git_rs_cache.payloads p "
                              "ON p.namespace=c.namespace AND p.id=c.child WHERE c.namespace=d.namespace "
                              "AND c.parent=d.payload AND p.object_key IS NOT NULL AND p.state='live') "
                              "FROM git_rs_cache.directory d JOIN git_rs_cache.jobs j USING(namespace,key_hash) "
                              "JOIN git_rs_cache.manifest_roots m ON m.namespace=d.namespace AND m.payload=d.payload "
                              f"WHERE d.namespace='{self.namespace}' AND m.kind=1 AND d.state='ready' "
                              "AND d.expires_at>clock_timestamp();")
        result = {}
        for row in rows.splitlines():
            canonical, digest, payload, s3_child = row.split("|")
            parsed = parse_key(canonical)
            if parsed is None:
                continue
            hashed, fields, inputs = parsed
            require(hashed == digest and s3_child == "t" and fields[0] == self.repo.encode()
                    and fields[2:5] == [b"sha1", b"complete-blame-v2", b"native-root-line-porcelain-v1"]
                    and len(inputs[0]) == 8 and len(inputs[1]) == 20 and inputs[2] == PATH,
                    "unexpected durable body key or missing live S3 body chunk")
            scope = fields[1].decode(), inputs[1].hex()
            require(scope not in result, "multiple complete bodies for one scope")
            result[scope] = (uuid(payload), digest, fields[5])
        return result

    def maps(self):
        rows = self.cache_sql("SELECT encode(j.canonical,'hex') FROM git_rs_cache.directory d "
                              "JOIN git_rs_cache.jobs j USING(namespace,key_hash) "
                              "JOIN git_rs_cache.manifest_roots m ON m.namespace=d.namespace AND m.payload=d.payload "
                              f"WHERE d.namespace='{self.namespace}' AND m.kind=1 AND d.state='ready' "
                              "AND d.expires_at>clock_timestamp() "
                              f"AND position(convert_to('{MAP_KIND}','UTF8') in j.canonical)>0;")
        scopes = set()
        for row in rows.splitlines():
            _, fields, inputs = parse_key(row, include_optional=True)
            require(fields[0] == self.repo.encode()
                    and fields[3:5] == [MAP_KIND.encode(), b"native-root-completed-map-v2"]
                    and len(inputs[0]) == 8 and len(inputs[1]) == 20 and inputs[2] == PATH,
                    "unexpected warm map key scope")
            scopes.add((fields[1].decode(), inputs[1].hex()))
        return scopes

    def refuse_body_publication(self):
        # Fail just the required root transaction. Other cache keys remain
        # writable so a map/seed staged out of order cannot hide behind the fault.
        self.cache_sql("CREATE SEQUENCE git_rs_cache.fixture_body_attempt; "
                       "CREATE FUNCTION git_rs_cache.fixture_refuse_body() RETURNS trigger "
                       "LANGUAGE plpgsql AS $$ BEGIN "
                       f"IF NEW.namespace='{self.namespace}' AND EXISTS (SELECT FROM git_rs_cache.jobs j "
                       "WHERE j.namespace=NEW.namespace AND j.key_hash=NEW.key_hash "
                       f"AND position(convert_to('{BODY_KIND}','UTF8') in j.canonical)>0) "
                       "THEN PERFORM nextval('git_rs_cache.fixture_body_attempt'); "
                       "RAISE EXCEPTION 'fixture refuses ordinary body publication'; END IF; "
                       "RETURN NEW; END $$; "
                       "CREATE TRIGGER fixture_refuse_body BEFORE INSERT OR UPDATE ON git_rs_cache.directory "
                       "FOR EACH ROW EXECUTE FUNCTION git_rs_cache.fixture_refuse_body();")
        try:
            self.once(failure=b"warm reconciliation pass had target errors")
            require(not self.bodies(), "refused ordinary body became durable")
            for kind in (MAP_KIND, SEED_KIND):
                require(self.cache_sql("SELECT count(*) FROM git_rs_cache.blame_builds "
                                       f"WHERE namespace='{self.namespace}' "
                                       f"AND position(convert_to('{kind}','UTF8') in canonical)>0;") == "0",
                        "optional artifact started before required body confirmation")
            require(self.cache_sql("SELECT is_called FROM git_rs_cache.fixture_body_attempt;") == "t",
                    "worker failed before it attempted the ordinary body root")
        finally:
            self.cache_sql("DROP TRIGGER fixture_refuse_body ON git_rs_cache.directory; "
                           "DROP FUNCTION git_rs_cache.fixture_refuse_body(); "
                           "DROP SEQUENCE git_rs_cache.fixture_body_attempt;")
        self.evict()

    def verify(self, label, principal, tip, hit, map_hit=False, maximum=256 * 1024):
        before = self.bodies()
        env = self.foreground
        args = [self.observer, "--socket", self.directory / "observer.sock", "--principal", principal,
                "--seconds", "30", "--concurrency", "1", "--waiters", "0", "--cache",
                "--checkpoint-key-file", env["GIT_RS_BLAME_CHECKPOINT_KEY_FILE"]]
        with self.service(label, args, env) as (health, listener):
            def metrics():
                status, body = http(health, "/metrics")
                require(status == 200, "observer metrics unavailable")
                return body.splitlines()

            def check_counts(hits):
                counters = metrics()
                require(f"git_rs_fixture_body_hits_total {hits}".encode() in counters
                        and b'git_rs_fixture_work_completions{kind="foreground"} 0' in counters
                        and all(f'git_rs_fixture_body_build_claims_total{{outcome="{outcome}"}} 0'.encode() in counters
                                for outcome in ("owned", "pending", "unavailable")),
                        "foreground did not take the expected exact cache path")

            check_counts(0)
            status, body = http(listener, f"/{SLUG}.git/blame/{tip}?path={quote_from_bytes(PATH, safe='')}", maximum)
            expected = self.native("--literal-pathspecs", "blame", "--root", "--line-porcelain", "--no-textconv",
                                   "--no-progress", tip, "--", os.fsdecode(PATH)) if hit or map_hit else b""
            require((status, body) == (200 if hit or map_hit else 500, expected),
                    f"{label}: unexpected status/native bytes ({status})")
            if map_hit:
                counters = metrics()
                prefix = b'git_rs_fixture_work_last_completed_map_lines{kind="foreground"} '
                require(any(line.startswith(prefix) and int(line[len(prefix):]) > 0 for line in counters)
                        and b'git_rs_fixture_body_hits_total 0' in counters
                        and b'git_rs_fixture_work_completions{kind="foreground"} 1' in counters,
                        "foreground did not consume the earlier warm map")
            else:
                check_counts(int(hit))
            self.drain()
        require(self.bodies() == before, "foreground unexpectedly published/re-signed a complete body")

    def evict(self):
        # Drive the owned namespace through its normal DB-clock expiry boundary;
        # never delete object bytes or re-use a retired payload identity.
        self.cache_sql(f"UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' "
                       f"WHERE namespace='{self.namespace}' AND state='ready';")
        require(self.cache_sql(f"SELECT count(*) FROM git_rs_cache.directory WHERE namespace='{self.namespace}' "
                               "AND state='ready' AND expires_at>clock_timestamp();") == "0",
                "eviction setup left reusable body/map/WorkSet evidence")
        eventually(lambda: self.cache_sql(f"SELECT count(*) FROM git_rs_cache.blame_builds WHERE namespace='{self.namespace}' "
                                          "AND least(lease_until,hard_until)>clock_timestamp();") == "0",
                   "old cache build leases did not expire", seconds=20)

    def exercise(self):
        self.setup()
        self.grant("alice", "reader")
        self.checkout.mkdir()
        self.native("init", "--quiet", "--initial-branch=main")
        args = [self.cli, "serve", "--socket", self.directory / "receive.sock", "--health", "127.0.0.1:0",
                "--dev-http", "127.0.0.1:0", "--dev-principal", "writer"]
        with self.service("push", args, self.writer) as (health, listener):
            eventually(lambda: http(health, "/readyz")[0] == 200, "receive metadata not ready")
            self.push(listener, b"stable\nroot\n", "root")
            first = self.push(listener, b"stable\nfirst\n", "first")
            self.drain()
            require(not self.bodies(), "receive unexpectedly populated cache")
            self.refuse_body_publication()
            self.once()
            initial = self.bodies()
            require(set(initial) == {("alice", first)}, "initial warm body missing")
            require(self.maps() == {("alice", first)}, "confirmed warm body did not yield a map")
            missed = self.push(listener, b"stable\nmissed\n", "missed")
            tip = self.push(listener, b"stable\nlatest\n", "latest")
            self.drain()
            require(self.bodies() == initial, "cache populated while warmer stopped")
        # Exercise only the map, not the older narrow seed, as the warm-derived
        # base for a later generation. The changed tip has no body of its own.
        self.cache_sql("UPDATE git_rs_cache.directory d SET expires_at=clock_timestamp()-interval '1 second' "
                       "FROM git_rs_cache.jobs j WHERE d.namespace=j.namespace AND d.key_hash=j.key_hash "
                       f"AND d.namespace='{self.namespace}' "
                       f"AND position(convert_to('{SEED_KIND}','UTF8') in j.canonical)>0;")
        self.verify("warm-map-later-generation", "alice", tip, False, map_hit=True)
        self.once()
        warmed = self.bodies()
        require(set(warmed) == {("alice", first), ("alice", tip)} and ("alice", missed) not in warmed,
                "missed pushes did not coalesce to the current changed tip")
        self.verify("restart-hit", "alice", tip, True)
        self.once()
        require(self.bodies() == warmed, "repeat scan replaced the ready signed body")
        self.evict()
        self.once()
        rebuilt = self.bodies()
        require(set(rebuilt) == {("alice", tip)} and rebuilt["alice", tip][0] != warmed["alice", tip][0]
                and rebuilt["alice", tip][1:] == warmed["alice", tip][1:], "evicted exact body not rebuilt with same scope")
        self.verify("eviction-hit", "alice", tip, True)
        self.grant("alice", "none")
        self.verify("revoked", "alice", tip, False)
        self.once()
        self.rebind("bob")
        self.once()
        require(self.bodies() == rebuilt, "unauthorized rebind changed complete bodies")
        self.verify("not-granted", "bob", tip, False)
        self.grant("bob", "reader")
        self.once()
        rebound = self.bodies()
        require(set(rebound) == {("alice", tip), ("bob", tip)} and rebound["alice", tip] == rebuilt["alice", tip]
                and rebound["bob", tip][1] != rebound["alice", tip][1], "rebinding did not publish a distinct principal scope")
        self.verify("rebound-hit", "bob", tip, True)
        self.verify("still-revoked", "alice", tip, False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "workdir", "git", "binary", "observer"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--owned-loopback", required=True, choices=["synthetic-only"])
    args = parser.parse_args()
    os.umask(0o077)
    signal.signal(signal.SIGTERM, stop)
    Fixture(args).exercise()
    print("warm-reconcile loopback contract passed: missed tip, exact hits, cold expiry and ACL rebinding")


def stop(_signum, _frame):
    raise SystemExit("warm-reconcile contract interrupted")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, KeyError, subprocess.TimeoutExpired) as exc:
        # Child SQL/stderr and config contents are deliberately not printed.
        sys.exit(f"warm-reconcile contract failed: {exc}")
