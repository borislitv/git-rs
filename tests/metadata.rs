#![forbid(unsafe_code)]

use git_rs::{
    metadata::{self, Barrier},
    process,
    protocol::{self, Oid, Update},
};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

// These tests initialize git_rs in a fresh, disposable PostgreSQL database. They
// deliberately exercise the public metadata API, without requiring S3 or Git RPCs.
#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn snapshot_stage_metrics_distinguish_committed_lease_catalog_and_failed_authorization() {
    let count = |name: &str, stage: &str| -> u64 {
        let text = git_rs::metrics::global().render(0, 0);
        let prefix = format!("git_rs_stage_{name}{{stage=\"{stage}\"}} ");
        text.lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap()
            .parse()
            .unwrap()
    };
    let stages = ["lease_commit", "catalog_read", "lease_release"];
    let before: Vec<_> = stages
        .iter()
        .map(|stage| {
            (
                count("duration_seconds_count", stage),
                count("errors_total", stage),
            )
        })
        .collect();
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!("metadata/stages-{}", git_rs::scratch::identifier().unwrap());
        metadata::create(&slug, "alice").unwrap();
        assert!(metadata::leased_snapshot(&slug, "bob", false).is_err());
        let (snapshot, lease) = metadata::leased_snapshot(&slug, "alice", false).unwrap();
        assert!(snapshot.refs.is_empty());
        assert_eq!(snapshot.default_ref, b"refs/heads/main");
        drop(lease);
    });
    for ((stage, (calls, errors)), (expected_calls, expected_errors)) in
        stages.iter().zip(before).zip([(2, 1), (1, 0), (1, 0)])
    {
        assert_eq!(
            count("duration_seconds_count", stage) - calls,
            expected_calls,
            "{stage}"
        );
        assert_eq!(
            count("errors_total", stage) - errors,
            expected_errors,
            "{stage}"
        );
    }
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn snapshot_budget_rejects_growth_without_partial_data_or_leaked_holds() {
    if std::env::var("GIT_RS_METADATA_BYTES").as_deref() != Ok("4096") {
        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "snapshot_budget_rejects_growth_without_partial_data_or_leaked_holds",
            ])
            .env("GIT_RS_METADATA_BYTES", "4096")
            .status()
            .unwrap();
        assert!(result.success());
        return;
    }
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!("metadata/budget-{}", git_rs::scratch::identifier().unwrap());
        metadata::create(&slug, "alice").unwrap();
        let initial = metadata::snapshot(&slug, "alice", false).unwrap();
        metadata::sql(&format!(
            "INSERT INTO refs VALUES({}::uuid,{},decode(repeat('01',20),'hex'));",
            metadata::text(&initial.id),
            metadata::bytes(b"refs/heads/small")
        ))
        .unwrap();
        let error = metadata::snapshot(&slug, "alice", false).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("metadata snapshot budget exceeded"),
            "{error}"
        );
        assert!(metadata::leased_snapshot(&slug, "alice", false).is_err());
        assert_eq!(
            metadata::sql(&format!(
                "SELECT count(*) FROM repository_leases WHERE repo_id={}::uuid;",
                metadata::text(&initial.id)
            ))
            .unwrap()
            .trim(),
            "0"
        );
        metadata::sql(&format!(
            "DELETE FROM refs WHERE repo_id={}::uuid;",
            metadata::text(&initial.id)
        ))
        .unwrap();
        assert!(
            metadata::snapshot(&slug, "alice", false)
                .unwrap()
                .refs
                .is_empty()
        );
    });
}

#[test]
#[ignore = "requires loopback networking; run serially with metadata contracts"]
fn sql_connection_creation_bounds_stalled_authentication() {
    if std::env::var("GIT_RS_TEST_STALLED_PG").as_deref() == Ok("1") {
        let started = Instant::now();
        assert!(metadata::sql("SELECT 1;").is_err());
        assert!(
            started.elapsed() < Duration::from_secs(7),
            "full connection creation exceeded five-second budget"
        );
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let (finished, stop) = std::sync::mpsc::channel();
    let connected = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|scope| {
        let connected = &connected;
        scope.spawn(move || {
            loop {
                if let Ok((connection, _)) = listener.accept() {
                    connected.store(true, std::sync::atomic::Ordering::SeqCst);
                    let _ = stop.recv_timeout(Duration::from_secs(10));
                    drop(connection);
                    break;
                }
                if stop.try_recv().is_ok() {
                    break;
                }
                std::thread::yield_now();
            }
        });
        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "sql_connection_creation_bounds_stalled_authentication",
            ])
            .env("GIT_RS_TEST_STALLED_PG", "1")
            .env("PGPORT", port.to_string())
            .status()
            .unwrap();
        let _ = finished.send(());
        assert!(result.success());
    });
    assert!(
        connected.load(std::sync::atomic::Ordering::SeqCst),
        "fixture must reach TCP establishment"
    );
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn sql_reuses_sessions_and_does_not_recycle_failed_transactions() {
    let _timing_guard = git_rs::metrics::init_timing_logs();
    process::Operation::new(120).enter(|| {
        let mut sessions = std::collections::BTreeSet::new();
        for _ in 0..32 {
            sessions.insert(metadata::sql("SELECT pg_backend_pid();").unwrap());
        }
        assert!(
            sessions.len() <= 8,
            "successful queries must reuse bounded PostgreSQL sessions"
        );
        metadata::sql("BEGIN; SET LOCAL statement_timeout=1;").unwrap();
        assert_eq!(
            metadata::sql("SHOW statement_timeout;").unwrap().trim(),
            "30s",
            "an unfinished transaction must not leak to the next borrower"
        );
        assert!(metadata::sql("BEGIN; SELECT 1/0; COMMIT;").is_err());
        for _ in 0..16 {
            assert_eq!(
                metadata::sql("SELECT 42;").unwrap().trim(),
                "42",
                "an aborted transaction must not be returned to another borrower"
            );
        }
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn large_sql_queries_release_their_oversized_connection_buffers() {
    process::Operation::new(30).enter(|| {
        let mut query = String::from("SELECT pg_backend_pid(); -- synthetic large-query buffer ");
        query.extend(std::iter::repeat_n('x', 4 * 1024 * 1024));
        let pid = metadata::sql(&query)
            .unwrap()
            .trim()
            .parse::<u32>()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while metadata::sql(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE pid={pid};"
        ))
        .unwrap()
        .trim()
            != "0"
        {
            assert!(
                Instant::now() < deadline,
                "oversized PostgreSQL session was retained in the pool"
            );
            std::thread::yield_now();
        }
        assert_eq!(metadata::sql("SELECT 42;").unwrap().trim(), "42");
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn cancelled_sql_rolls_back_and_returns_pool_capacity() {
    let _timing_guard = git_rs::metrics::init_timing_logs();
    metadata::migrate().unwrap();
    let name = format!(
        "cancel_{}",
        git_rs::scratch::identifier().unwrap().replace('-', "")
    );
    metadata::sql(&format!("CREATE TABLE git_rs.{name}(value integer);")).unwrap();
    let operation = process::Operation::new(120);
    std::thread::scope(|scope| {
        let request = scope.spawn(|| {
            operation.enter(|| {
                metadata::sql(&format!(
                    "BEGIN; INSERT INTO git_rs.{name} VALUES (1); SELECT pg_sleep(15); COMMIT;"
                ))
            })
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        let pid = loop {
            let running = metadata::sql(&format!(
                "SELECT pid FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND state='active' AND query LIKE '%INSERT INTO git_rs.{name}%';"
            )).unwrap();
            if !running.trim().is_empty() {
                break running.trim().parse::<u32>().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "SQL transaction never became active"
            );
            std::thread::yield_now();
        };
        let started = Instant::now();
        operation.cancel();
        assert!(request.join().unwrap().is_err());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "cancellation must interrupt an active SQL await"
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while metadata::sql(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE pid={pid};"
        ))
        .unwrap()
        .trim()
            != "0"
        {
            assert!(
                Instant::now() < deadline,
                "cancelled PostgreSQL backend still owns its transaction"
            );
            std::thread::yield_now();
        }
    });
    assert_eq!(
        metadata::sql(&format!("SELECT count(*) FROM git_rs.{name};"))
            .unwrap()
            .trim(),
        "0",
        "cancelled SQL must not commit its write"
    );
    metadata::sql(&format!("DROP TABLE git_rs.{name};")).unwrap();
    for _ in 0..12 {
        assert_eq!(metadata::sql("SELECT 1;").unwrap().trim(), "1");
    }
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn blocked_timing_sink_does_not_pin_sql_sessions() {
    use std::os::{fd::OwnedFd, unix::net::UnixStream};
    for scenario in [
        "sql_reuses_sessions_and_does_not_recycle_failed_transactions",
        "cancelled_sql_rolls_back_and_returns_pool_capacity",
    ] {
        let (_unread, mut writer) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        loop {
            match writer.write(&[b'x'; 8192]) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("fill diagnostic sink: {error}"),
            }
        }
        writer.set_nonblocking(false).unwrap();
        let fd: OwnedFd = writer.into();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", scenario, "--nocapture"])
            .env("GIT_RS_DEBUG_TIMINGS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::from(fd))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("blocked timings pinned SQL work: {scenario}");
            }
            std::thread::yield_now();
        };
        assert!(
            status.success(),
            "SQL reuse/cancellation must not depend on the timing sink: {scenario}"
        );
    }
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables"]
fn published_long_refs_remain_readable() {
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!(
            "metadata/long-refs-{}",
            git_rs::scratch::identifier().unwrap()
        );
        metadata::create(&slug, "alice").unwrap();
        let initial = metadata::snapshot(&slug, "alice", true).unwrap();
        let lease = Barrier::acquire(&initial.id, false).unwrap();
        // Each name is valid natively, has legal filesystem-sized components,
        // and reaches the documented per-ref limit without exceeding ref count.
        let component = "a".repeat(240);
        let mut wire = Vec::new();
        for index in 0..8100 {
            let name =
                format!("refs/heads/{component}/{component}/{component}/{component}/{index:049}");
            assert_eq!(name.len(), protocol::MAX_REF);
            if index == 0 {
                let status = Command::new("/usr/bin/git")
                    .env_clear()
                    .env("HOME", "/nonexistent")
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .env("GIT_CONFIG_GLOBAL", "/dev/null")
                    .args(["check-ref-format", &name])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .unwrap();
                assert!(status.success(), "external native ref-format oracle");
            }
            protocol::write_packet(
                &mut wire,
                &Update {
                    old: Oid::ZERO,
                    new: Oid([1; 20]),
                    name: name.into_bytes(),
                }
                .line(),
            )
            .unwrap();
        }
        protocol::flush(&mut wire).unwrap();
        let commands = protocol::read_updates(&mut wire.as_slice()).unwrap();
        let generation = metadata::publish(
            &initial,
            &lease,
            &git_rs::scratch::identifier().unwrap(),
            "alice",
            &commands,
            &vec![true; commands.len()],
            &[],
        )
        .unwrap();
        let snapshot = metadata::snapshot(&slug, "alice", false)
            .expect("a confirmed publication must remain readable");
        assert_eq!(snapshot.generation, generation);
        assert_eq!(snapshot.refs.len(), 8100);
        for command in commands {
            assert_eq!(snapshot.refs.get(&command.name), Some(&command.new));
        }
        assert!(metadata::snapshot(&slug, "bob", false).is_err());
    });
}

#[test]
#[ignore = "requires disposable PostgreSQL with CREATEDB and PG* variables; run serially"]
fn large_pack_catalog_remains_readable() {
    if !isolated_metadata_database("large_pack_catalog_remains_readable") {
        return;
    }
    process::Operation::new(120).enter(|| {
        // A real v1 schema has no maintenance triggers. Never disable the guard
        // in the shared test database just to manufacture legacy data.
        metadata::sql(&format!(
            "BEGIN; {} COMMIT;",
            include_str!("../migrations/001.sql")
        ))
        .unwrap();
        assert_eq!(
            metadata::sql("SELECT version FROM schema_version;").unwrap().trim(),
            "1"
        );
        let slug = format!("metadata/packs-{}", git_rs::scratch::identifier().unwrap());
        metadata::create(&slug, "alice").unwrap();
        let initial = metadata::snapshot(&slug, "alice", false).unwrap();
        // A catalog of small native packs from many prior pushes can exceed the
        // old wire cap while representing far less than the 12 GiB scratch limit.
        // Only metadata is under test: no S3 objects or transport are mocked.
        let repo = metadata::text(&initial.id);
        metadata::sql(&format!(
            "INSERT INTO packs SELECT {repo}::uuid, {repo}::uuid, lpad(to_hex(i),40,'0'), \
             'repos/' || {repo} || '/operations/' || {repo} || '/' || lpad(to_hex(i),40,'0') || '.pack', \
             'repos/' || {repo} || '/operations/' || {repo} || '/' || lpad(to_hex(i),40,'0') || '.idx', \
             repeat('a',64),repeat('b',64),1024,1096,0 FROM generate_series(1,40000) i;"
        ))
        .unwrap();
        let before = catalog_state(&initial.id);
        for migrated in [false, true, true] {
            if migrated {
                metadata::migrate().expect("oversized legacy data must not prevent migration");
                assert_eq!(
                    metadata::sql("SELECT version FROM schema_version;").unwrap().trim(),
                    "2"
                );
            }
            let snapshot = metadata::snapshot(&slug, "alice", false).unwrap();
            assert_eq!(snapshot.packs.len(), 40000);
            assert_eq!(snapshot.packs.first().unwrap().hash, format!("{:040x}", 1));
            assert_eq!(snapshot.packs.last().unwrap().hash, format!("{:040x}", 40000));
            assert_eq!(
                snapshot.packs.iter().map(|p| p.pack_size + p.idx_size).sum::<u64>(),
                84_800_000
            );
            assert!(snapshot.refs.is_empty());
            assert_eq!((snapshot.generation, snapshot.storage_epoch), (0, 0));
            assert_eq!(
                catalog_state(&initial.id), before,
                "migration must preserve every catalog field"
            );
        }
        let lease = Barrier::acquire(&initial.id, false).unwrap();
        let operation = git_rs::scratch::identifier().unwrap();
        let before = publication_state(&initial.id);
        assert!(
            metadata::publish(
                &initial, &lease, &operation, "alice", &[catalog_update("new")], &[true],
                &[catalog_pack(&initial.id, &operation, 40001)]
            ).is_err(),
            "migration must install the new guard"
        );
        assert_eq!(publication_state(&initial.id), before);
    });
}

// Schema migration fixtures need a genuinely fresh database, not a temporarily
// disabled shared trigger. Child processes also isolate the process-global pool.
fn isolated_metadata_database(test: &str) -> bool {
    const CHILD: &str = "GIT_RS_TEST_ISOLATED_METADATA";
    if std::env::var(CHILD).as_deref() == Ok(test) {
        return true;
    }
    let database = format!("metadata_{}", git_rs::scratch::identifier().unwrap());
    metadata::sql(&format!("CREATE DATABASE {database} TEMPLATE template0;")).unwrap();
    struct Database(String);
    impl Drop for Database {
        fn drop(&mut self) {
            // Only this freshly created database belongs to the fixture. FORCE
            // also handles an in-flight backend left after killing its child;
            // never terminate sessions in the caller's shared database.
            let _ = process::Operation::new(10).enter(|| {
                metadata::sql(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE);", self.0))
            });
        }
    }
    let cleanup = Database(database.clone());
    struct Child {
        process: std::process::Child,
        reaped: bool,
    }
    impl Drop for Child {
        fn drop(&mut self) {
            if !self.reaped {
                let _ = self.process.kill();
                let _ = self.process.wait();
            }
        }
    }
    let mut child = Child {
        process: Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", test, "--nocapture"])
            .env(CHILD, test)
            .env("PGDATABASE", &database)
            .spawn()
            .unwrap(),
        reaped: false,
    };
    let deadline = Instant::now() + Duration::from_secs(150);
    let status = loop {
        if let Some(status) = child.process.try_wait().unwrap() {
            child.reaped = true;
            break status;
        }
        if Instant::now() >= deadline {
            panic!("isolated metadata contract timed out: {test}");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    drop(child);
    metadata::sql(&format!("DROP DATABASE {database};")).unwrap();
    drop(cleanup);
    assert!(
        status.success(),
        "isolated metadata contract failed: {test}"
    );
    false
}

fn catalog_pack(repo: &str, operation: &str, number: u32) -> metadata::Pack {
    metadata::Pack {
        hash: format!("{number:040x}"),
        pack_key: format!("repos/{repo}/operations/{operation}/{number}.pack"),
        idx_key: format!("repos/{repo}/operations/{operation}/{number}.idx"),
        pack_digest: "a".repeat(64),
        idx_digest: "b".repeat(64),
        pack_size: 1024,
        idx_size: 1096,
    }
}

fn catalog_update(name: &str) -> Update {
    Update {
        old: Oid::ZERO,
        new: Oid([1; 20]),
        name: format!("refs/heads/{name}").into_bytes(),
    }
}

fn catalog_fixture(count: u32) -> (String, metadata::Snapshot, Barrier) {
    metadata::migrate().unwrap();
    let slug = format!("metadata/guard-{}", git_rs::scratch::identifier().unwrap());
    metadata::create(&slug, "alice").unwrap();
    let initial = metadata::snapshot(&slug, "alice", true).unwrap();
    let repo = metadata::text(&initial.id);
    metadata::sql(&format!(
        "INSERT INTO packs SELECT {repo}::uuid,{repo}::uuid,lpad(to_hex(i),40,'0'), \
         'repos/'||{repo}||'/seed/'||i||'.pack','repos/'||{repo}||'/seed/'||i||'.idx', \
         repeat('a',64),repeat('b',64),1024,1096,0 FROM generate_series(1,{count}) i;"
    ))
    .unwrap();
    let (snapshot, lease) = metadata::leased_snapshot(&slug, "alice", true).unwrap();
    assert_eq!(snapshot.packs.len(), count as usize);
    (slug, snapshot, lease)
}

fn catalog_state(repo: &str) -> String {
    metadata::sql(&format!(
        "SELECT encode(sha256(convert_to(coalesce(jsonb_agg(to_jsonb(p) ORDER BY native_hash),'[]'::jsonb)::text,'UTF8')),'hex') FROM packs p WHERE repo_id={}::uuid;",
        metadata::text(repo)
    )).unwrap()
}

// Compare all durable publication effects, not just counts. Canonical JSON plus
// a digest keeps the assertion bounded while covering identities and row values.
fn publication_state(repo: &str) -> String {
    let repo = metadata::text(repo);
    let mut expressions = Vec::new();
    for (table, order, predicate) in [
        ("repositories", "id", format!("id={repo}::uuid")),
        ("refs", "name", format!("repo_id={repo}::uuid")),
        ("packs", "native_hash", format!("repo_id={repo}::uuid")),
        ("operations", "id", format!("repo_id={repo}::uuid")),
        (
            "ref_updates",
            "operation_id,ordinal",
            format!("operation_id IN (SELECT id FROM operations WHERE repo_id={repo}::uuid)"),
        ),
        (
            "staged_objects",
            "object_key",
            format!("repo_id={repo}::uuid"),
        ),
        (
            "retired_objects",
            "object_key",
            format!("repo_id={repo}::uuid"),
        ),
        ("maintenance_jobs", "id", format!("repo_id={repo}::uuid")),
        ("repository_leases", "id", format!("repo_id={repo}::uuid")),
    ] {
        expressions.push(format!(
            "(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY {order}),'[]'::jsonb) FROM {table} t WHERE {predicate})"
        ));
    }
    metadata::sql(&format!(
        "SELECT encode(sha256(convert_to(jsonb_build_array({})::text,'UTF8')),'hex');",
        expressions.join(",")
    ))
    .unwrap()
}

fn stage_catalog_packs(repo: &str, operation: &str, packs: &[metadata::Pack]) {
    // Metadata-only fixture for intents committed before the attempted publish;
    // no S3 PUT or private Store implementation is exercised by these contracts.
    for pack in packs {
        metadata::sql(&format!(
            "INSERT INTO staged_objects(repo_id,operation_id,object_key,state) VALUES \
             ({}::uuid,{}::uuid,{},'uploaded'),({}::uuid,{}::uuid,{},'uploaded');",
            metadata::text(repo),
            metadata::text(operation),
            metadata::text(&pack.pack_key),
            metadata::text(repo),
            metadata::text(operation),
            metadata::text(&pack.idx_key)
        ))
        .unwrap();
    }
}

// Keep this function byte-for-byte from migrations/current_functions.sql at
// 43554e8b9bee6db611e160d53e1b83aed25a8dab. This is the installed old-v2
// function, not a test-time approximation made by deleting the current guard.
const OLD_V2_COMPACT_ENQUEUE: &str = r#"CREATE OR REPLACE FUNCTION maintenance_compact_enqueue() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, git_rs, pg_temp AS $$
DECLARE g bigint;
BEGIN
    SELECT generation INTO STRICT g FROM repositories WHERE id=NEW.repo_id;
    IF (SELECT count(*) FROM (SELECT 1 FROM packs WHERE repo_id=NEW.repo_id LIMIT 8) p)>=8 THEN
        PERFORM maintenance_enqueue(NEW.repo_id,'compact','compact:'||NEW.repo_id::text||':'||g::text||':v1',convert_to(g::text,'UTF8'));
    END IF;
    RETURN NEW;
END $$;
"#;

fn assert_catalog_insert_guard(repository: &str, rejected: bool) {
    let before = publication_state(repository);
    let repo = metadata::text(repository);
    // The nested exception block is a subtransaction. Even on historical SQL,
    // a successful INSERT is rolled back by our private success sentinel. Catch
    // only the guard SQLSTATE and that sentinel: constraints, timeouts and other
    // setup failures must never masquerade as evidence that the guard fired.
    metadata::sql(&format!(
        "DO $probe$ DECLARE guard_rejected boolean := false; BEGIN \
         BEGIN \
           INSERT INTO packs(repo_id,operation_id,native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size,generation) \
           VALUES({repo}::uuid,{repo}::uuid,lpad(to_hex(50000),40,'0'), \
             'repos/'||{repo}||'/probe.pack','repos/'||{repo}||'/probe.idx', \
             repeat('a',64),repeat('b',64),1024,1096,0); \
           RAISE EXCEPTION USING ERRCODE='P1T00', MESSAGE='rollback successful catalog probe'; \
         EXCEPTION \
           WHEN SQLSTATE 'P1C01' THEN guard_rejected := true; \
           WHEN SQLSTATE 'P1T00' THEN NULL; \
         END; \
         IF guard_rejected IS DISTINCT FROM {rejected} THEN \
           RAISE EXCEPTION USING ERRCODE='P1T01', MESSAGE='catalog guard behavior mismatch: expected P1C01 rejection={rejected}'; \
         END IF; END $probe$;"
    )).expect("catalog probe: P1T01 means wrong guard behavior; any other SQLSTATE is a fixture/dependency failure");
    assert_eq!(
        publication_state(repository),
        before,
        "even an accepted historical INSERT must roll back"
    );
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn catalog_insert_4097_is_rejected_by_the_guard_sqlstate() {
    process::Operation::new(120).enter(|| {
        let (_, snapshot, lease) = catalog_fixture(4096);
        assert_catalog_insert_guard(&snapshot.id, true);
        lease.check().unwrap();
    });
}

#[test]
#[ignore = "requires disposable PostgreSQL with CREATEDB and PG* variables; run serially"]
fn migration_refreshes_historical_v2_catalog_guard_without_changing_data() {
    if !isolated_metadata_database(
        "migration_refreshes_historical_v2_catalog_guard_without_changing_data",
    ) {
        return;
    }
    process::Operation::new(120).enter(|| {
        let (slug, snapshot, lease) = catalog_fixture(4096);
        metadata::sql(&format!(
            "BEGIN; SET LOCAL search_path=git_rs,pg_catalog; {OLD_V2_COMPACT_ENQUEUE} COMMIT;"
        ))
        .expect("install the genuine historical old-v2 function in the isolated database");
        assert_eq!(
            metadata::sql("SELECT version FROM schema_version;")
                .unwrap()
                .trim(),
            "2"
        );
        assert_catalog_insert_guard(&snapshot.id, false);
        let before = publication_state(&snapshot.id);
        for _ in 0..2 {
            metadata::migrate().expect("refresh an already-v2 installation");
            assert_eq!(
                metadata::sql("SELECT version FROM schema_version;")
                    .unwrap()
                    .trim(),
                "2"
            );
            assert_eq!(
                publication_state(&snapshot.id),
                before,
                "v2 refresh must preserve all publication state"
            );
            assert_catalog_insert_guard(&snapshot.id, true);
            let after = metadata::snapshot(&slug, "alice", true).unwrap();
            assert_eq!(
                (after.generation, after.storage_epoch, after.packs.len()),
                (0, 0, 4096)
            );
            assert_eq!(after.refs, snapshot.refs);
            lease.check().unwrap();
        }
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn catalog_insert_4097_rolls_back_publication_and_preserves_staged_intents() {
    process::Operation::new(120).enter(|| {
        let (slug, snapshot, lease) = catalog_fixture(4096);
        let operation = git_rs::scratch::identifier().unwrap();
        let pack = catalog_pack(&snapshot.id, &operation, 4097);
        stage_catalog_packs(&snapshot.id, &operation, std::slice::from_ref(&pack));
        let before = publication_state(&snapshot.id);
        assert!(
            metadata::publish(
                &snapshot,
                &lease,
                &operation,
                "alice",
                &[catalog_update("overflow")],
                &[true],
                &[pack]
            )
            .is_err()
        );
        assert_eq!(
            publication_state(&snapshot.id),
            before,
            "refs, generation, journals, catalog, jobs and committed intents must be unchanged"
        );
        let after = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!(after.packs.len(), 4096);
        assert!(after.refs.is_empty());
        lease.check().unwrap();
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn catalog_boundary_allows_dedup_and_ref_only_publications() {
    process::Operation::new(120).enter(|| {
        let (slug, snapshot, lease) = catalog_fixture(4096);
        let catalog = catalog_state(&snapshot.id);
        let operation = git_rs::scratch::identifier().unwrap();
        // Different object keys with the same native hash really reach DO NOTHING.
        let duplicate = catalog_pack(&snapshot.id, &operation, 1);
        stage_catalog_packs(&snapshot.id, &operation, std::slice::from_ref(&duplicate));
        let generation = metadata::publish(
            &snapshot, &lease, &operation, "alice", &[catalog_update("dedup")], &[true],
            &[duplicate]
        ).unwrap();
        assert_eq!(generation, 1);
        let next = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!(next.refs.get(b"refs/heads/dedup".as_slice()), Some(&Oid([1; 20])));
        assert_eq!(catalog_state(&snapshot.id), catalog, "dedup must retain the original physical identities");
        assert_eq!(metadata::sql(&format!(
            "SELECT count(*) FROM staged_objects WHERE operation_id={}::uuid AND retired_at IS NULL AND state='uploaded';",
            metadata::text(&operation))).unwrap().trim(), "2", "uncatalogued duplicate uploads remain cleanup intents");
        assert_eq!(metadata::publish(
            &next, &lease, &git_rs::scratch::identifier().unwrap(), "alice",
            &[catalog_update("no-pack")], &[true], &[]
        ).unwrap(), 2);
        let after = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!(after.refs.len(), 2);
        assert_eq!(after.refs.get(b"refs/heads/no-pack".as_slice()), Some(&Oid([1; 20])));
        assert_eq!(after.generation, 2);
        assert_eq!(after.packs.len(), 4096);
        assert_eq!(catalog_state(&snapshot.id), catalog);
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn catalog_multi_pack_overflow_rolls_back_earlier_attachment() {
    process::Operation::new(120).enter(|| {
        let (_, snapshot, lease) = catalog_fixture(4095);
        let operation = git_rs::scratch::identifier().unwrap();
        let packs = [
            catalog_pack(&snapshot.id, &operation, 4096),
            catalog_pack(&snapshot.id, &operation, 4097),
        ];
        stage_catalog_packs(&snapshot.id, &operation, &packs);
        let before = publication_state(&snapshot.id);
        assert!(metadata::publish(
            &snapshot, &lease, &operation, "alice", &[catalog_update("multi")], &[true], &packs
        ).is_err());
        assert_eq!(publication_state(&snapshot.id), before,
            "the allowed first attachment and both committed upload intents must roll back to their prepublication state");
        // The same first pack fits on its own, proving this was a boundary failure.
        assert_eq!(metadata::publish(
            &snapshot, &lease, &operation, "alice", &[catalog_update("multi")], &[true],
            &packs[..1]
        ).unwrap(), 1);
        assert_eq!(metadata::sql(&format!(
            "SELECT count(*) FROM staged_objects WHERE operation_id={}::uuid;",
            metadata::text(&operation))).unwrap().trim(), "2", "only the now-catalogued pack's intents are consumed");
    });
}

// Run exactly two publications only after the test-owned holder has acquired
// the repository row lock, and release it only after observing both DB waits.
fn with_observed_publication_contention<T: Send>(
    repository: &str,
    operations: &[String; 2],
    publish: impl Fn(usize) -> T + Sync,
) -> Vec<T> {
    std::thread::scope(|scope| {
        // Own the holder's Operation rather than cancelling a discovered PID.
        // Its RAII guard releases the row lock on assertion failure too,
        // before scope joins either writer. No unrelated backend is touched.
        struct Release(std::sync::Arc<process::Operation>);
        impl Drop for Release {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let holder = process::Operation::new(120);
        let holder_operation = holder.clone();
        let marker = format!("catalog-holder-{repository}");
        let holder_sql = format!(
            "BEGIN; SELECT id FROM repositories WHERE id={}::uuid FOR UPDATE; \
             SELECT pg_sleep(30); ROLLBACK; -- {marker}",
            metadata::text(repository)
        );
        let holder_request =
            scope.spawn(move || holder_operation.enter(|| metadata::sql(&holder_sql)));
        let release = Release(holder);
        let deadline = Instant::now() + Duration::from_secs(2);
        let holder_pid = loop {
            let running = process::Operation::new(1)
                .enter(|| {
                    metadata::sql(&format!(
                        "SELECT pid FROM pg_stat_activity WHERE datname=current_database() \
                 AND pid<>pg_backend_pid() AND state='active' AND wait_event='PgSleep' \
                 AND strpos(query,{})>0;",
                        metadata::text(&marker)
                    ))
                })
                .expect("observe the controlled holder after it acquired the repository row lock");
            if !running.trim().is_empty() {
                break running.trim().parse::<u32>().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "controlled repository holder never acquired its lock"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        let requests: Vec<_> = (0..2)
            .map(|i| {
                let publish = &publish;
                scope.spawn(move || process::Operation::new(120).enter(|| publish(i)))
            })
            .collect();
        // Leave margin below the application's unchanged five-second
        // lock_timeout. Follow blocker chains: the second writer may queue
        // on the first writer's tuple lock rather than directly on holder.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let waiting = process::Operation::new(1)
                .enter(|| {
                    metadata::sql(&format!(
                        "WITH RECURSIVE waiting AS ( \
                   SELECT pid FROM pg_stat_activity WHERE datname=current_database() \
                   AND state='active' AND wait_event_type='Lock' \
                   AND wait_event IN ('transactionid','tuple') \
                   AND (strpos(query,{first})>0 OR strpos(query,{second})>0) \
                 ), chain(root,pid) AS ( \
                   SELECT pid,unnest(pg_blocking_pids(pid)) FROM waiting \
                   UNION SELECT chain.root,unnest(pg_blocking_pids(chain.pid)) FROM chain \
                 ) SELECT count(DISTINCT root) FROM chain WHERE pid={holder_pid};",
                        first = metadata::text(&metadata::text(&operations[0])),
                        second = metadata::text(&metadata::text(&operations[1]))
                    ))
                })
                .expect("observe both publication lock waits before releasing the holder");
            if waiting.trim() == "2" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "both writers must reach repository DB lock waits within two seconds, before the 5s lock_timeout (observed {})",
                waiting.trim()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(release);
        assert!(
            holder_request.join().unwrap().is_err(),
            "controlled holder must be cancelled, never committed"
        );
        requests
            .into_iter()
            .map(|request| request.join().unwrap())
            .collect::<Vec<_>>()
    })
}

#[test]
#[ignore = "requires disposable PostgreSQL with CREATEDB and PG* variables; run serially"]
fn concurrent_unique_publications_compete_for_the_last_catalog_slot() {
    if !isolated_metadata_database(
        "concurrent_unique_publications_compete_for_the_last_catalog_slot",
    ) {
        return;
    }
    process::Operation::new(120).enter(|| {
        let (slug, snapshot, lease) = catalog_fixture(4095);
        let other = Barrier::acquire(&snapshot.id, false).unwrap();
        let operations = [
            git_rs::scratch::identifier().unwrap(),
            git_rs::scratch::identifier().unwrap(),
        ];
        let packs = [
            catalog_pack(&snapshot.id, &operations[0], 4096),
            catalog_pack(&snapshot.id, &operations[1], 4097),
        ];
        for i in 0..2 {
            stage_catalog_packs(&snapshot.id, &operations[i], std::slice::from_ref(&packs[i]));
        }
        let results = with_observed_publication_contention(&snapshot.id, &operations, |i| {
            metadata::publish(&snapshot, [&lease, &other][i], &operations[i], "alice",
                &[catalog_update(&format!("concurrent-{i}"))], &[true], std::slice::from_ref(&packs[i]))
        });
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        let winner = results.iter().position(|r| r.is_ok()).unwrap();
        let loser = 1 - winner;
        assert_eq!(*results[winner].as_ref().unwrap(), 1);
        let after = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!((after.generation, after.storage_epoch, after.packs.len(), after.refs.len()), (1, 0, 4096, 1));
        assert_eq!(after.refs.get(format!("refs/heads/concurrent-{winner}").as_bytes()), Some(&Oid([1; 20])));
        assert!(after.packs.iter().any(|p| p.pack_key == packs[winner].pack_key));
        assert!(!after.packs.iter().any(|p| p.hash == packs[loser].hash));
        for (i, expected) in [(winner, "1\t1\t0"), (loser, "0\t0\t2")] {
            assert_eq!(metadata::sql(&format!(
                "SELECT (SELECT count(*) FROM operations WHERE id={op}::uuid), \
                 (SELECT count(*) FROM ref_updates WHERE operation_id={op}::uuid), \
                 (SELECT count(*) FROM staged_objects WHERE operation_id={op}::uuid AND state='uploaded' AND retired_at IS NULL);",
                op=metadata::text(&operations[i]))).unwrap().trim(), expected);
        }
        lease.check().unwrap();
        other.check().unwrap();
    });
}

#[test]
#[ignore = "requires disposable PostgreSQL with CREATEDB and PG* variables; run serially"]
fn contended_sql_publication_loser_has_catalog_guard_sqlstate() {
    if !isolated_metadata_database("contended_sql_publication_loser_has_catalog_guard_sqlstate") {
        return;
    }
    process::Operation::new(120).enter(|| {
        let (slug, snapshot, lease) = catalog_fixture(4095);
        let repo = metadata::text(&snapshot.id);
        // Both complete transactions use the same live shared source lease.
        // Read its identity through SQL, not Barrier's private implementation.
        let lease_id = metadata::sql(&format!(
            "SELECT id FROM repository_leases WHERE repo_id={repo}::uuid \
             AND NOT exclusive AND expires_at>clock_timestamp();"
        )).unwrap();
        let lease_id = metadata::text(lease_id.trim());
        let operations = [
            git_rs::scratch::identifier().unwrap(),
            git_rs::scratch::identifier().unwrap(),
        ];
        let packs = [
            catalog_pack(&snapshot.id, &operations[0], 4096),
            catalog_pack(&snapshot.id, &operations[1], 4097),
        ];
        let commands = [catalog_update("sql-concurrent-0"), catalog_update("sql-concurrent-1")];
        let manifests: Vec<_> = commands.iter().map(|command| {
            let mut manifest = command.line();
            manifest.push(b'\n');
            manifest
        }).collect();
        let transactions: Vec<_> = (0..2).map(|i| {
            stage_catalog_packs(&snapshot.id, &operations[i], std::slice::from_ref(&packs[i]));
            let pack = &packs[i];
            // Reproduce metadata::publish's supported ordering: publish() owns
            // the repository row lock, then attach the pack, consume catalogued
            // intents, recheck the source lease, and synchronously commit. Raw
            // sql deliberately avoids public publish's unknown-COMMIT error
            // reconciliation, which hides the rejecting SQLSTATE.
            format!(
                "BEGIN; SET LOCAL synchronous_commit=on; \
                 SELECT publish({repo}::uuid,{op}::uuid,'alice',{manifest},0,{lease_id}::uuid, \
                   ARRAY[{name}]::bytea[],ARRAY[{old}]::bytea[],ARRAY[{new}]::bytea[],ARRAY[true]); \
                 INSERT INTO packs(repo_id,operation_id,native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size,generation) \
                   VALUES({repo}::uuid,{op}::uuid,{hash},{pack_key},{idx_key},{pd},{id},{ps},{is}, \
                     (SELECT generation FROM repositories WHERE id={repo}::uuid)) \
                   ON CONFLICT(repo_id,native_hash) DO NOTHING; \
                 DELETE FROM staged_objects s USING packs p WHERE s.repo_id=p.repo_id AND s.repo_id={repo}::uuid \
                   AND (s.object_key=p.pack_key OR s.object_key=p.idx_key); \
                 DO $lease$ BEGIN IF NOT EXISTS(SELECT 1 FROM repositories r JOIN repository_leases l ON l.repo_id=r.id \
                   WHERE r.id={repo}::uuid AND r.storage_epoch=0 AND l.id={lease_id}::uuid \
                   AND NOT l.exclusive AND l.expires_at>clock_timestamp()) \
                   THEN RAISE EXCEPTION 'publication source lease unavailable'; END IF; END $lease$; COMMIT;",
                op=metadata::text(&operations[i]), manifest=metadata::bytes(&manifests[i]),
                name=metadata::bytes(&commands[i].name), old=metadata::bytes(&commands[i].old.0),
                new=metadata::bytes(&commands[i].new.0), hash=metadata::text(&pack.hash),
                pack_key=metadata::text(&pack.pack_key), idx_key=metadata::text(&pack.idx_key),
                pd=metadata::text(&pack.pack_digest), id=metadata::text(&pack.idx_digest),
                ps=pack.pack_size, is=pack.idx_size,
            )
        }).collect();
        let results = with_observed_publication_contention(&snapshot.id, &operations, |i| {
            metadata::sql(&transactions[i])
        });
        // Historical SQL accepts both complete transactions: fail here for two
        // commits, not for a fixture failure. A transient error cannot stand in
        // for the guard rejection on the current SQL.
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1,
            "exactly one complete SQL publication must commit: {results:?}");
        let winner = results.iter().position(|result| result.is_ok()).unwrap();
        let loser = 1 - winner;
        assert_eq!(results[winner].as_ref().unwrap().trim(), "1");
        let error = results[loser].as_ref().unwrap_err().to_string();
        assert!(error.contains("SQLSTATE P1C01"), "loser must hit the catalog guard, not a transient DB failure: {error}");
        let after = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!((after.generation, after.storage_epoch, after.packs.len(), after.refs.len()), (1, 0, 4096, 1));
        assert_eq!(after.refs.get(&commands[winner].name), Some(&commands[winner].new));
        assert!(!after.refs.contains_key(&commands[loser].name));
        assert!(!after.packs.iter().any(|pack| pack.hash == packs[loser].hash));
        // Snapshots are ordered by native_hash; fixed-width seed hashes 1..4095
        // precede either winner (4096 or 4097), so the sole new row is last.
        assert!(snapshot.packs.windows(2).all(|pair| pair[0].hash < pair[1].hash));
        assert!(snapshot.packs.last().unwrap().hash < packs[winner].hash);
        for (actual, expected) in after.packs.iter().zip(
            snapshot.packs.iter().chain(std::iter::once(&packs[winner]))
        ) {
            assert_eq!((&actual.hash, &actual.pack_key, &actual.idx_key, &actual.pack_digest, &actual.idx_digest, actual.pack_size, actual.idx_size),
                (&expected.hash, &expected.pack_key, &expected.idx_key, &expected.pack_digest, &expected.idx_digest, expected.pack_size, expected.idx_size));
        }
        for (i, expected) in [(winner, "1\t1\t0"), (loser, "0\t0\t2")] {
            assert_eq!(metadata::sql(&format!(
                "SELECT (SELECT count(*) FROM operations WHERE id={op}::uuid), \
                 (SELECT count(*) FROM ref_updates WHERE operation_id={op}::uuid), \
                 (SELECT count(*) FROM staged_objects WHERE operation_id={op}::uuid AND state='uploaded' AND retired_at IS NULL);",
                op=metadata::text(&operations[i]))).unwrap().trim(), expected);
        }
        assert_eq!(metadata::sql(&format!(
            "SELECT count(*) FROM operations o JOIN ref_updates u ON u.operation_id=o.id \
             WHERE o.id={}::uuid AND o.repo_id={repo}::uuid AND o.principal='alice' AND o.commands={} \
             AND o.generation=1 AND o.confirmations=0 AND u.ordinal=1 AND u.name={} AND u.old_oid IS NULL AND u.new_oid={};",
            metadata::text(&operations[winner]), metadata::bytes(&manifests[winner]),
            metadata::bytes(&commands[winner].name), metadata::bytes(&commands[winner].new.0)
        )).unwrap().trim(), "1", "winner journal must describe the committed ref update exactly");
        lease.check().unwrap();
    });
}

#[test]
#[ignore = "requires disposable PostgreSQL with CREATEDB and PG* variables; run serially"]
fn compaction_can_replace_a_catalog_at_the_pack_bound() {
    if !isolated_metadata_database("compaction_can_replace_a_catalog_at_the_pack_bound") {
        return;
    }
    process::Operation::new(120).enter(|| {
        let (slug, snapshot, shared) = catalog_fixture(4096);
        drop(shared);
        let exclusive = Barrier::acquire(&snapshot.id, true).unwrap();
        let repo = metadata::text(&snapshot.id);
        let campaign = metadata::text(&git_rs::scratch::identifier().unwrap());
        let operation = git_rs::scratch::identifier().unwrap();
        let token = metadata::text(&operation);
        let fence = metadata::sql(&format!(
            "SELECT maintenance_acquire('catalog-test',{campaign}::uuid,120);"
        )).unwrap();
        let fence = fence.trim().parse::<u64>().unwrap();
        let job = metadata::sql(&format!(
            "SELECT id FROM maintenance_claim('catalog-test',{campaign}::uuid,{fence},{token}::uuid,120);"
        )).unwrap();
        let job = metadata::text(job.trim());
        let pack = catalog_pack(&snapshot.id, &operation, 5000);
        stage_catalog_packs(&snapshot.id, &operation, std::slice::from_ref(&pack));
        // Exercise the public fenced SQL API. The output witness uses the same
        // documented manifest framing as a worker; state assertions below are
        // independent of that witness, and require retirement of every old pack.
        let result = metadata::sql(&format!(
            "SELECT * FROM maintenance_swap('catalog-test',{campaign}::uuid,{fence},{job}::uuid, \
             {token}::uuid,1,(SELECT id FROM repository_leases WHERE repo_id={repo}::uuid AND exclusive), \
             0,0,maintenance_catalog_manifest({repo}::uuid), \
             sha256(convert_to('git-rs/catalog/v1','UTF8')||decode('00','hex')||int8send(1::bigint)|| \
             maintenance_manifest_entry({hash},{pack},{idx},{pd},{id},1024,1096)), \
             {hash},{pack},{idx},{pd},{id},1024,1096);",
            hash=metadata::text(&pack.hash), pack=metadata::text(&pack.pack_key),
            idx=metadata::text(&pack.idx_key), pd=metadata::text(&pack.pack_digest),
            id=metadata::text(&pack.idx_digest)
        )).expect("replacement must delete the old 4096 rows before its insert trigger runs");
        assert_eq!(result.trim(), "1\t1");
        let after = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!((after.generation, after.storage_epoch, after.packs.len()), (1, 1, 1));
        assert_eq!(after.packs[0].hash, pack.hash);
        assert_eq!(after.packs[0].pack_key, pack.pack_key);
        assert_eq!(after.packs[0].idx_key, pack.idx_key);
        assert_eq!(after.refs, snapshot.refs);
        assert_eq!(metadata::sql(&format!(
            "SELECT (SELECT count(*) FROM retired_objects WHERE repo_id={repo}::uuid AND reason='compaction' AND retired_epoch=1), \
             (SELECT count(*) FROM staged_objects WHERE repo_id={repo}::uuid), \
             (SELECT state FROM maintenance_jobs WHERE id={job}::uuid), \
             (SELECT count(*) FROM maintenance_operations WHERE operation_id={token}::uuid AND result_generation=1 AND result_epoch=1);"
        )).unwrap().trim(), "8192\t0\tsucceeded\t1");
        exclusive.check().unwrap();
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables"]
fn namespace_range_check_rejects_only_descendants_and_rolls_back() {
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!(
            "metadata/namespace-{}",
            git_rs::scratch::identifier().unwrap()
        );
        metadata::create(&slug, "alice").unwrap();
        let initial = metadata::snapshot(&slug, "alice", true).unwrap();
        let lease = Barrier::acquire(&initial.id, false).unwrap();
        // '-' sorts between the parent and its descendant; '0' is the excluded
        // upper bound. Include non-UTF-8 names to exercise bytea, not text order.
        let names = [
            b"refs/heads/a".as_slice(),
            b"refs/heads/a-",
            b"refs/heads/a0",
            b"refs/heads/a\xff",
        ];
        let commands: Vec<_> = names
            .iter()
            .map(|name| Update {
                old: Oid::ZERO,
                new: Oid([1; 20]),
                name: name.to_vec(),
            })
            .collect();
        metadata::publish(
            &initial,
            &lease,
            &git_rs::scratch::identifier().unwrap(),
            "alice",
            &commands,
            &[true; 4],
            &[],
        )
        .unwrap();
        let before = metadata::snapshot(&slug, "alice", true).unwrap();
        for name in [b"refs/heads/a/child".as_slice(), b"refs/heads/a\xff/child"] {
            let result = metadata::publish(
                &before,
                &lease,
                &git_rs::scratch::identifier().unwrap(),
                "alice",
                &[Update {
                    old: Oid::ZERO,
                    new: Oid([2; 20]),
                    name: name.to_vec(),
                }],
                &[true],
                &[],
            );
            assert!(result.is_err(), "namespace conflict must be rejected");
            let after = metadata::snapshot(&slug, "alice", false).unwrap();
            assert_eq!(after.generation, before.generation);
            assert_eq!(after.refs, before.refs);
        }
        // Migration refreshes the function but must not reset published state.
        metadata::migrate().unwrap();
        assert_eq!(
            metadata::snapshot(&slug, "alice", false).unwrap().refs,
            before.refs
        );
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn null_publication_fences_and_command_fields_cannot_authorize_writes() {
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!("metadata/null-{}", git_rs::scratch::identifier().unwrap());
        metadata::create(&slug, "alice").unwrap();
        let snapshot = metadata::snapshot(&slug, "alice", true).unwrap();
        let _lease = Barrier::acquire(&snapshot.id, false).unwrap();
        let repo = metadata::text(&snapshot.id);
        let operation = metadata::text(&git_rs::scratch::identifier().unwrap());
        let lease = format!("(SELECT id FROM repository_leases WHERE repo_id={repo}::uuid)");
        let names = "ARRAY[convert_to('refs/heads/main','UTF8')]";
        let old = "ARRAY[decode(repeat('00',20),'hex')]";
        let new = "ARRAY[decode(repeat('11',20),'hex')]";
        for (epoch, names, old, new, forward) in [
            ("NULL", names, old, new, "ARRAY[true]"),
            ("0", "NULL::bytea[]", old, new, "ARRAY[true]"),
            ("0", names, "NULL::bytea[]", new, "ARRAY[true]"),
            ("0", names, old, new, "ARRAY[NULL]::boolean[]"),
            ("0", names, "ARRAY[NULL]::bytea[]", new, "ARRAY[true]"),
        ] {
            let error = metadata::sql(&format!("SELECT publish({repo}::uuid,{operation}::uuid,'alice',''::bytea,{epoch},{lease},{names},{old},{new},{forward});")).unwrap_err();
            assert!(error.to_string().contains("SQLSTATE P0001"), "{error}");
            let after = metadata::snapshot(&slug, "alice", true).unwrap();
            assert_eq!((after.generation, after.storage_epoch, after.refs.len(), after.packs.len()), (0, 0, 0, 0));
            assert_eq!(metadata::sql(&format!("SELECT count(*) FROM operations WHERE repo_id={repo}::uuid;")).unwrap().trim(), "0");
        }
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn lease_expiry_during_catalog_attachment_rolls_back_the_entire_publication() {
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let nonce = git_rs::scratch::identifier().unwrap();
        let slug = format!("metadata/late-lease-{nonce}");
        metadata::create(&slug, "alice").unwrap();
        let snapshot = metadata::snapshot(&slug, "alice", true).unwrap();
        let lease = Barrier::acquire(&snapshot.id, false).unwrap();
        let name = format!("expire_source_{nonce}");
        struct Trigger(String);
        impl Drop for Trigger {
            fn drop(&mut self) {
                let _cleanup = process::Operation::new(5).enter(|| metadata::sql(&format!(
                    "DROP TRIGGER IF EXISTS {} ON packs; DROP FUNCTION IF EXISTS {}();", self.0, self.0)));
            }
        }
        metadata::sql(&format!(
            "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $body$ BEGIN IF NEW.repo_id={}::uuid THEN UPDATE repository_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE repo_id=NEW.repo_id; END IF; RETURN NEW; END $body$; CREATE TRIGGER {name} BEFORE INSERT ON packs FOR EACH ROW EXECUTE FUNCTION {name}();",
            metadata::text(&snapshot.id))).unwrap();
        let _trigger = Trigger(name);
        let operation = git_rs::scratch::identifier().unwrap();
        let pack = metadata::Pack {
            hash: "1".repeat(40), pack_key: format!("repos/{}/operations/{operation}/output.pack", snapshot.id),
            idx_key: format!("repos/{}/operations/{operation}/output.idx", snapshot.id),
            pack_digest: "a".repeat(64), idx_digest: "b".repeat(64), pack_size: 32, idx_size: 1096,
        };
        let result = metadata::publish(&snapshot, &lease, &operation, "alice", &[Update {
            old: Oid::ZERO, new: Oid([1; 20]), name: b"refs/heads/main".to_vec(),
        }], &[true], &[pack]);
        assert!(result.is_err(), "an initially live lease must be rechecked after catalog/trigger work");
        let after = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!((after.generation, after.storage_epoch, after.refs.len(), after.packs.len()), (0, 0, 0, 0));
        assert_eq!(metadata::sql(&format!("SELECT count(*) FROM operations WHERE repo_id={}::uuid;", metadata::text(&snapshot.id))).unwrap().trim(), "0");
        lease.check().expect("rollback also restores the lease's original live state");
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn failed_late_catalog_query_never_returns_a_partial_snapshot() {
    metadata::migrate().unwrap();
    let slug = format!(
        "metadata/interrupted-{}",
        git_rs::scratch::identifier().unwrap()
    );
    metadata::create(&slug, "alice").unwrap();
    // Hold a real dependency lock: snapshot can emit its authorized repository
    // header and refs, but its final catalog query cannot complete. The request
    // deadline must fail the snapshot rather than accept that partial stdout.
    let repo = metadata::snapshot(&slug, "alice", false).unwrap().id;
    struct Locker(std::process::Child);
    impl Drop for Locker {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut locker = Locker(
        Command::new("/usr/bin/psql")
            .args(["-X", "-qAt", "-v", "ON_ERROR_STOP=1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    let mut stdin = locker.0.stdin.take().unwrap();
    stdin
        .write_all(b"BEGIN; LOCK TABLE git_rs.packs IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
        .unwrap();
    stdin.flush().unwrap();
    let mut marker = String::new();
    BufReader::new(locker.0.stdout.take().unwrap())
        .read_line(&mut marker)
        .unwrap();
    assert_eq!(marker.trim(), "locked");
    let started = Instant::now();
    let result = process::Operation::new(1).enter(|| metadata::snapshot(&slug, "alice", false));
    assert!(
        result.is_err(),
        "partial snapshot must not escape after SQL failure"
    );
    assert!(started.elapsed() < Duration::from_secs(5));
    let result =
        process::Operation::new(1).enter(|| metadata::leased_snapshot(&slug, "alice", false));
    assert!(
        result.is_err(),
        "a failed leased snapshot must not return partial data"
    );
    assert_eq!(
        metadata::sql(&format!(
            "SELECT count(*) FROM repository_leases WHERE repo_id={}::uuid;",
            metadata::text(&repo)
        ))
        .unwrap()
        .trim(),
        "0",
        "a known committed hold must be released after failed catalog streaming"
    );
    drop(stdin);
    drop(locker);
    assert!(
        metadata::snapshot(&slug, "alice", false)
            .unwrap()
            .refs
            .is_empty()
    );
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn leased_snapshot_preserves_authorization_and_a_live_committed_hold() {
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!("metadata/leased-{}", git_rs::scratch::identifier().unwrap());
        metadata::create(&slug, "alice").unwrap();
        metadata::grant(&slug, "alice", "bob", "reader", b"refs/").unwrap();
        let (snapshot, lease) = metadata::leased_snapshot(&slug, "bob", false).unwrap();
        assert!(snapshot.refs.is_empty());
        lease
            .check()
            .expect("returned snapshot must have a committed live hold");
        let count = format!(
            "SELECT count(*) FROM repository_leases WHERE repo_id={}::uuid;",
            metadata::text(&snapshot.id)
        );
        assert_eq!(metadata::sql(&count).unwrap().trim(), "1");
        assert!(metadata::leased_snapshot(&slug, "bob", true).is_err());
        assert!(metadata::leased_snapshot(&slug, "unknown", false).is_err());
        assert_eq!(
            metadata::sql(&count).unwrap().trim(),
            "1",
            "denied requests must not acquire holds"
        );
        drop(lease);
        assert_eq!(metadata::sql(&count).unwrap().trim(), "0");
        metadata::grant(&slug, "alice", "bob", "none", b"refs/").unwrap();
        assert!(metadata::leased_snapshot(&slug, "bob", false).is_err());
        let (snapshot, lease) = metadata::leased_snapshot(&slug, "alice", true).unwrap();
        assert!(snapshot.refs.is_empty());
        lease.check().unwrap();
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn leased_snapshot_never_reads_through_exclusive_maintenance() {
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!(
            "metadata/exclusive-{}",
            git_rs::scratch::identifier().unwrap()
        );
        metadata::create(&slug, "alice").unwrap();
        let snapshot = metadata::snapshot(&slug, "alice", false).unwrap();
        let exclusive = Barrier::acquire(&snapshot.id, true).unwrap();
        assert!(
            metadata::leased_snapshot(&slug, "alice", false).is_err(),
            "a reader must not bypass an exclusive hold"
        );
        exclusive.check().unwrap();
        drop(exclusive);
        let (_, lease) = metadata::leased_snapshot(&slug, "alice", false).unwrap();
        lease.check().unwrap();
    });
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn durable_holds_cover_the_configured_remaining_operation_budget() {
    metadata::migrate().unwrap();
    let slug = format!(
        "metadata/budgets-{}",
        git_rs::scratch::identifier().unwrap()
    );
    metadata::create(&slug, "alice").unwrap();
    let initial = metadata::snapshot(&slug, "alice", true).unwrap();
    for seconds in [3600, 121, 120, 1] {
        process::Operation::new(seconds).enter(|| {
            for exclusive in [false, true] {
                let hold = Barrier::acquire(&initial.id, exclusive)
                    .expect("a bounded operation must acquire a duration-aligned durable hold");
                assert_eq!(
                    metadata::sql(&format!(
                        "SELECT expires_at > clock_timestamp() + interval '{seconds} seconds' \
                         AND expires_at <= clock_timestamp() + interval '{} seconds' \
                         FROM repository_leases WHERE repo_id={}::uuid;",
                        seconds + 30,
                        metadata::text(&initial.id)
                    ))
                    .unwrap()
                    .trim(),
                    "t",
                    "hold must cover the operation plus cleanup, without a fixed oversized lease"
                );
                drop(hold);
            }
            let (_, hold) = metadata::leased_snapshot(&slug, "alice", true)
                .expect("serving snapshots must support the same bounded operation budget");
            assert_eq!(
                metadata::sql(&format!(
                    "SELECT expires_at > clock_timestamp() + interval '{seconds} seconds' \
                     AND expires_at <= clock_timestamp() + interval '{} seconds' \
                     FROM repository_leases WHERE repo_id={}::uuid;",
                    seconds + 30,
                    metadata::text(&initial.id)
                ))
                .unwrap()
                .trim(),
                "t",
                "the committed serving hold must remain valid throughout its operation"
            );
            drop(hold);
        });
    }
    for acquire_snapshot in [false, true] {
        process::Operation::new(3601).enter(|| {
            let result = if acquire_snapshot {
                metadata::leased_snapshot(&slug, "alice", false).map(|_| ())
            } else {
                Barrier::acquire(&initial.id, false).map(|_| ())
            };
            assert!(
                result.is_err(),
                "operations beyond the hard budget must fail closed"
            );
        });
    }
    assert_eq!(
        metadata::sql(&format!(
            "SELECT count(*) FROM repository_leases WHERE repo_id={}::uuid;",
            metadata::text(&initial.id)
        ))
        .unwrap()
        .trim(),
        "0"
    );
}

#[test]
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables; run serially"]
fn long_holds_survive_owner_exit_and_fence_expired_publications() {
    const TEST: &str = "long_holds_survive_owner_exit_and_fence_expired_publications";
    const CHILD_REPO: &str = "GIT_RS_TEST_ABANDONED_LEASE_REPOSITORY";
    if let Ok(slug) = std::env::var(CHILD_REPO) {
        process::Operation::new(3600).enter(|| {
            let (_, hold) = metadata::leased_snapshot(&slug, "alice", true).unwrap();
            hold.check().unwrap();
            // Model a process disappearing without running the lease destructor.
            std::process::exit(0);
        });
    }
    process::Operation::new(3600).enter(|| {
        metadata::migrate().unwrap();
        let slug = format!(
            "metadata/abandoned-{}",
            git_rs::scratch::identifier().unwrap()
        );
        metadata::create(&slug, "alice").unwrap();
        let initial = metadata::snapshot(&slug, "alice", true).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", TEST])
            .env(CHILD_REPO, &slug)
            .stdout(Stdio::null())
            .status()
            .unwrap();
        assert!(
            status.success(),
            "the child must commit its hold before exiting"
        );
        let repo = metadata::text(&initial.id);
        assert_eq!(
            metadata::sql(&format!(
                "SELECT count(*) FROM repository_leases WHERE repo_id={repo}::uuid \
             AND expires_at > clock_timestamp() + interval '3500 seconds';"
            ))
            .unwrap()
            .trim(),
            "1",
            "connection/process loss must not release a live hold"
        );
        assert!(
            process::Operation::new(1)
                .enter(|| Barrier::acquire(&initial.id, true))
                .is_err()
        );

        let expire = format!(
            "UPDATE repository_leases SET expires_at=clock_timestamp()-interval '1 second' \
             WHERE repo_id={repo}::uuid;"
        );
        metadata::sql(&expire).unwrap();
        let maintenance = Barrier::acquire(&initial.id, true)
            .expect("expired abandoned holds must allow maintenance to recover");
        maintenance.check().unwrap();
        drop(maintenance);

        let (before, hold) = metadata::leased_snapshot(&slug, "alice", true).unwrap();
        metadata::sql(&expire).unwrap();
        assert!(hold.check().is_err());
        assert!(
            metadata::publish(
                &before,
                &hold,
                &git_rs::scratch::identifier().unwrap(),
                "alice",
                &[Update {
                    old: Oid::ZERO,
                    new: Oid([1; 20]),
                    name: b"refs/heads/stale".to_vec()
                }],
                &[true],
                &[],
            )
            .is_err(),
            "an active operation cannot publish through an expired hold"
        );
        let after = metadata::snapshot(&slug, "alice", true).unwrap();
        assert_eq!(after.refs, before.refs);
        assert_eq!(after.generation, before.generation);
        assert_eq!(
            metadata::sql(&format!(
                "SELECT count(*) FROM operations WHERE repo_id={repo}::uuid;"
            ))
            .unwrap()
            .trim(),
            "0"
        );
        drop(hold);
        Barrier::acquire(&initial.id, true)
            .unwrap()
            .check()
            .unwrap();
    });
}
