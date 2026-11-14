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
#[ignore = "requires a disposable PostgreSQL database configured with PG* variables"]
fn large_pack_catalog_remains_readable() {
    process::Operation::new(120).enter(|| {
        metadata::migrate().unwrap();
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
        let snapshot = metadata::snapshot(&slug, "alice", false).unwrap();
        assert_eq!(snapshot.packs.len(), 40000);
        assert_eq!(snapshot.packs.first().unwrap().hash, format!("{:040x}", 1));
        assert_eq!(snapshot.packs.last().unwrap().hash, format!("{:040x}", 40000));
        assert_eq!(snapshot.packs.iter().map(|p| p.pack_size + p.idx_size).sum::<u64>(), 84_800_000);
        assert!(snapshot.refs.is_empty());
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
