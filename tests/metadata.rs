#![forbid(unsafe_code)]

use git_rs::{
    metadata::{self, Barrier},
    process,
    protocol::{self, Oid, Update},
};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

// These tests initialize git_rs in a fresh, disposable PostgreSQL database. They
// deliberately exercise the public metadata API, without requiring S3 or Git RPCs.
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
                process::capture(process::git().args(["check-ref-format", &name]), b"", 4096)
                    .unwrap();
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
    let mut locker = process::Guard::new(
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
