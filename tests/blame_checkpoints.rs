#![forbid(unsafe_code)]
#[path = "support/blame_cache_support.rs"]
mod support;

use git_rs::{
    derived_cache::{
        ArtifactKind, BlameArtifacts, BlameBuilds, BuildLease, BuildLimits, BuildLookup,
        CacheConfig, CacheMaintenance, DerivedCache, Lookup,
    },
    process::Operation,
};
use support::*;

fn fixture() -> (DerivedCache, String) {
    let mut config = CacheConfig::new(id(), id());
    config.read_seconds = 60;
    config.grace_seconds = 1;
    let namespace = config.namespace.clone();
    let cache = DerivedCache::open(config).unwrap();
    cache.migrate().unwrap();
    (cache, namespace)
}

fn build(cache: &DerivedCache, key: &git_rs::derived_cache::ArtifactKey) -> BuildLease {
    match BlameBuilds::new(cache)
        .claim(
            key,
            authority(),
            BuildLimits {
                hard_seconds: 120,
                lease_seconds: 60,
            },
        )
        .unwrap()
    {
        BuildLookup::Claimed(build) => build,
        BuildLookup::Pending => panic!("fresh fixture build must be owned"),
    }
}

#[test]
#[ignore = "requires owned cache PostgreSQL/S3"]
fn checkpoint_swap_preserves_old_s3_reader_and_releases_only_build_pins() {
    Operation::new(60).enter(|| {
        let mut config = CacheConfig::new(id(), id());
        config.inline_bytes = 0; // Physical compression must not bypass the S3/GC seam.
        config.read_seconds = 60;
        config.grace_seconds = 1;
        let namespace = config.namespace.clone();
        let cache = DerivedCache::open(config).unwrap();
        cache.migrate().unwrap();
        let key = key(b"rolling-s3-units");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let admin = Admin::new();
        let hard_sql = format!("SELECT hard_until::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'");
        let hard = admin.scalar(&hard_sql);
        let old_bytes = vec![17; 524288];
        let first = vec![artifacts.stage_chunk(&build, &old_bytes).unwrap().unwrap()];
        let v1 = artifacts.replace_checkpoint(&build, None, &first).unwrap().unwrap();
        assert_eq!(admin.scalar(&format!("SELECT (p.encoding='zstd-v1' AND p.object_key IS NOT NULL AND p.stored_size<p.size AND e.size=p.size AND e.digest=p.digest)::text FROM git_rs_cache.manifest_children e JOIN git_rs_cache.payloads p ON p.namespace=e.namespace AND p.id=e.child WHERE e.namespace='{namespace}' AND e.parent='{}'", v1.payload_id())), "true", "checkpoint edges remain logical while the held child is compressed S3");
        drop(first);
        let old = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
        assert_eq!(old.version(), &v1);
        let next_bytes = vec![29; 524288];
        let next = vec![artifacts.stage_chunk(&build, &next_bytes).unwrap().unwrap()];
        let v2 = artifacts.replace_checkpoint(&build, Some(&v1), &next).unwrap().unwrap();
        drop(next);
        assert_ne!(v1.payload_id(), v2.payload_id());
        assert!(v2.generation() > v1.generation());
        assert_eq!(admin.scalar(&hard_sql), hard, "rolling must not reset the build's hard deadline");
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")), "0", "ready edges replace actual build pins, including the new children");
        let current = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
        assert_eq!(current.version(), &v2);
        old.with_chunks(|chunks| { assert_eq!(chunks, [old_bytes.as_slice()]); Ok(()) }).unwrap();
        current.with_chunks(|chunks| { assert_eq!(chunks, [next_bytes.as_slice()]); Ok(()) }).unwrap();
        admin.sql(&format!("UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}' AND payload<>'{}'", v2.payload_id()));
        let maintenance = CacheMaintenance::new(&cache);
        let lease = maintenance.claim().unwrap().unwrap();
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(), 1, "only the replaced root can retire while its reader retains the old S3 child");
        old.with_chunks(|chunks| { assert_eq!(chunks, [old_bytes.as_slice()]); Ok(()) }).unwrap();
        drop(old);
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(), 1, "old child is reclaimable after the last old root hold drains");
        current.with_chunks(|chunks| { assert_eq!(chunks, [next_bytes.as_slice()]); Ok(()) }).unwrap();
    });
}

#[test]
#[ignore = "requires owned cache PostgreSQL"]
fn rolling_checkpoint_recycles_sixty_four_live_pins_not_cumulative_history() {
    Operation::new(120).enter(|| {
        let (cache, namespace) = fixture();
        let key = key(b"rolling-live-pin-limit");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let admin = Admin::new();
        let mut version = None;
        for round in 0..3 {
            let chunks: Vec<_> = (0..64).map(|index| artifacts.stage_chunk(&build, format!("complete mapping {round}:{index}").as_bytes()).unwrap().unwrap()).collect();
            assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")), "64");
            assert!(artifacts.stage_chunk(&build, format!("excess complete mapping {round}").as_bytes()).is_err());
            version = artifacts.replace_checkpoint(&build, version.as_ref(), &chunks).unwrap();
            assert!(version.is_some(), "pin release before manifest validation or no release after commit must fail this rolling workload");
            assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")), "0");
            let checkpoint = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
            checkpoint.with_chunks(|actual| {
                assert_eq!(actual.len(), 64);
                for (index, bytes) in actual.iter().enumerate() { assert_eq!(*bytes, format!("complete mapping {round}:{index}").as_bytes()); }
                Ok(())
            }).unwrap();
        }
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_children WHERE namespace='{namespace}'")), "192", "old edges/history are not deleted to recycle pin slots");
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.payloads WHERE namespace='{namespace}'")), "198", "all published payload identities and three rejected excess children remain discoverable");
    });
}

#[test]
#[ignore = "requires owned cache PostgreSQL"]
fn checkpoint_stale_cas_and_expired_parent_cannot_replace_or_release_pins() {
    Operation::new(60).enter(|| {
        let (cache, namespace) = fixture();
        let key = key(b"checkpoint-cas");
        let old_build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let first = vec![artifacts.stage_chunk(&old_build, b"first complete edge").unwrap().unwrap()];
        let v1 = artifacts.replace_checkpoint(&old_build, None, &first).unwrap().unwrap();
        let second = vec![artifacts.stage_chunk(&old_build, b"first complete edge").unwrap().unwrap()];
        let v2 = artifacts.replace_checkpoint(&old_build, Some(&v1), &second).unwrap().unwrap();
        assert_eq!(v1.digest(), v2.digest(), "identical snapshot bytes may share a digest");
        assert_ne!(v1.payload_id(), v2.payload_id(), "even identical snapshots get fresh immutable root identities");
        assert!(v2.generation() > v1.generation());
        let third = vec![artifacts.stage_chunk(&old_build, b"third complete edge").unwrap().unwrap()];
        assert!(artifacts.replace_checkpoint(&old_build, Some(&v1), &third).unwrap().is_none());
        let admin = Admin::new();
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")), "1", "stale CAS must not consume live build pins");
        admin.sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'"));
        let fresh_build = build(&cache, &key);
        assert!(artifacts.replace_checkpoint(&old_build, Some(&v2), &third).is_err());
        assert!(!old_build.renew().unwrap());
        let replacement = vec![artifacts.stage_chunk(&fresh_build, b"fresh owner's complete edge").unwrap().unwrap()];
        let v3 = artifacts.replace_checkpoint(&fresh_build, Some(&v2), &replacement).unwrap().unwrap();
        assert!(v3.generation() > v2.generation());
        assert_ne!(v3.payload_id(), v2.payload_id());
        let loaded = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
        assert_eq!(loaded.version(), &v3);
        loaded.with_chunks(|chunks| { assert_eq!(chunks, [b"fresh owner's complete edge".as_slice()]); Ok(()) }).unwrap();
    });
}

#[test]
#[ignore = "requires owned cache PostgreSQL"]
fn checkpoint_api_cannot_replace_complete_body_or_accept_a_foreign_version() {
    Operation::new(30).enter(|| {
        let (cache, namespace) = fixture();
        let artifacts = BlameArtifacts::new(&cache);
        let body_key = key(b"immutable-body");
        let body_build = build(&cache, &body_key);
        let body = vec![
            artifacts
                .stage_chunk(&body_build, b"immutable complete body")
                .unwrap()
                .unwrap(),
        ];
        assert!(
            artifacts
                .publish(&body_build, ArtifactKind::CompleteBody, &body)
                .unwrap()
        );
        assert!(
            artifacts
                .replace_checkpoint(&body_build, None, &body)
                .unwrap()
                .is_none()
        );
        artifacts
            .get_verified(&body_key, ArtifactKind::CompleteBody, authority())
            .unwrap()
            .unwrap()
            .with_chunks(|chunks| {
                assert_eq!(chunks, [b"immutable complete body".as_slice()]);
                Ok(())
            })
            .unwrap();
        let opaque_key = key(b"immutable-opaque");
        let opaque_claim = match cache.get_or_claim(&opaque_key, authority()).unwrap() {
            Lookup::Claimed(claim) => claim,
            _ => panic!("fresh opaque key must be owned"),
        };
        assert!(cache.publish(&opaque_claim, authority().as_ref(), b"opaque immutable bytes").unwrap());
        let opaque_build = build(&cache, &opaque_key);
        let opaque_chunks = vec![artifacts.stage_chunk(&opaque_build, b"opaque replacement attempt").unwrap().unwrap()];
        let admin = Admin::new();
        admin.sql(&format!("UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'"));
        let state_sql = format!("SELECT jsonb_build_object('directory',(SELECT jsonb_agg(to_jsonb(d) ORDER BY key_hash) FROM git_rs_cache.directory d WHERE namespace='{namespace}'),'pins',(SELECT jsonb_agg(to_jsonb(c) ORDER BY child) FROM git_rs_cache.build_chunks c WHERE namespace='{namespace}'),'quota',(SELECT to_jsonb(n) FROM git_rs_cache.namespaces n WHERE id='{namespace}'))::text");
        let before = admin.scalar(&state_sql);
        assert!(artifacts.replace_checkpoint(&body_build, None, &body).unwrap().is_none(), "expired CompleteBody must not become a checkpoint");
        assert_eq!(admin.scalar(&state_sql), before, "rejected expired body replacement has no directory/pin/quota effects");
        assert!(artifacts.replace_checkpoint(&opaque_build, None, &opaque_chunks).unwrap().is_none(), "expired opaque artifact must not become a checkpoint");
        assert_eq!(admin.scalar(&state_sql), before, "rejected expired opaque replacement has no directory/pin/quota effects");
        let checkpoint_key = key(b"checkpoint-other-scope");
        let checkpoint_build = build(&cache, &checkpoint_key);
        let chunks = vec![
            artifacts
                .stage_chunk(&checkpoint_build, b"complete mapping")
                .unwrap()
                .unwrap(),
        ];
        let version = artifacts
            .replace_checkpoint(&checkpoint_build, None, &chunks)
            .unwrap()
            .unwrap();
        assert!(
            artifacts
                .replace_checkpoint(&body_build, Some(&version), &body)
                .is_err()
        );
    });
}

#[test]
#[ignore = "requires owned cache PostgreSQL"]
fn staged_but_unpublished_batch_never_replaces_last_complete_checkpoint() {
    Operation::new(30).enter(|| {
        let (cache, namespace) = fixture();
        let key = key(b"abort-checkpoint");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let first = vec![artifacts.stage_chunk(&build, b"last complete mapping batch").unwrap().unwrap()];
        let version = artifacts.replace_checkpoint(&build, None, &first).unwrap().unwrap();
        let pending = artifacts.stage_chunk(&build, b"one complete unit of an unfinished batch").unwrap().unwrap();
        let loaded = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
        assert_eq!(loaded.version(), &version);
        loaded.with_chunks(|chunks| { assert_eq!(chunks, [b"last complete mapping batch".as_slice()]); Ok(()) }).unwrap();
        assert_eq!(Admin::new().scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'")), "1");
        let repeated: Vec<_> = (0..64).map(|_| artifacts.stage_chunk(&build, b"last complete mapping batch").unwrap().unwrap()).collect();
        let admin = Admin::new();
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")), "2", "64 repeated child occurrences own one pin, alongside the unrelated pending pin");
        let rows_before: i64 = admin.scalar(&format!("SELECT used_rows::text FROM git_rs_cache.namespaces WHERE id='{namespace}'")).parse().unwrap();
        let v2 = artifacts.replace_checkpoint(&build, Some(&version), &repeated).unwrap().unwrap();
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_children WHERE namespace='{namespace}' AND parent='{}'", v2.payload_id())), "64");
        let rows_after: i64 = admin.scalar(&format!("SELECT used_rows::text FROM git_rs_cache.namespaces WHERE id='{namespace}'")).parse().unwrap();
        // Inline root payload + manifest + 64 edges, less exactly one pin.
        assert_eq!(rows_after - rows_before, 65, "refund actual pins once, not repeated edge occurrences");
        let repeated_reader = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
        repeated_reader.with_chunks(|chunks| {
            assert_eq!(chunks.len(), 64);
            assert!(chunks.iter().all(|bytes| *bytes == b"last complete mapping batch"));
            Ok(())
        }).unwrap();
        assert_eq!(Admin::new().scalar(&format!("SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")), "1", "a swap must not silently release an omitted, still-pending unit");
        assert!(artifacts.replace_checkpoint(&build, Some(&v2), &[pending]).unwrap().is_some(), "the omitted unit retains durable ownership until explicitly committed");
    });
}

#[test]
#[ignore = "requires owned cache PostgreSQL"]
fn checkpoint_edge_quota_rollback_preserves_previous_root_reader_and_pins() {
    Operation::new(30).enter(|| {
        let mut config = CacheConfig::new(id(), id());
        config.namespace_rows = 16;
        config.connections = 1;
        let namespace = config.namespace.clone();
        let cache = DerivedCache::open(config).unwrap();
        cache.migrate().unwrap();
        let key = key(b"checkpoint-quota");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let first = vec![artifacts.stage_chunk(&build, b"previous complete batch").unwrap().unwrap()];
        let version = artifacts.replace_checkpoint(&build, None, &first).unwrap().unwrap();
        let reader = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
        let next = vec![artifacts.stage_chunk(&build, b"next complete batch").unwrap().unwrap()];
        let admin = Admin::new();
        assert_eq!(admin.scalar(&format!("SELECT used_rows::text FROM git_rs_cache.namespaces WHERE id='{namespace}'")), "14", "fail at root/edge charge, not initial staging");
        assert!(artifacts.replace_checkpoint(&build, Some(&version), &next).is_err());
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'")), "1");
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")), "1", "failed swap must not release pending pins");
        assert_eq!(admin.scalar(&format!("SELECT payload::text FROM git_rs_cache.directory WHERE namespace='{namespace}' AND payload='{}' AND state='ready'", version.payload_id())), version.payload_id());
        reader.with_chunks(|chunks| { assert_eq!(chunks, [b"previous complete batch".as_slice()]); Ok(()) }).unwrap();
        assert!(CacheMaintenance::new(&cache).claim().unwrap().is_some());
        drop(reader);
        let loaded = artifacts.get_checkpoint_verified(&key, authority()).unwrap().unwrap();
        assert_eq!(loaded.version(), &version);
    });
}

#[test]
#[ignore = "requires owned cache PostgreSQL"]
fn checkpoint_expiry_during_child_fk_wait_rolls_back_swap_and_pin_release() {
    let (cache, namespace) = Operation::new(30).enter(fixture);
    let key = key(b"checkpoint-final-wait");
    let build = Operation::new(30).enter(|| build(&cache, &key));
    let artifacts = BlameArtifacts::new(&cache);
    let (version, old, next) = Operation::new(30).enter(|| {
        let first = vec![
            artifacts
                .stage_chunk(&build, b"previous complete checkpoint")
                .unwrap()
                .unwrap(),
        ];
        let version = artifacts
            .replace_checkpoint(&build, None, &first)
            .unwrap()
            .unwrap();
        let old = artifacts
            .get_checkpoint_verified(&key, authority())
            .unwrap()
            .unwrap();
        let next = vec![
            artifacts
                .stage_chunk(&build, b"new complete checkpoint")
                .unwrap()
                .unwrap(),
        ];
        (version, old, next)
    });
    let locker = Admin::new();
    let observer = Admin::new();
    let locker_pid = locker.scalar("SELECT pg_backend_pid()::text");
    // The production one-second lock timeout would otherwise abort before a
    // generously future parent lease expires, falsely proving the final fence.
    // Change only this fixture function's lock-wait budget, never the DB clock.
    let timeout_sql = "ALTER FUNCTION git_rs_cache.publish_checkpoint_v4(uuid,uuid,bytea,uuid,bigint,uuid,bytea,integer,bytea,uuid,bigint,text[],bytea[],bigint[],integer)";
    observer.sql(&format!("{timeout_sql} SET lock_timeout='4s'"));
    locker.sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()+interval '3 seconds' WHERE namespace='{namespace}'"));
    locker.sql(&format!("BEGIN; SELECT p.id FROM git_rs_cache.payloads p JOIN git_rs_cache.build_chunks c ON c.namespace=p.namespace AND c.child=p.id WHERE p.namespace='{namespace}' FOR UPDATE OF p"));
    let expected = version.clone();
    let publication = std::thread::spawn(move || {
        Operation::new(15).enter(|| artifacts.replace_checkpoint(&build, Some(&expected), &next))
    });
    observer.until(&format!("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='git-rs-cache' AND wait_event_type='Lock' AND wait_event='transactionid' AND query LIKE '%publish_checkpoint%' AND {locker_pid}=ANY(pg_blocking_pids(pid)))::text"));
    assert_eq!(observer.scalar(&format!("SELECT (lease_until>clock_timestamp())::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'")), "true", "publication must reach this exact child locker while parent authority is still live");
    assert_eq!(
        observer.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'"
        )),
        "1",
        "no separately committed early pin release"
    );
    assert_eq!(observer.scalar(&format!("SELECT payload::text FROM git_rs_cache.directory WHERE namespace='{namespace}' AND payload='{}' AND state='ready'", version.payload_id())), version.payload_id());
    let expiry_deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    while observer.scalar(&format!("SELECT (lease_until<=clock_timestamp())::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'")) != "true" {
        assert!(std::time::Instant::now() < expiry_deadline, "real database clock must cross the finite lease while blocked");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    locker.sql("ROLLBACK");
    let outcome = publication.join().unwrap();
    observer.sql(&format!("{timeout_sql} RESET lock_timeout"));
    let error = outcome
        .expect_err("expired parent must roll back the new ready root, edges and pin consumption");
    assert_eq!(
        error.to_string(),
        "PostgreSQL operation failed (SQLSTATE P0001)",
        "only the explicit final SQL fence is evidence, not a lock/statement timeout or cached-lifetime error"
    );
    assert_eq!(
        observer.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'"
        )),
        "1"
    );
    assert_eq!(
        observer.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'"
        )),
        "1"
    );
    Operation::new(15)
        .enter(|| {
            old.with_chunks(|chunks| {
                assert_eq!(chunks, [b"previous complete checkpoint".as_slice()]);
                Ok(())
            })
        })
        .unwrap();
}

#[test]
#[ignore = "requires owned cache PostgreSQL"]
fn checkpoint_operation_and_manifest_bounds_reject_before_a_root_claim() {
    let (cache, namespace) = Operation::new(30).enter(fixture);
    let key = key(b"checkpoint-boundaries");
    let build = Operation::new(30).enter(|| build(&cache, &key));
    let artifacts = BlameArtifacts::new(&cache);
    assert!(artifacts.replace_checkpoint(&build, None, &[]).is_err());
    assert!(
        Operation::new(0)
            .enter(|| artifacts.replace_checkpoint(&build, None, &[]))
            .is_err()
    );
    let admin = Admin::new();
    assert_eq!(
        admin.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.jobs WHERE namespace='{namespace}'"
        )),
        "0"
    );
    Operation::new(30).enter(|| {
        // Repeated content exercises the manifest count independently from the
        // distinct-pin limit (one content payload, 65 requested occurrences).
        let chunks: Vec<_> = (0..65).map(|_| artifacts.stage_chunk(&build, b"complete repeated unit").unwrap().unwrap()).collect();
        assert!(artifacts.replace_checkpoint(&build, None, &chunks).is_err());
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.jobs WHERE namespace='{namespace}' AND checkpoint_write")), "0");
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'")), "0");
    });
}
