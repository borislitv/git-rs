#![forbid(unsafe_code)]
#[path = "support/blame_artifact_process.rs"]
mod artifact_fixture;
#[path = "support/blame_cache_support.rs"]
mod support;
use git_rs::{
    derived_cache::{
        ArtifactKind, BlameArtifacts, BlameBuilds, BuildLease, BuildLimits, BuildLookup,
        CacheConfig, CacheMaintenance, DerivedCache,
    },
    process::Operation,
};
use support::*;

fn build(cache: &DerivedCache, key: &git_rs::derived_cache::ArtifactKey) -> BuildLease {
    match BlameBuilds::new(cache)
        .claim(key, authority(), BuildLimits::default())
        .unwrap()
    {
        BuildLookup::Claimed(build) => build,
        BuildLookup::Pending => panic!("fresh fixture build must be claimable"),
    }
}

#[test]
#[ignore = "requires an owned PostgreSQL/S3 cache fixture"]
fn complete_manifest_reuses_verified_chunks_across_independent_cache_pools() {
    Operation::new(30).enter(|| {
        let config = CacheConfig::new(id(), id());
        let cache = DerivedCache::open(config.clone()).unwrap();
        cache.migrate().unwrap();
        let key = key(b"binary-body");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let first = vec![0x91; 524288];
        let second = b"\0binary\r\nno-final-lf";
        let chunks = vec![
            artifacts.stage_chunk(&build, &first).unwrap().unwrap(),
            artifacts.stage_chunk(&build, second).unwrap().unwrap(),
        ];
        assert!(
            artifacts
                .get_verified(&key, ArtifactKind::CompleteBody, authority())
                .unwrap()
                .is_none(),
            "chunks alone are not a complete descriptor"
        );
        assert!(
            artifacts
                .publish(&build, ArtifactKind::CompleteBody, &chunks)
                .unwrap()
        );
        drop(chunks);
        let independent = DerivedCache::open(config).unwrap();
        let complete = BlameArtifacts::new(&independent)
            .get_verified(&key, ArtifactKind::CompleteBody, authority())
            .unwrap()
            .unwrap();
        assert_eq!(complete.len(), first.len() + second.len());
        complete
            .with_chunks(|actual| {
                assert_eq!(actual.len(), 2);
                assert_eq!(actual[0], first);
                assert_eq!(actual[1], second);
                Ok(())
            })
            .unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL/S3 cache fixture"]
fn root_and_live_root_hold_retain_children_after_independent_child_expiry() {
    Operation::new(30).enter(|| {
        let (cache, namespace) = cache();
        let key = key(b"root-retention");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let chunks = vec![artifacts.stage_chunk(&build, b"first complete unit").unwrap().unwrap(), artifacts.stage_chunk(&build, b"second complete unit").unwrap().unwrap()];
        assert!(artifacts.publish(&build, ArtifactKind::CompleteCheckpoint, &chunks).unwrap());
        drop(chunks);
        let admin = Admin::new();
        admin.sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'; UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}' AND payload NOT IN (SELECT payload FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}');"));
        let maintenance = CacheMaintenance::new(&cache);
        let lease = maintenance.claim().unwrap().unwrap();
        maintenance.expire_batch(&lease).unwrap();
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(),0,"a live root must retain expired children");
        let body = artifacts.get_verified(&key, ArtifactKind::CompleteCheckpoint, authority()).unwrap().unwrap();
        admin.sql(&format!("UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}';"));
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(),1,"the root may retire, but its live read hold still retains children");
        body.with_chunks(|chunks| { assert_eq!(chunks,[b"first complete unit".as_slice(), b"second complete unit".as_slice()]); Ok(()) }).unwrap();
        drop(body);
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(),2,"children become reclaimable only after the root hold drains");
        admin.sql(&format!("UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'"));
        assert_eq!(maintenance.delete_retired(&lease).unwrap(),3);
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL/S3 cache fixture"]
fn corrupt_child_or_wrong_manifest_kind_never_returns_partial_success() {
    Operation::new(30).enter(|| {
        let (cache, namespace) = cache();
        let key = key(b"corrupt-child");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        let chunks = vec![artifacts.stage_chunk(&build, b"valid").unwrap().unwrap()];
        assert!(artifacts.publish(&build, ArtifactKind::CompleteMappingBatch, &chunks).unwrap());
        drop(chunks);
        assert!(artifacts.get_verified(&key, ArtifactKind::CompleteBody, authority()).unwrap().is_none());
        assert!(artifacts.get_verified(&key, ArtifactKind::CompleteMappingBatch, authority()).unwrap().is_some(), "requesting another representation must not invalidate a healthy complete unit");
        let admin = Admin::new();
        admin.sql(&format!("UPDATE git_rs_cache.payloads SET body=convert_to('wrong','UTF8') WHERE namespace='{namespace}' AND id IN (SELECT child FROM git_rs_cache.manifest_children WHERE namespace='{namespace}')"));
        assert!(artifacts.get_verified(&key, ArtifactKind::CompleteMappingBatch, authority()).unwrap().is_none());
        assert!(artifacts.get_verified(&key, ArtifactKind::CompleteBody, authority()).unwrap().is_none());
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL/S3 cache fixture"]
fn expired_build_cannot_publish_a_ready_root_and_chunk_bounds_are_explicit() {
    Operation::new(30).enter(|| {
        let (cache, namespace) = cache();
        let key = key(b"expired-build");
        let build = build(&cache,&key);
        let artifacts = BlameArtifacts::new(&cache);
        assert!(artifacts.stage_chunk(&build,b"").is_err());
        assert!(artifacts.stage_chunk(&build,&vec![0;4*1024*1024+1]).is_err());
        let chunks = vec![artifacts.stage_chunk(&build,b"complete before owner expiry").unwrap().unwrap()];
        Admin::new().sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'"));
        assert!(artifacts.publish(&build,ArtifactKind::CompleteBody,&chunks).is_err());
        assert!(artifacts.get_verified(&key,ArtifactKind::CompleteBody,authority()).unwrap().is_none());
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL/S3 cache fixture"]
fn long_build_pins_complete_chunks_beyond_their_initial_short_read_holds() {
    Operation::new(30).enter(|| {
        let (cache, namespace) = cache();
        let key = key(b"build-pins");
        let build = build(&cache,&key);
        let artifacts = BlameArtifacts::new(&cache);
        let chunks = vec![artifacts.stage_chunk(&build,b"reusable complete edge mapping").unwrap().unwrap()];
        Admin::new().sql(&format!("UPDATE git_rs_cache.read_holds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'; UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}';"));
        let maintenance = CacheMaintenance::new(&cache);
        let lease = maintenance.claim().unwrap().unwrap();
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(),0,"live build pins must retain completed children");
        assert!(artifacts.publish(&build,ArtifactKind::CompleteMappingBatch,&chunks).unwrap());
        let result = artifacts.get_verified(&key,ArtifactKind::CompleteMappingBatch,authority()).unwrap().unwrap();
        result.with_chunks(|bytes| { assert_eq!(bytes,[b"reusable complete edge mapping".as_slice()]); Ok(()) }).unwrap();
    });
}

#[test]
#[ignore = "requires owned PostgreSQL/S3 and at least 512 MiB admitted test RAM"]
fn full_256_mib_body_uses_bounded_chunks_without_a_concatenated_buffer() {
    Operation::new(240).enter(|| {
        let mut config = CacheConfig::new(id(), id());
        config.memory_bytes = 512 * 1024 * 1024;
        config.read_seconds = 120;
        let cache = DerivedCache::open(config).unwrap();
        cache.migrate().unwrap();
        let key = key(b"maximum-body");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        build
            .run_with_renewal(|lease| {
                let input = vec![0xa5; 4 * 1024 * 1024];
                let mut chunks = Vec::new();
                // Repeated content deliberately exercises shared physical children;
                // the complete logical body is still exactly 256 MiB.
                for _ in 0..65 {
                    chunks.push(artifacts.stage_chunk(lease, &input).unwrap().unwrap());
                }
                assert!(
                    artifacts
                        .publish(lease, ArtifactKind::CompleteBody, &chunks)
                        .is_err()
                );
                chunks.truncate(64);
                assert!(
                    artifacts
                        .publish(lease, ArtifactKind::CompleteBody, &chunks)
                        .unwrap()
                );
                drop(chunks);
                let body = artifacts
                    .get_verified(&key, ArtifactKind::CompleteBody, authority())
                    .unwrap()
                    .unwrap();
                assert_eq!(body.len(), 256 * 1024 * 1024);
                body.with_chunks(|actual| {
                    assert_eq!(actual.len(), 64);
                    for chunk in actual {
                        assert_eq!(*chunk, input.as_slice());
                    }
                    Ok(())
                })?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            cache.allocated(),
            0,
            "last body owner must release cache RAM capacity"
        );
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn empty_body_still_requires_an_atomic_complete_descriptor() {
    Operation::new(30).enter(|| {
        let (cache, _) = cache();
        let key = key(b"empty-body");
        let build = build(&cache, &key);
        let artifacts = BlameArtifacts::new(&cache);
        assert!(
            artifacts
                .get_verified(&key, ArtifactKind::CompleteBody, authority())
                .unwrap()
                .is_none()
        );
        assert!(
            artifacts
                .publish(&build, ArtifactKind::CompleteBody, &[])
                .unwrap()
        );
        let body = artifacts
            .get_verified(&key, ArtifactKind::CompleteBody, authority())
            .unwrap()
            .unwrap();
        assert!(body.is_empty());
        body.with_chunks(|chunks| {
            assert!(chunks.is_empty());
            Ok(())
        })
        .unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn cold_staging_and_publication_without_an_operation_are_fallible_and_side_effect_free() {
    let (cache, namespace) = cache();
    let key = key(b"outside-claim-operation");
    let build = Operation::new(15).enter(|| build(&cache, &key));
    let artifacts = BlameArtifacts::new(&cache);
    let stage = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        artifacts.stage_chunk(&build, b"complete cold chunk")
    }));
    assert!(
        stage.is_ok(),
        "cold stage_chunk outside Operation must return Result, not panic"
    );
    assert!(
        stage.unwrap().is_err(),
        "missing bounded caller must fail at the public boundary"
    );
    let admin = Admin::new();
    assert_eq!(
        admin.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.jobs WHERE namespace='{namespace}'"
        )),
        "0",
        "rejection must precede cold-payload claim side effects"
    );
    assert!(
        Operation::new(0)
            .enter(|| artifacts.stage_chunk(&build, b"narrow caller already expired"))
            .is_err(),
        "a longer build lease must not widen the caller's narrower operation"
    );
    assert_eq!(
        admin.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.jobs WHERE namespace='{namespace}'"
        )),
        "0"
    );
    let chunk = Operation::new(15)
        .enter(|| artifacts.stage_chunk(&build, b"complete cold chunk"))
        .unwrap()
        .unwrap();
    let before = admin.scalar(&format!(
        "SELECT count(*)::text FROM git_rs_cache.jobs WHERE namespace='{namespace}'"
    ));
    let publish = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        artifacts.publish(&build, ArtifactKind::CompleteBody, &[chunk])
    }));
    assert!(
        publish.is_ok(),
        "publish outside Operation must return Result, not panic"
    );
    assert!(publish.unwrap().is_err());
    assert_eq!(
        admin.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.jobs WHERE namespace='{namespace}'"
        )),
        before,
        "missing-operation publication must not prepare a root"
    );
    assert_eq!(
        admin.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'"
        )),
        "0"
    );
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn final_publication_fence_rolls_back_ready_root_after_child_fk_lock_wait() {
    let (cache, namespace) = cache();
    let key = key(b"publication-fk-wait");
    let build = Operation::new(15).enter(|| build(&cache, &key));
    let artifacts = BlameArtifacts::new(&cache);
    let chunks = Operation::new(15).enter(|| {
        vec![
            artifacts
                .stage_chunk(&build, b"complete pinned child")
                .unwrap()
                .unwrap(),
        ]
    });
    let locker = Admin::new();
    let observer = Admin::new();
    locker.sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()+interval '500 milliseconds' WHERE namespace='{namespace}'"));
    locker.sql(&format!("BEGIN; SELECT p.id FROM git_rs_cache.payloads p JOIN git_rs_cache.build_chunks c ON c.namespace=p.namespace AND c.child=p.id WHERE p.namespace='{namespace}' FOR UPDATE OF p;"));
    let publication = std::thread::spawn(move || {
        Operation::new(5).enter(|| artifacts.publish(&build, ArtifactKind::CompleteBody, &chunks))
    });
    observer.until("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='git-rs-cache' AND wait_event_type='Lock' AND wait_event='transactionid' AND query LIKE '%publish_manifest%')::text");
    observer.until(&format!("SELECT (lease_until<=clock_timestamp())::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"));
    locker.sql("ROLLBACK");
    let answer = publication.join().unwrap();
    assert!(
        !matches!(answer, Ok(true)),
        "final publication fence must reject expiry after the actual child FK lock wait"
    );
    assert_eq!(
        observer.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'"
        )),
        "0",
        "root metadata must roll back atomically"
    );
    assert_eq!(observer.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_children WHERE namespace='{namespace}'")),"0","all edges must roll back atomically");
    let hash = git_rs::protocol::hex(key.digest());
    assert_eq!(observer.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.directory WHERE namespace='{namespace}' AND key_hash=decode('{hash}','hex') AND state='ready'")),"0","no ready descriptor may escape rollback");
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn sixty_four_distinct_build_pins_are_bounded_and_the_sixty_fifth_is_not_a_partial_root() {
    Operation::new(60).enter(|| {
        let (cache,namespace) = cache();
        let key = key(b"distinct-pins");
        let build = build(&cache,&key);
        let artifacts = BlameArtifacts::new(&cache);
        let mut chunks = Vec::new();
        for index in 0..64 { chunks.push(artifacts.stage_chunk(&build,format!("complete mapping {index}").as_bytes()).unwrap().unwrap()); }
        let admin = Admin::new();
        assert_eq!(admin.scalar(&format!("SELECT count(DISTINCT child)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")),"64");
        assert!(artifacts.stage_chunk(&build,b"a sixty-fifth distinct mapping").is_err(),"65th distinct build pin must be rejected");
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'")),"0");
        assert!(artifacts.get_verified(&key,ArtifactKind::CompleteMappingBatch,authority()).unwrap().is_none());
        assert!(artifacts.publish(&build,ArtifactKind::CompleteMappingBatch,&chunks).unwrap());
        drop(chunks);
        let body = artifacts.get_verified(&key,ArtifactKind::CompleteMappingBatch,authority()).unwrap().unwrap();
        body.with_chunks(|actual| {assert_eq!(actual.len(),64);for (index,bytes) in actual.iter().enumerate(){assert_eq!(*bytes,format!("complete mapping {index}").as_bytes());}Ok(())}).unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn manifest_edge_quota_failure_rolls_back_ready_state_and_preserves_maintenance_and_source() {
    Operation::new(30).enter(|| {
        let mut config = CacheConfig::new(id(),id());
        config.namespace_rows=16;config.connections=1;
        let namespace = config.namespace.clone();
        let cache = DerivedCache::open(config).unwrap();cache.migrate().unwrap();
        let key = key(b"edge-quota");
        let build = build(&cache,&key);
        let artifacts = BlameArtifacts::new(&cache);
        let chunks = vec![artifacts.stage_chunk(&build,b"one complete child").unwrap().unwrap()];
        for index in 0..7 {assert!(matches!(cache.get_or_claim(&support::key(format!("quota-fill-{index}").as_bytes()),authority()).unwrap(),git_rs::derived_cache::Lookup::Claimed(_)));}
        let admin=Admin::new();
        assert_eq!(admin.scalar(&format!("SELECT used_rows::text FROM git_rs_cache.namespaces WHERE id='{namespace}'")),"12","fixture must fail at the manifest-edge charge, not earlier admission");
        assert!(artifacts.publish(&build,ArtifactKind::CompleteBody,&chunks).is_err());
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'")),"0");
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.manifest_children WHERE namespace='{namespace}'")),"0");
        let hash=git_rs::protocol::hex(key.digest());
        assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs_cache.directory WHERE namespace='{namespace}' AND key_hash=decode('{hash}','hex')")),"0");
        assert!(CacheMaintenance::new(&cache).claim().unwrap().is_some());
        let slug=format!("synthetic/quota-source-{}",id());git_rs::metadata::create(&slug,"alice").unwrap();
        assert!(git_rs::metadata::snapshot(&slug,"alice",false).unwrap().refs.is_empty());
    });
}

#[test]
#[ignore = "requires owned source/cache PostgreSQL fixtures"]
fn independent_real_source_owner_survives_initiator_cancel_but_acl_revocation_fences_the_build() {
    use std::{sync::mpsc, time::Duration};
    let (cache, namespace) = cache();
    let key = key(b"owned-source-build");
    let source = Source::acquire(&key);
    let slug = source.slug.clone();
    let repo = source.repo.clone();
    let request = Operation::new(15);
    let lease = request.enter(|| {
        match BlameBuilds::new(&cache)
            .claim(
                &key,
                source.clone(),
                BuildLimits {
                    hard_seconds: 60,
                    lease_seconds: 15,
                },
            )
            .unwrap()
        {
            BuildLookup::Claimed(lease) => lease,
            _ => panic!("fresh source build"),
        }
    });
    drop(source);
    let admission = git_rs::process::Admission::new(1);
    let permit = admission.try_admit().unwrap();
    let (started, running) = mpsc::channel();
    let (finish, finished) = mpsc::channel();
    let cache_job = cache.clone();
    let work = std::thread::spawn(move || {
        let actual_work = permit;
        let result = lease.run_with_renewal(|context| {
            started.send(()).unwrap();
            loop {
                context.check_local()?;
                match finished.recv_timeout(Duration::from_millis(10)) {
                    Ok(()) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(_) => return Err(git_rs::invalid("fixture gate closed")),
                }
            }
            let artifacts = BlameArtifacts::new(&cache_job);
            let chunks = vec![
                artifacts
                    .stage_chunk(context, b"source-authorized complete mapping")?
                    .ok_or_else(|| git_rs::invalid("fixture claim pending"))?,
            ];
            artifacts.publish(context, ArtifactKind::CompleteBody, &chunks)
        });
        // The caller owns admission through source cleanup, not just CPU stop.
        drop(lease);
        drop(actual_work);
        result
    });
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    request.cancel();
    drop(request);
    assert_eq!(
        admission.active(),
        1,
        "initiator cancellation must not release actual owned work"
    );
    let admin = Admin::new();
    assert_eq!(admin.scalar(&format!("SELECT count(*)::text FROM git_rs.repository_leases WHERE repo_id='{repo}' AND expires_at>clock_timestamp()")),"1","the retained owner must be a real independent source lease");
    Operation::new(15)
        .enter(|| git_rs::metadata::grant(&slug, "alice", "bob", "none", b"refs/"))
        .unwrap();
    finish.send(()).unwrap();
    assert!(work.join().unwrap().is_err());
    assert_eq!(admission.active(), 0);
    assert_eq!(
        admin.scalar(&format!(
            "SELECT count(*)::text FROM git_rs_cache.manifest_roots WHERE namespace='{namespace}'"
        )),
        "0",
        "a revoked source must not publish a root"
    );
    assert_eq!(
        admin.scalar(&format!(
            "SELECT count(*)::text FROM git_rs.repository_leases WHERE repo_id='{repo}'"
        )),
        "0",
        "actual source owner must be released after work cleanup"
    );
}

#[test]
#[ignore = "requires owned source/cache PostgreSQL fixtures"]
fn complete_artifact_checks_real_acl_before_consumption_and_source_expiry_after_callback() {
    use std::sync::atomic::{AtomicBool, Ordering};
    Operation::new(30).enter(|| {
        let (cache,_)=cache();let key=key(b"source-result-fences");let source=Source::acquire(&key);
        let build=match BlameBuilds::new(&cache).claim(&key,source.clone(),BuildLimits{hard_seconds:60,lease_seconds:15}).unwrap(){BuildLookup::Claimed(lease)=>lease,_=>panic!("fresh source build")};
        let artifacts=BlameArtifacts::new(&cache);let chunks=vec![artifacts.stage_chunk(&build,b"complete source-owned result").unwrap().unwrap()];
        assert!(artifacts.publish(&build,ArtifactKind::CompleteBody,&chunks).unwrap());drop(chunks);
        let body=artifacts.get_verified(&key,ArtifactKind::CompleteBody,source.clone()).unwrap().unwrap();
        git_rs::metadata::grant(&source.slug,"alice","bob","none",b"refs/").unwrap();
        let consumed=AtomicBool::new(false);
        assert!(body.with_chunks(|_|{consumed.store(true,Ordering::SeqCst);Ok(())}).is_err());assert!(!consumed.load(Ordering::SeqCst),"revocation before consumption must not invoke the success consumer");
        git_rs::metadata::grant(&source.slug,"alice","bob","reader",b"refs/").unwrap();
        let returned=body.with_chunks(|chunks| {
            assert_eq!(chunks,[b"complete source-owned result".as_slice()]);
            Admin::new().sql(&format!("UPDATE git_rs.repository_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE repo_id='{}'",source.repo));
            Ok(b"buffered, not exposed success".to_vec())
        });
        assert!(returned.is_err(),"source expiry inside the buffered consumer must reject its final result");
        assert!(artifacts.get_verified(&key,ArtifactKind::CompleteBody,source.clone()).is_err());
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn chunks_bind_security_options_inputs_and_producer_even_with_identical_content() {
    use git_rs::derived_cache::{ArtifactKey, KeyParts};
    Operation::new(45).enter(|| {
        let (cache,namespace)=cache();let artifacts=BlameArtifacts::new(&cache);
        let make=|dimension| ArtifactKey::new(KeyParts {repository:b"synthetic/scope",security:if dimension==1{b"private"}else{b"public"},object_format:"opaque",kind:"complete-body",inputs:if dimension==3{&[b"other-input"]}else{&[b"input"]},options:if dimension==2{b"different-options"}else{b"options"},representation:"v1",producer:if dimension==4{"producer-v2"}else{"producer-v1"}}).unwrap();
        let first_key=make(0);let first_build=build(&cache,&first_key);let first=vec![artifacts.stage_chunk(&first_build,b"identical complete bytes").unwrap().unwrap()];
        assert!(artifacts.publish(&first_build,ArtifactKind::CompleteBody,&first).unwrap());
        for dimension in 1..=4 {
            let other_key=make(dimension);let other_build=build(&cache,&other_key);
            assert!(artifacts.get_verified(&other_key,ArtifactKind::CompleteBody,authority()).unwrap().is_none());
            assert!(artifacts.publish(&other_build,ArtifactKind::CompleteBody,&first).is_err(),"foreign staged chunks must not cross full-key scope");
            let own=vec![artifacts.stage_chunk(&other_build,b"identical complete bytes").unwrap().unwrap()];
            assert!(artifacts.publish(&other_build,ArtifactKind::CompleteBody,&own).unwrap());
        }
        assert_eq!(Admin::new().scalar(&format!("SELECT count(DISTINCT child)::text FROM git_rs_cache.build_chunks WHERE namespace='{namespace}'")),"5","content-addressed physical chunk identity must include the full root scope");
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn malformed_roots_and_root_as_child_admission_are_bounded_and_never_serve() {
    Operation::new(30).enter(|| {
        let (cache,namespace)=cache();let artifacts=BlameArtifacts::new(&cache);
        let mut missing_descriptor=b"git-rs-complete-artifact\0\x01".to_vec();missing_descriptor.push(1);missing_descriptor.extend_from_slice(&0u32.to_be_bytes());
        let mut oversized=b"git-rs-complete-artifact\0\x01".to_vec();oversized.push(1);oversized.extend_from_slice(&u32::MAX.to_be_bytes());
        for (index,bytes) in [b"invalid root header".to_vec(),oversized,missing_descriptor].iter().enumerate() {
            let key=key(format!("malformed-{index}").as_bytes());
            let claim=match cache.get_or_claim(&key,authority()).unwrap(){git_rs::derived_cache::Lookup::Claimed(claim)=>claim,_=>panic!("fresh malformed fixture")};
            assert!(cache.publish(&claim,authority().as_ref(),bytes).unwrap());
            assert!(artifacts.get_verified(&key,ArtifactKind::CompleteBody,authority()).unwrap().is_none(),"malformed or non-atomic descriptors must not return a complete owner");
        }
        let root_key=key(b"root-not-child");let build=build(&cache,&root_key);
        assert!(artifacts.publish(&build,ArtifactKind::CompleteBody,&[]).unwrap());
        let admin=Admin::new();admin.sql("SET ROLE git_rs_cache");
        let hash=git_rs::protocol::hex(root_key.digest());
        let rejected=admin.scalar(&format!("SELECT git_rs_cache.pin_build_chunk('{namespace}',(SELECT owner FROM git_rs_cache.namespaces WHERE id='{namespace}'),decode('{hash}','hex'),b.token,b.generation,m.payload,p.digest,p.size)::text FROM git_rs_cache.blame_builds b JOIN git_rs_cache.manifest_roots m ON m.namespace=b.namespace JOIN git_rs_cache.payloads p ON p.namespace=m.namespace AND p.id=m.payload WHERE b.namespace='{namespace}' AND b.key_hash=decode('{hash}','hex')"));
        assert_eq!(rejected,"false","flat manifests must reject an actual live manifest root as a child");
        admin.sql("RESET ROLE");
        assert_eq!(cache.allocated(),0,"bounded malformed-root rejection must release admitted cache RAM");
    });
}

#[test]
#[ignore = "test-only IPC fixture; invoke through tests/blame_artifacts.py"]
fn artifact_process() {
    if std::env::var("GIT_RS_TEST_ARTIFACT_PROCESS").as_deref() == Ok("1") {
        artifact_fixture::run().unwrap();
    }
}
