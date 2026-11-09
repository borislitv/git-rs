#![forbid(unsafe_code)]

use git_rs::{
    Result,
    derived_cache::{
        Access, ArtifactKey, CacheConfig, CacheMaintenance, Coordinator, DerivedCache, KeyParts,
        Lookup,
    },
    invalid,
    process::Operation,
};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

fn id() -> String {
    let id = git_rs::scratch::identifier().unwrap();
    format!(
        "{}-{}-{}-{}-{}",
        &id[..8],
        &id[8..12],
        &id[12..16],
        &id[16..20],
        &id[20..]
    )
}

fn key(name: &str) -> ArtifactKey {
    ArtifactKey::new(KeyParts {
        repository: b"synthetic/repo",
        security: b"readers-v1",
        object_format: "opaque",
        kind: "fixture",
        inputs: &[name.as_bytes()],
        options: b"",
        representation: "raw-v1",
        producer: "fixture-v1",
    })
    .unwrap()
}

struct Authority {
    allowed: AtomicBool,
}
impl Access for Authority {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        if self.allowed.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(invalid("synthetic source hold expired"))
        }
    }
}
fn access() -> Arc<Authority> {
    Arc::new(Authority {
        allowed: AtomicBool::new(true),
    })
}

fn scoped_key(dimension: usize) -> ArtifactKey {
    ArtifactKey::new(KeyParts {
        repository: if dimension == 1 {
            b"other/repo"
        } else {
            b"repo"
        },
        security: if dimension == 2 {
            b"private"
        } else {
            b"public"
        },
        object_format: if dimension == 3 { "sha256" } else { "sha1" },
        kind: if dimension == 4 { "mapping" } else { "blame" },
        inputs: if dimension == 5 {
            &[b"changed"]
        } else {
            &[b"input"]
        },
        options: if dimension == 6 {
            b"ignore-space"
        } else {
            b"exact"
        },
        representation: if dimension == 7 { "raw-v2" } else { "raw-v1" },
        producer: if dimension == 8 {
            "engine-v2"
        } else {
            "engine-v1"
        },
    })
    .unwrap()
}

fn wait_for_idle(coordinator: &Coordinator) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while coordinator.active() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "actual-work permit was not released after stop"
        );
        std::thread::yield_now();
    }
}

fn config() -> CacheConfig {
    let mut c = CacheConfig::new(id(), id());
    c.max_payload_bytes = 1024 * 1024;
    c.inline_bytes = c.max_payload_bytes;
    c.memory_bytes = 8 * 1024 * 1024;
    c.grace_seconds = 1;
    c
}

fn cache(c: &CacheConfig) -> DerivedCache {
    let cache = DerivedCache::open(c.clone()).unwrap();
    cache.migrate().unwrap();
    cache
}

fn claim(
    cache: &DerivedCache,
    key: &ArtifactKey,
    access: Arc<dyn Access>,
) -> git_rs::derived_cache::Claim {
    match cache.get_or_claim(key, access).unwrap() {
        Lookup::Claimed(claim) => claim,
        _ => panic!("expected a fresh exclusive claim"),
    }
}

// Fault control only: the harness points this at a newly owned loopback fixture.
// Production cache SQL always uses its independent parameterized binary API.
fn fault(sql: &str) {
    let dsn = std::env::var("GIT_RS_TEST_CACHE_ADMIN_DSN").expect("owned cache fixture admin DSN");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        tokio::time::timeout(Duration::from_secs(5), client.batch_execute(sql))
            .await
            .unwrap()
            .unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
    });
}

#[test]
fn canonical_keys_bind_every_scope_and_preserve_field_boundaries() {
    let build = |index: usize| {
        ArtifactKey::new(KeyParts {
            repository: if index == 1 { b"another/repo" } else { b"repo" },
            security: if index == 2 { b"private" } else { b"public" },
            object_format: if index == 3 { "sha256" } else { "sha1" },
            kind: if index == 4 { "mapping" } else { "blame" },
            inputs: if index == 5 {
                &[b"blob", b"commit"]
            } else {
                &[b"commit", b"blob"]
            },
            options: if index == 6 {
                b"ignore-space"
            } else {
                b"exact"
            },
            representation: if index == 7 { "raw-v2" } else { "raw-v1" },
            producer: if index == 8 { "engine-v2" } else { "engine-v1" },
        })
        .unwrap()
    };
    for index in 1..=8 {
        assert_ne!(build(0), build(index), "key dimension {index}");
    }
    assert_eq!(build(0), build(0));
    assert_ne!(key("a\0bc"), key("ab\0c"));
    assert!(
        ArtifactKey::new(KeyParts {
            repository: b"repo",
            security: b"tenant",
            object_format: "opaque",
            kind: "test",
            inputs: &[b"input"],
            options: &vec![0; 8192],
            representation: "raw",
            producer: "v1"
        })
        .is_err()
    );
}

#[test]
fn invalid_limits_and_namespace_paths_are_rejected_before_io() {
    let base = config();
    base.validate().unwrap();
    for index in 0..8 {
        let mut c = base.clone();
        match index {
            0 => c.namespace = "../repos/source".into(),
            1 => c.owner = "".into(),
            2 => c.max_payload_bytes = 8 * 1024 * 1024 + 1,
            3 => c.memory_bytes = c.max_payload_bytes,
            4 => c.connections = 0,
            5 => c.job_seconds = c.lease_seconds - 1,
            6 => c.batch_rows = 101,
            _ => c.inline_bytes = c.max_payload_bytes + 1,
        }
        assert!(c.validate().is_err(), "invalid limit {index}");
    }
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn real_source_revocation_and_lease_expiry_reject_cached_bytes() {
    struct Source {
        hold: git_rs::metadata::Barrier,
        slug: String,
        expected: ArtifactKey,
    }
    impl Access for Source {
        fn check(&self, key: &ArtifactKey) -> Result<()> {
            if key != &self.expected {
                return Err(invalid("artifact outside authorized source"));
            }
            self.hold.check()?;
            git_rs::metadata::snapshot(&self.slug, "bob", false)?;
            Ok(())
        }
    }
    Operation::new(30).enter(|| {
        let c = config();
        let cache = cache(&c);
        let slug = format!("synthetic/source-{}", id());
        git_rs::metadata::create(&slug, "alice").unwrap();
        git_rs::metadata::grant(&slug, "alice", "bob", "reader", b"refs/").unwrap();
        let (snapshot, hold) = git_rs::metadata::leased_snapshot(&slug, "bob", false).unwrap();
        let key = key(&slug);
        let access = Arc::new(Source { hold, slug: slug.clone(), expected: key.clone() });
        let owner = claim(&cache, &key, access.clone());
        assert!(cache.publish(&owner, access.as_ref(), b"authorized").unwrap());
        let payload = cache.get_verified(&key, access.clone()).unwrap().unwrap();
        git_rs::metadata::grant(&slug, "alice", "bob", "none", b"refs/").unwrap();
        assert!(payload.with_bytes(|_| Ok(())).is_err(), "ACL revocation rejects already cached bytes");
        assert!(cache.get_verified(&key, access.clone()).is_err());
        git_rs::metadata::grant(&slug, "alice", "bob", "reader", b"refs/").unwrap();
        payload.with_bytes(|bytes| { assert_eq!(bytes, b"authorized"); Ok(()) }).unwrap();
        fault(&format!("UPDATE git_rs.repository_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE repo_id='{}'", snapshot.id));
        assert!(payload.with_bytes(|_| Ok(())).is_err(), "a cache read hold cannot extend an expired source lease");
        assert!(cache.get_verified(&key, access).is_err());
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn payload_and_namespace_quotas_leave_source_operations_usable() {
    Operation::new(30).enter(|| {
        let mut c = config();
        c.max_payload_bytes = 32768;
        c.inline_bytes = 32768;
        c.namespace_bytes = 32768;
        let cache = cache(&c);
        let access = access();
        let first = key("first");
        let owner = claim(&cache, &first, access.clone());
        assert!(
            cache
                .publish(&owner, access.as_ref(), &vec![1; 32769])
                .is_err()
        );
        assert!(
            cache
                .publish(&owner, access.as_ref(), &vec![1; 32768])
                .unwrap()
        );
        let next = claim(&cache, &key("next"), access.clone());
        assert!(
            cache
                .publish(&next, access.as_ref(), b"over quota")
                .is_err()
        );
        cache
            .get_verified(&first, access)
            .unwrap()
            .unwrap()
            .with_bytes(|bytes| {
                assert_eq!(bytes, vec![1; 32768]);
                Ok(())
            })
            .unwrap();
        let slug = format!("synthetic/cache-quota-{}", id());
        git_rs::metadata::create(&slug, "alice").unwrap();
        assert!(
            git_rs::metadata::snapshot(&slug, "alice", false)
                .unwrap()
                .refs
                .is_empty()
        );
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn binary_payload_reuses_across_independent_pools_and_never_bypasses_authority() {
    Operation::new(30).enter(|| {
        let c = config();
        let first = cache(&c);
        let second = cache(&c);
        let access = access();
        let key = key("binary");
        let owner = claim(&first, &key, access.clone());
        assert!(matches!(
            second.get_or_claim(&key, access.clone()).unwrap(),
            Lookup::Pending
        ));
        let bytes: Vec<_> = (0..65536).map(|index| (index % 251) as u8).collect();
        assert!(first.publish(&owner, access.as_ref(), &bytes).unwrap());
        let payload = second.get_verified(&key, access.clone()).unwrap().unwrap();
        payload
            .with_bytes(|actual| {
                assert_eq!(actual, bytes);
                Ok(())
            })
            .unwrap();
        assert!(payload.identity().is_some());
        assert!(
            second.renew(&owner).is_err(),
            "a claim is bound to its configured pool"
        );
        access.allowed.store(false, Ordering::Release);
        assert!(payload.with_bytes(|_| Ok(())).is_err());
        assert!(second.get_verified(&key, access).is_err());
        assert!(
            git_rs::metadata::sql("SELECT repeat('x',8193)").is_err(),
            "source metadata cap must remain intact"
        );
        drop(payload);
        assert_eq!(
            second.allocated(),
            0,
            "last reader releases its RAM reservation"
        );
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn simultaneous_claims_have_one_live_owner_and_expired_tokens_cannot_return() {
    Operation::new(30).enter(|| {
        let c = config();
        let cache = Arc::new(cache(&c));
        let key = key("race");
        let access = access();
        let barrier = Arc::new(Barrier::new(9));
        let owners = std::thread::scope(|scope| {
            let mut threads = Vec::new();
            for _ in 0..8 {
                let (cache, key, access, barrier) = (cache.clone(), key.clone(), access.clone(), barrier.clone());
                threads.push(scope.spawn(move || Operation::new(15).enter(|| {
                    barrier.wait();
                    match cache.get_or_claim(&key, access).unwrap() { Lookup::Claimed(c) => Some(c), Lookup::Pending => None, _ => panic!("unpublished hit") }
                })));
            }
            barrier.wait();
            threads.into_iter().filter_map(|t| t.join().unwrap()).collect::<Vec<_>>()
        });
        assert_eq!(owners.len(), 1, "exactly one live healthy owner");
        fault(&format!("UPDATE git_rs_cache.jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{}'", c.namespace));
        let replacement = claim(&cache, &key, access.clone());
        assert!(!cache.publish(&owners[0], access.as_ref(), b"stale").unwrap(), "stale ownership must never publish after takeover");
        assert!(!cache.renew(&owners[0]).unwrap(), "expired generation must never revive");
        assert!(cache.publish(&replacement, access.as_ref(), b"replacement").unwrap());
        cache.get_verified(&key, access).unwrap().unwrap().with_bytes(|bytes| { assert_eq!(bytes, b"replacement"); Ok(()) }).unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn corrupt_inline_payload_is_a_miss_and_replacement_is_verified() {
    Operation::new(30).enter(|| {
        let c = config();
        let cache = cache(&c);
        let key = key("corrupt");
        let access = access();
        let owner = claim(&cache, &key, access.clone());
        assert!(cache.publish(&owner, access.as_ref(), b"valid").unwrap());
        fault(&format!(
            "UPDATE git_rs_cache.payloads SET body=convert_to('wrong','UTF8') WHERE namespace='{}'",
            c.namespace
        ));
        assert!(
            cache.get_verified(&key, access.clone()).unwrap().is_none(),
            "unverified bytes must not escape"
        );
        let owner = claim(&cache, &key, access.clone());
        assert!(cache.publish(&owner, access.as_ref(), b"fresh").unwrap());
        cache
            .get_verified(&key, access)
            .unwrap()
            .unwrap()
            .with_bytes(|bytes| {
                assert_eq!(bytes, b"fresh");
                Ok(())
            })
            .unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn retirement_preserves_an_admitted_reader_and_expired_hold_cannot_consume() {
    Operation::new(30).enter(|| {
        let c = config();
        let cache = cache(&c);
        let key = key("held");
        let access = access();
        let owner = claim(&cache, &key, access.clone());
        assert!(cache.publish(&owner, access.as_ref(), b"protected").unwrap());
        let payload = cache.get_verified(&key, access.clone()).unwrap().unwrap();
        assert!(cache.invalidate(&key, payload.identity().unwrap()).unwrap());
        let maintenance = CacheMaintenance::new(&cache);
        let lease = maintenance.claim().unwrap().unwrap();
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(), 1);
        fault(&format!("UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='{}'", c.namespace));
        assert_eq!(maintenance.delete_retired(&lease).unwrap(), 0, "retirement cannot delete a reader-held payload");
        payload.with_bytes(|bytes| { assert_eq!(bytes, b"protected"); Ok(()) }).unwrap();
        assert!(cache.get_verified(&key, access).unwrap().is_none(), "retirement closes new reader admission");
        fault(&format!("UPDATE git_rs_cache.read_holds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{}'", c.namespace));
        assert!(payload.with_bytes(|_| Ok(())).is_err(), "expired reader must not consume retained RAM");
        assert_eq!(maintenance.delete_retired(&lease).unwrap(), 1);
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn cancelled_waiter_keeps_actual_work_admitted_and_second_waiter_receives_result() {
    let c = config();
    let cache = Operation::new(15).enter(|| cache(&c));
    let coordinator = Arc::new(Coordinator::new(cache, 1).unwrap());
    let key = key("coalesced");
    let access = access();
    let cancelled = Operation::new(15);
    let (started, begin) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let first = {
            let coordinator = coordinator.clone();
            let key = key.clone();
            let access = access.clone();
            let calls = calls.clone();
            let cancelled = cancelled.clone();
            scope.spawn(move || {
                cancelled.enter(|| {
                    coordinator.get_or_compute(key, access, move |context| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.send(()).unwrap();
                        loop {
                            context.check()?;
                            match gate.recv_timeout(Duration::from_millis(10)) {
                                Ok(()) => break,
                                Err(mpsc::RecvTimeoutError::Timeout) => {}
                                Err(_) => return Err(invalid("fixture gate closed")),
                            }
                        }
                        Ok(b"shared answer".to_vec())
                    })
                })
            })
        };
        begin.recv_timeout(Duration::from_secs(5)).unwrap();
        cancelled.cancel();
        assert!(first.join().unwrap().is_err());
        assert_eq!(
            coordinator.active(),
            1,
            "waiter drop must not release a running computation's permit"
        );
        let rejected_called = Arc::new(AtomicBool::new(false));
        let attempted = rejected_called.clone();
        let overload = Operation::new(5)
            .enter(|| {
                coordinator.get_or_compute(super_key(), access.clone(), move |_| {
                    attempted.store(true, Ordering::Release);
                    Ok(vec![])
                })
            })
            .err()
            .expect("a distinct job must be rejected while actual capacity is occupied");
        assert!(overload.to_string().contains("admission exhausted"));
        assert!(
            !rejected_called.load(Ordering::Acquire),
            "overloaded work must not start"
        );
        let second = {
            let coordinator = coordinator.clone();
            let key = key.clone();
            let access = access.clone();
            scope.spawn(move || {
                Operation::new(15).enter(|| {
                    coordinator.get_or_compute(key, access, |_| {
                        panic!("shared derivation must not run twice")
                    })
                })
            })
        };
        release.send(()).unwrap();
        second
            .join()
            .unwrap()
            .unwrap()
            .with_bytes(|bytes| {
                assert_eq!(bytes, b"shared answer");
                Ok(())
            })
            .unwrap();
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    fn super_key() -> ArtifactKey {
        crate::key("other-admission")
    }
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn cached_lookup_isolates_all_key_dimensions_and_stale_invalidation_cannot_remove_replacement() {
    Operation::new(30).enter(|| {
        let c = config();
        let cache = cache(&c);
        let access = access();
        let original = scoped_key(0);
        let owner = claim(&cache, &original, access.clone());
        assert!(cache.publish(&owner, access.as_ref(), b"original").unwrap());
        for dimension in 1..=8 {
            assert!(
                cache
                    .get_verified(&scoped_key(dimension), access.clone())
                    .unwrap()
                    .is_none(),
                "cached bytes crossed key dimension {dimension}"
            );
        }
        let other_namespace = crate::cache(&config());
        assert!(
            other_namespace
                .get_verified(&original, access.clone())
                .unwrap()
                .is_none(),
            "another namespace must not see cached bytes"
        );
        let mut wrong_owner = c.clone();
        wrong_owner.owner = id();
        let unowned = DerivedCache::open(wrong_owner).unwrap();
        assert!(
            unowned.get_verified(&original, access.clone()).is_err(),
            "namespace identity alone does not grant ownership"
        );
        let maintenance = CacheMaintenance::new(&cache);
        let lease = maintenance.claim().unwrap().unwrap();
        assert!(
            CacheMaintenance::new(&other_namespace)
                .delete_retired(&lease)
                .is_err(),
            "a maintenance token cannot cross namespace/pool ownership"
        );
        let old = cache
            .get_verified(&original, access.clone())
            .unwrap()
            .unwrap();
        let old_id = old.identity().unwrap();
        assert!(cache.invalidate(&original, old_id).unwrap());
        let replacement = claim(&cache, &original, access.clone());
        assert!(
            cache
                .publish(&replacement, access.as_ref(), b"replacement")
                .unwrap()
        );
        assert!(
            !cache.invalidate(&original, old_id).unwrap(),
            "old reader must not invalidate a newer payload (ABA)"
        );
        cache
            .get_verified(&original, access)
            .unwrap()
            .unwrap()
            .with_bytes(|bytes| {
                assert_eq!(bytes, b"replacement");
                Ok(())
            })
            .unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn last_cancelled_waiter_retains_source_until_cooperative_cpu_deadline_and_cleanup() {
    struct Source(mpsc::Sender<()>);
    impl Access for Source {
        fn check(&self, _: &ArtifactKey) -> Result<()> {
            Ok(())
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    struct Stopped(mpsc::Sender<()>);
    impl Drop for Stopped {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let mut c = config();
    c.lease_seconds = 1;
    c.job_seconds = 2;
    let cache = Operation::new(15).enter(|| cache(&c));
    let observer = cache.clone();
    let coordinator = Arc::new(Coordinator::new(cache, 1).unwrap());
    let (released, source_released) = mpsc::channel();
    let source = Arc::new(Source(released));
    let weak = Arc::downgrade(&source);
    let operation = Operation::new(10);
    let (started, begin) = mpsc::channel();
    let (stopped, cpu_stopped) = mpsc::channel();
    let worker = {
        let coordinator = coordinator.clone();
        let operation = operation.clone();
        std::thread::spawn(move || {
            operation.enter(|| {
                coordinator.get_or_compute(key("last-waiter"), source, move |context| {
                    let _stopped = Stopped(stopped);
                    started.send(()).unwrap();
                    loop {
                        context.check()?;
                        for value in 0..1024 {
                            std::hint::black_box(value * value);
                        }
                    }
                })
            })
        })
    };
    begin.recv_timeout(Duration::from_secs(5)).unwrap();
    operation.cancel();
    assert!(worker.join().unwrap().is_err());
    assert_eq!(
        coordinator.active(),
        1,
        "last waiter must not release actual CPU capacity"
    );
    assert!(
        weak.upgrade().is_some(),
        "source ownership must survive the last waiter"
    );
    assert!(matches!(
        source_released.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    cpu_stopped
        .recv_timeout(Duration::from_secs(5))
        .expect("cooperative work did not stop at its finite job deadline");
    source_released
        .recv_timeout(Duration::from_secs(5))
        .expect("source hold retained after actual work stopped");
    wait_for_idle(&coordinator);
    Operation::new(10).enter(|| {
        assert!(
            observer
                .get_verified(&key("last-waiter"), access())
                .unwrap()
                .is_none(),
            "deadline must not publish partial work"
        );
        coordinator
            .get_or_compute(key("after-deadline"), access(), |_| Ok(b"healthy".to_vec()))
            .unwrap()
            .with_bytes(|bytes| {
                assert_eq!(bytes, b"healthy");
                Ok(())
            })
            .unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn panic_oversized_result_and_revoked_source_do_not_publish_or_poison_admission() {
    let c = config();
    let cache = Operation::new(15).enter(|| cache(&c));
    let coordinator = Arc::new(Coordinator::new(cache.clone(), 1).unwrap());
    Operation::new(30).enter(|| {
        let error = coordinator
            .get_or_compute(key("panic"), access(), |_| {
                panic!("synthetic callback panic")
            })
            .err()
            .expect("panic must become a computation error");
        assert!(error.to_string().contains("panicked"));
        wait_for_idle(&coordinator);
        let error = coordinator
            .get_or_compute(key("oversize"), access(), |context| {
                Ok(vec![0; context.max_bytes() + 1])
            })
            .err()
            .expect("raw output limit must be enforced independently of callback cooperation");
        assert!(error.to_string().contains("limit"));
        wait_for_idle(&coordinator);
    });
    let authority = access();
    let (started, begin) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    std::thread::scope(|scope| {
        let coordinator = coordinator.clone();
        let source = authority.clone();
        let worker = scope.spawn(move || {
            Operation::new(10).enter(|| {
                coordinator.get_or_compute(key("revoked-during-compute"), source, move |_| {
                    started.send(()).unwrap();
                    gate.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(b"no longer authorized".to_vec())
                })
            })
        });
        begin.recv_timeout(Duration::from_secs(5)).unwrap();
        authority.allowed.store(false, Ordering::Release);
        release.send(()).unwrap();
        assert!(worker.join().unwrap().is_err());
    });
    wait_for_idle(&coordinator);
    Operation::new(15).enter(|| {
        for name in ["panic", "oversize", "revoked-during-compute"] {
            assert!(
                cache.get_verified(&key(name), access()).unwrap().is_none(),
                "failed {name} published cached bytes"
            );
        }
        coordinator
            .get_or_compute(key("healthy-after-failure"), access(), |_| {
                Ok(b"healthy".to_vec())
            })
            .unwrap()
            .with_bytes(|bytes| {
                assert_eq!(bytes, b"healthy");
                Ok(())
            })
            .unwrap();
    });
}
