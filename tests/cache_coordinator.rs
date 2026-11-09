#![forbid(unsafe_code)]

//! Coordinator lock contracts against owned PostgreSQL fixtures, not mocked SQL.
use git_rs::{
    Result,
    derived_cache::{
        Access, ArtifactKey, CacheConfig, Coordinator, Derivation, DerivedCache, KeyParts, Lookup,
        Payload,
    },
    invalid,
    process::Operation,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    time::{Duration, Instant},
};

struct Admin {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    runtime: tokio::runtime::Runtime,
}

impl Admin {
    fn connect() -> Self {
        let dsn =
            std::env::var("GIT_RS_TEST_CACHE_ADMIN_DSN").expect("owned loopback fixture admin DSN");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (client, driver) = runtime.block_on(async {
            let (client, connection) = tokio::time::timeout(
                Duration::from_secs(5),
                tokio_postgres::connect(&dsn, tokio_postgres::NoTls),
            )
            .await
            .unwrap()
            .unwrap();
            (client, tokio::spawn(connection))
        });
        Self {
            client,
            driver,
            runtime,
        }
    }

    fn sql(&self, sql: &str) {
        self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), self.client.batch_execute(sql))
                .await
                .unwrap()
                .unwrap();
        });
    }

    fn is_waiting(&self, application: &str, query: &str) -> bool {
        self.runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(1),
                    self.client.query_one(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name=$1 AND wait_event_type='Lock' AND query LIKE $2)",
                        &[&application, &format!("%{query}%")],
                    ),
                )
                .await
                .unwrap()
                .unwrap()
                .get::<_, bool>(0)
        })
    }

    fn wait_for_lock(&self, application: &str, query: &str) {
        let observed = until(Duration::from_secs(3), || {
            self.is_waiting(application, query)
        });
        assert!(observed, "expected actual PostgreSQL lock wait for {query}");
    }
}

impl Drop for Admin {
    fn drop(&mut self) {
        // Closing the owned session also rolls back any lock transaction on panic.
        self.driver.abort();
    }
}

fn until(timeout: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        // Poll observable public state with a deadline, not a timing-based gate.
        std::thread::sleep(Duration::from_millis(2));
    }
}

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
        repository: b"synthetic/coordinator",
        security: b"fixture-readers-v1",
        object_format: "opaque",
        kind: "fixture",
        inputs: &[name.as_bytes()],
        options: b"",
        representation: "raw-v1",
        producer: "fixture-v1",
    })
    .unwrap()
}

fn cache() -> (DerivedCache, String) {
    let mut config = CacheConfig::new(id(), id());
    config.max_payload_bytes = 65536;
    config.inline_bytes = 65536;
    config.memory_bytes = 1024 * 1024;
    let namespace = config.namespace.clone();
    let cache = DerivedCache::open(config).unwrap();
    Operation::new(15).enter(|| cache.migrate()).unwrap();
    (cache, namespace)
}

struct Authority;
impl Access for Authority {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        Ok(())
    }
}

fn finish_on_signal(context: &Derivation<'_>, finish: Receiver<()>) -> Result<Vec<u8>> {
    loop {
        context.check()?;
        match finish.recv_timeout(Duration::from_millis(10)) {
            Ok(()) => return Ok(b"derived".to_vec()),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => return Err(invalid("fixture computation gate closed")),
        }
    }
}

fn verify(payload: Payload, expected: &[u8]) -> Result<()> {
    Operation::new(5).enter(|| {
        payload.with_bytes(|bytes| {
            if bytes != expected {
                return Err(invalid("unexpected synthetic artifact bytes"));
            }
            Ok(())
        })
    })
}

struct Source {
    hold: git_rs::metadata::Barrier,
    initial: Mutex<Option<(Sender<()>, Receiver<()>)>>,
}
impl Access for Source {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        // This is the real source lease check. The fixture blocks its SQL using
        // an actual table lock after the first, pre-flight check has succeeded.
        self.hold.check()?;
        let initial = self.initial.lock().unwrap().take();
        if let Some((checked, proceed)) = initial {
            checked.send(())?;
            proceed.recv_timeout(Duration::from_secs(5))?;
        }
        Ok(())
    }
}

#[test]
#[ignore = "requires the owned source/cache PostgreSQL fixture"]
fn slow_source_check_does_not_pin_result_publication_or_unrelated_cancellation() {
    let admin = Admin::connect();
    let locker = Admin::connect();
    let (cache, _) = cache();
    let coordinator = Arc::new(Coordinator::new(cache, 2).unwrap());
    let (started, running) = mpsc::channel();
    let (finish, finished) = mpsc::channel();
    let (produced, production) = mpsc::channel();
    let producer = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            let result = Operation::new(20).enter(|| {
                coordinator.get_or_compute(
                    key("source-gated"),
                    Arc::new(Authority),
                    move |context| {
                        started.send(())?;
                        finish_on_signal(context, finished)
                    },
                )
            });
            produced
                .send(result.and_then(|payload| verify(payload, b"derived")))
                .unwrap();
        })
    };
    running.recv_timeout(Duration::from_secs(5)).unwrap();

    let slug = format!("synthetic/coordinator-{}", id());
    let hold = Operation::new(20).enter(|| {
        git_rs::metadata::create(&slug, "alice").unwrap();
        git_rs::metadata::leased_snapshot(&slug, "alice", false)
            .unwrap()
            .1
    });
    let (checked, initial) = mpsc::channel();
    let (proceed, continued) = mpsc::channel();
    let source = Arc::new(Source {
        hold,
        initial: Mutex::new(Some((checked, continued))),
    });
    let waiter = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            Operation::new(20)
                .enter(|| {
                    coordinator.get_or_compute(key("source-gated"), source, |_| {
                        panic!("an existing flight must not derive twice")
                    })
                })
                .and_then(|payload| verify(payload, b"derived"))
        })
    };
    initial.recv_timeout(Duration::from_secs(5)).unwrap();
    locker.sql("BEGIN; LOCK TABLE git_rs.repository_leases IN ACCESS EXCLUSIVE MODE;");
    proceed.send(()).unwrap();
    admin.wait_for_lock("git-rs", "repository_leases");
    finish.send(()).unwrap();

    let released = until(Duration::from_millis(300), || coordinator.active() == 0);
    let original_result = production.recv_timeout(Duration::from_millis(100));
    let cancellation = Operation::new(15);
    let (other_started, other_running) = mpsc::channel();
    let (other_finish, other_finished) = mpsc::channel();
    let (returned, response) = mpsc::channel();
    let other = {
        let coordinator = coordinator.clone();
        let cancellation = cancellation.clone();
        std::thread::spawn(move || {
            let result = cancellation.enter(|| {
                coordinator.get_or_compute(key("unrelated"), Arc::new(Authority), move |context| {
                    other_started.send(())?;
                    finish_on_signal(context, other_finished)
                })
            });
            returned.send(result.is_err()).unwrap();
        })
    };
    let unrelated_started = other_running
        .recv_timeout(Duration::from_millis(200))
        .is_ok();
    cancellation.cancel();
    let cancelled_promptly = response.recv_timeout(Duration::from_millis(200)) == Ok(true);
    let independent_permit = coordinator.active();
    let source_still_blocked = admin.is_waiting("git-rs", "repository_leases");

    // Release all real I/O and compute gates before checking assertions/joining.
    locker.sql("ROLLBACK");
    let _ = other_finish.send(());
    producer.join().unwrap();
    waiter.join().unwrap().unwrap();
    other.join().unwrap();
    assert!(
        source_still_blocked,
        "source SQL must remain blocked through the cancellation observation"
    );
    assert!(
        released,
        "a slow source check must not pin a completed job's permit"
    );
    original_result.unwrap().unwrap();
    assert!(
        unrelated_started,
        "source authorization must not hold the flights/result locks"
    );
    assert!(
        cancelled_promptly,
        "unrelated cancellation must finish while source SQL remains blocked"
    );
    assert_eq!(
        independent_permit, 1,
        "cancelled waiter must retain actual-work admission"
    );
    assert!(until(Duration::from_secs(3), || coordinator.active() == 0));
}

struct ObservedAuthority {
    checks: AtomicUsize,
    waiting: Sender<()>,
}
impl Access for ObservedAuthority {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        if self.checks.fetch_add(1, Ordering::SeqCst) == 1 {
            self.waiting.send(())?;
        }
        Ok(())
    }
}

#[test]
#[ignore = "requires the owned source/cache PostgreSQL fixture"]
fn blocked_read_hold_cleanup_does_not_lock_out_another_flight_waiter() {
    let admin = Admin::connect();
    let locker = Admin::connect();
    let (cache, namespace) = cache();
    Operation::new(15).enter(|| {
        let claim = match cache
            .get_or_claim(&key("cached"), Arc::new(Authority))
            .unwrap()
        {
            Lookup::Claimed(claim) => claim,
            _ => panic!("fresh fixture key must be claimable"),
        };
        assert!(cache.publish(&claim, &Authority, b"cached bytes").unwrap());
    });
    let coordinator = Arc::new(Coordinator::new(cache, 2).unwrap());
    let (started, running) = mpsc::channel();
    let (finish, finished) = mpsc::channel();
    let keeper = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            Operation::new(20)
                .enter(|| {
                    coordinator.get_or_compute(
                        key("running"),
                        Arc::new(Authority),
                        move |context| {
                            started.send(())?;
                            finish_on_signal(context, finished)
                        },
                    )
                })
                .and_then(|payload| verify(payload, b"derived"))
        })
    };
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    let cached = Operation::new(15)
        .enter(|| {
            coordinator.get_or_compute(key("cached"), Arc::new(Authority), |_| {
                panic!("verified hit must not derive")
            })
        })
        .unwrap();
    assert!(cached.identity().is_some());
    verify(cached, b"cached bytes").unwrap();
    assert!(until(Duration::from_secs(3), || coordinator.active() == 1));

    locker.sql(&format!(
        "BEGIN; SELECT id FROM git_rs_cache.namespaces WHERE id='{namespace}' FOR UPDATE;"
    ));
    let cleanup = {
        let coordinator = coordinator.clone();
        std::thread::spawn(move || {
            Operation::new(15)
                .enter(|| {
                    coordinator.get_or_compute(key("prune"), Arc::new(Authority), |_| {
                        Ok(b"pruned".to_vec())
                    })
                })
                .and_then(|payload| verify(payload, b"pruned"))
        })
    };
    admin.wait_for_lock("git-rs-cache", "release_read");
    let cancellation = Operation::new(15);
    let (waiting, attached) = mpsc::channel();
    let (returned, response) = mpsc::channel();
    let waiter = {
        let coordinator = coordinator.clone();
        let cancellation = cancellation.clone();
        std::thread::spawn(move || {
            let authority = Arc::new(ObservedAuthority {
                checks: AtomicUsize::new(0),
                waiting,
            });
            let result = cancellation.enter(|| {
                coordinator.get_or_compute(key("running"), authority, |_| {
                    panic!("joining an existing job must not derive twice")
                })
            });
            returned.send(result.is_err()).unwrap();
        })
    };
    let joined_while_cleanup_blocked = attached.recv_timeout(Duration::from_millis(200)).is_ok();
    cancellation.cancel();
    let cancelled_promptly = response.recv_timeout(Duration::from_millis(200)) == Ok(true);
    let still_running = coordinator.active();
    let cleanup_still_blocked = admin.is_waiting("git-rs-cache", "release_read");
    locker.sql("ROLLBACK");
    finish.send(()).unwrap();
    keeper.join().unwrap().unwrap();
    cleanup.join().unwrap().unwrap();
    waiter.join().unwrap();
    assert!(
        cleanup_still_blocked,
        "read-hold cleanup must remain blocked through the cancellation observation"
    );
    assert!(
        joined_while_cleanup_blocked,
        "read-hold destruction must happen outside the global flights lock"
    );
    assert!(
        cancelled_promptly,
        "cancellation must not wait for another flight's read-hold cleanup"
    );
    assert_eq!(
        still_running, 1,
        "the cancelled waiter must not release its running job's permit"
    );
    assert!(until(Duration::from_secs(3), || coordinator.active() == 0));
}
