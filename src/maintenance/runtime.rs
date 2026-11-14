use super::{
    Outcome, compaction, gc,
    leader::{ElectionView, LeaderConfig, LeaderSession},
    queue::{self, Job},
};
use crate::{
    Result, blob::Store, cache::Cache, cpu_jobs::CpuJobs, invalid, metadata, metrics, process,
};
use std::{
    fs,
    io::{BufReader, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

pub struct Options {
    pub scratch: PathBuf,
    pub health: SocketAddr,
    pub once: bool,
    pub wait_seconds: u64,
}

struct Config {
    leader: LeaderConfig,
    job_lease: u64,
    job_seconds: u64,
    scan_batch: usize,
    scan_seconds: u64,
    scan_millis: u64,
    delete_batch: usize,
    cache_bytes: usize,
    shutdown_seconds: u64,
}

fn setting(name: &str, default: u64, min: u64, max: u64) -> Result<u64> {
    let value = match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| invalid(&format!("{name} must be an unsigned integer")))?,
        Err(std::env::VarError::NotPresent) => default,
        Err(_) => return Err(invalid(&format!("{name} must be UTF-8"))),
    };
    if value < min || value > max {
        return Err(invalid(&format!("{name} is outside its supported range")));
    }
    Ok(value)
}

impl Config {
    fn from_env() -> Result<Self> {
        let config = Self {
            leader: LeaderConfig::from_env()?,
            job_lease: setting("GIT_RS_MAINTENANCE_JOB_LEASE_SECONDS", 20, 10, 120)?,
            job_seconds: setting(
                "GIT_RS_MAINTENANCE_JOB_SECONDS",
                120,
                1,
                process::MAX_OPERATION_SECONDS,
            )?,
            scan_batch: setting("GIT_RS_MAINTENANCE_SCAN_BATCH", 100, 1, 1000)? as usize,
            scan_seconds: setting("GIT_RS_MAINTENANCE_SCAN_SECONDS", 60, 1, 3600)?,
            scan_millis: setting("GIT_RS_MAINTENANCE_SCAN_BUDGET_MILLIS", 1000, 1, 1000)?,
            delete_batch: setting("GIT_RS_MAINTENANCE_DELETE_BATCH", 100, 1, 1000)? as usize,
            cache_bytes: setting(
                "GIT_RS_CACHE_BYTES",
                512 * 1024 * 1024,
                0,
                512 * 1024 * 1024,
            )? as usize,
            shutdown_seconds: setting("GIT_RS_MAINTENANCE_SHUTDOWN_SECONDS", 90, 60, 120)?,
        };
        setting("GIT_RS_MAINTENANCE_CONCURRENCY", 1, 1, 1)?;
        if config.leader.heartbeat_seconds * 2 + 5 + 2 >= config.job_lease {
            return Err(invalid(
                "maintenance job lease is too short for its heartbeat margin",
            ));
        }
        Ok(config)
    }
}

struct Status {
    election: ElectionView,
    primary_until: Mutex<Option<Instant>>,
    running: AtomicBool,
    completed: AtomicU64,
    failed: AtomicU64,
}

impl Status {
    fn primary(&self, available: bool, seconds: u64) {
        *self
            .primary_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            available.then(|| Instant::now() + Duration::from_secs(seconds));
    }
    fn ready(&self) -> bool {
        self.election.eligible()
            && self
                .primary_until
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some_and(|until| Instant::now() < until)
    }
}

struct Health {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Health {
    fn start(address: SocketAddr, status: Arc<Status>, cache: Arc<Cache>) -> Result<Self> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let ending = stop.clone();
        let thread = std::thread::Builder::new()
            .name("git-rs-maintenance-health".into())
            .spawn(move || {
                let admission = process::Admission::new(8);
                let mut requests: Vec<JoinHandle<()>> = Vec::with_capacity(8);
                while !ending.load(Ordering::Acquire) {
                    for index in (0..requests.len()).rev() {
                        if requests[index].is_finished() {
                            let _joined = requests.swap_remove(index).join();
                        }
                    }
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let Some(permit) = admission.try_admit() else {
                                continue;
                            };
                            let (status, cache) = (status.clone(), cache.clone());
                            match std::thread::Builder::new()
                                .name("git-rs-maintenance-probe".into())
                                .spawn(move || {
                                    let _permit = permit;
                                    let _response = probe(stream, &status, &cache);
                                }) {
                                Ok(thread) => requests.push(thread),
                                Err(_) => break,
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(25))
                        }
                        Err(_) => break,
                    }
                }
                for request in requests {
                    let _joined = request.join();
                }
            })?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Health {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

fn probe(mut stream: TcpStream, status: &Status, cache: &Cache) -> Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    let interrupt = stream.try_clone()?;
    process::Operation::new(1).with_interrupt(|| { let _closed = interrupt.shutdown(std::net::Shutdown::Both); }, || {
        let path = crate::health::path(&mut BufReader::new(&mut stream))?;
        let (code, body) = match path.as_str() {
            "/livez" => ("200 OK", "live\n".into()),
            "/readyz" if status.ready() => ("200 OK", "eligible maintenance standby\n".into()),
            "/readyz" => ("503 Service Unavailable", "maintenance unavailable\n".into()),
            "/leaderz" if status.election.fence().is_some() => ("200 OK", "authoritative maintenance leader\n".into()),
            "/leaderz" => ("503 Service Unavailable", "no maintenance authority\n".into()),
            "/metrics" => {
                let mut body = metrics::global().render(usize::from(status.running.load(Ordering::Acquire)), cache.allocated());
                use std::fmt::Write;
                writeln!(body, "git_rs_maintenance_leader {}", usize::from(status.election.fence().is_some()))?;
                writeln!(body, "git_rs_maintenance_jobs_completed_total {}", status.completed.load(Ordering::Acquire))?;
                writeln!(body, "git_rs_maintenance_jobs_failed_total {}", status.failed.load(Ordering::Acquire))?;
                ("200 OK", body)
            }
            _ => ("404 Not Found", String::new()),
        };
        write!(stream,"HTTP/1.1 {code}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len())?;
        stream.shutdown(std::net::Shutdown::Write)?;
        Ok(())
    })
}

/// Entry-point watchdog, not a detached worker. A non-cooperative dependency
/// cannot make process shutdown indefinitely retain authority or admission. A
/// forced exit intentionally leaves durable intents/leases for safe recovery.
struct Shutdown {
    done: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Shutdown {
    fn new(election: ElectionView, seconds: u64) -> Result<Self> {
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let completion = done.clone();
        let thread = std::thread::Builder::new()
            .name("git-rs-maintenance-shutdown".into())
            .spawn(move || {
                let mut deadline = None;
                let (finished, changed) = &*completion;
                loop {
                    let lock = finished
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if *lock {
                        return;
                    }
                    if election.stopping() {
                        let until = *deadline
                            .get_or_insert_with(|| Instant::now() + Duration::from_secs(seconds));
                        if Instant::now() >= until {
                            drop(lock);
                            // No further destructors execute. PostgreSQL expiry,
                            // permanent tombstones and journals own crash recovery.
                            std::process::exit(1);
                        }
                    }
                    drop(
                        changed
                            .wait_timeout(lock, Duration::from_millis(50))
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                    );
                }
            })?;
        Ok(Self {
            done,
            thread: Some(thread),
        })
    }
}
impl Drop for Shutdown {
    fn drop(&mut self) {
        let (finished, changed) = &*self.done;
        *finished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_one();
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

struct Worker {
    job: Arc<Job>,
    operation: Arc<process::Operation>,
    thread: Option<JoinHandle<Result<Outcome>>>,
}
impl Worker {
    fn join(&mut self) -> Result<Outcome> {
        self.thread
            .take()
            .ok_or_else(|| invalid("maintenance worker already joined"))?
            .join()
            .map_err(|_| invalid("maintenance worker panicked"))?
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.operation.cancel();
        if let Some(thread) = self.thread.take() {
            let _joined = thread.join();
        }
    }
}

struct Runtime {
    worker: Option<Worker>,
    _health: Health,
    session: LeaderSession,
    cpu: Arc<CpuJobs>,
    store: Store,
    cache: Arc<Cache>,
    config: Config,
    status: Arc<Status>,
    // Declared last: all work, source holds, listeners, election and CPU runtime
    // must finish destruction before the watchdog is acknowledged.
    _shutdown: Shutdown,
}

impl Runtime {
    fn finish_worker(&mut self) -> Result<bool> {
        if !self
            .worker
            .as_ref()
            .is_some_and(|worker| worker.thread.as_ref().is_some_and(JoinHandle::is_finished))
        {
            return Ok(false);
        }
        let mut worker = self
            .worker
            .take()
            .ok_or_else(|| invalid("maintenance worker missing"))?;
        let result = worker.join();
        self.status.running.store(false, Ordering::Release);
        let finish = match result {
            Ok(Outcome::Committed) => Ok(()),
            Ok(Outcome::Complete) => {
                process::Operation::new(5).enter(|| worker.job.finish("succeeded", None))
            }
            Ok(Outcome::Dead(code)) => {
                self.status.failed.fetch_add(1, Ordering::AcqRel);
                process::Operation::new(5).enter(|| worker.job.finish("dead", Some(code)))
            }
            Err(error) => {
                let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
                let mut capacity = false;
                let mut versioning = false;
                while let Some(error) = cause {
                    if matches!(
                        error.downcast_ref::<git_rs_core::CapacityError>(),
                        Some(
                            git_rs_core::CapacityError::Exhausted
                                | git_rs_core::CapacityError::Allocation
                        )
                    ) {
                        capacity = true;
                    }
                    if error.is::<crate::blob::VersioningBlocked>() {
                        versioning = true;
                    }
                    cause = error.source();
                }
                self.status.failed.fetch_add(1, Ordering::AcqRel);
                process::Operation::new(5).enter(|| {
                    worker.job.finish(
                        if capacity || versioning {
                            "dead"
                        } else {
                            "retry"
                        },
                        Some(if capacity {
                            "capacity_exhausted"
                        } else if versioning {
                            "bucket_versioning_unsupported"
                        } else {
                            "worker_failed"
                        }),
                    )
                })
            }
        };
        if finish.is_err() {
            worker.job.authority.lose();
        } else {
            self.status.completed.fetch_add(1, Ordering::AcqRel);
        }
        Ok(true)
    }

    fn dispatch(&mut self, job: Arc<Job>) -> Result<()> {
        let operation = process::Operation::new(self.config.job_seconds);
        job.authority.attach(&operation, &job)?;
        job.check_local()?;
        let (work, op, cpu, store, cache, batch) = (
            job.clone(),
            operation.clone(),
            self.cpu.clone(),
            self.store.clone(),
            self.cache.clone(),
            self.config.delete_batch,
        );
        let thread = std::thread::Builder::new()
            .name("git-rs-source-job".into())
            .spawn(move || {
                op.enter(|| match work.kind.as_str() {
                    "compact" => compaction::run(work.clone(), &cpu, &store, &cache, op.clone()),
                    "orphan_scan" => {
                        gc::inspect(&work, &store, batch)?;
                        gc::delete(&work, &store, batch)?;
                        Ok(Outcome::Complete)
                    }
                    "retire_gc" => {
                        gc::delete(&work, &store, batch)?;
                        Ok(Outcome::Complete)
                    }
                    "full_gc" => Ok(Outcome::Dead("reachability_retention_policy_required")),
                    _ => Ok(Outcome::Dead("unsupported_job_kind")),
                })
            })?;
        self.status.running.store(true, Ordering::Release);
        self.worker = Some(Worker {
            job,
            operation,
            thread: Some(thread),
        });
        Ok(())
    }

    fn run(&mut self, options: &Options) -> Result<()> {
        let waiting_until = Instant::now() + Duration::from_secs(options.wait_seconds);
        let mut next_heartbeat = Instant::now();
        let mut next_scan = Instant::now();
        let mut next_acquire = Instant::now();
        let mut next_claim = Instant::now();
        let mut primary = false;
        loop {
            if self.session.election().stopping() {
                return Ok(());
            }
            let finished = self.finish_worker()?;
            if options.once && finished {
                return Ok(());
            }
            if options.once && self.worker.is_none() && Instant::now() >= waiting_until {
                return Err(invalid(
                    "maintenance --once timed out awaiting leadership/work",
                ));
            }
            if Instant::now() >= next_heartbeat {
                next_heartbeat =
                    Instant::now() + Duration::from_secs(self.config.leader.heartbeat_seconds);
                primary = process::Operation::new(5)
                    .enter(|| {
                        metadata::sql("SELECT version,NOT pg_is_in_recovery() FROM schema_version;")
                    })
                    .is_ok_and(|row| row.trim() == "2\tt");
                self.status
                    .primary(primary, self.config.leader.pg_lease_seconds);
                if !primary {
                    if let Some(authority) = self.session.authority() {
                        authority.lose();
                    }
                } else if self.session.authority().is_some() {
                    let renewed = self.session.renew_fence();
                    if renewed.is_ok()
                        && let Some(worker) = &self.worker
                    {
                        let _heartbeat = process::Operation::new(5)
                            .enter(|| worker.job.heartbeat(self.config.job_lease));
                    }
                }
            }
            if self.worker.is_none() && primary && self.session.election().eligible() {
                if self.session.authority().is_none() && Instant::now() >= next_acquire {
                    next_acquire = Instant::now() + Duration::from_secs(1);
                    let _acquired = self.session.acquire_fence();
                }
                if let Some(authority) = self.session.authority() {
                    if Instant::now() >= next_scan {
                        next_scan = Instant::now() + Duration::from_secs(self.config.scan_seconds);
                        if process::Operation::new(5)
                            .enter(|| {
                                queue::scan(
                                    &authority,
                                    self.config.scan_batch,
                                    self.config.scan_seconds,
                                    self.config.scan_millis,
                                )
                            })
                            .is_err()
                        {
                            authority.lose();
                        }
                    }
                    if authority.check().is_ok() && Instant::now() >= next_claim {
                        next_claim = Instant::now() + Duration::from_secs(1);
                        match process::Operation::new(5)
                            .enter(|| queue::claim(authority.clone(), self.config.job_lease))
                        {
                            Ok(Some(job)) => {
                                if self.dispatch(job).is_err() {
                                    authority.lose();
                                }
                            }
                            Ok(None) if options.once => return Ok(()),
                            Ok(None) => {}
                            Err(_) => authority.lose(),
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.status.election.stop();
        self.status.primary(false, 0);
        if let Some(mut worker) = self.worker.take() {
            worker.operation.cancel();
            let _stopped = worker.join();
        }
        self.status.running.store(false, Ordering::Release);
        let _released = self.session.release_fence();
    }
}

/// Same-binary program entry point. It does not migrate metadata, create a Lease,
/// infer user kubeconfig or fall back to unfenced one-shot deletion.
pub fn run(options: Options) -> Result<()> {
    if !(1..=3600).contains(&options.wait_seconds) {
        return Err(invalid("maintenance wait must be in 1..=3600 seconds"));
    }
    if !options.scratch.is_absolute() {
        return Err(invalid("maintenance scratch must be absolute"));
    }
    if !options.scratch.exists() {
        fs::DirBuilder::new().mode(0o700).create(&options.scratch)?;
    }
    let directory = fs::symlink_metadata(&options.scratch)?;
    if !directory.is_dir() || directory.permissions().mode() & 0o077 != 0 {
        return Err(invalid("maintenance scratch must be a private directory"));
    }
    let config = Config::from_env()?;
    let _logs = metrics::init_service_logs()?;
    let session = LeaderSession::new(config.leader.clone())?;
    let view = session.election().view();
    let shutdown = Shutdown::new(view.clone(), config.shutdown_seconds)?;
    let cache = Arc::new(Cache::new(config.cache_bytes, 8 * 1024 * 1024));
    let status = Arc::new(Status {
        election: view,
        primary_until: Mutex::new(None),
        running: AtomicBool::new(false),
        completed: AtomicU64::new(0),
        failed: AtomicU64::new(0),
    });
    let health = Health::start(options.health, status.clone(), cache.clone())?;
    let mut runtime = Runtime {
        worker: None,
        _health: health,
        session,
        cpu: Arc::new(CpuJobs::new(1, 0)?),
        store: Store::from_env()?,
        cache,
        config,
        status,
        _shutdown: shutdown,
    };
    runtime.run(&options)
}
