//! Process-local Prometheus instrumentation. Label values are closed enums, never
//! repository names, principals, object keys, SQL, commands or error messages.
use crate::Result;
use std::fmt::Write;
use std::io::Write as _;
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing_appender::non_blocking::{NonBlocking, NonBlockingBuilder, WorkerGuard};

struct TimingLogs {
    writer: NonBlocking,
    origin: Instant,
}
static TIMING_LOGS: OnceLock<Option<TimingLogs>> = OnceLock::new();

#[must_use]
pub struct TimingGuard(Option<WorkerGuard>);

impl Drop for TimingGuard {
    fn drop(&mut self) {
        let Some(worker) = self.0.take() else { return };
        // tracing-appender 0.2.5 can print to stdout on a full shutdown queue.
        // Run its destructor off-thread with a bounded wait, including when both
        // process log sinks have stopped. Never join a blocked diagnostic sink.
        let worker = std::mem::ManuallyDrop::new(worker);
        let (done, finished) = std::sync::mpsc::sync_channel(1);
        if std::thread::Builder::new()
            .name("git-rs-timings-stop".into())
            .spawn(move || {
                drop(std::mem::ManuallyDrop::into_inner(worker));
                let _ = done.send(());
            })
            .is_ok()
        {
            let _ = finished.recv_timeout(Duration::from_millis(1200));
        }
        // If a shutdown thread cannot be created, its closure's ManuallyDrop
        // intentionally leaves this process-lifetime diagnostic worker behind.
    }
}

/// Initialize opt-in diagnostics once at process entry. Keep the returned guard
/// outside request paths until process shutdown; it performs bounded flushing.
pub fn init_timing_logs() -> Option<TimingGuard> {
    let mut guard = None;
    TIMING_LOGS.get_or_init(|| {
        if std::env::var("GIT_RS_DEBUG_TIMINGS").as_deref() != Ok("1") {
            return None;
        }
        let (writer, worker) = NonBlockingBuilder::default()
            .buffered_lines_limit(1024)
            .lossy(true)
            .thread_name("git-rs-timings")
            .finish(std::io::stderr());
        guard = Some(TimingGuard(Some(worker)));
        Some(TimingLogs {
            writer,
            origin: Instant::now(),
        })
    });
    guard
}

const BUCKETS: [(u64, &str); 15] = [
    (1_000_000, "0.001"),
    (5_000_000, "0.005"),
    (10_000_000, "0.01"),
    (25_000_000, "0.025"),
    (50_000_000, "0.05"),
    (100_000_000, "0.1"),
    (250_000_000, "0.25"),
    (500_000_000, "0.5"),
    (1_000_000_000, "1"),
    (2_500_000_000, "2.5"),
    (5_000_000_000, "5"),
    (10_000_000_000, "10"),
    (30_000_000_000, "30"),
    (60_000_000_000, "60"),
    (120_000_000_000, "120"),
];

#[derive(Clone, Copy)]
pub enum Transport {
    Ssh,
    Http,
}
const TRANSPORTS: [&str; 2] = ["ssh", "http"];

#[derive(Clone, Copy)]
pub enum Outcome {
    Success,
    Error,
    Unauthorized,
    BadRequest,
}
const OUTCOMES: [&str; 4] = ["success", "error", "unauthorized", "bad_request"];

#[derive(Clone, Copy)]
pub enum Phase {
    Snapshot,
    Materialize,
    ScratchInit,
    Git,
    PreReceive,
    Validate,
    Graph,
    Upload,
    Publish,
}
const PHASES: [&str; 9] = [
    "snapshot",
    "materialize",
    "scratch_init",
    "git",
    "pre_receive",
    "validate",
    "graph",
    "upload",
    "publish",
];

#[derive(Clone, Copy)]
pub enum Stage {
    SqlInit,
    SqlAcquire,
    SqlQuery,
    SqlReset,
    LeaseCommit,
    CatalogRead,
    LeaseRelease,
    CacheFill,
    CacheLink,
    ObjectHash,
    RestoreRefs,
    RestoreHead,
    HookSetup,
    GitInput,
    GitOutput,
    GitWait,
    RefValidate,
    TipValidate,
    FastForward,
    ScratchCleanup,
    ProcessSpawn,
}
const STAGES: [&str; 21] = [
    "sql_init",
    "sql_acquire",
    "sql_query",
    "sql_reset",
    "lease_commit",
    "catalog_read",
    "lease_release",
    "cache_fill",
    "cache_link",
    "object_hash",
    "restore_refs",
    "restore_head",
    "hook_setup",
    "git_input",
    "git_output",
    "git_wait",
    "ref_validate",
    "tip_validate",
    "fast_forward",
    "scratch_cleanup",
    "process_spawn",
];

#[derive(Clone, Copy)]
pub enum Dependency {
    Sql,
    S3Get,
    S3Put,
    S3Delete,
    S3Versioning,
}
const DEPENDENCIES: [&str; 5] = ["sql", "s3_get", "s3_put", "s3_delete", "s3_versioning"];

#[derive(Clone, Copy)]
pub enum Program {
    Git,
    Curl,
    Sha256sum,
    Other,
}
const PROGRAMS: [&str; 4] = ["git", "curl", "sha256sum", "other"];

#[derive(Clone, Copy)]
pub enum Listener {
    Ssh,
    Http,
    Health,
}
const LISTENERS: [&str; 3] = ["ssh", "http", "health"];

#[derive(Clone, Copy)]
pub enum CacheResult {
    Hit,
    Miss,
    Bypass,
}
const CACHE_RESULTS: [&str; 3] = ["hit", "miss", "bypass"];

#[derive(Clone, Copy)]
pub enum Direction {
    Download,
    Upload,
}
const DIRECTIONS: [&str; 2] = ["download", "upload"];

#[derive(Clone, Copy)]
pub enum Metric {
    Phase(Phase),
    Stage(Stage),
    Dependency(Dependency),
    Subprocess(Program),
    CacheWait,
}

struct Histogram {
    // Noncumulative cells make observations O(1). Render a single set of cell
    // snapshots cumulatively, so +Inf and count agree even during concurrent work.
    buckets: [AtomicU64; BUCKETS.len() + 1],
    nanos: AtomicU64,
    errors: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            nanos: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    fn observe(&self, elapsed: Duration, success: bool) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let bucket = BUCKETS.partition_point(|(bound, _)| *bound < nanos);
        self.nanos.fetch_add(nanos, Ordering::Relaxed);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        if !success {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn render(&self, output: &mut String, name: &str, label: &str) {
        let cells: [u64; BUCKETS.len() + 1] =
            std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed));
        let mut count = 0;
        for (i, (_, bound)) in BUCKETS.iter().enumerate() {
            count += cells[i];
            writeln!(output, "{name}_bucket{{{label}le=\"{bound}\"}} {count}").unwrap();
        }
        count += cells[BUCKETS.len()];
        writeln!(output, "{name}_bucket{{{label}le=\"+Inf\"}} {count}").unwrap();
        let labels = if label.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", label.trim_end_matches(','))
        };
        writeln!(
            output,
            "{name}_sum{labels} {:.9}",
            self.nanos.load(Ordering::Relaxed) as f64 / 1e9
        )
        .unwrap();
        writeln!(output, "{name}_count{labels} {count}").unwrap();
    }
}

pub struct Registry {
    started: f64,
    requests: [[AtomicU64; OUTCOMES.len()]; TRANSPORTS.len()],
    request_durations: [Histogram; TRANSPORTS.len()],
    phases: [Histogram; PHASES.len()],
    stages: [Histogram; STAGES.len()],
    dependencies: [Histogram; DEPENDENCIES.len()],
    subprocesses: [Histogram; PROGRAMS.len()],
    cache_wait: Histogram,
    cache_results: [AtomicU64; CACHE_RESULTS.len()],
    admission_rejections: [AtomicU64; LISTENERS.len()],
    bytes: [AtomicU64; DIRECTIONS.len()],
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            started: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
            requests: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            request_durations: std::array::from_fn(|_| Histogram::new()),
            phases: std::array::from_fn(|_| Histogram::new()),
            stages: std::array::from_fn(|_| Histogram::new()),
            dependencies: std::array::from_fn(|_| Histogram::new()),
            subprocesses: std::array::from_fn(|_| Histogram::new()),
            cache_wait: Histogram::new(),
            cache_results: std::array::from_fn(|_| AtomicU64::new(0)),
            admission_rejections: std::array::from_fn(|_| AtomicU64::new(0)),
            bytes: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn histogram(&self, metric: Metric) -> &Histogram {
        match metric {
            Metric::Phase(phase) => &self.phases[phase as usize],
            Metric::Stage(stage) => &self.stages[stage as usize],
            Metric::Dependency(dependency) => &self.dependencies[dependency as usize],
            Metric::Subprocess(program) => &self.subprocesses[program as usize],
            Metric::CacheWait => &self.cache_wait,
        }
    }

    pub fn observe(&self, metric: Metric, elapsed: Duration, success: bool) {
        self.histogram(metric).observe(elapsed, success);
    }

    pub fn request(&self, transport: Transport, outcome: Outcome, elapsed: Duration) {
        self.requests[transport as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
        self.request_durations[transport as usize].observe(elapsed, true);
    }

    pub fn cache(&self, result: CacheResult) {
        self.cache_results[result as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn reject(&self, listener: Listener) {
        self.admission_rejections[listener as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn transferred(&self, direction: Direction, bytes: u64) {
        self.bytes[direction as usize].fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn render(&self, active: usize, cache_allocated: usize) -> String {
        let mut output = String::with_capacity(96 * 1024);
        header(
            &mut output,
            "git_rs_build_info",
            "Backend build version.",
            "gauge",
        );
        writeln!(
            output,
            "git_rs_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        )
        .unwrap();
        header(
            &mut output,
            "git_rs_process_start_time_seconds",
            "Process instrumentation start time since Unix epoch.",
            "gauge",
        );
        writeln!(output, "git_rs_process_start_time_seconds {}", self.started).unwrap();
        header(
            &mut output,
            "git_rs_active_requests",
            "Currently admitted Git transport connections.",
            "gauge",
        );
        writeln!(output, "git_rs_active_requests {active}").unwrap();
        header(
            &mut output,
            "git_rs_cache_allocated_bytes",
            "RAM cache bytes including reservations and reader-pinned entries.",
            "gauge",
        );
        writeln!(output, "git_rs_cache_allocated_bytes {cache_allocated}").unwrap();
        header(
            &mut output,
            "git_rs_timing_logs_dropped_total",
            "Diagnostic records discarded by the bounded timing queue; not sink write errors.",
            "counter",
        );
        let dropped = TIMING_LOGS
            .get()
            .and_then(Option::as_ref)
            .map_or(0, |logs| logs.writer.error_counter().dropped_lines());
        writeln!(output, "git_rs_timing_logs_dropped_total {dropped}").unwrap();
        header(
            &mut output,
            "git_rs_requests_total",
            "Completed admitted transport requests, including parsing and authentication failures.",
            "counter",
        );
        for (i, transport) in TRANSPORTS.iter().enumerate() {
            for (j, outcome) in OUTCOMES.iter().enumerate() {
                writeln!(
                    output,
                    "git_rs_requests_total{{transport=\"{transport}\",outcome=\"{outcome}\"}} {}",
                    self.requests[i][j].load(Ordering::Relaxed)
                )
                .unwrap();
            }
        }
        header(
            &mut output,
            "git_rs_request_duration_seconds",
            "Completed transport connection lifetime including request cleanup.",
            "histogram",
        );
        for (i, name) in TRANSPORTS.iter().enumerate() {
            self.request_durations[i].render(
                &mut output,
                "git_rs_request_duration_seconds",
                &format!("transport=\"{name}\","),
            );
        }
        for (prefix, label, names, histograms) in [
            ("phase", "phase", PHASES.as_slice(), self.phases.as_slice()),
            ("stage", "stage", STAGES.as_slice(), self.stages.as_slice()),
            (
                "dependency",
                "operation",
                DEPENDENCIES.as_slice(),
                self.dependencies.as_slice(),
            ),
            (
                "subprocess",
                "program",
                PROGRAMS.as_slice(),
                self.subprocesses.as_slice(),
            ),
        ] {
            let duration = format!("git_rs_{prefix}_duration_seconds");
            header(
                &mut output,
                &duration,
                "Completed inclusive wall duration; nested measurements overlap.",
                "histogram",
            );
            for (name, histogram) in names.iter().zip(histograms) {
                histogram.render(&mut output, &duration, &format!("{label}=\"{name}\","));
            }
            let errors = format!("git_rs_{prefix}_errors_total");
            header(
                &mut output,
                &errors,
                "Failed completed operations; subprocess errors mean failed spawn or nonzero exit.",
                "counter",
            );
            for (name, histogram) in names.iter().zip(histograms) {
                writeln!(
                    output,
                    "{errors}{{{label}=\"{name}\"}} {}",
                    histogram.errors.load(Ordering::Relaxed)
                )
                .unwrap();
            }
        }
        header(
            &mut output,
            "git_rs_cache_lookups_total",
            "RAM cache outcomes; miss means this caller attempted a fill.",
            "counter",
        );
        for (i, name) in CACHE_RESULTS.iter().enumerate() {
            writeln!(
                output,
                "git_rs_cache_lookups_total{{result=\"{name}\"}} {}",
                self.cache_results[i].load(Ordering::Relaxed)
            )
            .unwrap();
        }
        header(
            &mut output,
            "git_rs_cache_wait_duration_seconds",
            "Time waiting for an existing single-flight cache fill.",
            "histogram",
        );
        self.cache_wait
            .render(&mut output, "git_rs_cache_wait_duration_seconds", "");
        header(
            &mut output,
            "git_rs_admission_rejections_total",
            "Connections closed because listener admission was full.",
            "counter",
        );
        for (i, name) in LISTENERS.iter().enumerate() {
            writeln!(
                output,
                "git_rs_admission_rejections_total{{listener=\"{name}\"}} {}",
                self.admission_rejections[i].load(Ordering::Relaxed)
            )
            .unwrap();
        }
        header(
            &mut output,
            "git_rs_s3_transferred_bytes_total",
            "Verified downloaded or acknowledged uploaded object bytes; excludes cache hits and protocol overhead.",
            "counter",
        );
        for (i, name) in DIRECTIONS.iter().enumerate() {
            writeln!(
                output,
                "git_rs_s3_transferred_bytes_total{{direction=\"{name}\"}} {}",
                self.bytes[i].load(Ordering::Relaxed)
            )
            .unwrap();
        }
        output
    }
}

fn header(output: &mut String, name: &str, help: &str, kind: &str) {
    writeln!(output, "# HELP {name} {help}\n# TYPE {name} {kind}").unwrap();
}

pub fn global() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::new)
}

pub struct Timer {
    metric: Metric,
    start: Instant,
    success: bool,
    timing: Option<(u64, Instant)>,
}
impl Timer {
    pub fn start(metric: Metric) -> Self {
        Self {
            metric,
            start: Instant::now(),
            success: false,
            timing: timing_context(),
        }
    }
    pub fn finish(mut self, success: bool) {
        self.success = success;
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        global().observe(self.metric, elapsed, self.success);
        let (kind, name) = match self.metric {
            Metric::Phase(value) => ("phase", PHASES[value as usize]),
            Metric::Stage(value) => ("stage", STAGES[value as usize]),
            Metric::Dependency(value) => ("dependency", DEPENDENCIES[value as usize]),
            Metric::Subprocess(value) => ("subprocess", PROGRAMS[value as usize]),
            Metric::CacheWait => ("cache", "wait"),
        };
        timing_log(
            self.timing,
            kind,
            name,
            elapsed,
            if self.success { "success" } else { "error" },
        );
    }
}

pub fn measure<T>(metric: Metric, run: impl FnOnce() -> Result<T>) -> Result<T> {
    let timer = Timer::start(metric);
    let result = run();
    timer.finish(result.is_ok());
    result
}

pub struct RequestTimer {
    transport: Transport,
    start: Instant,
    outcome: Outcome,
    timing: Option<(u64, Instant)>,
}
impl RequestTimer {
    pub fn start(transport: Transport) -> Self {
        Self {
            transport,
            start: Instant::now(),
            outcome: Outcome::Error,
            timing: timing_context(),
        }
    }
    pub fn outcome(&mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}
impl Drop for RequestTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        global().request(self.transport, self.outcome, elapsed);
        timing_log(
            self.timing,
            "request",
            TRANSPORTS[self.transport as usize],
            elapsed,
            OUTCOMES[self.outcome as usize],
        );
    }
}

fn timing_context() -> Option<(u64, Instant)> {
    TIMING_LOGS
        .get()?
        .as_ref()
        .map(|logs| crate::process::Operation::current().map_or((0, logs.origin), |op| op.timing()))
}

fn timing_log(
    context: Option<(u64, Instant)>,
    kind: &str,
    name: &str,
    elapsed: Duration,
    outcome: &str,
) {
    if let Some((operation, origin)) = context {
        let Some(Some(logs)) = TIMING_LOGS.get() else {
            return;
        };
        // Enqueue one complete, fixed-label record. Lossy mode never waits for
        // stderr capacity while a request owns a checkout, mutex or admission.
        let line = format!(
            "git_rs_timing level=debug pid={} operation={operation} kind={kind} name={name} elapsed_ms={:.3} duration_ms={:.3} outcome={outcome}\n",
            std::process::id(),
            origin.elapsed().as_secs_f64() * 1000.0,
            elapsed.as_secs_f64() * 1000.0
        );
        let _ = logs.writer.clone().write_all(line.as_bytes());
    }
}
