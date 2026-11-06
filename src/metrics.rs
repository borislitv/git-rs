//! Process-local Prometheus instrumentation. Label values are closed enums, never
//! repository names, principals, object keys, SQL, commands or error messages.
use crate::Result;
use std::fmt::Write;
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    Psql,
    Curl,
    Sha256sum,
    Other,
}
const PROGRAMS: [&str; 5] = ["git", "psql", "curl", "sha256sum", "other"];

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
        let mut output = String::with_capacity(48 * 1024);
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
}
impl Timer {
    pub fn start(metric: Metric) -> Self {
        Self {
            metric,
            start: Instant::now(),
            success: false,
        }
    }
    pub fn finish(mut self, success: bool) {
        self.success = success;
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        global().observe(self.metric, self.start.elapsed(), self.success);
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
}
impl RequestTimer {
    pub fn start(transport: Transport) -> Self {
        Self {
            transport,
            start: Instant::now(),
            outcome: Outcome::Error,
        }
    }
    pub fn outcome(&mut self, outcome: Outcome) {
        self.outcome = outcome;
    }
}
impl Drop for RequestTimer {
    fn drop(&mut self) {
        global().request(self.transport, self.outcome, self.start.elapsed());
    }
}
