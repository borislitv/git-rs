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
static SERVICE_LOGS: OnceLock<std::result::Result<NonBlocking, &'static str>> = OnceLock::new();

/// Closed, request-independent diagnostics. Never enqueue formatted errors,
/// client identities, paths or other unbounded request data.
pub enum ServiceEvent {
    HttpOperationFailed,
    SshOperationFailed,
    HttpAcceptFailed,
    RequestThreadStartFailed,
    HookRejected,
    HookSocketCleanupFailed,
    ServingSocketCleanupFailed,
    LeaseReleaseFailed,
    ScratchCleanupFailed,
    MultipartAbortUnconfirmed,
    TerminalFailure,
}

impl ServiceEvent {
    fn record(self) -> &'static [u8] {
        match self {
            Self::HttpOperationFailed => b"git_rs_error event=http_operation_failed\n",
            Self::SshOperationFailed => b"git_rs_error event=ssh_operation_failed\n",
            Self::HttpAcceptFailed => b"git_rs_error event=http_accept_failed\n",
            Self::RequestThreadStartFailed => b"git_rs_error event=request_thread_start_failed\n",
            Self::HookRejected => b"git_rs_error event=hook_rejected\n",
            Self::HookSocketCleanupFailed => b"git_rs_error event=hook_socket_cleanup_failed\n",
            Self::ServingSocketCleanupFailed => {
                b"git_rs_error event=serving_socket_cleanup_failed\n"
            }
            Self::LeaseReleaseFailed => b"git_rs_error event=lease_release_failed\n",
            Self::ScratchCleanupFailed => b"git_rs_error event=scratch_cleanup_failed\n",
            Self::MultipartAbortUnconfirmed => b"git_rs_error event=multipart_abort_unconfirmed\n",
            Self::TerminalFailure => b"git_rs_error event=terminal_failure\n",
        }
    }
}

/// Initialize once at serving startup, independently of optional timing logs.
/// The process/serve owner, never a request, retains the shutdown guard. The
/// queue holds at most 1024 fixed-size records and one worker owns sink I/O.
pub fn init_service_logs() -> Result<Option<TimingGuard>> {
    use std::os::fd::AsFd;
    let mut guard = None;
    SERVICE_LOGS
        .get_or_init(|| {
            // An owned descriptor avoids holding Rust's global stderr mutex
            // while the sink is blocked. Do not change its shared file flags.
            let sink = std::fs::File::from(
                std::io::stderr()
                    .as_fd()
                    .try_clone_to_owned()
                    .map_err(|_| "cannot initialize service diagnostics")?,
            );
            let (writer, worker) = NonBlockingBuilder::default()
                .buffered_lines_limit(1024)
                .lossy(true)
                .thread_name("git-rs-errors")
                .finish(sink);
            guard = Some(TimingGuard(Some(worker)));
            Ok(writer)
        })
        .as_ref()
        .map_err(|message| crate::invalid(message))?;
    Ok(guard)
}

/// True means serving owns this diagnostic, even if its bounded queue drops it.
/// CLI/library callers without serving initialization keep their existing error
/// reporting; requests must never fall back to synchronous stderr on overflow.
pub fn service_event(event: ServiceEvent) -> bool {
    let Some(Ok(writer)) = SERVICE_LOGS.get() else {
        return false;
    };
    let _ = writer.clone().write_all(event.record());
    true
}

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

pub(crate) fn timing_details_enabled() -> bool {
    matches!(TIMING_LOGS.get(), Some(Some(_)))
}

pub(crate) fn timing_details(record: std::fmt::Arguments<'_>) {
    if let Some(Some(logs)) = TIMING_LOGS.get() {
        let line = format!("{record}\n");
        let _ = logs.writer.clone().write_all(line.as_bytes());
    }
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
    ReceivePackPrepare,
    UploadServe,
}
const PHASES: [&str; 11] = [
    "snapshot",
    "materialize",
    "scratch_init",
    "git",
    "pre_receive",
    "validate",
    "graph",
    "upload",
    "publish",
    "receive_pack_prepare",
    "upload_serve",
];

#[derive(Clone, Copy)]
pub(crate) enum BootstrapStage {
    Admission,
    Source,
    Body,
    Inventory,
    InventoryChecksum,
    InventoryEntries,
    InventorySort,
    Graph,
    Index,
    Upload,
    Publish,
    Status,
}

const BOOTSTRAP_STAGES: [&str; 12] = [
    "admission",
    "source",
    "body",
    "inventory",
    "inventory_checksum",
    "inventory_entries",
    "inventory_sort",
    "graph",
    "index",
    "upload",
    "publish",
    "status",
];

static BOOTSTRAP_RESULTS: [[AtomicU64; 2]; BOOTSTRAP_STAGES.len()] =
    [const { [const { AtomicU64::new(0) }; 2] }; BOOTSTRAP_STAGES.len()];
static BOOTSTRAP_NS: [AtomicU64; BOOTSTRAP_STAGES.len()] =
    [const { AtomicU64::new(0) }; BOOTSTRAP_STAGES.len()];
static BOOTSTRAP_ACTIVE: [AtomicU64; BOOTSTRAP_STAGES.len()] =
    [const { AtomicU64::new(0) }; BOOTSTRAP_STAGES.len()];
static BOOTSTRAP_WORK: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static BOOTSTRAP_INVENTORY: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static BOOTSTRAP_REUSE: [[AtomicU64; 2]; 3] = [const { [const { AtomicU64::new(0) }; 2] }; 3];
static BOOTSTRAP_REJECTIONS: [AtomicU64; 5] = [const { AtomicU64::new(0) }; 5];

pub(crate) fn bootstrap_reuse(
    done: Option<bool>,
    delta: git_rs_core::receive::packed::ReuseStatistics,
) {
    let counters = &BOOTSTRAP_REUSE[done.map_or(0, |ok| 1 + usize::from(ok))];
    counters[0].fetch_add(delta.hits, Ordering::Relaxed);
    counters[1].fetch_add(delta.avoided_ancestor_bytes, Ordering::Relaxed);
}

pub(crate) fn bootstrap_inventory_failure(reason: git_rs_core::receive::packed::InventoryFailure) {
    use git_rs_core::receive::packed::InventoryFailure::*;
    let index = match reason {
        DecoderCheckpoints => 0,
        LogicalBytes => 1,
        ChainDepth => 2,
        ChainWorkspace => 3,
        ObjectBytes => 4,
    };
    BOOTSTRAP_REJECTIONS[index].fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn bootstrap_inventory(objects: u64, logical: u64, polls: u64) {
    for (counter, n) in BOOTSTRAP_INVENTORY.iter().zip([objects, logical, polls]) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

impl BootstrapStage {
    pub(crate) fn start(self, enabled: bool) -> BootstrapStageGuard {
        BootstrapStageGuard {
            stage: enabled.then(|| {
                BOOTSTRAP_ACTIVE[self as usize].fetch_add(1, Ordering::Relaxed);
                (self, Instant::now())
            }),
            ok: false,
        }
    }
}

pub(crate) struct BootstrapStageGuard {
    stage: Option<(BootstrapStage, Instant)>,
    ok: bool,
}

impl BootstrapStageGuard {
    pub(crate) fn complete(mut self) {
        self.ok = true;
    }
}

impl Drop for BootstrapStageGuard {
    fn drop(&mut self) {
        if let Some((stage, start)) = self.stage {
            BOOTSTRAP_RESULTS[stage as usize][usize::from(self.ok)].fetch_add(1, Ordering::Relaxed);
            let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
            BOOTSTRAP_NS[stage as usize].fetch_add(nanos, Ordering::Relaxed);
            BOOTSTRAP_ACTIVE[stage as usize].fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub(crate) fn bootstrap_work(bytes: u64, objects: u64, logical: u64, polls: u64) {
    for (counter, n) in BOOTSTRAP_WORK.iter().zip([bytes, objects, logical, polls]) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
pub(crate) enum BlameSegmentStage {
    Planning,
    Execute,
    Reduce,
    Encode,
    Fallback,
}

const SEGMENT_STAGES: [&str; 5] = ["planning", "execute", "reduce", "encode", "fallback"];
const SEGMENT_OUTCOMES: [&str; 4] = ["completed", "declined", "failed", "stopped"];
static SEGMENT_STAGES_TOTAL: [[AtomicU64; 4]; SEGMENT_STAGES.len()] =
    [const { [const { AtomicU64::new(0) }; 4] }; SEGMENT_STAGES.len()];
static SEGMENT_STAGES_NS: [[AtomicU64; 4]; SEGMENT_STAGES.len()] =
    [const { [const { AtomicU64::new(0) }; 4] }; SEGMENT_STAGES.len()];
static SEGMENT_ACTIVE: [AtomicU64; SEGMENT_STAGES.len()] =
    [const { AtomicU64::new(0) }; SEGMENT_STAGES.len()];
static SEGMENT_WORK: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static SEGMENT_CAPACITY_MAX: AtomicU64 = AtomicU64::new(0);

impl BlameSegmentStage {
    pub(crate) fn start(self, control: &git_rs_core::Control) -> BlameSegmentTimer<'_> {
        SEGMENT_ACTIVE[self as usize].fetch_add(1, Ordering::Relaxed);
        BlameSegmentTimer {
            stage: self as usize,
            started: Instant::now(),
            control,
            outcome: 2,
        }
    }
}

pub(crate) struct BlameSegmentTimer<'a> {
    stage: usize,
    started: Instant,
    control: &'a git_rs_core::Control,
    outcome: usize,
}

impl BlameSegmentTimer<'_> {
    pub(crate) fn completed(&mut self) {
        self.outcome = 0;
    }
    pub(crate) fn declined(&mut self) {
        self.outcome = 1;
    }
}

impl Drop for BlameSegmentTimer<'_> {
    fn drop(&mut self) {
        let outcome = if self.control.check().is_err() || crate::process::check_operation().is_err()
        {
            3
        } else {
            self.outcome
        };
        let ns = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        SEGMENT_STAGES_TOTAL[self.stage][outcome].fetch_add(1, Ordering::Relaxed);
        SEGMENT_STAGES_NS[self.stage][outcome].fetch_add(ns, Ordering::Relaxed);
        SEGMENT_ACTIVE[self.stage].fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) fn blame_segment_work([edges, lanes, cpu_ns, cpu_samples, capacity]: [u64; 5]) {
    for (counter, value) in SEGMENT_WORK.iter().zip([edges, lanes, cpu_ns, cpu_samples]) {
        counter.fetch_add(value, Ordering::Relaxed);
    }
    SEGMENT_CAPACITY_MAX.fetch_max(capacity, Ordering::Relaxed);
}

fn render_blame_segments(output: &mut String) {
    let total = "git_rs_blame_segment_stage_total";
    let seconds = "git_rs_blame_segment_stage_seconds_sum";
    let active = "git_rs_blame_segment_stage_active";
    let work = "git_rs_blame_segment_work_total";
    let capacity = "git_rs_blame_segment_sampled_capacity_bytes_max";
    header(
        output,
        total,
        "Terminal cold blame stage outcomes, including stop and fallback.",
        "counter",
    );
    header(
        output,
        seconds,
        "Wall time by terminal cold blame stage outcome.",
        "counter",
    );
    header(
        output,
        active,
        "Cold blame stages in progress, including scoped cleanup.",
        "gauge",
    );
    header(
        output,
        work,
        "Cold blame work, including declined and failed attempts.",
        "counter",
    );
    header(
        output,
        capacity,
        "Maximum sampled charged job bytes, not an RSS or allocator peak.",
        "gauge",
    );
    for (i, stage) in SEGMENT_STAGES.iter().enumerate() {
        for (j, outcome) in SEGMENT_OUTCOMES.iter().enumerate() {
            writeln!(
                output,
                "{total}{{stage=\"{stage}\",outcome=\"{outcome}\"}} {}",
                SEGMENT_STAGES_TOTAL[i][j].load(Ordering::Relaxed)
            )
            .unwrap();
            writeln!(
                output,
                "{seconds}{{stage=\"{stage}\",outcome=\"{outcome}\"}} {:.9}",
                SEGMENT_STAGES_NS[i][j].load(Ordering::Relaxed) as f64 / 1e9
            )
            .unwrap();
        }
        writeln!(
            output,
            "{active}{{stage=\"{stage}\"}} {}",
            SEGMENT_ACTIVE[i].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    for (unit, value) in ["scanned_edges", "lanes", "lane_cpu_ns", "lane_cpu_samples"]
        .into_iter()
        .zip(&SEGMENT_WORK)
    {
        writeln!(
            output,
            "{work}{{unit=\"{unit}\"}} {}",
            value.load(Ordering::Relaxed)
        )
        .unwrap();
    }
    writeln!(
        output,
        "{capacity} {}",
        SEGMENT_CAPACITY_MAX.load(Ordering::Relaxed)
    )
    .unwrap();
}

fn render_bootstrap(output: &mut String) {
    let reuse = "git_rs_receive_bootstrap_verified_base_reuse_total";
    header(
        output,
        reuse,
        "Verified base reuse during inventory and by terminal inventory outcome.",
        "counter",
    );
    for (outcome, counters) in ["progress", "failed", "completed"]
        .into_iter()
        .zip(&BOOTSTRAP_REUSE)
    {
        for (unit, counter) in ["hits", "avoided_ancestor_bytes"].into_iter().zip(counters) {
            writeln!(
                output,
                "{reuse}{{outcome=\"{outcome}\",unit=\"{unit}\"}} {}",
                counter.load(Ordering::Relaxed)
            )
            .unwrap();
        }
    }
    let failures = "git_rs_receive_bootstrap_inventory_admission_failures_total";
    header(
        output,
        failures,
        "Typed physical inventory admission failures.",
        "counter",
    );
    for (reason, counter) in [
        "decoder_checkpoints",
        "logical_bytes",
        "chain_depth",
        "chain_workspace",
        "object_bytes",
    ]
    .into_iter()
    .zip(&BOOTSTRAP_REJECTIONS)
    {
        writeln!(
            output,
            "{failures}{{reason=\"{reason}\"}} {}",
            counter.load(Ordering::Relaxed)
        )
        .unwrap();
    }
    header(
        output,
        "git_rs_receive_bootstrap_inventory_progress_total",
        "Verified physical-entry work, sampled in bounded batches.",
        "counter",
    );
    for (metric, counter) in ["objects", "logical_bytes", "decoder_checkpoints"]
        .into_iter()
        .zip(BOOTSTRAP_INVENTORY.iter())
    {
        writeln!(
            output,
            "git_rs_receive_bootstrap_inventory_progress_total{{unit=\"{metric}\"}} {}",
            counter.load(Ordering::Relaxed)
        )
        .unwrap();
    }
    header(
        output,
        "git_rs_receive_bootstrap_stage_active",
        "Bootstrap attempts in each closed stage.",
        "gauge",
    );
    header(
        output,
        "git_rs_receive_bootstrap_stage_total",
        "Closed bootstrap stage outcomes.",
        "counter",
    );
    header(
        output,
        "git_rs_receive_bootstrap_stage_seconds_sum",
        "Wall time elapsed across bootstrap stage attempts.",
        "counter",
    );
    for (index, stage) in BOOTSTRAP_STAGES.iter().enumerate() {
        let active = BOOTSTRAP_ACTIVE[index].load(Ordering::Relaxed);
        writeln!(
            output,
            "git_rs_receive_bootstrap_stage_active{{stage=\"{stage}\"}} {active}"
        )
        .unwrap();
        for (index_outcome, outcome) in ["failed", "completed"].iter().enumerate() {
            let count = BOOTSTRAP_RESULTS[index][index_outcome].load(Ordering::Relaxed);
            writeln!(output, "git_rs_receive_bootstrap_stage_total{{stage=\"{stage}\",outcome=\"{outcome}\"}} {count}").unwrap();
        }
        let seconds = BOOTSTRAP_NS[index].load(Ordering::Relaxed) as f64 / 1e9;
        writeln!(
            output,
            "git_rs_receive_bootstrap_stage_seconds_sum{{stage=\"{stage}\"}} {seconds:.9}"
        )
        .unwrap();
    }
    for (counter, name) in BOOTSTRAP_WORK.iter().zip([
        "pack_bytes",
        "objects",
        "logical_bytes",
        "decoder_checkpoints",
    ]) {
        let metric = format!("git_rs_receive_bootstrap_{name}_total");
        header(
            output,
            &metric,
            "Work for bootstrap requests that completed physical inventory.",
            "counter",
        );
        writeln!(output, "{metric} {}", counter.load(Ordering::Relaxed)).unwrap();
    }
}

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
    BlamePreflight,
    BlameAttribution,
    BlameEncode,
    BlameSend,
}
const STAGES: [&str; 25] = [
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
    "blame_preflight",
    "blame_attribution",
    "blame_encode",
    "blame_send",
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
    cache_waiters: AtomicU64,
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
            cache_waiters: AtomicU64::new(0),
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
        render_bootstrap(&mut output);
        render_blame_segments(&mut output);
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
            "git_rs_service_logs_dropped_total",
            "Fixed service diagnostics discarded by the bounded queue; not sink write errors.",
            "counter",
        );
        let dropped = SERVICE_LOGS
            .get()
            .and_then(|logs| logs.as_ref().ok())
            .map_or(0, |logs| logs.error_counter().dropped_lines());
        writeln!(output, "git_rs_service_logs_dropped_total {dropped}").unwrap();
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
                "Completed operations that failed or whose measurement scope ended without success; subprocess errors mean failed spawn or nonzero exit.",
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
            "git_rs_cache_waiters",
            "Callers currently in a single-flight cache wait scope.",
            "gauge",
        );
        writeln!(
            output,
            "git_rs_cache_waiters {}",
            self.cache_waiters.load(Ordering::Relaxed)
        )
        .unwrap();
        header(
            &mut output,
            "git_rs_admission_rejections_total",
            "Connections closed because listener admission or request thread creation failed.",
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
            "Length-checked downloaded payload bytes or acknowledged uploaded payload bytes, including multipart parts; excludes cache hits and protocol overhead; does not imply object validation or publication.",
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
        if matches!(metric, Metric::CacheWait) {
            global().cache_waiters.fetch_add(1, Ordering::Relaxed);
        }
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
        if matches!(self.metric, Metric::CacheWait) {
            global().cache_waiters.fetch_sub(1, Ordering::Relaxed);
        }
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
