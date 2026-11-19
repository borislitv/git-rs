//! Scheduling only. The pinned producer decides segment applicability and
//! performs attribution and reduction. No segment can outlive this source job.
use super::{
    super::{Phase, ResourceProfile, observe},
    Environment, encode_complete,
};
use crate::{
    Result, invalid,
    metrics::{self, BlameSegmentStage},
    object_source::RangeStore,
    process,
    repository::blame_source::Source,
};
use git_rs_core::{
    CapacityBudget, CapacityError, Control, SnapshotSession, VerifiedObjectProvider,
    blame::{
        BlameError, BlameProgress, BlameRequest, CompleteBlame, segments::Endpoint as CoreEndpoint,
    },
    porcelain::CompletePorcelain,
};
use std::{
    io::Read as _,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

const LANES: usize = 4;
const MIN_EDGES_PER_LANE: usize = 8;
const MIB: usize = 1024 * 1024;
const LANE_FIXED: usize = 128 * MIB;
const LANE_CACHE: usize = 192 * MIB;
const REDUCE_HEADROOM: usize = 256 * MIB;
const METADATA_WEIGHT: u64 = 1;
const CHANGE_WEIGHT: u64 = 256;
const MIN_ESTIMATED_EDGES: usize = 1024;
const MAX_SIZE_SAMPLES: usize = 64;
const SIZE_UNIT: u64 = 64 * 1024;

struct LinearStep {
    parent: Option<gix::ObjectId>,
    blob: gix::ObjectId,
}

// Cheap scheduling proof only: use verified objects and the maintained
// fallible parsers. The producer checks applicability again at each lane.
fn linear_step<P: VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    request: BlameRequest<'_>,
    control: &Control,
    metadata_limit: usize,
) -> Result<Option<LinearStep>> {
    use gix::objs::{CommitRefIter, Kind, TreeRefIter, commit::ref_iter::Token};

    let commit = session
        .verified_object(request.commit)?
        .ok_or_else(|| invalid("missing segment commit"))?;
    if commit.header().kind != Kind::Commit {
        return Err(invalid("segment commit has wrong object type"));
    }
    let bytes = commit.as_slice();
    let mut tokens = CommitRefIter::from_bytes(
        &bytes[..bytes.len().min(metadata_limit)],
        request.commit.kind(),
    );
    let Some(Ok(Token::Tree { id: mut tree })) = tokens.next() else {
        return Err(invalid("invalid segment commit tree"));
    };
    let mut parent = None;
    loop {
        control.check()?;
        match tokens.next() {
            Some(Ok(Token::Parent { id })) if !id.is_null() => {
                if parent.replace(id).is_some() {
                    return Ok(None);
                }
            }
            Some(Ok(Token::Author { .. })) => break,
            _ => return Err(invalid("invalid segment commit prefix")),
        }
    }
    drop(commit);
    let mut parts = request.path.split(|&byte| byte == b'/').peekable();
    while let Some(part) = parts.next() {
        control.check()?;
        let object = session
            .verified_object(&tree)?
            .ok_or_else(|| invalid("missing segment tree"))?;
        if object.header().kind != Kind::Tree {
            return Err(invalid("segment tree has wrong object type"));
        }
        let mut found = None;
        for entry in TreeRefIter::from_bytes(object.as_slice(), tree.kind()) {
            control.check()?;
            let entry = entry?;
            let name: &[u8] = entry.filename.as_ref();
            if name == part && found.replace((entry.oid.to_owned(), entry.mode)).is_some() {
                return Err(invalid("ambiguous segment tree path"));
            }
        }
        let Some((id, mode)) = found else {
            return Ok(None);
        };
        if parts.peek().is_none() {
            return Ok(mode.is_blob().then_some(LinearStep { blob: id, parent }));
        }
        if !mode.is_tree() {
            return Ok(None);
        }
        tree = id;
    }
    Ok(None)
}

/// Scheduler-only window. The summary producer independently validates the
/// entire window, including the exact endpoint blob and path.
pub(in crate::blame) fn warm_window<P: VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    request: BlameRequest<'_>,
    control: &Control,
    metadata_limit: usize,
    edges: usize,
) -> Result<Option<(Option<CoreEndpoint>, usize)>> {
    let mut cursor = request.commit.to_owned();
    for traversed in 0..=edges {
        let Some(step) = linear_step(
            session,
            BlameRequest {
                commit: &cursor,
                path: request.path,
            },
            control,
            metadata_limit,
        )?
        else {
            return Ok(None);
        };
        if step.parent.is_none() {
            return Ok(Some((None, traversed)));
        }
        if traversed == edges {
            return Ok(Some((
                Some(CoreEndpoint {
                    commit: cursor,
                    blob: step.blob,
                }),
                traversed,
            )));
        }
        cursor = step.parent.expect("checked parent");
    }
    unreachable!("inclusive segment window")
}

pub(super) type EncodedSegment = (CompleteBlame, Option<CompletePorcelain>);

#[derive(Clone, Copy, Debug, Default)]
pub struct SegmentStatistics {
    pub attempted: bool,
    pub scan: Duration,
    pub execute: Duration,
    pub reduce: Duration,
    pub encode: Duration,
    pub lane_cpu_ns: u64,
    pub lane_cpu_samples: usize,
    pub scanned_edges: usize,
    pub size_samples: usize,
    pub lanes: usize,
    pub sampled_capacity_bytes: usize,
    pub used: bool,
}

struct WorkReport<'a>(&'a mut SegmentStatistics);

impl Drop for WorkReport<'_> {
    fn drop(&mut self) {
        let stats = &*self.0;
        metrics::blame_segment_work([
            stats.scanned_edges as u64,
            stats.lanes as u64,
            stats.lane_cpu_ns,
            stats.lane_cpu_samples as u64,
            stats.sampled_capacity_bytes as u64,
        ]);
    }
}

#[derive(Clone, Copy)]
struct Point {
    commit: gix::ObjectId,
    blob: gix::ObjectId,
    weight: u64,
}

#[derive(Clone, Copy)]
struct Endpoint {
    start: gix::ObjectId,
    end: Option<CoreEndpoint>,
}

/// Balanced by estimated changed-blob work, not just by commit count. The
/// estimate selects a width; the one shared admission remains the hard limit.
fn endpoints(points: &[Point], lanes: usize) -> Vec<Endpoint> {
    let last = points.len() - 1;
    let total = points[last].weight;
    let mut start = 0;
    let mut result = Vec::with_capacity(lanes);
    for index in 1..lanes {
        let wanted = total.saturating_mul(index as u64) / lanes as u64;
        let end = points.partition_point(|point| point.weight < wanted).clamp(
            start + MIN_EDGES_PER_LANE,
            last - (lanes - index) * MIN_EDGES_PER_LANE,
        );
        result.push(Endpoint {
            start: points[start].commit,
            end: Some(CoreEndpoint {
                commit: points[end].commit,
                blob: points[end].blob,
            }),
        });
        start = end;
    }
    result.push(Endpoint {
        start: points[start].commit,
        end: None,
    });
    result
}

// The graph walk only reads verified commits/trees. Physical blob headers can
// replay deep delta chains; any sizing work is separate and strictly sampled.
fn scan<P: VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    request: BlameRequest<'_>,
    control: &Control,
    metadata_limit: usize,
    ceiling: usize,
    points: &mut Vec<Point>,
    stats: &mut SegmentStatistics,
) -> Result<Option<usize>> {
    let mut cursor = request.commit.to_owned();
    let mut weight = 0u64;
    let mut changes = 0;
    for _ in 0..ceiling {
        control.check()?;
        let Some(step) = linear_step(
            session,
            BlameRequest {
                commit: &cursor,
                path: request.path,
            },
            control,
            metadata_limit,
        )?
        else {
            // A merge or absent path, including after a good prefix, is not
            // a terminal root. Let the ordinary backend follow the history.
            return Ok(None);
        };
        if points
            .last()
            .is_some_and(|previous| previous.blob != step.blob)
        {
            weight += CHANGE_WEIGHT;
            changes += 1;
        }
        weight += METADATA_WEIGHT;
        points.push(Point {
            commit: cursor,
            blob: step.blob,
            weight,
        });
        let Some(parent) = step.parent else {
            return Ok(Some(changes));
        };
        stats.scanned_edges += 1;
        cursor = parent;
    }
    Ok(None)
}

fn estimate_blob_work<P: VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    points: &mut [Point],
    control: &Control,
    stats: &mut SegmentStatistics,
) -> Result<()> {
    let edges = points.len() - 1;
    if edges < MIN_ESTIMATED_EDGES {
        return Ok(());
    }
    let mut previous = points[0].blob;
    let mut weight = points[0].weight;
    for chunk in points[1..].chunks_mut(edges.div_ceil(MAX_SIZE_SAMPLES)) {
        control.check()?;
        // A metadata-only stretch should not re-probe the same unchanged blob.
        let changed = chunk.iter().any(|point| point.blob != previous);
        let change_weight = if changed {
            let header = session
                .header(&chunk[chunk.len() / 2].blob)?
                .ok_or_else(|| invalid("missing sampled segment blob"))?;
            if header.kind != gix::objs::Kind::Blob {
                return Err(invalid("sampled segment object is not a blob"));
            }
            stats.size_samples += 1;
            CHANGE_WEIGHT.saturating_mul(header.size.div_ceil(SIZE_UNIT).max(1))
        } else {
            0
        };
        for point in chunk {
            weight = weight.saturating_add(METADATA_WEIGHT);
            if point.blob != previous {
                weight = weight.saturating_add(change_weight);
            }
            point.weight = weight;
            previous = point.blob;
        }
    }
    Ok(())
}

// Linux scheduler CPU time, not wall time or a process-wide interval. A host
// without schedstat gets no sample; the wall/edge/capacity counters still work.
fn thread_cpu_ns() -> Option<u64> {
    let mut bytes = [0; 96];
    let n = std::fs::File::open("/proc/thread-self/schedstat")
        .ok()?
        .read(&mut bytes)
        .ok()?;
    std::str::from_utf8(&bytes[..n])
        .ok()?
        .split_ascii_whitespace()
        .next()?
        .parse()
        .ok()
}

// Only typed admission failures are a miss. Never turn an opaque provider,
// codec or stop error into permission to start another traversal.
fn admission(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<CapacityError>(),
        Some(CapacityError::Allocation | CapacityError::Exhausted)
    ) || matches!(
        error.downcast_ref::<BlameError>(),
        Some(BlameError::Capacity(
            CapacityError::Allocation | CapacityError::Exhausted
        ))
    ) || error.is::<std::collections::TryReserveError>()
}

struct Lane<T> {
    output: Result<Option<T>>,
    cpu_ns: Option<u64>,
    capacity: usize,
}

fn execute<T: Send>(
    endpoints: &[Endpoint],
    control: &Control,
    budget: &CapacityBudget,
    run: impl Fn(Endpoint) -> Result<Option<T>> + Sync,
    stats: &mut SegmentStatistics,
) -> Result<Option<Vec<T>>> {
    let run = |endpoint| {
        let started = thread_cpu_ns();
        let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(endpoint)))
            .unwrap_or_else(|_| Err(invalid("blame segment lane panicked")));
        Lane {
            output,
            cpu_ns: started
                .zip(thread_cpu_ns())
                .and_then(|(a, b)| b.checked_sub(a)),
            capacity: budget.used(),
        }
    };
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(LANES - 1);
        let mut spawn_failed = false;
        for &endpoint in &endpoints[1..] {
            match thread::Builder::new()
                .name("git-rs-blame-segment".into())
                .spawn_scoped(scope, {
                    let run = &run;
                    move || run(endpoint)
                }) {
                Ok(handle) => handles.push(handle),
                Err(_) => {
                    spawn_failed = true;
                    break;
                }
            }
        }
        let mut results = Vec::with_capacity(LANES);
        if !spawn_failed {
            results.push(run(endpoints[0]));
        }
        stats.lanes = handles.len() + results.len();
        for handle in handles {
            results.push(
                handle
                    .join()
                    .map_err(|_| invalid("blame segment join failed"))?,
            );
        }
        let mut declined = spawn_failed;
        let mut failed = None;
        let mut outputs = Vec::with_capacity(endpoints.len());
        for lane in results {
            stats.sampled_capacity_bytes = stats.sampled_capacity_bytes.max(lane.capacity);
            if let Some(cpu_ns) = lane.cpu_ns {
                stats.lane_cpu_samples += 1;
                stats.lane_cpu_ns = stats.lane_cpu_ns.saturating_add(cpu_ns);
            }
            match lane.output {
                Ok(Some(output)) => outputs.push(output),
                Ok(None) => declined = true,
                Err(error) if admission(error.as_ref()) => declined = true,
                Err(error) => {
                    failed.get_or_insert(error);
                }
            }
        }
        if let Some(error) = failed {
            return Err(error);
        }
        control.check()?;
        Ok((!declined).then_some(outputs))
    })
}

pub(super) fn cold(
    env: &Environment,
    source: &Arc<Source>,
    request: BlameRequest<'_>,
    control: &Control,
    budget: &CapacityBudget,
    stats: &mut SegmentStatistics,
) -> Result<Option<EncodedSegment>> {
    if env.resources != ResourceProfile::LargeV1 {
        return Ok(None);
    }
    let width = thread::available_parallelism().map_or(1, |n| n.get().min(LANES));
    if width < 2 {
        return Ok(None);
    }
    stats.attempted = true;
    let attempt = WorkReport(stats);
    let result = (|| -> Result<_> {
        let stats = &mut *attempt.0;
        let Some(blame) = cold_inner(env, source, request, control, budget, width, stats)? else {
            return Ok(None);
        };
        let mut encoding = BlameSegmentStage::Encode.start(control);
        let started = Instant::now();
        let body = encode_complete(&blame, control, budget);
        stats.encode = started.elapsed();
        stats.sampled_capacity_bytes = stats.sampled_capacity_bytes.max(budget.used());
        let body = match body {
            Ok(body) => Some(body),
            Err(BlameError::OutputTooLarge) => None,
            Err(error) => {
                if admission(&error) {
                    encoding.declined();
                }
                return Err(error.into());
            }
        };
        encoding.completed();
        Ok(Some((blame, body)))
    })();
    let result = match result {
        Err(error) if admission(error.as_ref()) => None,
        result => result?,
    };
    // cold_inner owns every probe/provider/segment. Re-fence before accepting
    // reduction or entering the ordinary attempt after a speculative miss.
    control.check()?;
    source.check()?;
    attempt.0.used = result.is_some();
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn cold_inner(
    env: &Environment,
    source: &Arc<Source>,
    request: BlameRequest<'_>,
    control: &Control,
    budget: &CapacityBudget,
    width: usize,
    stats: &mut SegmentStatistics,
) -> Result<Option<CompleteBlame>> {
    let operation = process::Operation::current()
        .ok_or_else(|| invalid("missing admitted blame segment operation"))?;
    let identity = source.identity();
    let limits = env.resources.read_limits(&env.options);
    let open = |decoder, verify_replays| {
        // Retain ordinary physical/work limits. Only the optional decoder
        // cache is smaller; no lane gets an independent job budget.
        let packs = RangeStore::open_with_decoder_options(
            source.clone(),
            env.store.clone(),
            env.files.clone(),
            budget.clone(),
            limits,
            decoder,
            control,
        )?
        .with_prepared_reads();
        // The planning and reduction sessions read metadata once. Only the
        // workers can repeatedly reconstruct evicted bases in the same job.
        let packs = if verify_replays {
            packs
                .with_streamed_blobs()
                .with_verification_journal(control)?
        } else {
            packs
        };
        SnapshotSession::open(packs, &identity, control.clone(), budget.clone(), limits)
    };
    let decoder = git_rs_core::pack::DecoderOptions {
        cache_bytes: LANE_CACHE,
        ..env.resources.decoder_options()
    };
    let mut planning = BlameSegmentStage::Planning.start(control);
    source.check()?;
    observe(&env.observer, Phase::SegmentPlanning, control)?;
    let mut points = Vec::new();
    let ceiling = limits
        .max_graph_objects
        .min(limits.max_graph_edges.saturating_add(1));
    let mut point_charge = budget.reservation(ceiling * size_of::<Point>())?;
    points.try_reserve_exact(ceiling)?;
    point_charge.resize(points.capacity() * size_of::<Point>())?;
    let started = Instant::now();
    let scanned = (|| {
        let provider = crate::repository::blame_source::LazySnapshot::new(
            source.clone(),
            env.store.clone(),
            env.files.clone(),
            budget.clone(),
            limits,
            control,
        )?
        .with_observer(env.observer.clone());
        let session =
            SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), limits)?;
        let scanned = scan(
            &session,
            request,
            control,
            limits.max_metadata_bytes,
            ceiling,
            &mut points,
            stats,
        );
        if scanned
            .as_ref()
            .is_ok_and(|changes| changes.is_some_and(|n| n >= width))
        {
            estimate_blob_work(&session, &mut points, control, stats)?;
        }
        stats.sampled_capacity_bytes = stats.sampled_capacity_bytes.max(budget.used());
        scanned
    })();
    stats.scan = started.elapsed();
    let Some(changes) = scanned? else {
        planning.declined();
        return Ok(None);
    };
    let index_bytes = source
        .snapshot
        .packs
        .iter()
        .fold(0usize, |n, pack| n.saturating_add(pack.idx_size as usize));
    let per_lane = LANE_FIXED
        .saturating_add(index_bytes)
        .saturating_add(LANE_CACHE)
        .saturating_add(git_rs_core::blame::MAX_BLOB_BYTES * 4);
    let feasible = width
        .min((points.len() - 1) / MIN_EDGES_PER_LANE)
        .min(changes);
    let Some(lanes) = (2..=feasible).rev().find(|&n| {
        budget
            .reservation(REDUCE_HEADROOM.saturating_add(n.saturating_mul(per_lane)))
            .is_ok()
    }) else {
        planning.declined();
        return Ok(None);
    };
    let endpoints = endpoints(&points, lanes);
    drop(points);
    drop(point_charge);
    observe(&env.observer, Phase::SegmentPlanned, control)?;
    source.check()?;
    planning.completed();
    drop(planning);
    let mut executing = BlameSegmentStage::Execute.start(control);
    let started = Instant::now();
    let observe_progress = metrics::timing_details_enabled();
    if observe_progress {
        metrics::timing_details(format_args!(
            "blame_segment_plan size_samples={}",
            stats.size_samples
        ));
    }
    let segments = execute(
        &endpoints,
        control,
        budget,
        |endpoint| {
            operation.enter(|| {
                control.check()?;
                let session = open(decoder, true)?;
                let progress = observe_progress.then(BlameProgress::new);
                let result = session.summarize_blame_segment_observed(
                    BlameRequest {
                        commit: &endpoint.start,
                        path: request.path,
                    },
                    endpoint.end,
                    &env.options,
                    progress.as_ref(),
                );
                if let Some(progress) = progress {
                    let progress = progress.snapshot();
                    let verification = session.range_verification_stats();
                    metrics::timing_details(format_args!(
                        "blame_segment_producer commit_watermark={} raw_blob_calls={} raw_blob_ns={} sha1_ns={} sha256_ns={}",
                        progress.commit_watermark,
                        progress.raw_blob_calls,
                        progress.raw_blob_elapsed.as_nanos(),
                        verification.sha1_time.as_nanos(),
                        verification.sha256_time.as_nanos(),
                    ));
                    report_verification("cold_producer", verification);
                }
                Ok(result?)
            })
        },
        stats,
    );
    stats.execute = started.elapsed();
    let Some(segments) = segments? else {
        executing.declined();
        return Ok(None);
    };
    source.check()?;
    executing.completed();
    drop(executing);
    let mut reducing = BlameSegmentStage::Reduce.start(control);
    let started = Instant::now();
    let result = (|| -> Result<Option<CompleteBlame>> {
        let session = open(decoder, false)?;
        let mut segments = segments.into_iter();
        let Some(mut reduced) = segments.next() else {
            return Ok(None);
        };
        for older in segments {
            let Some(next) = session.compose_blame_segments(&reduced, &older, &env.options)? else {
                return Ok(None);
            };
            reduced = next;
        }
        Ok(session.finish_blame_segments(reduced, request, &env.options)?)
    })();
    stats.reduce = started.elapsed();
    stats.sampled_capacity_bytes = stats.sampled_capacity_bytes.max(budget.used());
    match &result {
        Ok(Some(_)) => reducing.completed(),
        Ok(None) => reducing.declined(),
        Err(error) if admission(error.as_ref()) => reducing.declined(),
        Err(_) => {}
    }
    result
}

pub(in crate::blame) fn report_verification(
    phase: &str,
    counts: git_rs_core::pack::ranges::VerificationStats,
) {
    metrics::timing_details(format_args!(
        "blame_segment_verification phase={phase} sha1_calls={} sha1_bytes={} sha256_bytes={} journal_hits={} avoided_sha1_bytes={} journal_records={} mismatches={}",
        counts.sha1_calls,
        counts.sha1_bytes,
        counts.sha256_bytes,
        counts.journal_hits,
        counts.avoided_sha1_bytes,
        counts.journal_records,
        counts.journal_mismatches,
    ));
}
