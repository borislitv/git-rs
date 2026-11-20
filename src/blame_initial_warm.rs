//! Explicit single-invocation initial warming. Only a promptly reduced local
//! summary crosses quanta; no snapshot, provider, cache claim or hold does.
use super::super::{
    Request, SourceAccess, artifact_key, body_stage_time, completed,
    derive::{self, Environment},
    lookup, optional_body_io, publish_body, with_body_renewal,
};
use crate::{
    Result,
    derived_cache::{BlameBuilds, BuildLimits, BuildLookup, MAX_CHUNK_BYTES},
    invalid,
    object_source::{RangeStore, WarmSource},
    process::Operation,
    repository::blame_source::Source,
};
use git_rs_core::{
    CapacityBudget, Control, SnapshotSession,
    blame::{BlameRequest, segments::LocalSegmentRun},
    porcelain::Porcelain,
};
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub total: Duration,
    quantum: Duration,
    slices: u64,
    edges: usize,
    lanes: usize,
    bytes: usize,
    pub range_bytes: usize,
    pause: Duration,
}

impl Limits {
    pub fn load(seconds: u64) -> Result<Self> {
        use super::setting;
        let pause = if std::env::var_os("GIT_RS_WARM_QUANTUM_PAUSE_MS").is_some() {
            setting("GIT_RS_WARM_QUANTUM_PAUSE_MS", 1, 5000)?
        } else {
            0
        };
        Ok(Self {
            total: Duration::from_secs(setting("GIT_RS_WARM_INITIAL_SECONDS", 600, 7200)?),
            quantum: Duration::from_secs(seconds),
            slices: setting("GIT_RS_WARM_INITIAL_SEGMENTS", 2048, 4096)?,
            edges: setting("GIT_RS_WARM_SEGMENT_EDGES", 256, 16384)? as usize,
            lanes: (setting("GIT_RS_WARM_INITIAL_LANES", 1, 16)? as usize)
                .min(thread::available_parallelism().map_or(1, |n| n.get())),
            bytes: (setting("GIT_RS_WARM_INITIAL_MIB", 4096, 32768)? as usize) << 20,
            range_bytes: (setting("GIT_RS_WARM_INITIAL_RANGE_MIB", 32, 1024)? as usize) << 20,
            pause: Duration::from_millis(pause),
        })
    }

    fn operation(self, end: Instant) -> Arc<Operation> {
        Operation::until(end.min(Instant::now() + self.quantum))
    }
}

struct Target {
    repo: String,
    branch: Vec<u8>,
    principal: String,
    query: Request,
    generation: u64,
    epoch: u64,
}

impl Target {
    fn bind(held: &WarmSource) -> Result<Self> {
        let s = &held.source.snapshot;
        let tip = *s
            .refs
            .get(&s.default_ref)
            .ok_or_else(|| invalid("missing warm branch tip"))?;
        Ok(Self {
            repo: s.id.clone(),
            branch: s.default_ref.clone(),
            principal: held.principal.clone(),
            query: Request::from_raw(tip, held.path.clone())?,
            generation: s.generation,
            epoch: s.storage_epoch,
        })
    }

    fn check(&self, held: &WarmSource) -> Result<()> {
        let s = &held.source.snapshot;
        if self.repo != s.id
            || self.branch != s.default_ref
            || self.principal != held.principal
            || self.query.path != held.path
            || self.generation != s.generation
            || self.epoch != s.storage_epoch
            || s.refs.get(&self.branch) != Some(&self.query.commit)
        {
            return Err(invalid("initial warm target changed between quanta"));
        }
        held.source.check()
    }
}

enum Slice {
    Skip,
    Ordinary,
    More(Option<gix::ObjectId>), // None means terminal summary; finalize in a new quantum
}

pub(super) fn run(env: &Environment, slot: u8, limits: Limits, end: Instant) -> Result<()> {
    let budget = CapacityBudget::new(limits.bytes);
    let read = env.resources.read_limits(&env.options);
    let mut receipt = LocalSegmentRun::default();
    let mut target: Option<Target> = None;
    let mut cursor = None;
    let mut traversed = 0usize;
    let mut segments = 0u64;
    for index in 0..limits.slices {
        if segments == limits.slices {
            break;
        }
        let width = limits.lanes.min((limits.slices - segments) as usize);
        let started = Instant::now();
        let before = traversed;
        let operation = limits.operation(end);
        let control = Control::new(operation.deadline());
        let result = operation.enter(|| -> Result<_> {
            control.check()?;
            let Some(held) = Source::acquire_warm(slot, operation.clone(), &budget)? else {
                return if target.is_none() {
                    Ok(Slice::Skip)
                } else {
                    Err(invalid("initial warm source unavailable between quanta"))
                };
            };
            if target.is_none() {
                target = Some(Target::bind(&held)?);
            }
            let scope = target.as_ref().expect("bound target");
            scope.check(&held)?;
            if index == 0 && is_ready(env, &held, scope, &control)? {
                return Ok(Slice::Skip);
            }
            let tip = gix::ObjectId::from(scope.query.commit.0);
            let current = cursor.as_ref().unwrap_or(&tip);
            let request = BlameRequest {
                commit: current,
                path: &scope.query.path,
            };
            let session = session(env, &held.source, &budget, &control)?;
            if index == 0 && !session.verify_published_blame_commit(&tip)? {
                return Ok(Slice::Ordinary);
            }
            let remaining = read
                .max_graph_edges
                .min(read.max_graph_objects.saturating_sub(1))
                .saturating_sub(traversed);
            let Some((windows, edges)) = plan_quantum(
                &session,
                request,
                &control,
                read.max_metadata_bytes,
                limits.edges,
                remaining,
                width,
            )?
            else {
                return Ok(Slice::Ordinary);
            };
            let next = windows
                .last()
                .expect("nonempty plan")
                .end
                .map(|end| end.commit);
            derive::segments::report_verification(
                "warm_planner",
                session.range_verification_stats(),
            );
            let result = if windows.len() == 1 {
                receipt.append(&session, request, windows[0].end, &env.options)?
            } else {
                let mut stats = derive::segments::SegmentStatistics::default();
                let Some(pieces) = derive::segments::execute(
                    &windows,
                    &control,
                    &budget,
                    |window| {
                        operation.enter(|| {
                            let producer = self::session(env, &held.source, &budget, &control)?;
                            let result = producer.summarize_blame_segment(
                                BlameRequest {
                                    commit: &window.start,
                                    path: request.path,
                                },
                                window.end,
                                &env.options,
                            );
                            derive::segments::report_verification(
                                "warm_producer",
                                producer.range_verification_stats(),
                            );
                            Ok(result?)
                        })
                    },
                    &mut stats,
                )?
                else {
                    return Ok(Slice::Ordinary);
                };
                for piece in pieces {
                    if !receipt.append_segment(&session, piece, &env.options)? {
                        return Ok(Slice::Ordinary);
                    }
                }
                true
            };
            derive::segments::report_verification(
                "warm_planner_and_producer",
                session.range_verification_stats(),
            );
            if !result {
                return Ok(Slice::Ordinary);
            }
            scope.check(&held)?;
            control.check()?;
            traversed += edges;
            segments += windows.len() as u64;
            Ok(Slice::More(next))
        });
        if std::env::var_os("GIT_RS_DEBUG_TIMINGS").as_deref() == Some(std::ffi::OsStr::new("1")) {
            let outcome = match &result {
                Ok(Slice::More(_)) => "completed",
                Ok(Slice::Skip) => "skipped",
                Ok(Slice::Ordinary) => "fallback",
                Err(_) => "failed",
            };
            eprintln!(
                "initial_warm_quantum index={} edges={} total_edges={traversed} elapsed_ms={} outcome={outcome} segments={segments} capacity_bytes={}",
                index + 1,
                traversed - before,
                started.elapsed().as_millis(),
                budget.used(),
            );
        }
        let result = result?;
        // No source/reader/build lease survives this boundary.
        match result {
            Slice::Skip => return Ok(()),
            Slice::Ordinary => {
                drop(receipt);
                return ordinary(
                    env,
                    slot,
                    limits,
                    end,
                    target.expect("bound target"),
                    &budget,
                );
            }
            Slice::More(next) => {
                std::thread::sleep(
                    limits
                        .pause
                        .min(end.saturating_duration_since(Instant::now())),
                );
                if let Some(next) = next {
                    cursor = Some(next);
                } else {
                    return finalize(
                        env,
                        slot,
                        limits,
                        end,
                        target.expect("bound target"),
                        &budget,
                        receipt,
                    );
                }
            }
        }
    }
    Err(invalid("initial warm segment count exhausted"))
}

fn plan_quantum<P: git_rs_core::VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    request: BlameRequest<'_>,
    control: &Control,
    metadata_limit: usize,
    edges_per_window: usize,
    remaining: usize,
    width: usize,
) -> Result<Option<(Vec<derive::segments::Endpoint>, usize)>> {
    let mut windows = Vec::with_capacity(width);
    let mut start = request.commit.to_owned();
    let mut traversed = 0;
    for _ in 0..width {
        let Some((end, edges)) = derive::segments::warm_window(
            session,
            BlameRequest {
                commit: &start,
                path: request.path,
            },
            control,
            metadata_limit,
            edges_per_window.min(remaining - traversed),
        )?
        else {
            return Ok(None);
        };
        if end.is_some() && edges == 0 {
            return Err(invalid("initial warm graph bound exceeded"));
        }
        windows.push(derive::segments::Endpoint { start, end });
        traversed += edges;
        match end {
            Some(endpoint) => start = endpoint.commit,
            None => break,
        }
    }
    Ok(Some((windows, traversed)))
}

fn session(
    env: &Environment,
    source: &Arc<Source>,
    budget: &CapacityBudget,
    control: &Control,
) -> Result<SnapshotSession<git_rs_core::pack::ranges::RangePackSet<RangeStore>>> {
    let read = env.resources.read_limits(&env.options);
    let provider = RangeStore::open_with_decoder_options(
        source.clone(),
        env.store.clone(),
        env.files.clone(),
        budget.clone(),
        read,
        git_rs_core::pack::DecoderOptions {
            cache_bytes: 192 << 20,
            ..env.resources.decoder_options()
        },
        control,
    )?
    .with_prepared_reads()
    .with_verification_journal(control)?;
    SnapshotSession::open(
        provider,
        &source.identity(),
        control.clone(),
        budget.clone(),
        read,
    )
}

fn is_ready(
    env: &Environment,
    held: &WarmSource,
    target: &Target,
    control: &Control,
) -> Result<bool> {
    let key = artifact_key(
        &held.source.snapshot,
        &target.principal,
        &target.query,
        &env.options,
        env.resources,
    )?;
    Ok(lookup::exact_body(env, &held.source, &key, control)?.is_some())
}

fn ordinary(
    env: &Environment,
    slot: u8,
    limits: Limits,
    end: Instant,
    target: Target,
    budget: &CapacityBudget,
) -> Result<()> {
    let operation = limits.operation(end);
    let control = Control::new(operation.deadline());
    operation.enter(|| {
        let held = Source::acquire_warm(slot, operation.clone(), budget)?
            .ok_or_else(|| invalid("initial warm fallback source unavailable"))?;
        target.check(&held)?;
        drop(derive::derive_source(
            env.clone(),
            held.source,
            target.principal,
            target.query,
            derive::Lane::Reconcile,
            &control,
            budget,
        )?);
        Ok(())
    })
}

fn finalize(
    env: &Environment,
    slot: u8,
    limits: Limits,
    end: Instant,
    target: Target,
    budget: &CapacityBudget,
    receipt: LocalSegmentRun,
) -> Result<()> {
    let operation = limits.operation(end);
    let control = Control::new(operation.deadline());
    operation.enter(|| {
        let held = Source::acquire_warm(slot, operation.clone(), budget)?
            .ok_or_else(|| invalid("initial warm final source unavailable"))?;
        target.check(&held)?;
        if is_ready(env, &held, &target, &control)? {
            return Ok(());
        }
        let source = &held.source;
        let cache = env
            .derived
            .as_ref()
            .ok_or_else(|| invalid("missing warm cache"))?;
        let writer = env
            .writer
            .as_ref()
            .ok_or_else(|| invalid("missing warm signing key"))?;
        let key = artifact_key(
            &source.snapshot,
            &target.principal,
            &target.query,
            &env.options,
            env.resources,
        )?;
        let seconds = control.remaining().as_secs() as i32;
        let BuildLookup::Claimed(build) = BlameBuilds::new(cache).claim(
            &key,
            Arc::new(SourceAccess {
                source: source.clone(),
                key: key.clone(),
            }),
            BuildLimits {
                hard_seconds: seconds,
                lease_seconds: seconds.min(15),
            },
        )?
        else {
            return Err(invalid("initial warm final body already claimed"));
        };
        let tip = gix::ObjectId::from(target.query.commit.0);
        let request = BlameRequest {
            commit: &tip,
            path: &target.query.path,
        };
        let certificate = optional_body_io(&control, build.deadline(), |owner| {
            with_body_renewal(&build, owner, || {
                let session = session(env, source, budget, &control)?;
                let blame = receipt
                    .finish(&session, request, &env.options)?
                    .ok_or_else(|| invalid("initial warm summary not complete"))?;
                if !blame.can_certify_local_attribution() {
                    return Err(invalid("initial warm summary is not locally attributable"));
                }
                let body = Porcelain::encode_complete(
                    &blame,
                    &control,
                    budget,
                    super::super::MAX_OUTPUT as usize,
                )?;
                // Retain local attribution for the optional map only if the
                // required body's staging buffer still fits.
                let certificate = budget
                    .reservation(MAX_CHUNK_BYTES)
                    .is_ok()
                    .then_some((session, blame));
                target.check(&held)?;
                let published = optional_body_io(
                    &control,
                    Instant::now() + body_stage_time(body.len()),
                    |_| {
                        publish_body(
                            cache,
                            &build,
                            &body,
                            Some(writer),
                            &control,
                            budget,
                            &env.observer,
                        )
                    },
                )?;
                if published != Some(true) {
                    return Err(invalid("initial warm body publication not confirmed"));
                }
                Ok(certificate)
            })
        })?
        .flatten()
        .ok_or_else(|| invalid("initial warm body publication not confirmed"))?;
        target.check(&held)?;
        if let Some((session, blame)) = certificate {
            completed::publish_local(
                env,
                source,
                &session,
                &target.principal,
                request,
                &blame,
                &control,
                budget,
            )?;
        }
        target.check(&held)
    })
}
