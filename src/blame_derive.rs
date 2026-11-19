//! One foreground/background derivation with one admitted shared workset.
use super::{
    Body, Bytes, Observer, Outcome, Phase, Request, SourceAccess, artifact_key,
    batch::{BatchSnapshot, WriterProof},
    codec_runtime_failure, completed, lookup,
    memo::{Checkpoints, Profile, WorkSet},
    observe, optional_cache, optional_cache_for, publish_body, replacement,
};
use crate::{
    Result,
    blob::Store,
    cache::Cache,
    derived_cache::{
        Access, BlameBuilds, BuildLease, BuildLimits, BuildLookup, DerivedCache, MAX_CHUNK_BYTES,
    },
    invalid,
    metrics::{self, Metric, Stage, Timer},
    process,
    repository::blame_source::{LazySnapshot, Source},
};
use git_rs_core::{
    CapacityBudget, Control, SnapshotSession,
    blame::{BlameError, BlameOptions, BlameRequest, CompleteBlame, CompletedUse},
    porcelain::{CompletePorcelain, Porcelain},
};
use std::sync::Arc;

fn encode_complete(
    blame: &CompleteBlame,
    control: &Control,
    budget: &CapacityBudget,
) -> std::result::Result<CompletePorcelain, BlameError> {
    let timer = Timer::start(Metric::Stage(Stage::BlameEncode));
    let result = Porcelain::encode_complete(blame, control, budget, super::MAX_OUTPUT as usize);
    timer.finish(result.is_ok());
    result
}

#[path = "blame_segments.rs"]
pub(super) mod segments;

#[derive(Clone)]
pub(super) struct Environment {
    pub store: Store,
    pub files: Arc<Cache>,
    pub derived: Option<DerivedCache>,
    pub writer: Option<WriterProof>,
    pub observer: Option<Arc<dyn Observer>>,
    pub options: BlameOptions,
    pub resources: super::ResourceProfile,
    pub reuse: bool,
    pub body_lookup: bool,
}
pub(super) struct Expected {
    pub repository: String,
    pub generation: u64,
    pub epoch: u64,
    pub branch: Vec<u8>,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkStatistics {
    pub segments: segments::SegmentStatistics,
    pub commits_traversed: usize,
    /// Edges checked by complete-seed composition, separate from traversal.
    pub replacement_suffix_edges: usize,
    pub replacement_edges: usize,
    /// Final target lines actually attributed by an authenticated complete map.
    pub completed_map_lines: u32,
    /// A general-map lookup was attempted but did not supply attribution.
    pub completed_map_miss: bool,
    pub edges_visited: u64,
    pub mapping_hits: u64,
    pub fresh_mapping_computations: u64,
    pub rejected_mappings: u64,
    /// Complete units delivered by core, not successful retention/publication.
    pub completed_mappings: u64,
    /// Actual owned physical-object copies returned from the workset.
    pub objects_reused: u64,
    /// Copies loaded from authenticated checkpoints, excluding job-local reuse.
    pub imported_objects_reused: u64,
    /// Imported complete wire copies returned; core separately validates/counts hits.
    pub imported_mapping_loads: u64,
    /// Measured attribution/checkpoint wall time, excluding porcelain encoding.
    pub elapsed: std::time::Duration,
}

pub(super) fn derive(
    env: Environment,
    slug: String,
    principal: String,
    query: Request,
    expected: Option<Expected>,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<Outcome> {
    observe(&env.observer, Phase::Preparing, control)?;
    let operation = process::Operation::current()
        .ok_or_else(|| invalid("missing independent blame operation"))?;
    let source = Source::acquire(&slug, &principal, false, operation, budget)?;
    if let Some(expected) = &expected
        && (source.snapshot.id != expected.repository
            || source.snapshot.generation != expected.generation
            || source.snapshot.storage_epoch != expected.epoch
            || source.snapshot.refs.get(&expected.branch) != Some(&query.commit))
    {
        return Ok(Outcome::NotFound); // stale warm hint, no old push lease used
    }
    derive_source(
        env,
        source,
        principal,
        query,
        if expected.is_some() {
            Lane::LegacyHint
        } else {
            Lane::Foreground
        },
        control,
        budget,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Lane {
    Foreground,
    LegacyHint,
    Reconcile,
}

pub(super) fn derive_source(
    env: Environment,
    source: Arc<Source>,
    principal: String,
    query: Request,
    lane: Lane,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<Outcome> {
    let reconcile = lane == Lane::Reconcile;
    observe(&env.observer, Phase::SourceAcquired, control)?;
    let options = env.options.clone();
    let limits = env.resources.read_limits(&options);
    let identity = source.identity();
    let open_session = |work| -> Result<_> {
        let provider = if env.resources == super::ResourceProfile::BaselineV1 {
            LazySnapshot::new(
                source.clone(),
                env.store.clone(),
                env.files.clone(),
                budget.clone(),
                limits,
                control,
            )
        } else {
            LazySnapshot::new_with_resource_profile(
                source.clone(),
                env.store.clone(),
                env.files.clone(),
                budget.clone(),
                limits,
                env.resources,
                control,
            )
        }?
        .with_observer(env.observer.clone())
        .with_workset(work)?;
        SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), limits)
    };
    let commit = gix::ObjectId::from(query.commit.0);
    let request = BlameRequest {
        commit: &commit,
        path: &query.path,
    };
    let (preflight, reachable) = metrics::measure(Metric::Stage(Stage::BlamePreflight), || {
        let preflight = open_session(None)?;
        let reachable = lookup::reachable(&preflight, &source, commit, &env.observer, control)?;
        source.check()?;
        Ok((preflight, reachable))
    })?;
    if !reachable {
        return Ok(Outcome::NotFound);
    }
    let body_key = artifact_key(
        &source.snapshot,
        &principal,
        &query,
        &options,
        env.resources,
    )?;
    let read_body = || lookup::exact_body(&env, &source, &body_key, control);
    let claim_body = || {
        let seconds = control.remaining().as_secs().min(3600) as i32;
        if let Some(cache) = env.derived.as_ref().filter(|_| seconds > 0) {
            optional_cache(control, || {
                BlameBuilds::new(cache).claim(
                    &body_key,
                    Arc::new(SourceAccess {
                        source: source.clone(),
                        key: body_key.clone(),
                    }),
                    BuildLimits {
                        hard_seconds: seconds,
                        lease_seconds: seconds.min(15),
                    },
                )
            })
        } else {
            Ok(None)
        }
    };
    let mut early_claim = None;
    let mut cached = read_body()?;
    if reconcile && cached.is_none() {
        early_claim = claim_body()?;
        match &early_claim {
            Some(BuildLookup::Claimed(_)) => {
                // A foreground writer might have completed between our miss and claim.
                cached = read_body()?;
            }
            Some(BuildLookup::Pending) => return Ok(Outcome::NotFound),
            None => return Err(invalid("warm cache build unavailable")),
        }
    }
    let attempt_map = !reconcile && env.reuse && completed::enabled(&env);
    let mut imported = None;
    let mut phase = Phase::CacheHit;
    if !reconcile && cached.is_none() {
        if attempt_map {
            imported = completed::exact(&env, &source, &principal, request, control, budget)?;
        }
        // An exact map may be the only surviving artifact for the target. Do
        // not read below it while looking for an unchanged-parent body.
        if imported.is_none() {
            cached = lookup::parent_body(&env, &source, &preflight, &principal, request, control)?;
            phase = Phase::ParentBodyHit;
        }
    }
    if let Some(body) = cached {
        drop(preflight);
        observe(&env.observer, phase, control)?;
        observe(&env.observer, Phase::Complete, control)?;
        source.check()?;
        return Ok(Outcome::Complete(Body {
            bytes: Bytes::Cached(body),
            source,
            work: None,
        }));
    }
    let started = std::time::Instant::now();
    // A reconciler which claimed the exact target must produce a locally
    // certifiable result. It must not re-sign a composed answer.
    let discovery = if !reconcile && imported.is_none() {
        replacement::discover(
            &env, &source, &preflight, &principal, request, control, budget,
        )?
    } else {
        replacement::Discovery::Miss
    };
    match discovery {
        replacement::Discovery::Miss => {}
        replacement::Discovery::Completed(found) => imported = Some(found),
        replacement::Discovery::Legacy(result) => {
            drop(preflight);
            let statistics = WorkStatistics {
                replacement_suffix_edges: result.suffix_edges,
                replacement_edges: result.replacement_edges,
                elapsed: started.elapsed(),
                ..WorkStatistics::default()
            };
            observe(&env.observer, Phase::ReplacementSeedHit, control)?;
            if let Some(observer) = &env.observer {
                observer.work_completed(lane != Lane::Foreground, statistics, control);
            }
            observe(&env.observer, Phase::Complete, control)?;
            source.check()?;
            return Ok(Outcome::Complete(Body {
                bytes: Bytes::Local(result.body),
                source,
                work: Some(statistics),
            }));
        }
    }
    let mut preflight = if !reconcile && env.writer.is_some() && env.derived.is_some() {
        // Release discovery state before admitting any attribution attempt.
        drop(preflight);
        None
    } else {
        Some(preflight)
    };
    let body_claim = if reconcile {
        early_claim
    } else {
        claim_body()?
    };
    let (body_build, phase) = match body_claim {
        Some(BuildLookup::Claimed(build)) => (Some(build), Phase::BodyBuildClaimed),
        Some(BuildLookup::Pending) => (None, Phase::BodyBuildPending),
        None => (None, Phase::BodyBuildUnavailable),
    };
    let Attempt {
        outcome,
        publishable,
        mut certificate,
    } = completed::with_capacity_fallback(
        imported,
        |candidate| {
            run_attempt(
                &env,
                &source,
                &principal,
                &query,
                request,
                lane,
                control,
                budget,
                preflight.take(),
                open_session,
                candidate,
                attempt_map,
                body_build.as_ref(),
                phase,
            )
        },
        || {
            control.check()?;
            source.check()
        },
    )?;
    source.check()?;
    let published = if let (
        Some(cache),
        Some(build),
        Outcome::Complete(Body {
            bytes: Bytes::Local(body),
            ..
        }),
    ) = (&env.derived, &body_build, &outcome)
        && publishable
    {
        // Do not let attribution retained solely for optional certification
        // crowd out the required body's staging buffer.
        if certificate.is_some() && budget.reservation(MAX_CHUNK_BYTES).is_err() {
            certificate = None;
        }
        let stage = || {
            publish_body(
                cache,
                build,
                body,
                env.writer.as_ref(),
                control,
                budget,
                &env.observer,
            )
        };
        if env.writer.is_some() && env.resources == super::ResourceProfile::LargeV1 {
            let deadline = build
                .deadline()
                .min(std::time::Instant::now() + super::body_stage_time(body.len()));
            super::optional_body_io(control, deadline, |operation| {
                super::with_body_renewal(build, operation, stage)
            })?
            .flatten()
                == Some(true)
        } else {
            optional_cache(control, stage)? == Some(true)
        }
    } else {
        false
    };
    if reconcile && matches!(&outcome, Outcome::Complete(_)) && !published {
        return Err(invalid("warm cache publication not confirmed"));
    }
    if published && let Some((session, blame)) = certificate {
        if completed::enabled(&env)
            && completed::publish_local(
                &env, &source, &session, &principal, request, &blame, control, budget,
            )?
        {
            observe(&env.observer, Phase::CompletedMapPublished, control)?;
        }
        if replacement::enabled(&env) {
            let allowance = (control.remaining() / 2).min(std::time::Duration::from_secs(15));
            let _ = optional_cache_for(control, allowance, || {
                if let Some(seed) = session.certify_replacement_seed(request, &options, &blame)? {
                    let wire = session.encode_replacement_seed(&seed)?;
                    replacement::publish(
                        &env,
                        &source,
                        &principal,
                        &query,
                        wire.as_slice(),
                        control,
                    )?;
                }
                Ok(())
            })?;
        }
    }
    control.check()?;
    if matches!(&outcome, Outcome::Complete(_)) {
        observe(&env.observer, Phase::Complete, control)?;
    }
    source.check()?;
    Ok(outcome)
}

struct Attempt {
    outcome: Outcome,
    publishable: bool,
    certificate: Option<(SnapshotSession<LazySnapshot>, CompleteBlame)>,
}

#[allow(clippy::too_many_arguments)] // Same live source, admission, and ordinary body claim across attempts.
fn run_attempt(
    env: &Environment,
    source: &Arc<Source>,
    principal: &str,
    query: &Request,
    request: BlameRequest<'_>,
    lane: Lane,
    control: &Control,
    budget: &CapacityBudget,
    preflight: Option<SnapshotSession<LazySnapshot>>,
    open_session: impl FnOnce(Option<WorkSet>) -> Result<SnapshotSession<LazySnapshot>>,
    mut imported: Option<completed::Imported>,
    attempt_map: bool,
    body_build: Option<&BuildLease>,
    phase: Phase,
) -> Result<Attempt> {
    let reconcile = lane == Lane::Reconcile;
    let options = &env.options;
    let limits = env.resources.read_limits(options);
    let work = if !reconcile && env.writer.is_some() && env.derived.is_some() {
        let batch = if imported.is_some() {
            // Admission with an imported wire is speculative even at setup.
            Some(BatchSnapshot::new(budget.clone())?)
        } else {
            optional_cache(control, || BatchSnapshot::new(budget.clone()))?
        };
        batch.map(|batch| WorkSet::new(batch, env.resources, limits))
    } else {
        None
    };
    let session = match preflight {
        Some(session) => session,
        None => open_session(work.clone())?,
    };
    if work.is_some() {
        observe(&env.observer, Phase::WorkSetPrepared, control)?;
    }
    let mut checkpoints = if let (Some(cache), Some(writer), Some(work)) =
        (&env.derived, env.writer.as_ref(), work.as_ref())
    {
        let profile = Profile::verified(options, env.resources)?;
        let key =
            super::memo::checkpoint_key(&source.snapshot, principal, query, options, &profile)?;
        let access: Arc<dyn Access> = Arc::new(SourceAccess {
            source: source.clone(),
            key: key.clone(),
        });
        Some(Checkpoints::open(
            cache.clone(),
            writer.clone(),
            key,
            &profile,
            source.clone(),
            access,
            work.clone(),
            budget,
            control,
            env.observer.clone(),
            env.reuse,
        )?)
    } else {
        None
    };
    source.check()?;
    observe(&env.observer, phase, control)?;
    let checkpoint_build = checkpoints.as_ref().and_then(Checkpoints::build);
    let attempted = std::cell::Cell::new(false);
    let mut outcome = None;
    let mut certificate = None;
    let mut publishable = false;
    let result = {
        let mut compute = || -> Result<()> {
            attempted.set(true);
            let started = std::time::Instant::now();
            let speculative = imported.is_some();
            let mut segment_stats = segments::SegmentStatistics::default();
            let cold = env.resources == super::ResourceProfile::LargeV1
                && !speculative
                && match &work {
                    Some(work) => {
                        work.lock()
                            .map_err(|_| invalid("blame workset poisoned"))?
                            .revision
                            == 0
                    }
                    None => true,
                };
            let segmented = if cold {
                match segments::cold(env, source, request, control, budget, &mut segment_stats) {
                    Ok(result) => result,
                    Err(error)
                        if matches!(
                            error.downcast_ref::<BlameError>(),
                            Some(BlameError::BlobTooLarge)
                        ) =>
                    {
                        outcome = Some(Outcome::TooLarge);
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            let mut fallback = (segment_stats.attempted && segmented.is_none())
                .then(|| crate::metrics::BlameSegmentStage::Fallback.start(control));
            let (blamed, segment_body) = if let Some((blame, body)) = segmented {
                (Ok(blame), Some(body))
            } else {
                (
                    completed::attribute(
                        &session,
                        request,
                        options,
                        imported.take(),
                        |namespace, map| {
                            let mut memory = checkpoints.as_ref().map(Checkpoints::memory);
                            let memo = checkpoints.as_mut().zip(memory.as_mut()).map(
                                |(checkpoints, memory)| checkpoints.request(memory, env.reuse),
                            );
                            let observer = env
                                .observer
                                .as_ref()
                                .filter(|observer| observer.wants_attribution_progress());
                            let progress =
                                observer.map(|_| git_rs_core::blame::BlameProgress::new());
                            let attempt_started = observer.map(|_| std::time::Instant::now());
                            let timer = Timer::start(Metric::Stage(Stage::BlameAttribution));
                            let result = if attempt_map {
                                session.blame_complete_with_completed_observed(
                                    request,
                                    options,
                                    namespace,
                                    map,
                                    memo,
                                    progress.as_ref(),
                                )
                            } else {
                                session.blame_complete_observed(
                                    request,
                                    options,
                                    memo,
                                    progress.as_ref(),
                                )
                            };
                            timer.finish(result.is_ok());
                            if let (Some(observer), Some(progress), Some(started)) =
                                (observer, progress, attempt_started)
                            {
                                observer.attribution_progress(
                                    progress.snapshot(),
                                    started.elapsed(),
                                    result.is_ok(),
                                );
                            }
                            result
                        },
                    ),
                    None,
                )
            };
            if let Some(error) = checkpoints.as_ref().and_then(Checkpoints::take_failure) {
                return Err(error);
            }
            // Do not do optional final checkpoint I/O before unwinding a
            // failed map attempt or let that I/O mask its original error.
            let blamed = match blamed {
                Err(error)
                    if speculative
                        && !matches!(
                            error,
                            BlameError::NotFound
                                | BlameError::BlobTooLarge
                                | BlameError::OutputTooLarge
                        ) =>
                {
                    return Err(error.into());
                }
                other => other,
            };
            let mut statistics = WorkStatistics {
                segments: segment_stats,
                ..WorkStatistics::default()
            };
            if let Some(checkpoints) = &mut checkpoints {
                source.check()?;
                checkpoints.flush(true)?; // last complete units, even if porcelain later exceeds output bound
                let batch = checkpoints
                    .work
                    .lock()
                    .map_err(|_| invalid("blame workset poisoned"))?;
                statistics.objects_reused = batch.objects_reused;
                statistics.imported_objects_reused = batch.imported_objects_reused;
                statistics.imported_mapping_loads = batch.imported_mapping_loads;
            }
            outcome = Some(match blamed {
                Ok(blame) => {
                    let stats = blame.statistics();
                    statistics.commits_traversed = stats.commits_traversed;
                    statistics.edges_visited = stats.mapping.edges_visited;
                    statistics.mapping_hits = stats.mapping.hits;
                    statistics.fresh_mapping_computations = stats.mapping.computations;
                    statistics.rejected_mappings = stats.mapping.rejected;
                    statistics.completed_mappings = stats.mapping.completed;
                    // A capacity failure here is also speculative. Do not
                    // report a completed map hit for the attempt we discard.
                    statistics.elapsed = started.elapsed().saturating_sub(segment_stats.encode);
                    let encoded = match segment_body {
                        Some(body) => body,
                        None => match encode_complete(&blame, control, budget) {
                            Ok(body) => Some(body),
                            Err(BlameError::OutputTooLarge) => None,
                            Err(error) => return Err(error.into()),
                        },
                    };
                    match blame.completed_use() {
                        CompletedUse::Hit { lines } => {
                            statistics.completed_map_lines = lines;
                            observe(&env.observer, Phase::CompletedMapHit, control)?;
                        }
                        CompletedUse::Miss => statistics.completed_map_miss = true,
                        CompletedUse::NotRequested => {}
                    }
                    publishable = blame.can_certify_local_attribution();
                    if let Some(observer) = &env.observer {
                        observer.work_completed(lane != Lane::Foreground, statistics, control);
                    }
                    match encoded {
                        Some(body) => {
                            if body_build.is_some()
                                && publishable
                                && (completed::enabled(env) || replacement::enabled(env))
                            {
                                certificate = Some(blame);
                            }
                            Outcome::Complete(Body {
                                bytes: Bytes::Local(body),
                                source: source.clone(),
                                work: (checkpoints.is_some() || segment_stats.attempted)
                                    .then_some(statistics),
                            })
                        }
                        None => Outcome::TooLarge,
                    }
                }
                Err(BlameError::NotFound) => Outcome::NotFound,
                Err(BlameError::BlobTooLarge | BlameError::OutputTooLarge) => Outcome::TooLarge,
                Err(error) => return Err(error.into()),
            });
            if let Some(fallback) = &mut fallback {
                fallback.completed();
            }
            Ok(())
        };
        // Both optional cache leases remain bounded by the same original CPU/source
        // deadline. Heartbeats do I/O only; they neither spawn attribution nor reset it.
        let result = {
            let mut run = || -> Result<()> {
                if let Some(build) = &checkpoint_build {
                    build.run_with_renewal(|_| compute())
                } else {
                    compute()
                }
            };
            if let Some(build) = body_build {
                build.run_with_renewal(|_| run())
            } else {
                run()
            }
        };
        if !attempted.get() {
            if reconcile {
                return result.and_then(|_| Err(invalid("warm build did not start")));
            }
            if result
                .as_ref()
                .is_err_and(|error| codec_runtime_failure(error.as_ref()))
            {
                return Err(result.expect_err("matched runtime error"));
            }
            control.check()?;
            source.check()?;
            compute()?;
        }
        result
    }; // nested closures and their mutable borrows end before inspecting outcome
    if let Err(error) = result {
        if reconcile || codec_runtime_failure(error.as_ref()) {
            return Err(error);
        }
        control.check()?;
        source.check()?;
        if outcome.is_none() {
            return Err(error);
        }
    }
    let outcome = outcome.ok_or_else(|| invalid("missing complete Rust blame outcome"))?;
    source.check()?;
    Ok(Attempt {
        outcome,
        publishable,
        certificate: certificate.map(|blame| (session, blame)),
    })
}
