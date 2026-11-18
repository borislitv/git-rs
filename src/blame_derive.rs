//! One foreground/background derivation with one admitted shared workset.
use super::{
    Body, Bytes, Observer, Outcome, Phase, Request, SourceAccess, artifact_key,
    batch::{BatchSnapshot, WriterProof},
    codec_runtime_failure, lookup,
    memo::{Checkpoints, Profile, WorkSet},
    observe, optional_cache, optional_cache_for, publish_body, replacement,
};
use crate::{
    Result,
    blob::Store,
    cache::Cache,
    derived_cache::{Access, BlameBuilds, BuildLimits, BuildLookup, DerivedCache},
    invalid, process,
    repository::blame_source::{LazySnapshot, Source},
};
use git_rs_core::{
    CapacityBudget, Control, SnapshotSession,
    blame::{BlameError, BlameOptions, BlameRequest},
    porcelain::Porcelain,
};
use std::sync::Arc;

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
    pub commits_traversed: usize,
    /// Edges checked by complete-seed composition, separate from traversal.
    pub replacement_suffix_edges: usize,
    pub replacement_edges: usize,
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
        .with_workset(work)?;
        SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), limits)
    };
    let preflight = open_session(None)?;
    let commit = gix::ObjectId::from(query.commit.0);
    let request = BlameRequest {
        commit: &commit,
        path: &query.path,
    };
    let reachable = lookup::reachable(&preflight, &source, commit, &env.observer, control)?;
    source.check()?;
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
    if let Some((body, phase)) = lookup::body(
        &env, &source, &preflight, &principal, request, &body_key, control,
    )? {
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
    if let Some(result) = replacement::lookup(
        &env, &source, &preflight, &principal, request, control, budget,
    )? {
        drop(preflight);
        let statistics = WorkStatistics {
            replacement_suffix_edges: result.suffix_edges,
            replacement_edges: result.replacement_edges,
            elapsed: started.elapsed(),
            ..WorkStatistics::default()
        };
        observe(&env.observer, Phase::ReplacementSeedHit, control)?;
        if let Some(observer) = &env.observer {
            observer.work_completed(expected.is_some(), statistics, control);
        }
        observe(&env.observer, Phase::Complete, control)?;
        source.check()?;
        return Ok(Outcome::Complete(Body {
            bytes: Bytes::Local(result.body),
            source,
            work: Some(statistics),
        }));
    }
    let (session, work) = if env.writer.is_some() && env.derived.is_some() {
        // Release the preflight catalog and decoder before admitting the WorkSet.
        drop(preflight);
        let work = optional_cache(control, || BatchSnapshot::new(budget.clone()))?
            .map(|batch| WorkSet::new(batch, env.resources, limits));
        let session = open_session(work.clone())?;
        if work.is_some() {
            observe(&env.observer, Phase::WorkSetPrepared, control)?;
        }
        (session, work)
    } else {
        (preflight, None)
    };
    let mut checkpoints = if let (Some(cache), Some(writer), Some(work)) =
        (&env.derived, env.writer.as_ref(), work.as_ref())
    {
        let profile = Profile::verified(&options, env.resources)?;
        let key =
            super::memo::checkpoint_key(&source.snapshot, &principal, &query, &options, &profile)?;
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
    let seconds = control.remaining().as_secs().min(3600) as i32;
    let body_claim = if let Some(cache) = env.derived.as_ref().filter(|_| seconds > 0) {
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
        })?
    } else {
        None
    };
    let (body_build, phase) = match body_claim {
        Some(BuildLookup::Claimed(build)) => (Some(build), Phase::BodyBuildClaimed),
        Some(BuildLookup::Pending) => (None, Phase::BodyBuildPending),
        None => (None, Phase::BodyBuildUnavailable),
    };
    observe(&env.observer, phase, control)?;
    let checkpoint_build = checkpoints.as_ref().and_then(Checkpoints::build);
    let attempted = std::cell::Cell::new(false);
    let mut outcome = None;
    let mut seed = None;
    let result = {
        let mut compute = || -> Result<()> {
            attempted.set(true);
            let started = std::time::Instant::now();
            let blamed = if let Some(checkpoints) = &mut checkpoints {
                let mut memory = checkpoints.memory();
                session.blame_complete_with_memo(
                    request,
                    &options,
                    checkpoints.request(&mut memory, env.reuse),
                )
            } else {
                session.blame_complete(request, &options)
            };
            if let Some(error) = checkpoints.as_ref().and_then(Checkpoints::take_failure) {
                return Err(error);
            }
            let mut statistics = WorkStatistics::default();
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
                    statistics.elapsed = started.elapsed();
                    if let Some(observer) = &env.observer {
                        observer.work_completed(expected.is_some(), statistics, control);
                    }
                    match Porcelain::encode_complete(
                        &blame,
                        control,
                        budget,
                        super::MAX_OUTPUT as usize,
                    ) {
                        Ok(body) => {
                            if body_build.is_some() && replacement::enabled(&env) {
                                let allowance = (control.remaining() / 2)
                                    .min(std::time::Duration::from_secs(15));
                                seed = optional_cache_for(control, allowance, || {
                                    session
                                        .certify_replacement_seed(request, &options, &blame)?
                                        .map(|seed| session.encode_replacement_seed(&seed))
                                        .transpose()
                                        .map_err(Into::into)
                                })?
                                .flatten();
                                source.check()?;
                            }
                            Outcome::Complete(Body {
                                bytes: Bytes::Local(body),
                                source: source.clone(),
                                work: checkpoints.as_ref().map(|_| statistics),
                            })
                        }
                        Err(BlameError::OutputTooLarge) => Outcome::TooLarge,
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(BlameError::NotFound) => Outcome::NotFound,
                Err(BlameError::BlobTooLarge | BlameError::OutputTooLarge) => Outcome::TooLarge,
                Err(error) => return Err(error.into()),
            });
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
            if let Some(build) = &body_build {
                build.run_with_renewal(|_| run())
            } else {
                run()
            }
        };
        if !attempted.get() {
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
        if codec_runtime_failure(error.as_ref()) {
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
    if let (
        Some(cache),
        Some(build),
        Outcome::Complete(Body {
            bytes: Bytes::Local(body),
            ..
        }),
    ) = (&env.derived, &body_build, &outcome)
    {
        let _ = optional_cache(control, || {
            publish_body(
                cache,
                build,
                body,
                env.writer.as_ref(),
                control,
                budget,
                &env.observer,
            )
        })?;
    }
    if let Some(seed) = seed {
        let _ = optional_cache(control, || {
            replacement::publish(&env, &source, &principal, &query, seed.as_slice(), control)
        })?;
        source.check()?;
    }
    control.check()?;
    if matches!(&outcome, Outcome::Complete(_)) {
        observe(&env.observer, Phase::Complete, control)?;
    }
    source.check()?;
    Ok(outcome)
}
