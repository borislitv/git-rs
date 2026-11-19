//! Immutable signed seeds. Directory existence is a lookup hint, never access
//! or producer authority. A composed result is not a new certified seed.
use super::batch::WriterProof;
use super::{
    Request, SourceAccess, codec_runtime_failure, completed, derive::Environment, memo::Profile,
    optional_cache_for,
};
use crate::derived_cache::{
    ArtifactKey, BlameArtifacts, BlameBuilds, BuildLimits, BuildLookup, KeyParts,
};
use crate::repository::blame_source::{LazySnapshot, Source};
use crate::{Result, invalid, process};
use git_rs_core::{
    CapacityBudget, Control, SnapshotSession,
    blame::{BlameOptions, BlameRequest, ReplacementBlame},
};
use std::{sync::Arc, time::Duration};

const MAX_SUFFIX: usize = 100;

pub(super) enum Discovery {
    Miss,
    Legacy(ReplacementBlame),
    Completed(completed::Imported),
}

fn key(
    env: &Environment,
    source: &Source,
    principal: &str,
    request: BlameRequest<'_>,
    profile: &Profile,
) -> Result<ArtifactKey> {
    ArtifactKey::new(KeyParts {
        repository: source.snapshot.id.as_bytes(),
        security: principal.as_bytes(),
        object_format: "sha1",
        kind: "blame-replacement-seed-v1",
        inputs: &[
            &source.snapshot.storage_epoch.to_be_bytes(),
            request.commit.as_bytes(),
            request.path,
        ],
        options: env.options.canonical_bytes(),
        representation: "fixed-coordinate-complete-attribution-v1",
        producer: &profile.id,
    })
}

pub(super) fn enabled(env: &Environment) -> bool {
    env.body_lookup
        && env.writer.is_some()
        && env.derived.is_some()
        && env.options.canonical_bytes() == BlameOptions::default().canonical_bytes()
}

/// One bounded, nearest-first pass for an already authorized request. Probe a
/// candidate *before* advancing from it: collecting a batch of ancestor OIDs
/// would read commits below a usable base. Only exact raw-path candidates are
/// discovered; there is no independent rename heuristic. Normal blame handles
/// renames, merges and other discovery misses.
pub(super) fn discover(
    env: &Environment,
    source: &Arc<Source>,
    session: &SnapshotSession<LazySnapshot>,
    principal: &str,
    request: BlameRequest<'_>,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<Discovery> {
    let legacy = enabled(env) && !request.path.contains(&b'/');
    let general = env.reuse && completed::enabled(env);
    if !legacy && !general {
        return Ok(Discovery::Miss);
    }
    let (Some(cache), Some(writer)) = (&env.derived, &env.writer) else {
        return Ok(Discovery::Miss);
    };
    if session.identity() != &source.identity() {
        return Err(invalid("blame discovery source mismatch"));
    }
    let profile = Profile::verified(&env.options, env.resources)?;
    let limits = env.resources.read_limits(&env.options);
    let ceiling = MAX_SUFFIX
        .min(limits.max_graph_edges)
        .min(limits.max_graph_objects.saturating_sub(1));
    let allowance =
        (control.remaining().min(process::remaining()?) / 4).min(Duration::from_secs(10));
    let result = optional_cache_for(control, allowance, || {
        // At most two canonical keys, hash query parameters and a scoped read
        // exist at any depth. Never retain a full suffix of candidate keys.
        let _keys = budget.reservation(8 * 8192)?;
        let artifacts = BlameArtifacts::new(cache);
        let mut cursor = request.commit.to_owned();
        for depth in 0..=ceiling {
            control.check()?;
            process::check_operation()?;
            let candidate = BlameRequest {
                commit: &cursor,
                path: request.path,
            };
            // At the same base preserve the legacy specialized response when
            // its original physical suffix proof succeeds. It cannot suppress
            // general lookup on a miss or at a nearer base.
            let legacy_key = (legacy && depth != 0)
                .then(|| key(env, source, principal, candidate, &profile))
                .transpose()?;
            // The caller has already probed depth zero, before parent-body lookup.
            let completed_key = (general && depth != 0)
                .then(|| completed::key(env, source, principal, candidate, &profile))
                .transpose()?;
            // Query both at one depth together. A failed legacy hit gets one
            // exact general read, not a duplicate hint query.
            let hint = match (legacy_key.as_ref(), completed_key.as_ref()) {
                (Some(legacy), Some(map)) => cache.first_ready_hint(&[legacy, map])?,
                (Some(key), None) | (None, Some(key)) => cache.first_ready_hint(&[key])?,
                (None, None) => None,
            };
            if let Some(index) = hint {
                if index == 0
                    && let Some(key) = legacy_key.as_ref()
                {
                    match try_legacy(
                        &artifacts, writer, source, session, key, candidate, request, control,
                    ) {
                        Ok(Some(result)) => return Ok(Discovery::Legacy(result)),
                        Ok(None) => {}
                        Err(error) if codec_runtime_failure(error.as_ref()) => return Err(error),
                        Err(_) => {
                            control.check()?;
                            process::check_operation()?;
                        }
                    }
                }
                if let Some(key) = completed_key.as_ref()
                    && let Some(map) =
                        completed::load(env, source, &artifacts, candidate, key, control, budget)?
                {
                    return Ok(Discovery::Completed(map));
                }
            }
            if depth == ceiling {
                break;
            }
            let Some(parent) = session.completed_candidate_parent(candidate)? else {
                break;
            };
            cursor = parent;
        }
        Ok(Discovery::Miss)
    })?;
    source.check()?;
    Ok(result.unwrap_or(Discovery::Miss))
}

#[allow(clippy::too_many_arguments)] // Exact requested source, key and suffix proof.
fn try_legacy(
    artifacts: &BlameArtifacts,
    writer: &WriterProof,
    source: &Arc<Source>,
    session: &SnapshotSession<LazySnapshot>,
    key: &ArtifactKey,
    base: BlameRequest<'_>,
    target: BlameRequest<'_>,
    control: &Control,
) -> Result<Option<ReplacementBlame>> {
    let access = Arc::new(SourceAccess {
        source: source.clone(),
        key: key.clone(),
    });
    let Some(artifact) = artifacts.get_body_authenticated(key, access, |manifest, tag| {
        writer.verify_body(key, manifest, tag, control)
    })?
    else {
        return Ok(None);
    };
    let seed = artifact.with_chunks(|chunks| match chunks {
        [bytes] => Ok(session.decode_authenticated_replacement_seed(base, bytes)?),
        _ => Ok(None),
    })?;
    let Some(seed) = seed else {
        return Ok(None);
    };
    drop(artifact);
    Ok(session.reuse_replacement(target, &seed, super::MAX_OUTPUT as usize)?)
}

pub(super) fn publish(
    env: &Environment,
    source: &Arc<Source>,
    principal: &str,
    request: &Request,
    bytes: &[u8],
    control: &Control,
) -> Result<()> {
    if !enabled(env) {
        return Ok(());
    }
    let (Some(cache), Some(writer)) = (&env.derived, &env.writer) else {
        return Ok(());
    };
    let profile = Profile::verified(&env.options, env.resources)?;
    let commit = gix::ObjectId::from(request.commit.0);
    let key = key(
        env,
        source,
        principal,
        BlameRequest {
            commit: &commit,
            path: &request.path,
        },
        &profile,
    )?;
    let access = Arc::new(SourceAccess {
        source: source.clone(),
        key: key.clone(),
    });
    let seconds = control
        .remaining()
        .min(process::remaining()?)
        .as_secs()
        .min(15) as i32;
    if seconds == 0 {
        return Ok(());
    }
    if let BuildLookup::Claimed(build) = BlameBuilds::new(cache).claim(
        &key,
        access,
        BuildLimits {
            hard_seconds: seconds,
            lease_seconds: seconds,
        },
    )? {
        build.run_with_renewal(|_| {
            let artifacts = BlameArtifacts::new(cache);
            if let Some(chunk) = artifacts.stage_chunk(&build, bytes)? {
                artifacts.publish_body_authenticated(&build, &[chunk], |key, manifest| {
                    writer.body_tag(key, manifest, control)
                })?;
            }
            Ok(())
        })?;
    }
    Ok(())
}
