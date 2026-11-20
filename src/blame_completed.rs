//! Serving transport for complete native attribution maps. Cache ownership
//! authenticates bytes, never Git ancestry or permission to read a ref.
use super::{SourceAccess, derive::Environment, memo::Profile, optional_cache_for};
use crate::derived_cache::{
    ArtifactKey, BlameArtifacts, BlameBuilds, BuildLimits, BuildLookup, KeyParts, MAX_BODY_BYTES,
    MAX_CHUNK_BYTES, StagedChunk,
};
use crate::repository::blame_source::{LazySnapshot, Source};
use crate::{Result, invalid, process};
use git_rs_core::{
    CapacityBudget, CapacityBuffer, CapacityError, CapacityReservation, Control, SnapshotSession,
    blame::{BlameError, BlameRequest, CompleteBlame, CompletedBlameMap, MAX_COMPLETED_MAP_BYTES},
};
use std::{sync::Arc, time::Duration};

const MAX_WIRE: usize = if MAX_BODY_BYTES < MAX_COMPLETED_MAP_BYTES {
    MAX_BODY_BYTES
} else {
    MAX_COMPLETED_MAP_BYTES
};
const MAX_PARTS: usize = MAX_WIRE.div_ceil(MAX_CHUNK_BYTES);

pub(super) struct Imported {
    pub base_commit: gix::ObjectId,
    pub base_path: Vec<u8>,
    /// Digest of the complete *base* artifact key, for both decode and replay.
    pub namespace: [u8; 32],
    pub wire: CapacityBuffer,
    _path: CapacityReservation,
}

impl Imported {
    pub(super) fn base(&self) -> BlameRequest<'_> {
        BlameRequest {
            commit: &self.base_commit,
            path: &self.base_path,
        }
    }
}

pub(super) fn enabled(env: &Environment) -> bool {
    env.body_lookup && env.writer.is_some() && env.derived.is_some()
}

/// An exact map survives independent eviction of its porcelain body. Try it
/// before reading even the immediate parent for an unchanged-body proof.
pub(super) fn exact(
    env: &Environment,
    source: &Arc<Source>,
    principal: &str,
    request: BlameRequest<'_>,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<Option<Imported>> {
    let Some(cache) = env.derived.as_ref().filter(|_| env.reuse && enabled(env)) else {
        return Ok(None);
    };
    let profile = Profile::verified(&env.options, env.resources)?;
    let key = key(env, source, principal, request, &profile)?;
    let candidate = super::optional_cache(control, || {
        load(
            env,
            source,
            &BlameArtifacts::new(cache),
            request,
            &key,
            control,
            budget,
        )
    })?;
    source.check()?;
    Ok(candidate.flatten())
}

/// The caller owns recovery: retrying here would retain the failed attempt's
/// provider and WorkSet, even after releasing the decoded map and wire.
pub(super) fn attribute(
    session: &SnapshotSession<LazySnapshot>,
    request: BlameRequest<'_>,
    options: &git_rs_core::blame::BlameOptions,
    imported: Option<Imported>,
    run: impl FnOnce(
        [u8; 32],
        Option<&CompletedBlameMap<'_>>,
    ) -> std::result::Result<CompleteBlame, BlameError>,
) -> std::result::Result<CompleteBlame, BlameError> {
    if let Some(imported) = imported
        && let Some(map) = session.decode_authenticated_completed_blame(
            imported.base(),
            request,
            options,
            imported.namespace,
            &imported.wire,
        )?
    {
        return run(imported.namespace, Some(&map));
    }
    run([0; 32], None)
}

/// A candidate gets at most one attempt. Its owner moves into the attempt
/// along with any budget it pins; by the time the fence runs, the failed
/// attempt and its scoped cache heartbeats have unwound.
pub(super) fn with_capacity_fallback<M, T>(
    candidate: Option<M>,
    mut attempt: impl FnMut(Option<M>) -> Result<T>,
    fence: impl FnOnce() -> Result<()>,
) -> Result<T> {
    let had_candidate = candidate.is_some();
    match attempt(candidate) {
        Err(error) if had_candidate && is_admission_failure(error.as_ref()) => {
            fence()?;
            attempt(None)
        }
        result => result,
    }
}

fn is_admission_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        error.downcast_ref::<BlameError>(),
        Some(BlameError::Capacity(
            CapacityError::Allocation | CapacityError::Exhausted
        ))
    ) || matches!(
        error.downcast_ref::<CapacityError>(),
        Some(CapacityError::Allocation | CapacityError::Exhausted)
    ) || error.is::<std::collections::TryReserveError>()
}

#[cfg(test)]
#[expect(
    clippy::items_after_test_module,
    reason = "fallback contract tests adjacent to the retry implementation"
)]
mod tests {
    use super::*;

    #[test]
    fn completed_map_capacity_retry_starts_after_full_attempt_cleanup() -> Result<()> {
        let budget = CapacityBudget::new(128);
        let _source = budget.reservation(16)?;
        let wire = budget.buffer(48)?;
        // With the candidate, setup leaves too little capacity for the
        // provider. Without it, setup and the ordinary result fit together.
        let ordinary = with_capacity_fallback(
            Some(wire),
            |wire| {
                let _workset = budget.buffer(32)?;
                let mut provider = budget.buffer(64)?;
                assert!(wire.is_none());
                provider.extend_from_slice(b"ordinary")?;
                Ok(provider)
            },
            || {
                assert_eq!(budget.used(), 16);
                Ok(())
            },
        )?;
        assert_eq!(ordinary.as_slice(), b"ordinary");
        Ok(())
    }

    #[test]
    fn completed_map_retry_respects_authority_and_does_not_loop() {
        for reject in [false, true] {
            let mut attempts = 0;
            let error = with_capacity_fallback(
                Some(()),
                |_| -> Result<()> {
                    attempts += 1;
                    Err(BlameError::Capacity(CapacityError::Exhausted).into())
                },
                || {
                    if reject {
                        Err(invalid("source authority changed"))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
            assert_eq!(attempts, if reject { 1 } else { 2 });
            if reject {
                assert_eq!(error.to_string(), "source authority changed");
            }
        }
        let error = with_capacity_fallback(
            Some(()),
            |candidate| -> Result<()> {
                assert!(candidate.is_some());
                Err(invalid("source read failed"))
            },
            || panic!("source errors must not reach the retry fence"),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "source read failed");
    }
}

pub(super) fn key(
    env: &Environment,
    source: &Source,
    principal: &str,
    base: BlameRequest<'_>,
    profile: &Profile,
) -> Result<ArtifactKey> {
    ArtifactKey::new(KeyParts {
        repository: source.snapshot.id.as_bytes(),
        security: principal.as_bytes(),
        object_format: "sha1",
        kind: "blame-completed-attribution-v1",
        inputs: &[
            &source.snapshot.storage_epoch.to_be_bytes(),
            base.commit.as_bytes(),
            base.path,
        ],
        options: env.options.canonical_bytes(),
        representation: "native-root-completed-map-v2",
        producer: &profile.id,
    })
}

/// Called only for a hinted exact key computed for `base` in the live source.
pub(super) fn load(
    env: &Environment,
    source: &Arc<Source>,
    artifacts: &BlameArtifacts,
    base: BlameRequest<'_>,
    key: &ArtifactKey,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<Option<Imported>> {
    let Some(writer) = &env.writer else {
        return Ok(None);
    };
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

    // Authentication binds ordered lengths/digests; the artifact reader checks
    // each physical ordinal and pins all verified children during this copy.
    let total = artifact.len();
    if total == 0 || total > MAX_WIRE {
        return Ok(None);
    }
    artifact.with_chunks(|chunks| {
        if chunks.is_empty() || chunks.len() > MAX_PARTS {
            return Ok(None);
        }
        let size = chunks.iter().try_fold(0usize, |sum, part| {
            if part.is_empty() || part.len() > MAX_CHUNK_BYTES {
                return None;
            }
            sum.checked_add(part.len()).filter(|size| *size <= total)
        });
        if size != Some(total) {
            return Ok(None);
        }
        let path = budget.reservation(base.path.len())?;
        let mut wire = budget.buffer(total)?;
        for part in chunks.iter().flat_map(|part| part.chunks(32 * 1024)) {
            control.check()?;
            process::check_operation()?;
            wire.extend_from_slice(part)?;
        }
        Ok(Some(Imported {
            base_commit: base.commit.to_owned(),
            base_path: base.path.to_vec(),
            namespace: *key.digest(),
            wire,
            _path: path,
        }))
    })
}

/// Best-effort publication after a complete local result. Core is the signing
/// admission boundary: it validates the complete result's request, options,
/// producer and live resources, and rejects results that consumed a map.
/// No caller can supply import bytes to the signer.
#[allow(clippy::too_many_arguments)] // Explicit live source and CPU/capacity owners.
pub(super) fn publish_local<P: git_rs_core::VerifiedObjectProvider>(
    env: &Environment,
    source: &Arc<Source>,
    session: &SnapshotSession<P>,
    principal: &str,
    request: BlameRequest<'_>,
    complete: &CompleteBlame,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<bool> {
    let (Some(cache), Some(writer)) = (&env.derived, &env.writer) else {
        return Ok(false);
    };
    if !env.body_lookup || !complete.can_certify_local_attribution() {
        return Ok(false);
    }
    if session.identity() != &source.identity() {
        return Err(invalid("completed blame publication source mismatch"));
    }
    source.check()?;
    let allowance =
        (control.remaining().min(process::remaining()?) / 4).min(Duration::from_secs(15));
    let result = optional_cache_for(control, allowance, || {
        let profile = Profile::verified(&env.options, env.resources)?;
        let key = key(env, source, principal, request, &profile)?;
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
            return Ok(false);
        }
        let BuildLookup::Claimed(build) = BlameBuilds::new(cache).claim(
            &key,
            access,
            BuildLimits {
                hard_seconds: seconds,
                lease_seconds: seconds,
            },
        )?
        else {
            return Ok(false);
        };
        build.run_with_renewal(|_| {
            let wire = session.encode_completed_blame(
                request,
                &env.options,
                *key.digest(),
                complete,
                MAX_WIRE,
            )?;
            build.check_local()?;
            let part_count = wire.as_slice().len().div_ceil(MAX_CHUNK_BYTES);
            let _parts = budget.reservation(part_count * (size_of::<StagedChunk>() + 256))?;
            let mut chunks = Vec::with_capacity(part_count);
            let artifacts = BlameArtifacts::new(cache);
            for bytes in wire.as_slice().chunks(MAX_CHUNK_BYTES) {
                control.check()?;
                process::check_operation()?;
                let Some(chunk) = artifacts.stage_chunk(&build, bytes)? else {
                    return Ok(false);
                };
                chunks.push(chunk);
            }
            artifacts.publish_body_authenticated(&build, &chunks, |key, manifest| {
                writer.body_tag(key, manifest, control)
            })
        })
    })?;
    source.check()?;
    Ok(result.unwrap_or(false))
}
