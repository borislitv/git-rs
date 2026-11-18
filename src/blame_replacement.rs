//! Immutable signed seeds. Directory existence is a lookup hint, never access
//! or producer authority. A composed result is not a new certified seed.
use super::{Request, SourceAccess, derive::Environment, memo::Profile, optional_cache_for};
use crate::derived_cache::{
    ArtifactKey, BlameArtifacts, BlameBuilds, BuildLimits, BuildLookup, KeyParts,
};
use crate::protocol::Oid;
use crate::repository::blame_source::{LazySnapshot, Source};
use crate::{Result, invalid};
use git_rs_core::{
    CapacityBudget, Control, SnapshotSession,
    blame::{BlameOptions, BlameRequest, ReplacementBlame},
};
use std::sync::Arc;

fn key(
    env: &Environment,
    source: &Source,
    principal: &str,
    request: &Request,
    profile: &Profile,
) -> Result<ArtifactKey> {
    ArtifactKey::new(KeyParts {
        repository: source.snapshot.id.as_bytes(),
        security: principal.as_bytes(),
        object_format: "sha1",
        kind: "blame-replacement-seed-v1",
        inputs: &[
            &source.snapshot.storage_epoch.to_be_bytes(),
            &request.commit.0,
            &request.path,
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

pub(super) fn lookup(
    env: &Environment,
    source: &Arc<Source>,
    session: &SnapshotSession<LazySnapshot>,
    principal: &str,
    request: BlameRequest<'_>,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<Option<ReplacementBlame>> {
    if !enabled(env) {
        return Ok(None);
    }
    let (Some(cache), Some(writer)) = (&env.derived, &env.writer) else {
        return Ok(None);
    };
    let profile = Profile::verified(&env.options, env.resources)?;
    let allowance = (control.remaining() / 4).min(std::time::Duration::from_secs(10));
    let result = optional_cache_for(control, allowance, || {
        // Charge the bounded candidate keys, index entries and query parameters.
        let _index = budget.reservation(128 * (request.path.len() + 1024))?;
        let artifacts = BlameArtifacts::new(cache);
        let mut cursor = request.commit.to_owned();
        let mut candidate = Request {
            commit: Oid([0; 20]),
            path: request.path.to_vec(),
        };
        // Probe the immediate parent before going deeper. Geometric batches
        // amortize the database lookup while bounding source work at 100 edges.
        for width in [1, 7, 24, 68] {
            let mut candidates = Vec::with_capacity(width);
            for _ in 0..width {
                crate::process::check_operation()?;
                let Some(parent) = session.replacement_candidate_parent(
                    BlameRequest {
                        commit: &cursor,
                        path: request.path,
                    },
                    cursor.as_ref() == request.commit,
                )?
                else {
                    break;
                };
                cursor = parent;
                candidate.commit = Oid(cursor.as_slice().try_into()?);
                candidates.push((parent, key(env, source, principal, &candidate, &profile)?));
            }
            if candidates.is_empty() {
                break;
            }
            let keys: Vec<_> = candidates.iter().map(|(_, key)| key).collect();
            if let Some(index) = cache.first_ready_hint(&keys)? {
                let (id, key) = &candidates[index];
                let access = Arc::new(SourceAccess {
                    source: source.clone(),
                    key: key.clone(),
                });
                if let Some(artifact) =
                    artifacts.get_body_authenticated(key, access, |manifest, tag| {
                        writer.verify_body(key, manifest, tag, control)
                    })?
                {
                    let seed = artifact.with_chunks(|chunks| {
                        let [bytes] = chunks else {
                            return Err(invalid("invalid blame replacement seed chunks"));
                        };
                        Ok(session.decode_authenticated_replacement_seed(
                            BlameRequest {
                                commit: id,
                                path: request.path,
                            },
                            bytes,
                        )?)
                    })?;
                    if let Some(seed) = seed {
                        return Ok(session.reuse_replacement(
                            request,
                            &seed,
                            super::MAX_OUTPUT as usize,
                        )?);
                    }
                }
            }
            if candidates.len() != width {
                break;
            }
        }
        Ok(None)
    })?;
    source.check()?;
    Ok(result.flatten())
}

pub(super) fn publish(
    env: &Environment,
    source: &Arc<Source>,
    principal: &str,
    request: &Request,
    bytes: &[u8],
    control: &Control,
) -> Result<()> {
    let (Some(cache), Some(writer)) = (&env.derived, &env.writer) else {
        return Ok(());
    };
    let profile = Profile::verified(&env.options, env.resources)?;
    let key = key(env, source, principal, request, &profile)?;
    let access = Arc::new(SourceAccess {
        source: source.clone(),
        key: key.clone(),
    });
    let seconds = control.remaining().as_secs().min(15) as i32;
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
