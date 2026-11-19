//! Reachability and complete-body lookup under the current leased source.
use super::{Observer, Phase, Request, SourceAccess, artifact_key, derive::Environment, observe};
use crate::{
    Result,
    derived_cache::{ArtifactKey, ArtifactKind, BlameArtifacts, CompleteArtifact},
    protocol::Oid,
    repository::blame_source::{LazySnapshot, Source},
};
use git_rs_core::{
    Control, SnapshotSession,
    blame::{BlameError, BlameRequest},
};
use std::sync::Arc;
use std::time::Instant;

pub(super) fn reachable(
    session: &SnapshotSession<LazySnapshot>,
    source: &Source,
    commit: gix::ObjectId,
    observer: &Option<Arc<dyn Observer>>,
    control: &Control,
) -> Result<bool> {
    let refs = &source.snapshot.refs;
    for (name, oid) in refs {
        control.check()?;
        if name.starts_with(b"refs/heads/") && gix::ObjectId::from(oid.0) == commit {
            if session.verify_published_blame_commit(&commit)? {
                observe(observer, Phase::PreflightTip, control)?;
                return Ok(true);
            }
            break;
        }
    }
    observe(observer, Phase::PreflightWalk, control)?;
    let reachable = session.reachable_from_refs(
        refs.iter()
            .map(|(name, oid)| (name.as_slice(), gix::ObjectId::from(oid.0))),
    )?;
    Ok(reachable.iter().any(|id| id == commit))
}

pub(super) fn exact_body(
    env: &Environment,
    source: &Arc<Source>,
    key: &ArtifactKey,
    control: &Control,
) -> Result<Option<CompleteArtifact>> {
    read_body(env, source, key, control, true)
}

fn read_body(
    env: &Environment,
    source: &Arc<Source>,
    key: &ArtifactKey,
    control: &Control,
    exact: bool,
) -> Result<Option<CompleteArtifact>> {
    let Some(cache) = env.derived.as_ref().filter(|_| env.body_lookup) else {
        return Ok(None);
    };
    let artifacts = BlameArtifacts::new(cache);
    let read = || {
        let access = Arc::new(SourceAccess {
            source: source.clone(),
            key: key.clone(),
        });
        if let Some(writer) = &env.writer {
            artifacts.get_body_authenticated(key, access, |manifest, tag| {
                writer.verify_body(key, manifest, tag, control)
            })
        } else {
            artifacts.get_verified(key, ArtifactKind::CompleteBody, access)
        }
    };
    let body = if exact && env.writer.is_some() && env.resources == super::ResourceProfile::LargeV1
    {
        let start = Instant::now();
        // No large window for absent, pending or unavailable keys. The hint
        // grants no source, cache or signing authority; normal authenticated
        // acquisition still verifies the root and every child under holds.
        let ready = super::optional_body_io(control, start + super::BODY_PROBE_TIME, |_| {
            Ok(cache.first_ready_hint(&[key])?.is_some())
        })?;
        if ready == Some(true) {
            super::optional_body_io(control, start + super::LARGE_BODY_READ_TIME, |_| read())?
        } else {
            None
        }
    } else {
        super::optional_cache(control, read)?
    };
    source.check()?;
    Ok(body.flatten())
}

pub(super) fn parent_body(
    env: &Environment,
    source: &Arc<Source>,
    session: &SnapshotSession<LazySnapshot>,
    principal: &str,
    request: BlameRequest<'_>,
    control: &Control,
) -> Result<Option<CompleteArtifact>> {
    if env.writer.is_none() || env.derived.is_none() || !env.body_lookup {
        return Ok(None);
    }
    // The physical proof is not an optional cache operation. The immediate
    // parent is authorized through the already reachable child in this source.
    let parent = match session.unchanged_blame_parent(request) {
        // Let the regular path report the public TooLarge outcome.
        Err(BlameError::BlobTooLarge) => None,
        result => result?,
    };
    source.check()?;
    let Some(parent) = parent else {
        return Ok(None);
    };
    let parent_request = Request {
        commit: Oid(parent.as_slice().try_into()?),
        path: request.path.to_vec(),
    };
    let parent_key = artifact_key(
        &source.snapshot,
        principal,
        &parent_request,
        &env.options,
        env.resources,
    )?;
    // Retain the original signed artifact and its holds, not a child publication.
    read_body(env, source, &parent_key, control, false)
}
