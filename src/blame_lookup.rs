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

pub(super) fn body(
    env: &Environment,
    source: &Arc<Source>,
    session: &SnapshotSession<LazySnapshot>,
    principal: &str,
    request: BlameRequest<'_>,
    exact_key: &ArtifactKey,
    control: &Control,
) -> Result<Option<(CompleteArtifact, Phase)>> {
    let Some(cache) = env.derived.as_ref().filter(|_| env.body_lookup) else {
        return Ok(None);
    };
    let artifacts = BlameArtifacts::new(cache);
    let read = |key: &ArtifactKey| -> Result<_> {
        let body = super::optional_cache(control, || {
            let access = Arc::new(SourceAccess {
                source: source.clone(),
                key: key.clone(),
            });
            if let Some(writer) = &env.writer {
                artifacts.get_body_authenticated(key, access, |manifest, tag| {
                    writer.verify_body(key, manifest, tag, control)
                })
            } else {
                // This is the explicit trusted-writer cache mode, for exact lookup only.
                artifacts.get_verified(key, ArtifactKind::CompleteBody, access)
            }
        })?;
        // A cache-side failure can be optional; loss of the source is not.
        source.check()?;
        Ok(body.flatten())
    };
    if let Some(body) = read(exact_key)? {
        return Ok(Some((body, Phase::CacheHit)));
    }
    if env.writer.is_none() {
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
    Ok(read(&parent_key)?.map(|body| (body, Phase::ParentBodyHit)))
}
