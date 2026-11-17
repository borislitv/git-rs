//! Public Gitoxide attribution probe, not a native porcelain producer.
#![forbid(unsafe_code)]

use std::{ops::Range, path::PathBuf};

use gix_diff::blob::{self, pipeline};
use gix_hash::ObjectId;
use gix_object::{
    FindExt, Kind,
    bstr::{BStr, BString},
};

/// An upstream group candidate, with zero-based, end-exclusive line ranges.
#[derive(Debug, PartialEq, Eq)]
pub struct AttributedRange {
    pub commit: ObjectId,
    pub source: Range<u32>,
    pub target: Range<u32>,
    pub source_path: BString,
}

/// Complete line coverage, not a guarantee of native attribution or metadata.
#[derive(Debug)]
pub struct Annotation {
    pub blob: Vec<u8>,
    pub ranges: Vec<AttributedRange>,
    /// Upstream traversal diagnostics. Absence of an edge does not prove absence
    /// of a porcelain `previous` field; a record is not a native origin trace.
    pub debug_path: Option<Vec<gix_blame::BlamePathEntry>>,
}

/// Prove a same-path regular-file predecessor in the sole physical parent.
/// `None` means this helper has no proof, not that native porcelain omits
/// `previous`. It does not search for renames or select a merge parent.
pub fn same_path_previous(
    objects: &(impl gix_object::Find + gix_object::FindHeader),
    commit: ObjectId,
    path: &BStr,
) -> gix_error::Result<Option<ObjectId>> {
    let mut buf = Vec::new();
    let (parent, child_tree) = {
        let child = objects.find_commit(commit.as_ref(), &mut buf)?;
        let mut parents = child.parents();
        match (parents.next(), parents.next()) {
            (Some(parent), None) => (parent, child.tree()),
            _ => return Ok(None),
        }
    };
    let parent_tree = objects.find_commit(parent.as_ref(), &mut buf)?.tree();
    if regular_blob_at(objects, child_tree, path, &mut buf)?
        && regular_blob_at(objects, parent_tree, path, &mut buf)?
    {
        return Ok(Some(parent));
    }
    Ok(None)
}

fn regular_blob_at(
    objects: &(impl gix_object::Find + gix_object::FindHeader),
    mut tree: ObjectId,
    path: &BStr,
    buf: &mut Vec<u8>,
) -> gix_error::Result<bool> {
    let path: &[u8] = path.as_ref();
    let mut components = path.split(|&b| b == b'/').peekable();
    while let Some(component) = components.next() {
        // Do not use a lookup that discards malformed entries or treats an
        // unavailable subtree as an absent path.
        let mut matched = None;
        for entry in objects.find_tree_iter(tree.as_ref(), buf)? {
            let entry = entry?;
            let name: &[u8] = entry.filename.as_ref();
            if name == component
                && matched
                    .replace((entry.mode, entry.oid.to_owned()))
                    .is_some()
            {
                gix_error::bail!(gix_error::validation("ambiguous blame tree path"));
            }
        }
        let Some((mode, id)) = matched else {
            return Ok(false);
        };
        if components.peek().is_none() {
            if !mode.is_blob() {
                return Ok(false);
            }
            let header = objects.try_header(id.as_ref())?.ok_or_else(|| {
                gix_error::validation("blame file entry points to a missing object")
            })?;
            if header.kind != Kind::Blob {
                gix_error::bail!(gix_error::validation("blame file entry is not a blob"));
            }
            return Ok(true);
        }
        if !mode.is_tree() {
            return Ok(false);
        }
        tree = id;
    }
    Ok(false)
}

/// Run whole-file, immutable-commit blame against an object provider. No worktree
/// roots, repository configuration, attributes, filters or diff drivers are loaded.
pub fn annotate(
    objects: &(impl gix_object::Find + gix_object::FindHeader),
    commit: ObjectId,
    path: &BStr,
) -> gix_error::Result<Annotation> {
    let mut resources = blob::Platform::new(
        Default::default(),
        blob::Pipeline::new(
            pipeline::WorktreeRoots::default(),
            gix_filter::Pipeline::new(Default::default(), commit.kind(), Default::default()),
            Vec::new(),
            pipeline::Options {
                large_file_threshold_bytes: 0,
                ..Default::default()
            },
        ),
        pipeline::Mode::ToGit,
        gix_worktree::Stack::new(
            PathBuf::new(),
            gix_worktree::stack::State::AttributesStack(Default::default()),
            gix_glob::pattern::Case::Sensitive,
            Vec::new(),
            Vec::new(),
        ),
    );
    let out = gix_blame::file(
        objects,
        gix_blame::Start::Commit(commit),
        None,
        &mut resources,
        path,
        gix_blame::Options {
            diff_algorithm: blob::Algorithm::Myers,
            ranges: gix_blame::BlameRanges::WholeFile,
            since: None,
            rewrites: Some(gix_diff::Rewrites {
                copies: None,
                percentage: Some(0.5),
                limit: 1000,
                track_empty: false,
            }),
            debug_track_path: true,
        },
    )?;

    let lines = out.blob.iter().filter(|&&b| b == b'\n').count()
        + out.blob.last().is_some_and(|&b| b != b'\n') as usize;
    let mut next = 0;
    let mut ranges = Vec::with_capacity(out.entries.len());
    for entry in out.entries {
        let len = entry.len.get();
        let source_path = entry.source_file_name.unwrap_or_else(|| path.to_owned());
        let (Some(target_end), Some(source_end)) = (
            entry.start_in_blamed_file.checked_add(len),
            entry.start_in_source_file.checked_add(len),
        ) else {
            gix_error::bail!(gix_error::validation("blame range overflow"));
        };
        if entry.start_in_blamed_file != next
            || target_end as usize > lines
            || entry.commit_id.is_null()
            || source_path.is_empty()
        {
            gix_error::bail!(gix_error::validation(
                "invalid attributed range or line coverage"
            ));
        }
        ranges.push(AttributedRange {
            commit: entry.commit_id,
            source: entry.start_in_source_file..source_end,
            target: next..target_end,
            source_path,
        });
        next = target_end;
    }
    if next as usize != lines {
        gix_error::bail!(gix_error::validation(
            "blame did not cover the complete blob"
        ));
    }
    Ok(Annotation {
        blob: out.blob,
        ranges,
        debug_path: out.blame_path,
    })
}
