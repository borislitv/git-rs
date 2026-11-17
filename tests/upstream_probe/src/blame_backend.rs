//! Public Gitoxide attribution probe, not a native porcelain producer.
#![forbid(unsafe_code)]

use std::{
    ops::{ControlFlow, Range},
    path::PathBuf,
};

use gix_diff::{
    blob::{self, pipeline},
    tree_with_rewrites::{ChangeRef, Options},
};
use gix_error::ResultExt;
use gix_hash::{ObjectId, oid};
use gix_object::{
    Data, FindExt, Kind, TreeRefIter,
    bstr::{BStr, BString},
    tree::{EntryKind, EntryRef},
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
}

/// Prove a changed same-path blob in the sole physical parent. Regular and
/// executable files are one class; symlinks must be symlinks on both sides.
/// `None` means this helper has no proof, not that native porcelain omits
/// `previous`. It does not search for renames or select a merge parent.
pub fn same_path_previous(
    objects: &impl gix_object::Find,
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
    if let (Some((child_class, child_id)), Some((parent_class, parent_id))) = (
        blob_entry_at(objects, child_tree, path, &mut buf)?,
        blob_entry_at(objects, parent_tree, path, &mut buf)?,
    ) && child_class == parent_class
        && child_id != parent_id
    {
        for id in [child_id, parent_id] {
            let object = objects.find(id.as_ref(), &mut buf)?;
            if object.kind != Kind::Blob {
                gix_error::bail!(gix_error::validation("blame file entry is not a blob"));
            }
            object.verify_checksum(id.as_ref())?;
        }
        return Ok(Some(parent));
    }
    Ok(None)
}

fn blob_entry_at(
    objects: &impl gix_object::Find,
    mut tree: ObjectId,
    path: &BStr,
    buf: &mut Vec<u8>,
) -> gix_error::Result<Option<(EntryKind, ObjectId)>> {
    let path: &[u8] = path.as_ref();
    let mut components = path.split(|&b| b == b'/').peekable();
    while let Some(component) = components.next() {
        if component.is_empty() {
            return Ok(None);
        }
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
            return Ok(None);
        };
        if components.peek().is_none() {
            let class = match mode.kind() {
                EntryKind::Blob | EntryKind::BlobExecutable => EntryKind::Blob,
                EntryKind::Link => EntryKind::Link,
                _ => return Ok(None),
            };
            return Ok(Some((class, id)));
        }
        if !mode.is_tree() {
            return Ok(None);
        }
        tree = id;
    }
    Ok(None)
}

/// Positive evidence for an edited rename of the origin, not a general rename
/// resolver. Unproven cases (including merges) return `None`.
pub fn rename_previous(
    objects: &(impl gix_object::Find + gix_object::FindHeader),
    origin: ObjectId,
    path: &BStr,
) -> gix_error::Result<Option<(ObjectId, BString)>> {
    let (mut old, mut new) = (Vec::new(), Vec::new());
    let (parent, child_tree) = {
        let Some(child) = bounded_data(objects, &origin, Kind::Commit, 64 * 1024, &mut new)? else {
            return Ok(None);
        };
        let child = gix_object::CommitRef::from_bytes(child.data, origin.kind())?;
        let mut parents = child.parents();
        match (parents.next(), parents.next()) {
            (Some(parent), None) => (parent, child.tree()),
            _ => return Ok(None),
        }
    };
    let Some(base) = bounded_data(objects, &parent, Kind::Commit, 64 * 1024, &mut old)? else {
        return Ok(None);
    };
    let base_tree = gix_object::CommitRef::from_bytes(base.data, parent.kind())?.tree();
    let (Some(lhs), Some(rhs)) = (
        bounded_data(objects, &base_tree, Kind::Tree, 4096, &mut old)?,
        bounded_data(objects, &child_tree, Kind::Tree, 4096, &mut new)?,
    ) else {
        return Ok(None);
    };
    let (lhs, rhs) = (
        TreeRefIter::from_bytes(lhs.data, origin.kind()),
        TreeRefIter::from_bytes(rhs.data, origin.kind()),
    );
    let (Some(src), Some(dst)) = (one_regular(lhs)?, one_regular(rhs)?) else {
        return Ok(None);
    };
    if dst.filename != path || src.filename == path || src.oid == dst.oid {
        return Ok(None);
    }
    let (mut before, mut after) = (Vec::new(), Vec::new());
    let (Some(a), Some(b)) = (
        bounded_data(objects, src.oid, Kind::Blob, 8192, &mut before)?,
        bounded_data(objects, dst.oid, Kind::Blob, 8192, &mut after)?,
    ) else {
        return Ok(None);
    };
    if !span_majority(a.data, b.data) {
        return Ok(None);
    }
    let mut changes = 0;
    let mut matched = false;
    let _ = gix_diff::tree_with_rewrites(
        lhs, rhs, &mut raw_platform(origin.kind()), &mut Default::default(), objects,
        |change| {
            changes += 1;
            matched = matches!(change, ChangeRef::Rewrite {
                source_location, source_entry_mode, source_id, entry_mode, id, location,
                copy: false, similarity: Some(_), ..
            } if source_location == src.filename && source_entry_mode == src.mode && source_id.as_ref() == src.oid
                && location == dst.filename && entry_mode == dst.mode && id.as_ref() == dst.oid);
            Ok(ControlFlow::Continue(()))
        },
        Options { location: Some(gix_diff::tree::recorder::Location::Path), rewrites: Some(rewrites()) },
    ).or_error()?;
    Ok((changes == 1 && matched).then(|| (parent, src.filename.to_owned())))
}

fn one_regular(mut tree: TreeRefIter<'_>) -> gix_error::Result<Option<EntryRef<'_>>> {
    let Some(entry) = tree.next().transpose()? else {
        return Ok(None);
    };
    let name: &[u8] = entry.filename.as_ref();
    Ok((tree.next().transpose()?.is_none()
        && entry.mode.is_blob()
        && !name.is_empty()
        && !name.contains(&b'/'))
    .then_some(entry))
}

fn bounded_data<'a>(
    objects: &(impl gix_object::Find + gix_object::FindHeader),
    id: &oid,
    kind: Kind,
    cap: u64,
    buf: &'a mut Vec<u8>,
) -> gix_error::Result<Option<Data<'a>>> {
    let header = objects
        .try_header(id)?
        .ok_or_else(|| gix_error::validation("missing rename object header"))?;
    if header.size > cap {
        return Ok(None);
    }
    let data = objects.find(id, buf)?;
    if header.kind != kind || data.kind != kind || data.data.len() as u64 != header.size {
        gix_error::bail!(gix_error::validation("rename object kind or size mismatch"));
    }
    data.verify_checksum(id)?;
    Ok(Some(data))
}

fn span_majority(a: &[u8], b: &[u8]) -> bool {
    // Git v2.56.0 diffcore-delta counts through LF (or 64 bytes) and
    // restarts at LF. CR-free matching complete spans lower-bound copied
    // bytes regardless of any hash collisions. Count only disjoint spans.
    if a.contains(&b'\r') || b.contains(&b'\r') {
        return false;
    }
    let common = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    let prefix = a[..common]
        .iter()
        .rposition(|&c| c == b'\n')
        .map_or(0, |i| i + 1);
    fn last_complete_line(bytes: &[u8]) -> Option<&[u8]> {
        bytes.strip_suffix(b"\n")?.rsplit(|&c| c == b'\n').next()
    }
    let suffix = match (
        last_complete_line(&a[prefix..]),
        last_complete_line(&b[prefix..]),
    ) {
        (Some(x), Some(y)) if x == y => x.len() + 1,
        _ => 0,
    };
    prefix + suffix > a.len().max(b.len()) / 2
}

fn rewrites() -> gix_diff::Rewrites {
    gix_diff::Rewrites {
        copies: None,
        percentage: Some(0.5),
        limit: 1000,
        track_empty: false,
    }
}

fn raw_platform(hash: gix_hash::Kind) -> blob::Platform {
    blob::Platform::new(
        Default::default(),
        blob::Pipeline::new(
            pipeline::WorktreeRoots::default(),
            gix_filter::Pipeline::new(Default::default(), hash, Default::default()),
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
    )
}

/// Run whole-file, immutable-commit blame against an object provider. No worktree
/// roots, repository configuration, attributes, filters or diff drivers are loaded.
pub fn annotate(
    objects: &(impl gix_object::Find + gix_object::FindHeader),
    commit: ObjectId,
    path: &BStr,
) -> gix_error::Result<Annotation> {
    let out = gix_blame::file(
        objects,
        gix_blame::Start::Commit(commit),
        None,
        &mut raw_platform(commit.kind()),
        path,
        gix_blame::Options {
            diff_algorithm: blob::Algorithm::Myers,
            ranges: gix_blame::BlameRanges::WholeFile,
            since: None,
            rewrites: Some(rewrites()),
            debug_track_path: false,
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
    })
}
