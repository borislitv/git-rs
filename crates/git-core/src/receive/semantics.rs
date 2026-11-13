use super::{nonnull, storage::Set};
use crate::objects::{Result, invalid};
use crate::{CapacityBudget, Control};
use gix_hash::ObjectId;
use gix_object::Kind;
use sha2::{Digest, Sha256};

// Observe the real maintained path checkpoints in unit tests without replacing
// either the validator or request Control. Integration callers have no test hook.
#[cfg(test)]
thread_local! {
    static PATH_CHECKPOINT: std::cell::RefCell<Option<Box<dyn Fn()>>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
pub(super) fn with_path_checkpoint<T>(observe: impl Fn() + 'static, run: impl FnOnce() -> T) -> T {
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            drop(PATH_CHECKPOINT.with(|slot| slot.replace(None)));
        }
    }
    PATH_CHECKPOINT.with(|slot| assert!(slot.replace(Some(Box::new(observe))).is_none()));
    let _clear = Clear;
    run()
}

/// Non-fatal diagnostic, retained separately from strict errors. Native-oracle
/// qualification of these INFO cases is required before normal receive cutover.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WarningKind {
    MissingTagger,
    BadTagName,
    BadFileMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Warning {
    pub object: ObjectId,
    pub kind: WarningKind,
}

#[derive(Clone, Copy)]
pub(super) struct Edge {
    pub id: ObjectId,
    pub kind: Kind,
    pub parent: bool,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn inspect(
    id: ObjectId,
    kind: Kind,
    bytes: &[u8],
    control: &Control,
    budget: &CapacityBudget,
    metadata_limit: usize,
    max_entries: usize,
    mut edge: impl FnMut(Edge) -> Result<()>,
    mut warning: impl FnMut(Warning) -> Result<()>,
) -> Result<()> {
    control.check()?;
    match kind {
        Kind::Blob => {}
        Kind::Commit => {
            let header = headers(bytes, metadata_limit, control)?;
            validate_headers(header, false)?;
            let _parser_capacity = parser_capacity(header.len(), budget)?;
            // Bounded header only: opaque 128 MiB messages and signatures are
            // never scanned by the maintained whole-object parser.
            let commit = gix_object::CommitRef::from_bytes(header, id.kind())?;
            signature(commit.author)?;
            signature(commit.committer)?;
            let tree = ObjectId::from_hex(commit.tree.as_ref())?;
            nonnull(&tree)?;
            edge(Edge {
                id: tree,
                kind: Kind::Tree,
                parent: false,
            })?;
            for parent in commit.parents {
                control.check()?;
                let parent = ObjectId::from_hex(parent.as_ref())?;
                nonnull(&parent)?;
                edge(Edge {
                    id: parent,
                    kind: Kind::Commit,
                    parent: true,
                })?;
            }
        }
        Kind::Tag => {
            let header = headers(bytes, metadata_limit, control)?;
            validate_headers(header, true)?;
            let _parser_capacity = parser_capacity(header.len(), budget)?;
            let tag = gix_object::TagRef::from_bytes(header, id.kind())?;
            if let Some(tagger) = tag.tagger {
                signature(tagger)?;
            } else {
                warning(Warning {
                    object: id,
                    kind: WarningKind::MissingTagger,
                })?;
            }
            if gix_validate::tag::name(tag.name).is_err() {
                warning(Warning {
                    object: id,
                    kind: WarningKind::BadTagName,
                })?;
            }
            let target = tag.target();
            nonnull(&target)?;
            edge(Edge {
                id: target,
                kind: tag.target_kind,
                parent: false,
            })?;
        }
        Kind::Tree => tree(bytes, id, control, budget, max_entries, edge, warning)?,
    }
    control.check()?;
    Ok(())
}

fn parser_capacity(length: usize, budget: &CapacityBudget) -> Result<crate::CapacityReservation> {
    // Each parent/header requires at least one input byte. 128 bytes per input
    // byte conservatively covers geometric SmallVec/Vec slot growth, unfolded
    // header copies and allocator-visible capacity for this <=64 KiB parser.
    // This is deliberately a peak reservation, not retained logical byte weight.
    let bytes = length
        .checked_mul(128)
        .ok_or_else(|| invalid("metadata parser capacity overflow"))?;
    Ok(budget.reservation(bytes)?)
}

fn headers<'a>(bytes: &'a [u8], limit: usize, control: &Control) -> Result<&'a [u8]> {
    let limit = limit.min(64 * 1024);
    let mut previous_lf = false;
    for (chunk_index, chunk) in bytes[..bytes.len().min(limit)].chunks(8192).enumerate() {
        control.check()?;
        for (index, &byte) in chunk.iter().enumerate() {
            if byte == 0 {
                return Err(invalid("NUL in object metadata header"));
            }
            if previous_lf && byte == b'\n' {
                return Ok(&bytes[..chunk_index * 8192 + index + 1]);
            }
            previous_lf = byte == b'\n';
        }
    }
    Err(invalid("missing or oversized object header separator"))
}

/// Enforce reserved header ordering/cardinality before the tolerant maintained
/// parser treats a repeated reserved field as an arbitrary extension header.
fn validate_headers(header: &[u8], tag: bool) -> Result<()> {
    let mut state = 0u8;
    let mut extra = false;
    let mut encoding = false;
    for line in header.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        if line[0] == b' ' {
            if !extra || tag {
                return Err(invalid("continuation of reserved metadata header"));
            }
            continue;
        }
        let space = line
            .iter()
            .position(|byte| *byte == b' ')
            .ok_or_else(|| invalid("invalid metadata header field"))?;
        let key = &line[..space];
        if tag {
            let expected: &[u8] = match state {
                0 => b"object",
                1 => b"type",
                2 => b"tag",
                3 => b"tagger",
                _ => return Err(invalid("extra or repeated tag header")),
            };
            if key != expected {
                return Err(invalid("invalid tag header order"));
            }
            state += 1;
        } else {
            match key {
                b"tree" if state == 0 => state = 1,
                b"parent" if state == 1 => {}
                b"author" if state == 1 => state = 2,
                b"committer" if state == 2 => state = 3,
                b"encoding" if state == 3 && !encoding && !extra => encoding = true,
                b"tree" | b"parent" | b"author" | b"committer" | b"encoding" => {
                    return Err(invalid("repeated or misplaced reserved commit header"));
                }
                _ if state == 3 => extra = true,
                _ => return Err(invalid("missing required commit header")),
            }
        }
    }
    if state < 3 {
        return Err(invalid("missing required metadata header"));
    }
    Ok(())
}

fn signature(raw: &gix_object::bstr::BStr) -> Result<()> {
    let mut remaining: &[u8] = raw.as_ref();
    let parsed = gix_actor::SignatureRef::from_bytes_consuming(&mut remaining)?;
    if !remaining.is_empty() {
        return Err(invalid("trailing actor signature bytes"));
    }
    // Actor decoding intentionally accepts malformed dates for ordinary reads.
    // Native strict receive requires an unsigned timestamp and four-digit timezone.
    let Some((seconds, zone)) = parsed.time.rsplit_once(' ') else {
        return Err(invalid("missing actor timestamp or timezone"));
    };
    let digits = seconds;
    if digits.is_empty()
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
        || digits.len() > 1 && digits.starts_with('0')
        || zone.len() != 5
        || !matches!(zone.as_bytes()[0], b'+' | b'-')
        || !zone.as_bytes()[1..].iter().all(u8::is_ascii_digit)
    {
        return Err(invalid("invalid actor timestamp or timezone"));
    }
    parsed.time()?;
    let bytes: &[u8] = raw.as_ref();
    let left = bytes.iter().position(|byte| *byte == b'<');
    let right = bytes.iter().position(|byte| *byte == b'>');
    match (left, right) {
        (Some(left), Some(right))
            if left > 0
                && bytes[left - 1] == b' '
                && right > left
                && bytes.get(right + 1) == Some(&b' ')
                && !bytes[left + 1..right].contains(&b'<')
                && !bytes[right + 1..].contains(&b'>') => {}
        _ => return Err(invalid("invalid actor identity delimiters")),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn tree(
    bytes: &[u8],
    id: ObjectId,
    control: &Control,
    budget: &CapacityBudget,
    max_entries: usize,
    mut edge: impl FnMut(Edge) -> Result<()>,
    mut warning: impl FnMut(Warning) -> Result<()>,
) -> Result<()> {
    let mut entries = gix_object::TreeRefIter::from_bytes(bytes, id.kind());
    let check = || {
        control
            .check()
            .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
    };
    let mut previous: Option<(&[u8], bool)> = None;
    let mut names = Set::new(budget.clone(), max_entries)?;
    let mut count = 0usize;
    loop {
        let start = entries.offset_to_next_entry(bytes);
        let Some(entry) = entries.next_controlled(&check)? else {
            break;
        };
        control.check()?;
        count += 1;
        if count > max_entries {
            return Err(invalid("tree entry limit exceeded"));
        }
        let mut mode_bytes = [0; 6];
        let mode: &[u8] = entry.mode.as_bytes(&mut mode_bytes).as_ref();
        if !bytes[start..].starts_with(mode) || bytes.get(start + mode.len()) != Some(&b' ') {
            return Err(invalid("noncanonical tree mode representation"));
        }
        let kind = match mode {
            b"40000" => Kind::Tree,
            b"100644" | b"100755" | b"120000" => Kind::Blob,
            b"160000" => Kind::Commit,
            _ if entry.mode.value() & 0o170000 == 0o100000 => {
                // Git 2.56 index-pack --strict reports badFilemode as a
                // nonfatal warning for these historical regular-file modes.
                warning(Warning {
                    object: id,
                    kind: WarningKind::BadFileMode,
                })?;
                Kind::Blob
            }
            _ => return Err(invalid("noncanonical or invalid tree mode")),
        };
        let name: &[u8] = entry.filename.as_ref();
        // Do not impose Windows filename restrictions on arbitrary Git bytes.
        // Keep the maintained HFS/NTFS .git and symlinked-.gitmodules checks.
        // The actual request checker must reach every maintained inner scan.
        gix_validate::path::component_controlled(
            entry.filename,
            (mode == b"120000").then_some(gix_validate::path::component::Mode::Symlink),
            gix_validate::path::component::Options {
                protect_windows: false,
                protect_hfs: true,
                protect_ntfs: true,
            },
            &|| {
                #[cfg(test)]
                PATH_CHECKPOINT.with(|slot| {
                    if let Some(observe) = slot.borrow().as_ref() {
                        observe();
                    }
                });
                check()
            },
        )?;
        let is_tree = kind == Kind::Tree;
        if let Some((prior, prior_tree)) = previous
            && name_order(prior, prior_tree, name, is_tree, control)? != std::cmp::Ordering::Less
        {
            return Err(invalid("tree entries not in canonical order"));
        }
        let mut hash = Sha256::new();
        for chunk in name.chunks(8192) {
            control.check()?;
            hash.update(chunk);
        }
        if !names.insert(<[u8; 32]>::from(hash.finalize()), control)? {
            return Err(invalid("duplicate tree filename"));
        }
        nonnull(entry.oid)?;
        if mode != b"160000" {
            edge(Edge {
                id: entry.oid.to_owned(),
                kind,
                parent: false,
            })?;
        }
        previous = Some((name, is_tree));
    }
    Ok(())
}

fn name_order(
    a: &[u8],
    a_tree: bool,
    b: &[u8],
    b_tree: bool,
    control: &Control,
) -> Result<std::cmp::Ordering> {
    let common = a.len().min(b.len());
    let mut offset = 0;
    while offset < common {
        control.check()?;
        let end = common.min(offset + 8192);
        let order = a[offset..end].cmp(&b[offset..end]);
        if order != std::cmp::Ordering::Equal {
            return Ok(order);
        }
        offset = end;
    }
    Ok(gix_object::tree::name_order(
        &a[common..],
        a_tree,
        &b[common..],
        b_tree,
    ))
}
