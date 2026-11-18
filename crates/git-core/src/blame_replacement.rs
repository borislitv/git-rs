//! Compose a completed native attribution across a certified linear suffix.
use super::*;
use crate::porcelain::{self, CompletePorcelain};

const MAX_SUFFIX: usize = 100;
const MAX_SEED_COMMITS: usize = 25_000;
const MAX_FILE: usize = 16 * 1024;
const MAX_LINES: usize = 256;
const MAX_METADATA: usize = 64 * 1024;
const WIRE: &[u8; 8] = b"GRREPL01";
const FIXED: usize = 8 + 20 + 20 + 2 + 2;

/// In-memory certificate made from completed attribution and verified source
/// objects. A caller still owns authorization, leases and publication fences.
pub struct ReplacementSeed {
    base: ObjectId,
    root: ObjectId,
    path: CapacityBuffer,
    root_metadata: CapacityBuffer,
    slot: usize,
    lines: usize,
}

pub struct ReplacementBlame {
    pub body: CompletePorcelain,
    pub suffix_edges: usize,
    pub replacement_edges: usize,
}

struct Step {
    parent: Option<ObjectId>,
    blob: ObjectId,
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Propose one physical, same-raw-path ancestry edge for completed-map
    /// discovery. Verify the current commit and its exact path entry, but do
    /// not read the parent or require a singleton tree or an unchanged blob.
    /// The caller must already authorize the starting commit and bound the
    /// walk. Ordinary blame still proves applicability (including the base
    /// blob and any rename) before it can consume a completed map.
    pub fn completed_candidate_parent(
        &self,
        request: BlameRequest<'_>,
    ) -> Result<Option<ObjectId>, BlameError> {
        self.validate_blame_request(request)?;
        if self.limits.max_graph_objects < 2 || self.limits.max_graph_edges == 0 {
            return Ok(None);
        }
        let prefix = {
            let header = self.blame_commit_header(request.commit)?;
            let object = self.blame_commit(request.commit, header)?;
            self.blame_commit_prefix(request.commit, &object)?
        };
        let Some((tree, Some(parent))) = prefix else {
            return Ok(None);
        };
        Ok(self
            .blame_path_entry(tree, request.path, true)?
            .map(|_| parent))
    }

    /// Run ordinary blame once and certify its eligible stable origins. The
    /// proof requires one canonical regular file in each flat tree and no merges.
    pub fn prepare_replacement_seed(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
    ) -> Result<Option<ReplacementSeed>, BlameError> {
        let complete = self.blame_complete(request, options)?;
        self.certify_replacement_seed(request, options, &complete)
    }

    /// Reuse an attribution already produced by this job instead of running
    /// blame again. The path must also match every attributed origin.
    pub fn certify_replacement_seed(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        complete: &CompleteBlame,
    ) -> Result<Option<ReplacementSeed>, BlameError> {
        self.validate_blame_request(request)?;
        if options.canonical_bytes() != BlameOptions::default().canonical_bytes()
            || !complete.can_certify_local_attribution()
            || request.path.contains(&b'/')
            || request.commit != complete.target.as_ref()
            || !complete.uses_resources(&self.control, &self.capacity)
            || complete.trace().len() > 3
            || complete.blob().len() > MAX_FILE
        {
            return Ok(None);
        }
        let Some(lines) = distinct_lines(complete.blob()) else {
            return Ok(None);
        };
        let count = lines.len;
        // Decline most results before rereading any ancestry. Stable origins
        // have one owner at their own coordinates and at most two groups.
        let trace = complete.trace();
        if trace.len() < 2
            || !trace.iter().any(|row| row.group_count == 1)
            || trace.iter().any(|row| {
                let path: &[u8] = row.source_file_name.as_ref();
                path != request.path
            })
        {
            return Ok(None);
        }
        let Some(base) = self.replacement_step(request.commit, request.path)? else {
            return Ok(None);
        };
        let Some(blob) = self.replacement_blob(&base.blob)? else {
            return Ok(None);
        };
        if blob.as_slice() != complete.blob() {
            return Ok(None);
        }
        drop(blob);

        let mut root = request.commit.to_owned();
        let mut parent = base.parent;
        let mut edges = 0;
        while let Some(id) = parent {
            if edges + 1 >= MAX_SEED_COMMITS.min(self.limits.max_graph_objects)
                || edges >= self.limits.max_graph_edges
            {
                return Ok(None);
            }
            let Some(step) = self.replacement_step(&id, request.path)? else {
                return Ok(None);
            };
            parent = step.parent;
            root = id;
            edges += 1;
        }
        let mut slot = None;
        for row in complete.trace() {
            self.control.check()?;
            let path: &[u8] = row.source_file_name.as_ref();
            if path != request.path {
                return Ok(None);
            }
            if row.commit_id == root {
                if row.previous.is_some() || row.source_range != row.final_range {
                    return Ok(None);
                }
            } else if row.group_count != 1 || slot.replace(row.final_range.start as usize).is_some()
            {
                return Ok(None);
            }
        }
        let (Some(slot), Some(metadata)) = (slot, complete.metadata(&root)) else {
            return Ok(None);
        };
        if metadata.len() > MAX_METADATA {
            return Ok(None);
        }
        Ok(Some(ReplacementSeed {
            base: request.commit.to_owned(),
            root,
            path: self.replacement_copy(request.path)?,
            root_metadata: self.replacement_copy(metadata)?,
            slot,
            lines: count,
        }))
    }

    /// Propose a physical candidate parent for cache lookup. This is not a
    /// proof that a seed is usable; reuse verifies the suffix independently.
    pub fn replacement_candidate_parent(
        &self,
        request: BlameRequest<'_>,
        screen_path: bool,
    ) -> Result<Option<ObjectId>, BlameError> {
        self.validate_blame_request(request)?;
        if request.path.contains(&b'/') {
            return Ok(None);
        }
        if !screen_path {
            return Ok(self
                .replacement_commit(request.commit)?
                .and_then(|(_, parent)| parent));
        }
        let Some(step) = self.replacement_step(request.commit, request.path)? else {
            return Ok(None);
        };
        Ok(
            (self.blame_blob_header(&step.blob)?.size <= MAX_FILE as u64)
                .then_some(step.parent)
                .flatten(),
        )
    }

    /// Write an independently signed cache payload. The signer must bind the
    /// full base request, source epoch, producer and default NativeRootV1 profile.
    pub fn encode_replacement_seed(
        &self,
        seed: &ReplacementSeed,
    ) -> Result<CapacityBuffer, BlameError> {
        let mut bytes = self
            .capacity
            .buffer(FIXED + seed.root_metadata.as_slice().len())?;
        bytes.resize_controlled(
            FIXED + seed.root_metadata.as_slice().len(),
            0,
            &self.control,
        )?;
        let dest = bytes.as_mut_slice();
        dest[..8].copy_from_slice(WIRE);
        dest[8..28].copy_from_slice(seed.base.as_slice());
        dest[28..48].copy_from_slice(seed.root.as_slice());
        dest[48..50].copy_from_slice(&(seed.slot as u16).to_be_bytes());
        dest[50..52].copy_from_slice(&(seed.lines as u16).to_be_bytes());
        for (dst, src) in dest[FIXED..]
            .chunks_mut(8192)
            .zip(seed.root_metadata.as_slice().chunks(8192))
        {
            self.control.check()?;
            dst.copy_from_slice(src);
        }
        Ok(bytes)
    }

    /// Only import producer-authenticated bytes scoped to exactly `request`.
    /// Syntax and a matching base OID do not authenticate a certificate.
    pub fn decode_authenticated_replacement_seed(
        &self,
        request: BlameRequest<'_>,
        bytes: &[u8],
    ) -> Result<Option<ReplacementSeed>, BlameError> {
        self.validate_blame_request(request)?;
        if bytes.len() <= FIXED
            || bytes.len() > FIXED + MAX_METADATA
            || !bytes.starts_with(WIRE)
            || &bytes[8..28] != request.commit.as_bytes()
        {
            return Ok(None);
        }
        let root = ObjectId::from(<[u8; 20]>::try_from(&bytes[28..48]).expect("fixed-width field"));
        let slot = usize::from(u16::from_be_bytes([bytes[48], bytes[49]]));
        let lines = usize::from(u16::from_be_bytes([bytes[50], bytes[51]]));
        if root.is_null()
            || root.as_ref() == request.commit
            || slot >= lines
            || !(2..=MAX_LINES).contains(&lines)
        {
            return Ok(None);
        }
        Ok(Some(ReplacementSeed {
            base: request.commit.to_owned(),
            root,
            slot,
            lines,
            path: self.replacement_copy(request.path)?,
            root_metadata: self.replacement_copy(&bytes[FIXED..])?,
        }))
    }

    /// Reuse the stable origins from `seed`. Check each changed edge's unique
    /// whole-file alignment, regenerate groups, and write with the normal
    /// porcelain encoder. No source below the base is consulted.
    pub fn reuse_replacement(
        &self,
        request: BlameRequest<'_>,
        seed: &ReplacementSeed,
        max_bytes: usize,
    ) -> Result<Option<ReplacementBlame>, BlameError> {
        self.validate_blame_request(request)?;
        if request.path != seed.path.as_slice() || request.commit == seed.base.as_ref() {
            return Ok(None);
        }
        let Some(mut step) = self.replacement_step(request.commit, request.path)? else {
            return Ok(None);
        };
        let Some(target) = self.replacement_blob(&step.blob)? else {
            return Ok(None);
        };
        let (mut current, mut current_blob) = (request.commit.to_owned(), None::<VerifiedObject>);
        let (mut edges, mut replacements, mut newest) = (0, 0, None);
        let ceiling = MAX_SUFFIX
            .min(self.limits.max_graph_edges)
            .min(self.limits.max_graph_objects.saturating_sub(1));
        while current != seed.base {
            if edges == ceiling {
                return Ok(None);
            }
            let Some(parent) = step.parent else {
                return Ok(None);
            };
            let Some(next) = self.replacement_step(&parent, request.path)? else {
                return Ok(None);
            };
            if next.blob != step.blob {
                let Some(old) = self.replacement_blob(&next.blob)? else {
                    return Ok(None);
                };
                let new = current_blob.as_ref().unwrap_or(&target);
                if !same_slot(old.as_slice(), new.as_slice(), seed.lines, seed.slot) {
                    return Ok(None);
                }
                replacements += 1;
                newest.get_or_insert((current, parent));
                current_blob = Some(old);
            }
            step = next;
            current = parent;
            edges += 1;
        }
        let Some((changed, previous)) = newest else {
            return Ok(None);
        };
        drop(current_blob);
        let fields = porcelain::commit_metadata(self, &changed)?;
        let stable = porcelain::RenderOrigin {
            commit: &seed.root,
            metadata: seed.root_metadata.as_slice(),
            path: request.path,
            previous: None,
            boundary: false,
        };
        let update = porcelain::RenderOrigin {
            commit: &changed,
            metadata: fields.as_slice(),
            previous: Some((&previous, request.path)),
            ..stable
        };
        let body = porcelain::replacement_body(
            target.as_slice(),
            [&stable, &update],
            seed.slot,
            seed.lines,
            &self.control,
            &self.capacity,
            max_bytes,
        )?;
        Ok(Some(ReplacementBlame {
            body,
            suffix_edges: edges,
            replacement_edges: replacements,
        }))
    }

    fn replacement_copy(&self, bytes: &[u8]) -> Result<CapacityBuffer, BlameError> {
        let mut out = self.capacity.buffer(bytes.len())?;
        out.resize_controlled(bytes.len(), 0, &self.control)?;
        for (dest, src) in out.as_mut_slice().chunks_mut(8192).zip(bytes.chunks(8192)) {
            self.control.check()?;
            dest.copy_from_slice(src);
        }
        Ok(out)
    }

    fn replacement_blob(&self, id: &oid) -> Result<Option<VerifiedObject>, BlameError> {
        let header = self.blame_blob_header(id)?;
        if header.size > MAX_FILE as u64 {
            return Ok(None);
        }
        self.blame_object(id, header, "missing replacement blob")
            .map(Some)
    }

    fn replacement_step(&self, id: &oid, path: &[u8]) -> Result<Option<Step>, BlameError> {
        let Some((tree, parent)) = self.replacement_commit(id)? else {
            return Ok(None);
        };
        let tree_header = self
            .header(&tree)
            .map_err(|e| source_error(e.as_ref()))?
            .ok_or(BlameError::Corrupt("missing replacement tree"))?;
        if tree_header.kind != Kind::Tree {
            return Err(BlameError::Corrupt("replacement tree type mismatch"));
        }
        if tree_header.size != (7 + path.len() + 1 + tree.kind().len_in_bytes()) as u64 {
            return Ok(None);
        }
        let tree_data = self.blame_object(&tree, tree_header, "missing replacement tree")?;
        let bytes = tree_data.as_slice();
        if !bytes.starts_with(b"100644 ") {
            return Ok(None);
        }
        let mut entries = gix_object::TreeRefIter::from_bytes(bytes, tree.kind());
        let Some(Ok(entry)) = entries.next() else {
            return Ok(None);
        };
        let name: &[u8] = entry.filename.as_ref();
        Ok((name == path && entries.next().is_none()).then(|| Step {
            parent,
            blob: entry.oid.to_owned(),
        }))
    }

    fn replacement_commit(
        &self,
        id: &oid,
    ) -> Result<Option<(ObjectId, Option<ObjectId>)>, BlameError> {
        self.control.check()?;
        let header = self.blame_commit_header(id)?;
        if header.size > 64 * 1024 {
            return Ok(None);
        }
        let commit = self.blame_commit(id, header)?;
        let header = self.blame_commit_header_bytes(&commit)?;
        let iter = gix_object::CommitRefIter::from_bytes(header, id.kind());
        if !iter.committer().is_ok_and(|sig| sig.time().is_ok()) {
            return Ok(None);
        }
        let mut tokens = gix_object::CommitRefIter::from_bytes(header, id.kind());
        let Some(Ok(Token::Tree { id: tree })) = tokens.next() else {
            return Ok(None);
        };
        let parent = match tokens.next() {
            Some(Ok(Token::Author { .. })) => None,
            Some(Ok(Token::Parent { id })) if !id.is_null() => {
                if !matches!(tokens.next(), Some(Ok(Token::Author { .. }))) {
                    return Ok(None);
                }
                Some(id)
            }
            _ => return Ok(None),
        };
        Ok(Some((tree, parent)))
    }
}

struct Lines<'a> {
    values: [&'a [u8]; MAX_LINES],
    len: usize,
}

fn distinct_lines(bytes: &[u8]) -> Option<Lines<'_>> {
    if !bytes.ends_with(b"\n")
        || bytes
            .iter()
            .any(|b| !matches!(*b, b' '..=b'~' | b'\t' | b'\n'))
    {
        return None;
    }
    let mut values = [b"".as_slice(); MAX_LINES];
    let mut len = 0;
    for line in bytes.split_inclusive(|b| *b == b'\n') {
        *values.get_mut(len)? = line;
        len += 1;
    }
    let mut sorted = values;
    sorted[..len].sort_unstable();
    if sorted[..len].windows(2).any(|pair| pair[0] == pair[1]) {
        return None;
    }
    Some(Lines { values, len })
}

fn same_slot(old: &[u8], new: &[u8], count: usize, slot: usize) -> bool {
    let (Some(old), Some(new)) = (distinct_lines(old), distinct_lines(new)) else {
        return false;
    };
    old.len == count
        && new.len == count
        && old.values[..count]
            .iter()
            .zip(&new.values[..count])
            .enumerate()
            .all(|(i, (a, b))| (a != b) == (i == slot))
}
