//! Complete NativeRootV1 attribution wire. All integers are big-endian. The caller
//! authenticates the entire ordered byte string and its namespace out of band.

use super::{
    BlameError, BlameOptions, BlameRequest, CompleteBlame, MAX_BLOB_BYTES, MAX_COMMITS, Metadata,
    ProducerVersion, memo,
};
use crate::{
    CapacityBudget, CapacityBuffer, CapacityError, CapacityReservation, Control, SnapshotSession,
    VerifiedObjectProvider,
};
use gix_blame::native::{
    COMPLETED_VERSION, CompletedAttribution, CompletedIdentity, NativeOrigin, PreviousOrigin,
};
use gix_hash::{Kind, ObjectId, oid};
use gix_object::bstr::BString;
use sha2::{Digest, Sha256};

/// Maximum encoded map size, independent of the transport's per-part limit.
pub const MAX_COMPLETED_MAP_BYTES: usize = 256 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"GRCAT002";
const ORIGIN_PATH_LIMIT: usize = 64 * 1024;
const TARGET_PATH_LIMIT: usize = 4096;
const CHUNK: usize = 8192;
// SHA1, source start, count, path length + at least one byte, previous flag, metadata length.
const MIN_ROW_BYTES: usize = 20 + 4 + 4 + 4 + 1 + 1 + 4;
const INVALID: BlameError = BlameError::Corrupt("invalid completed attribution map");

/// Immutable decoded map. Borrows its admitted wire for paths used as the base
/// key and canonical porcelain metadata. Vendor row/path copies are separately
/// admitted to the same budget. No constructor bypasses the wire checks.
pub struct CompletedBlameMap<'a> {
    identity: CompletedIdentity,
    namespace: [u8; 32],
    path: &'a [u8],
    target: ObjectId,
    target_path: [u8; 32],
    live_scope: [u8; 32],
    origins: Vec<NativeOrigin>,
    metadata: Metadata<&'a [u8]>,
    _rows: CapacityReservation,
    budget: CapacityBudget,
    control: &'a Control,
}

impl CompletedBlameMap<'_> {
    pub(super) fn check_live<P: VerifiedObjectProvider>(
        &self,
        session: &SnapshotSession<P>,
        live_scope: [u8; 32],
        target: BlameRequest<'_>,
    ) -> Result<(), BlameError> {
        let path: [u8; 32] = Sha256::digest(target.path).into();
        if !std::ptr::eq(self.control, &session.control)
            || !self.budget.shares_with(&session.capacity)
            || self.live_scope != live_scope
            || self.target.as_ref() != target.commit
            || self.target_path != path
        {
            return Err(BlameError::InvalidRequest);
        }
        Ok(())
    }

    pub(super) fn applies(&self, scope: [u8; 32], namespace: [u8; 32]) -> bool {
        self.identity.scope == scope && self.namespace == namespace
    }

    pub(super) fn as_vendor(&self) -> CompletedAttribution<'_> {
        CompletedAttribution {
            identity: self.identity,
            path: self.path.into(),
            origins: &self.origins,
        }
    }

    pub(super) fn fields(&self, id: &oid) -> Option<&[u8]> {
        self.metadata.get(id).map(|entry| entry.fields)
    }
}

impl Drop for CompletedBlameMap<'_> {
    fn drop(&mut self) {
        while !self.origins.is_empty() {
            self.origins.truncate(self.origins.len().saturating_sub(32));
            if !std::thread::panicking() {
                let _ = self.control.check();
            }
        }
        drop(std::mem::take(&mut self.origins));
    }
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Export a certificate/map from a locally completed result, never a graft hit.
    /// This performs no object reads. Use the same source, request, options and
    /// live job resources. `namespace` is the caller's authenticated digest of
    /// the stable repository/principal/storage epoch (not the ref generation).
    pub fn encode_completed_blame(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        namespace: [u8; 32],
        complete: &CompleteBlame,
        max_bytes: usize,
    ) -> Result<CapacityBuffer, BlameError> {
        self.validate_blame_request(request)?;
        let scopes = scopes(self, options)?;
        let path_digest: [u8; 32] = Sha256::digest(request.path).into();
        let identity = complete
            .admitted
            .completed_identity()
            .ok_or(BlameError::UnsupportedContract)?;
        if !complete.can_certify_local_attribution()
            || !complete.uses_resources(&self.control, &self.capacity)
            || complete.target.as_ref() != request.commit
            || complete.path_digest != path_digest
            || identity.scope != scopes.immutable
            || complete.live_scope != scopes.live
            || identity.tuning != options.tuning()
            || max_bytes > MAX_COMPLETED_MAP_BYTES
        {
            return Err(BlameError::InvalidRequest);
        }
        let mut sizing = Writer {
            output: None,
            len: 0,
            limit: max_bytes,
            control: &self.control,
        };
        encode(&mut sizing, request.path, identity, namespace, complete)?;
        let mut output = self.capacity.buffer(sizing.len)?;
        let mut writer = Writer {
            output: Some(&mut output),
            len: 0,
            ..sizing
        };
        encode(&mut writer, request.path, identity, namespace, complete)?;
        self.control.check()?;
        Ok(output)
    }

    /// Decode only after the caller has authenticated the *whole* record as the
    /// expected producer's, checked the namespace, and passed the final authority
    /// fence. Structural validation is not authentication, proof of provenance,
    /// or authorization. `base` is the caller's expected cache key. `target` is
    /// the currently authorized commit/path to blame. Only the wire is portable
    /// across ref generations; the decoded map is bound to this session and target.
    ///
    /// Multipart transport: concatenate authenticated ordered parts once into one
    /// `CapacityBuffer` admitted to this job; pass it by reference and keep it
    /// pinned until the decoded map drops. No 1 MiB unit limit applies inside core.
    /// Invalid/incompatible records are `Ok(None)`; stop/admission failures are errors.
    pub fn decode_authenticated_completed_blame<'a>(
        &'a self,
        base: BlameRequest<'_>,
        target: BlameRequest<'_>,
        options: &BlameOptions,
        namespace: [u8; 32],
        wire: &'a CapacityBuffer,
    ) -> Result<Option<CompletedBlameMap<'a>>, BlameError> {
        self.validate_blame_request(base)?;
        self.validate_blame_request(target)?;
        if !wire.shares_budget(&self.capacity) {
            return Err(BlameError::InvalidRequest);
        }
        match decode(self, base, target, options, namespace, wire.as_slice()) {
            Ok(map) => Ok(Some(map)),
            Err(BlameError::Corrupt(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Run the ordinary native blame engine with an optional graft at the map's
    /// verified commit/path. A changed tip path is allowed (normal rename following).
    /// If lookup/decode already missed, pass `None`; this explicit reuse entrypoint
    /// reports `Miss` rather than `NotRequested` even in that case.
    /// A map decoded for another live session, generation or target is a request
    /// error, not an authority grant and not a fallback to that other target.
    pub fn blame_complete_with_completed(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        namespace: [u8; 32],
        map: Option<&CompletedBlameMap<'_>>,
        memo: Option<memo::MemoryMappingRequest<'_>>,
    ) -> Result<CompleteBlame, BlameError> {
        self.blame_complete_with_completed_observed(request, options, namespace, map, memo, None)
    }

    /// The same completed-map attribution with optional job-local observation.
    pub fn blame_complete_with_completed_observed(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        namespace: [u8; 32],
        map: Option<&CompletedBlameMap<'_>>,
        memo: Option<memo::MemoryMappingRequest<'_>>,
        progress: Option<&super::BlameProgress>,
    ) -> Result<CompleteBlame, BlameError> {
        let mut result = self.blame_complete_inner(
            request,
            options,
            memo,
            map.map(|map| (namespace, map)),
            progress,
        )?;
        if map.is_none() {
            result.completed_use = super::CompletedUse::Miss;
        }
        Ok(result)
    }
}

pub(super) struct Scopes {
    pub immutable: [u8; 32],
    pub live: [u8; 32],
}

pub(super) fn scopes<P: VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    options: &BlameOptions,
) -> Result<Scopes, BlameError> {
    let producer = ProducerVersion::verified()?;
    let identity = session.identity();
    let limits = session.limits;
    let mut hash = Sha256::new();
    hash.update(b"git-rs/completed-attribution/native-root/sha1/v2");
    for part in [
        producer.cache_id().as_bytes(),
        options.canonical_bytes(),
        &identity.source,
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        for chunk in part.chunks(CHUNK) {
            session.control.check()?;
            hash.update(chunk);
        }
    }
    for limit in [
        limits.max_object_bytes,
        limits.max_metadata_bytes,
        limits.max_graph_objects,
        limits.max_graph_edges,
        limits.max_delta_depth,
        limits.max_decode_bytes,
    ] {
        hash.update((limit as u64).to_be_bytes());
    }
    let immutable: [u8; 32] = hash.finalize().into();
    let mut live = Sha256::new();
    live.update(b"git-rs/completed-attribution/live-snapshot/v1");
    for part in [&identity.source, &identity.generation] {
        live.update((part.len() as u64).to_be_bytes());
        for chunk in part.chunks(CHUNK) {
            session.control.check()?;
            live.update(chunk);
        }
    }
    Ok(Scopes {
        immutable,
        live: live.finalize().into(),
    })
}

pub(super) fn copy_fields<P: VerifiedObjectProvider>(
    session: &SnapshotSession<P>,
    fields: &[u8],
) -> Result<CapacityBuffer, BlameError> {
    session.control.check()?;
    let mut output = session.capacity.buffer(fields.len())?;
    for chunk in fields.chunks(CHUNK) {
        session.control.check()?;
        output.extend_from_slice(chunk)?;
    }
    Ok(output)
}

struct Writer<'a> {
    output: Option<&'a mut CapacityBuffer>,
    len: usize,
    limit: usize,
    control: &'a Control,
}
impl Writer<'_> {
    fn bytes(&mut self, bytes: &[u8]) -> Result<(), BlameError> {
        self.len = self
            .len
            .checked_add(bytes.len())
            .filter(|len| *len <= self.limit)
            .ok_or(BlameError::OutputTooLarge)?;
        if let Some(output) = self.output.as_deref_mut() {
            for chunk in bytes.chunks(CHUNK) {
                self.control.check()?;
                output.extend_from_slice(chunk)?;
            }
        }
        Ok(())
    }
    fn u32(&mut self, value: u32) -> Result<(), BlameError> {
        self.bytes(&value.to_be_bytes())
    }
    fn counted(&mut self, bytes: &[u8]) -> Result<(), BlameError> {
        self.u32(u32::try_from(bytes.len()).map_err(|_| BlameError::OutputTooLarge)?)?;
        self.bytes(bytes)
    }
    fn id(&mut self, id: &oid) -> Result<(), BlameError> {
        if id.kind() != Kind::Sha1 || id.is_null() {
            return Err(INVALID);
        }
        self.bytes(id.as_bytes())
    }
}

fn encode(
    writer: &mut Writer<'_>,
    path: &[u8],
    identity: CompletedIdentity,
    namespace: [u8; 32],
    complete: &CompleteBlame,
) -> Result<(), BlameError> {
    writer.bytes(MAGIC)?;
    writer.u32(identity.version)?;
    writer.bytes(&identity.scope)?;
    writer.bytes(&namespace)?;
    writer.id(&identity.commit)?;
    writer.id(&identity.blob)?;
    writer.bytes(&[u8::from(identity.projectable)])?;
    writer.u32(identity.lines)?;
    writer.counted(path)?;
    writer.u32(u32::try_from(complete.trace().len()).map_err(|_| BlameError::OutputTooLarge)?)?;
    writer.u32(
        u32::try_from(complete.metadata.table.len()).map_err(|_| BlameError::OutputTooLarge)?,
    )?;
    for (index, row) in complete.trace().iter().enumerate() {
        writer.control.check()?;
        writer.id(&row.commit_id)?;
        writer.u32(row.source_range.start)?;
        writer.u32(row.group_count)?;
        let source: &[u8] = row.source_file_name.as_ref();
        validate_path(source, writer.control)?;
        writer.counted(source)?;
        writer.bytes(&[u8::from(row.previous.is_some())])?;
        if let Some(previous) = &row.previous {
            writer.id(&previous.commit_id)?;
            validate_path(previous.source_file_name.as_ref(), writer.control)?;
            writer.counted(previous.source_file_name.as_ref())?;
        }
        let metadata = complete.metadata.get(&row.commit_id).ok_or(INVALID)?;
        if index == metadata.first_row {
            writer.counted(metadata.fields.as_slice())?;
        } else {
            writer.u32(0)?;
        }
    }
    Ok(())
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn bytes(&mut self, len: usize) -> Result<&'a [u8], BlameError> {
        let (head, tail) = self.0.split_at_checked(len).ok_or(INVALID)?;
        self.0 = tail;
        Ok(head)
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], BlameError> {
        self.bytes(N)?.try_into().map_err(|_| INVALID)
    }
    fn u32(&mut self) -> Result<u32, BlameError> {
        Ok(u32::from_be_bytes(self.fixed()?))
    }
    fn flag(&mut self) -> Result<bool, BlameError> {
        match self.fixed()? {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(INVALID),
        }
    }
    fn id(&mut self) -> Result<ObjectId, BlameError> {
        let id = ObjectId::from_bytes_or_panic(&self.fixed::<20>()?);
        if id.is_null() {
            return Err(INVALID);
        }
        Ok(id)
    }
    fn counted(&mut self, max: usize) -> Result<&'a [u8], BlameError> {
        let count = self.u32()? as usize;
        if count > max {
            return Err(INVALID);
        }
        self.bytes(count)
    }
}

fn decode<'a, P: VerifiedObjectProvider>(
    session: &'a SnapshotSession<P>,
    base: BlameRequest<'_>,
    target: BlameRequest<'_>,
    options: &BlameOptions,
    namespace: [u8; 32],
    bytes: &'a [u8],
) -> Result<CompletedBlameMap<'a>, BlameError> {
    if bytes.len() > MAX_COMPLETED_MAP_BYTES {
        return Err(INVALID);
    }
    let mut reader = Reader(bytes);
    let scopes = scopes(session, options)?;
    if reader.bytes(MAGIC.len())? != MAGIC
        || reader.u32()? != COMPLETED_VERSION
        || reader.fixed::<32>()? != scopes.immutable
        || reader.fixed::<32>()? != namespace
    {
        return Err(INVALID);
    }
    let commit = reader.id()?;
    if commit.as_ref() != base.commit {
        return Err(INVALID);
    }
    let blob = reader.id()?;
    let projectable = reader.flag()?;
    let lines = reader.u32()?;
    let path = reader.counted(TARGET_PATH_LIMIT)?;
    if path != base.path || lines as usize > session.limits.max_object_bytes.min(MAX_BLOB_BYTES) {
        return Err(INVALID);
    }
    let rows = reader.u32()? as usize;
    let commits = reader.u32()? as usize;
    if rows > lines as usize
        || rows > reader.0.len() / MIN_ROW_BYTES
        || commits > rows
        || commits > session.limits.max_graph_objects.min(MAX_COMMITS)
        || (rows == 0) != (commits == 0)
    {
        return Err(INVALID);
    }
    let mut map = CompletedBlameMap {
        identity: CompletedIdentity {
            version: COMPLETED_VERSION,
            scope: scopes.immutable,
            commit,
            blob,
            lines,
            tuning: options.tuning(),
            projectable,
        },
        namespace,
        path,
        target: target.commit.to_owned(),
        target_path: Sha256::digest(target.path).into(),
        live_scope: scopes.live,
        origins: Vec::new(),
        metadata: Metadata::new(commits, &session.capacity, &session.control)?,
        _rows: session.capacity.reservation(0)?,
        budget: session.capacity.clone(),
        control: &session.control,
    };
    let row_bytes = |capacity: usize| {
        capacity
            .checked_mul(std::mem::size_of::<NativeOrigin>())
            .ok_or(CapacityError::Exhausted)
    };
    map._rows.resize(row_bytes(rows)?)?;
    session.control.check()?;
    map.origins
        .try_reserve_exact(rows)
        .map_err(|_| CapacityError::Allocation)?;
    let mut charged = row_bytes(map.origins.capacity())?;
    map._rows.resize(charged)?;
    let mut final_start = 0u32;
    for index in 0..rows {
        session.control.check()?;
        let id = reader.id()?;
        let source_start = reader.u32()?;
        let count = reader.u32()?;
        let final_end = final_start
            .checked_add(count)
            .filter(|end| count != 0 && *end <= lines)
            .ok_or(INVALID)?;
        let source_end = source_start
            .checked_add(count)
            .filter(|end| *end as usize <= session.limits.max_object_bytes.min(MAX_BLOB_BYTES))
            .ok_or(INVALID)?;
        let source = reader.counted(ORIGIN_PATH_LIMIT)?;
        validate_path(source, &session.control)?;
        if id == commit && (source_start != final_start || source != path) {
            return Err(INVALID);
        }
        let previous = if reader.flag()? {
            let parent = reader.id()?;
            if parent == id {
                return Err(INVALID);
            }
            let path = reader.counted(ORIGIN_PATH_LIMIT)?;
            validate_path(path, &session.control)?;
            Some((parent, path))
        } else {
            None
        };
        let fields = reader.counted(MAX_COMPLETED_MAP_BYTES)?;
        if map.metadata.get(&id).is_some() {
            if !fields.is_empty() {
                return Err(INVALID);
            }
        } else {
            if map.metadata.table.len() == commits {
                return Err(INVALID);
            }
            validate_fields(fields, &session.control)?;
            map.metadata.insert(id, index, fields);
        }
        let mut copy = |bytes| copy_path(bytes, &mut map._rows, &mut charged, &session.control);
        let source_file_name = copy(source)?;
        let previous = previous
            .map(|(id, path)| {
                copy(path).map(|source_file_name| PreviousOrigin {
                    commit_id: id,
                    source_file_name,
                })
            })
            .transpose()?;
        map.origins.push(NativeOrigin {
            commit_id: id,
            source_file_name,
            previous,
            boundary: false,
            source_range: source_start..source_end,
            final_range: final_start..final_end,
            group_id: index as u32,
            group_count: count,
        });
        final_start = final_end;
    }
    if !reader.0.is_empty() || final_start != lines || map.metadata.table.len() != commits {
        return Err(INVALID);
    }
    session.control.check()?;
    Ok(map)
}

fn copy_path(
    bytes: &[u8],
    admission: &mut CapacityReservation,
    charged: &mut usize,
    control: &Control,
) -> Result<BString, BlameError> {
    control.check()?;
    admission.resize(
        charged
            .checked_add(bytes.len())
            .ok_or(CapacityError::Exhausted)?,
    )?;
    let mut path = Vec::new();
    path.try_reserve_exact(bytes.len())
        .map_err(|_| CapacityError::Allocation)?;
    *charged = charged
        .checked_add(path.capacity())
        .ok_or(CapacityError::Exhausted)?;
    admission.resize(*charged)?;
    for chunk in bytes.chunks(CHUNK) {
        control.check()?;
        path.extend_from_slice(chunk);
    }
    Ok(path.into())
}

fn validate_path(path: &[u8], control: &Control) -> Result<(), BlameError> {
    if path.is_empty() || path.len() > ORIGIN_PATH_LIMIT {
        return Err(INVALID);
    }
    let mut start = 0;
    for (block, chunk) in path.chunks(CHUNK).enumerate() {
        control.check()?;
        for (index, byte) in chunk.iter().enumerate() {
            if *byte == 0 {
                return Err(INVALID);
            }
            if *byte == b'/' {
                let end = block * CHUNK + index;
                if matches!(&path[start..end], b"" | b"." | b"..") {
                    return Err(INVALID);
                }
                start = end + 1;
            }
        }
    }
    if matches!(&path[start..], b"" | b"." | b"..") {
        return Err(INVALID);
    }
    Ok(())
}

// Accept only the renderer's fixed metadata fields and one LF per field. Values
// may be raw non-UTF8 on native codec fallback. No filename/porcelain injection.
fn validate_fields(mut bytes: &[u8], control: &Control) -> Result<(), BlameError> {
    for prefix in [
        b"author ".as_slice(),
        b"author-mail ",
        b"author-time ",
        b"author-tz ",
        b"committer ",
        b"committer-mail ",
        b"committer-time ",
        b"committer-tz ",
        b"summary ",
    ] {
        bytes = bytes.strip_prefix(prefix).ok_or(INVALID)?;
        let mut end = None;
        for (block, chunk) in bytes.chunks(CHUNK).enumerate() {
            control.check()?;
            if let Some(index) = chunk.iter().position(|byte| matches!(*byte, b'\n' | 0)) {
                if chunk[index] == 0 {
                    return Err(INVALID);
                }
                end = Some(block * CHUNK + index + 1);
                break;
            }
        }
        bytes = &bytes[end.ok_or(INVALID)?..];
    }
    if !bytes.is_empty() {
        return Err(INVALID);
    }
    Ok(())
}
