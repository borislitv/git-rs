//! Library-only operations on an application-owned request repository.

use crate::{Result, invalid, process, protocol::MAX_REF, scratch::Scratch};
use git_rs_core::pack::{PackLocation, PackSet};
use git_rs_core::{
    CapacityBudget, Control, Object, ReadLimits, SnapshotIdentity, SnapshotOdb,
    VerifiedObjectProvider,
};
use gix::bstr::ByteSlice;
use gix::odb::{find::Header, pack};
use std::fs;
use std::io::Read;

const MAX_BASE_RECURSION: usize = 32;

pub(crate) fn ref_name(name: &[u8]) -> Result<gix::refs::FullName> {
    if name.len() > MAX_REF || !name.starts_with(b"refs/") {
        return Err(invalid("invalid ref namespace"));
    }
    Ok(gix::refs::FullName::try_from(name.as_bstr())?)
}

/// Physical object headers from one request's verified, immutable snapshot.
///
/// gix-odb's directory discovery ignores symlinked indexes. Open the published
/// pack/index pairs explicitly through gix-pack so sealed RAM links remain
/// zero-copy. Borrowing the workspace keeps its cache pins alive until these
/// read-only library mappings have been released.
pub(crate) struct ObjectHeaders<'a> {
    packs: Vec<pack::Bundle>,
    loose: gix::odb::loose::Store,
    _workspace: &'a Scratch,
}

impl<'a> ObjectHeaders<'a> {
    pub(crate) fn open(workspace: &'a Scratch) -> Result<Self> {
        let objects = workspace.path.join("objects");
        let mut packs = Vec::new();
        for entry in fs::read_dir(objects.join("pack"))? {
            process::check_operation()?;
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "idx") {
                packs.push(pack::Bundle::at(path, gix::hash::Kind::Sha1)?);
            }
        }
        Ok(Self {
            packs,
            loose: gix::odb::loose::Store::at(objects, gix::hash::Kind::Sha1),
            _workspace: workspace,
        })
    }

    fn try_header_at(
        &self,
        id: &gix::hash::oid,
        depth: usize,
    ) -> gix::error::Result<Option<Header>> {
        process::check_operation().map_err(gix::error::Error::from_boxed)?;
        if depth >= MAX_BASE_RECURSION {
            return Err(gix::error::corruption("pack delta base recursion limit exceeded").into());
        }
        for bundle in &self.packs {
            process::check_operation().map_err(gix::error::Error::from_boxed)?;
            let Some(index) = bundle.index.lookup(id) else {
                continue;
            };
            let entry = bundle
                .pack
                .entry(bundle.index.pack_offset_at_index(index))?;
            let header =
                bundle
                    .pack
                    .decode_header(entry, &mut gix::zlib::Inflate::default(), &|base| {
                        use pack::data::decode::header::ResolvedBase;
                        if let Some(index) = bundle.index.lookup(base) {
                            return bundle
                                .pack
                                .entry(bundle.index.pack_offset_at_index(index))
                                .map(ResolvedBase::InPack)
                                .map(Some);
                        }
                        self.try_header_at(base, depth + 1).map(|header| {
                            header.map(|header| ResolvedBase::OutOfPack {
                                kind: header.kind(),
                                num_deltas: header.num_deltas(),
                            })
                        })
                    })?;
            process::check_operation().map_err(gix::error::Error::from_boxed)?;
            return Ok(Some(header.into()));
        }
        self.loose
            .try_header(id)
            .map(|header| header.map(Into::into))
    }
}

impl gix::odb::Header for ObjectHeaders<'_> {
    fn try_header(&self, id: &gix::hash::oid) -> gix::error::Result<Option<Header>> {
        self.try_header_at(id, 0)
    }
}

/// Full physical reads for one already authorized, held snapshot. No gix
/// repository open/discovery occurs here. Borrowing Scratch preserves every
/// sealed-RAM cache pin until all mappings and synchronous reads finish.
pub(crate) struct PhysicalSnapshot<'a, G> {
    packs: PackSet<(&'a Scratch, G)>,
    workspace: &'a Scratch,
    identity: SnapshotIdentity,
    capacity: CapacityBudget,
    limits: ReadLimits,
}

pub(crate) fn open_snapshot<'a, G: 'a>(
    workspace: &'a Scratch,
    identity: SnapshotIdentity,
    source_hold: G,
    control: Control,
    capacity: CapacityBudget,
    limits: ReadLimits,
) -> Result<SnapshotOdb<PhysicalSnapshot<'a, G>>> {
    if identity.object_hash != gix::hash::Kind::Sha1 {
        return Err(invalid("snapshot object format must be SHA-1"));
    }
    let mut indexes = Vec::new();
    for entry in fs::read_dir(workspace.path.join("objects/pack"))? {
        control.check()?;
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "idx") {
            if indexes.len() >= limits.max_graph_objects {
                return Err(invalid("snapshot pack count limit exceeded"));
            }
            indexes.push(path);
        }
    }
    let packs = PackSet::open(
        indexes,
        (workspace, source_hold),
        identity.object_hash,
        &control,
        limits,
    )?;
    let expected = identity.clone();
    SnapshotOdb::open(
        PhysicalSnapshot {
            packs,
            workspace,
            identity,
            capacity: capacity.clone(),
            limits,
        },
        &expected,
        control,
        capacity,
        limits,
    )
}

impl<G> PhysicalSnapshot<'_, G> {
    fn loose(
        &self,
        id: &gix::hash::oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        control.check()?;
        let hex = id.to_hex().to_string();
        let path = self
            .workspace
            .path
            .join("objects")
            .join(&hex[..2])
            .join(&hex[2..]);
        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(invalid("loose snapshot object is not a regular file"));
        }
        let length = usize::try_from(metadata.len())?;
        if length > limits.max_decode_bytes {
            return Err(invalid("compressed loose object byte limit exceeded"));
        }
        let mut compressed = budget.buffer(length)?;
        while compressed.as_slice().len() < length {
            control.check()?;
            let start = compressed.as_slice().len();
            let end = length.min(start.saturating_add(32 * 1024));
            compressed.resize(end, 0)?;
            file.read_exact(&mut compressed.as_mut_slice()[start..end])?;
        }
        let mut extra = [0];
        if file.read(&mut extra)? != 0 {
            return Err(invalid("loose object changed during snapshot read"));
        }
        control.check()?;
        let (kind, size, header) = loose_header(compressed.as_slice(), control)?;
        let size = usize::try_from(size)?;
        if size > limits.max_object_bytes {
            return Err(invalid("loose object byte limit exceeded"));
        }
        let total = size
            .checked_add(header)
            .filter(|total| *total <= limits.max_decode_bytes)
            .ok_or_else(|| invalid("loose decoded-byte limit exceeded"))?;
        let mut decoded = budget.buffer(total)?;
        while decoded.as_slice().len() < total {
            control.check()?;
            decoded.resize(
                total.min(decoded.as_slice().len().saturating_add(32 * 1024)),
                0,
            )?;
        }
        let check = || {
            control
                .check()
                .map_err(|error| gix::error::Error::from_boxed(Box::new(error)))
        };
        let (_, read, _) = gix::zlib::Inflate::default().once_controlled(
            compressed.as_slice(),
            decoded.as_mut_slice(),
            &check,
        )?;
        if read != compressed.as_slice().len() {
            return Err(invalid("trailing compressed loose object bytes"));
        }
        let actual = gix::objs::decode::loose_header(decoded.as_slice())?;
        if actual != (kind, size as u64, header) {
            return Err(invalid("inconsistent loose object header"));
        }
        let mut data = budget.buffer(size)?;
        for chunk in decoded.as_slice()[header..].chunks(32 * 1024) {
            control.check()?;
            let start = data.as_slice().len();
            data.resize(start + chunk.len(), 0)?;
            data.as_mut_slice()[start..].copy_from_slice(chunk);
        }
        let mut hash = gix::hash::hasher(id.kind());
        hash.update(&gix::objs::encode::loose_header(kind, size as u64));
        for chunk in data.as_slice().chunks(32 * 1024) {
            control.check()?;
            hash.update(chunk);
        }
        if hash.try_finalize()?.as_ref() != id {
            return Err(invalid("physical loose object hash mismatch"));
        }
        control.check()?;
        Ok(Some(Object { kind, data }))
    }
}

fn loose_header(input: &[u8], control: &Control) -> Result<(gix::objs::Kind, u64, usize)> {
    let mut decoder = gix::zlib::Decompress::default();
    let mut bytes = [0u8; 64];
    let (mut read, mut written) = (0usize, 0usize);
    loop {
        control.check()?;
        let before_in = decoder.total_in();
        let before_out = decoder.total_out();
        let end = input.len().min(read.saturating_add(32 * 1024));
        let status = decoder.decompress(
            &input[read..end],
            &mut bytes[written..],
            gix::zlib::FlushDecompress::None,
        )?;
        let consumed = usize::try_from(decoder.total_in() - before_in)?;
        let produced = usize::try_from(decoder.total_out() - before_out)?;
        read += consumed;
        written += produced;
        if bytes[..written].contains(&0) {
            return Ok(gix::objs::decode::loose_header(&bytes[..written])?);
        }
        if written == bytes.len()
            || status == gix::zlib::Status::StreamEnd
            || (consumed == 0 && produced == 0)
        {
            return Err(invalid("invalid physical loose object header"));
        }
    }
}

impl<G> VerifiedObjectProvider for PhysicalSnapshot<'_, G> {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<gix::objs::Header>> {
        self.object(id, control, &self.capacity, self.limits)
            .map(|object| {
                object.map(|object| gix::objs::Header {
                    kind: object.kind,
                    size: object.data.as_slice().len() as u64,
                })
            })
    }
    fn object(
        &self,
        id: &gix::hash::oid,
        control: &Control,
        capacity: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        if let Some(object) = self.packs.decode(id, control, capacity, limits)? {
            return Ok(Some(object));
        }
        self.loose(id, control, capacity, limits)
    }
    fn pack_location(
        &self,
        id: &gix::hash::oid,
        control: &Control,
    ) -> Result<Option<PackLocation>> {
        self.packs.locate(id, control)
    }
}
