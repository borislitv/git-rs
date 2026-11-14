//! Immutable object-store ranges, not a mapped temporary repository. Index bytes
//! are authenticated by the source adapter; every returned object is rehashed.
//! Only a requested entry and its bounded delta dependencies are reconstructed.

use std::{cell::RefCell, ops::Deref, sync::Arc};

use gix_hash::{ObjectId, oid};
use sha2::{Digest, Sha256};

use super::{DecodePlan, DecoderOptions, PackLocation, Runtime, acceleration::Key};
use crate::{
    CapacityBudget, CapacityBuffer, CapacityReservation, Control, Object, ReadLimits,
    SnapshotIdentity, VerifiedObjectProvider,
    objects::{Result, VerifiedObject, invalid},
};

/// An authorized immutable catalog with a source hold owned by the implementor.
/// Index acquisition must verify its complete catalog digest. Range reads must
/// return exactly the requested bytes or fail, enforce an absolute deadline and
/// never interpret keys/configuration as local paths or executable programs.
pub trait PackSource {
    fn pack_count(&self) -> usize;
    fn pack_size(&self, pack: usize) -> Result<u64>;
    fn pack_hash(&self, pack: usize) -> Result<ObjectId>;
    fn pack_digest(&self, pack: usize) -> Result<[u8; 32]>;

    /// Discard optional cached ranges after a physical source read/validation
    /// fails. This is bounded local cleanup, never network I/O or authorization.
    /// Existing pins must retain their backing/admission until their last owner.
    fn discard_cached(&self, _pack: usize) {}
    fn index(
        &self,
        pack: usize,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<CapacityBuffer>;
    fn read_exact(
        &self,
        pack: usize,
        offset: u64,
        output: &mut [u8],
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<()>;

    /// An immutable owner, never a detached/unadmitted Vec. Adapters can return a
    /// cache pin without copying; the default owns exact job-admitted backing.
    fn read_shared(
        &self,
        pack: usize,
        offset: u64,
        length: usize,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<SharedRange> {
        let mut bytes = budget.buffer(length)?;
        bytes.resize_controlled(length, 0, control)?;
        self.read_exact(pack, offset, bytes.as_mut_slice(), control, budget)?;
        Ok(SharedRange::new(Arc::new(bytes)))
    }
}

trait RangeBytes: AsRef<[u8]> + Send + Sync {}
impl<T: AsRef<[u8]> + Send + Sync> RangeBytes for T {}

#[derive(Clone)]
pub struct SharedRange(Arc<dyn RangeBytes>);
impl SharedRange {
    pub fn new<T: AsRef<[u8]> + Send + Sync + 'static>(owner: Arc<T>) -> Self {
        Self(owner)
    }
}
impl Deref for SharedRange {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.0.as_ref().as_ref()
    }
}

const PROOF_BLOCK: u64 = 4 * 1024 * 1024;
const MAX_SOURCE_PACK: u64 = 12 * 1024 * 1024 * 1024;

struct PackProof {
    blocks: Vec<[u8; 32]>,
    _capacity: CapacityReservation,
}

#[derive(Clone)]
struct VerifiedBlock {
    pack: usize,
    offset: u64,
    bytes: SharedRange,
}

/// Exact encoded bytes from a catalog-SHA-256-verified pack and independently
/// authenticated index. Its private construction is separate from source/want
/// authorization. Re-encoding OFS headers still requires the output base ID.
pub struct PackedObject {
    pub(crate) id: ObjectId,
    pub(crate) entry: gix_pack::data::Entry,
    pub(crate) base: Option<ObjectId>,
    pub(crate) bytes: CapacityBuffer,
}

struct IndexBytes(CapacityBuffer);
impl Deref for IndexBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

struct Index {
    file: gix_pack::index::File<IndexBytes>,
    offsets: Vec<(u64, u32)>,
    _capacity: CapacityReservation,
}

#[derive(Clone, Copy)]
struct Frame {
    location: PackLocation,
    id: ObjectId,
    end: u64,
}

/// One admitted job's range-backed physical provider. All indexes, decoder
/// caches and temporary backing drop before the source hold. No network operation
/// is performed with a state/cache lock or RefCell borrow held.
pub struct RangePackSet<S> {
    indexes: RefCell<Vec<Option<Index>>>,
    proofs: RefCell<Vec<Option<PackProof>>>,
    verified_block: RefCell<Option<VerifiedBlock>>,
    runtime: Arc<Runtime>,
    identity: SnapshotIdentity,
    budget: CapacityBudget,
    limits: ReadLimits,
    _capacity: CapacityReservation,
    source: S,
}

impl<S: PackSource> RangePackSet<S> {
    pub fn new(
        source: S,
        identity: SnapshotIdentity,
        budget: CapacityBudget,
        limits: ReadLimits,
        options: DecoderOptions,
        control: &Control,
    ) -> Result<Self> {
        control.check()?;
        limits.validate()?;
        let count = source.pack_count();
        if count > 4096 || count > limits.max_graph_objects {
            return Err(invalid("range-backed source pack count exceeded"));
        }
        let mut capacity = budget.reservation(
            count
                * (std::mem::size_of::<Option<Index>>() + std::mem::size_of::<Option<PackProof>>()),
        )?;
        let mut indexes = Vec::new();
        indexes.try_reserve_exact(count)?;
        indexes.resize_with(count, || None);
        let mut proofs = Vec::new();
        proofs.try_reserve_exact(count)?;
        proofs.resize_with(count, || None);
        capacity.resize(
            indexes.capacity() * std::mem::size_of::<Option<Index>>()
                + proofs.capacity() * std::mem::size_of::<Option<PackProof>>(),
        )?;
        Ok(Self {
            indexes: RefCell::new(indexes),
            proofs: RefCell::new(proofs),
            verified_block: RefCell::new(None),
            runtime: Runtime::new(options, budget.clone(), control)?,
            identity,
            budget,
            limits,
            _capacity: capacity,
            source,
        })
    }

    fn index(&self, pack: usize, control: &Control) -> Result<()> {
        let result = self.load_index(pack, control);
        self.source_result(pack, control, result)
    }

    fn source_result<T>(&self, pack: usize, control: &Control, result: Result<T>) -> Result<T> {
        if result.is_err() && control.check().is_ok() {
            // A transient corrupt range must not become a permanent in-process
            // denial of service after the immutable object store is repaired.
            self.source.discard_cached(pack);
        }
        result
    }

    fn load_index(&self, pack: usize, control: &Control) -> Result<()> {
        control.check()?;
        if self.indexes.borrow().get(pack).is_none() {
            return Err(invalid("range-backed pack ordinal is out of bounds"));
        }
        if self.indexes.borrow()[pack].is_some() {
            return Ok(());
        }
        let bytes = self.source.index(pack, control, &self.budget)?;
        let len = bytes.as_slice().len();
        let hash_len = self.identity.object_hash.len_in_bytes();
        if len < hash_len * 2 {
            return Err(invalid("range-backed index is truncated"));
        }
        let mut hash = gix_hash::hasher(self.identity.object_hash);
        for chunk in bytes.as_slice()[..len - hash_len].chunks(32 * 1024) {
            control.check()?;
            hash.update(chunk);
        }
        if hash.try_finalize()?.as_slice() != &bytes.as_slice()[len - hash_len..] {
            return Err(invalid("range-backed index checksum mismatch"));
        }
        let file = gix_pack::index::File::from_data_controlled(
            IndexBytes(bytes),
            Default::default(),
            self.identity.object_hash,
            &|| check(control),
        )?;
        if file.pack_checksum() != self.source.pack_hash(pack)? {
            return Err(invalid("index does not belong to the catalog pack"));
        }
        let count = file.num_objects() as usize;
        if count > 4_000_000 {
            return Err(invalid("range-backed index object count exceeded"));
        }
        let size = self.source.pack_size(pack)?;
        let end = size
            .checked_sub(hash_len as u64)
            .filter(|end| *end >= 12)
            .ok_or_else(|| invalid("range-backed pack is truncated"))?;
        let mut header = [0u8; 12];
        self.source
            .read_exact(pack, 0, &mut header, control, &self.budget)?;
        let (_, physical_count) = gix_pack::data::header::decode(&header)?;
        if physical_count != file.num_objects() {
            return Err(invalid("pack header and index object counts disagree"));
        }
        let mut trailer = gix_hash::Kind::buf();
        self.source
            .read_exact(pack, end, &mut trailer[..hash_len], control, &self.budget)?;
        if &trailer[..hash_len] != file.pack_checksum().as_slice() {
            return Err(invalid("pack trailer and catalog identity disagree"));
        }
        let metadata_bytes = count
            .checked_mul(std::mem::size_of::<(u64, u32)>())
            .ok_or_else(|| invalid("range-backed index metadata overflow"))?;
        let mut capacity = self.budget.reservation(metadata_bytes)?;
        let mut offsets = Vec::new();
        offsets.try_reserve_exact(count)?;
        capacity.resize(offsets.capacity() * std::mem::size_of::<(u64, u32)>())?;
        let mut previous: Option<ObjectId> = None;
        for position in 0..file.num_objects() {
            if position % 1024 == 0 {
                control.check()?;
            }
            let id = file.oid_at_index(position);
            if previous.is_some_and(|before| before.as_ref() >= id) {
                return Err(invalid("pack index object IDs are not strictly ordered"));
            }
            previous = Some(id.to_owned());
            let offset = file.pack_offset_at_index(position);
            if offset < 12 || offset >= end {
                return Err(invalid("pack index points outside its physical entries"));
            }
            offsets.push((offset, position));
        }
        sort_offsets(&mut offsets, control, &self.budget)?;
        if offsets.first().is_some_and(|(offset, _)| *offset != 12) || (count == 0 && end != 12) {
            return Err(invalid(
                "pack index has inconsistent physical entry offsets",
            ));
        }
        for chunk in offsets.chunks(1024) {
            control.check()?;
            if chunk.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(invalid("pack index has duplicate physical entry offsets"));
            }
        }
        for boundary in (1024..offsets.len()).step_by(1024) {
            control.check()?;
            if offsets[boundary - 1].0 == offsets[boundary].0 {
                return Err(invalid("pack index has duplicate physical entry offsets"));
            }
        }
        control.check()?;
        self.indexes.borrow_mut()[pack] = Some(Index {
            file,
            offsets,
            _capacity: capacity,
        });
        Ok(())
    }

    fn authenticate_pack(&self, pack: usize, control: &Control) -> Result<()> {
        let result = self.authenticate_source(pack, control);
        self.source_result(pack, control, result)
    }

    fn authenticate_source(&self, pack: usize, control: &Control) -> Result<()> {
        control.check()?;
        if self.proofs.borrow()[pack].is_some() {
            return Ok(());
        }
        let size = self.source.pack_size(pack)?;
        if size > MAX_SOURCE_PACK {
            return Err(invalid("source pack exceeds authenticated range profile"));
        }
        let count = usize::try_from(size.div_ceil(PROOF_BLOCK))?;
        let mut capacity = self
            .budget
            .reservation(count * std::mem::size_of::<[u8; 32]>())?;
        let mut blocks = Vec::new();
        blocks.try_reserve_exact(count)?;
        capacity.resize(blocks.capacity() * std::mem::size_of::<[u8; 32]>())?;
        let mut full = Sha256::new();
        for offset in (0..size).step_by(PROOF_BLOCK as usize) {
            control.check()?;
            let length = usize::try_from((size - offset).min(PROOF_BLOCK))?;
            let bytes = self
                .source
                .read_shared(pack, offset, length, control, &self.budget)?;
            if bytes.len() != length {
                return Err(invalid("shared source range size mismatch"));
            }
            let mut block = Sha256::new();
            for chunk in bytes.chunks(32 * 1024) {
                control.check()?;
                full.update(chunk);
                block.update(chunk);
            }
            blocks.push(block.finalize().into());
        }
        let actual: [u8; 32] = full.finalize().into();
        if actual != self.source.pack_digest(pack)? {
            return Err(invalid("complete immutable source pack digest mismatch"));
        }
        control.check()?;
        self.proofs.borrow_mut()[pack] = Some(PackProof {
            blocks,
            _capacity: capacity,
        });
        Ok(())
    }

    fn authenticated_bytes(&self, frame: Frame, control: &Control) -> Result<CapacityBuffer> {
        let result = self.read_authenticated_bytes(frame, control);
        self.source_result(frame.location.pack, control, result)
    }

    fn read_authenticated_bytes(&self, frame: Frame, control: &Control) -> Result<CapacityBuffer> {
        self.authenticate_pack(frame.location.pack, control)?;
        let length = usize::try_from(frame.end - frame.location.offset)?;
        let max = self
            .limits
            .max_decode_bytes
            .checked_add(self.limits.max_decode_bytes / 1000)
            .and_then(|n| n.checked_add(65536))
            .ok_or_else(|| invalid("encoded entry limit overflow"))?;
        if length > max {
            return Err(invalid("encoded entry exceeds bounded copy profile"));
        }
        let mut output = self.budget.buffer(length)?;
        output.resize_controlled(length, 0, control)?;
        let mut offset = frame.location.offset;
        let mut written = 0;
        while offset < frame.end {
            control.check()?;
            let start = offset / PROOF_BLOCK * PROOF_BLOCK;
            let existing = self
                .verified_block
                .borrow()
                .as_ref()
                .filter(|block| block.pack == frame.location.pack && block.offset == start)
                .cloned();
            let block = if let Some(block) = existing {
                block
            } else {
                let length = usize::try_from(
                    (self.source.pack_size(frame.location.pack)? - start).min(PROOF_BLOCK),
                )?;
                let bytes = self.source.read_shared(
                    frame.location.pack,
                    start,
                    length,
                    control,
                    &self.budget,
                )?;
                if bytes.len() != length {
                    return Err(invalid("shared source range size mismatch"));
                }
                let expected = self.proofs.borrow()[frame.location.pack]
                    .as_ref()
                    .expect("complete pack digest was verified")
                    .blocks[usize::try_from(start / PROOF_BLOCK)?];
                let mut hash = Sha256::new();
                for chunk in bytes.chunks(32 * 1024) {
                    control.check()?;
                    hash.update(chunk);
                }
                let actual: [u8; 32] = hash.finalize().into();
                if actual != expected {
                    return Err(invalid(
                        "immutable source range changed after authentication",
                    ));
                }
                let block = VerifiedBlock {
                    pack: frame.location.pack,
                    offset: start,
                    bytes,
                };
                *self.verified_block.borrow_mut() = Some(block.clone());
                block
            };
            let within = usize::try_from(offset - start)?;
            let count = usize::try_from(frame.end - offset)?.min(block.bytes.len() - within);
            for (source, destination) in block.bytes[within..within + count]
                .chunks(32 * 1024)
                .zip(output.as_mut_slice()[written..written + count].chunks_mut(32 * 1024))
            {
                control.check()?;
                destination.copy_from_slice(source);
            }
            written += count;
            offset += count as u64;
        }
        Ok(output)
    }

    pub fn packed_object(&self, id: &oid, control: &Control) -> Result<Option<PackedObject>> {
        let Some(location) = self.locate(id, control)? else {
            return Ok(None);
        };
        self.prepare(location, control, self.limits, &mut [None; 65], 0, true)?;
        let frame = self.frame(location, control)?;
        let bytes = self.authenticated_bytes(frame, control)?;
        let entry = gix_pack::data::Entry::from_bytes(
            bytes.as_slice(),
            location.offset,
            self.identity.object_hash,
        )?;
        let base = match entry.header {
            gix_pack::data::entry::Header::RefDelta { base_id } => Some(base_id),
            gix_pack::data::entry::Header::OfsDelta { .. } => Some(
                self.frame(self.base(location, &entry, control)?, control)?
                    .id,
            ),
            _ => None,
        };
        control.check()?;
        Ok(Some(PackedObject {
            id: id.to_owned(),
            entry,
            base,
            bytes,
        }))
    }

    pub fn locate(&self, id: &oid, control: &Control) -> Result<Option<PackLocation>> {
        control.check()?;
        if id.kind() != self.identity.object_hash {
            return Err(invalid("range-backed object hash format mismatch"));
        }
        for pack in 0..self.source.pack_count() {
            self.index(pack, control)?;
            let indexes = self.indexes.borrow();
            let index = indexes[pack].as_ref().expect("index was acquired");
            if let Some(position) = index.file.lookup(id) {
                return Ok(Some(PackLocation {
                    pack,
                    offset: index.file.pack_offset_at_index(position),
                }));
            }
        }
        control.check()?;
        Ok(None)
    }

    fn frame(&self, location: PackLocation, control: &Control) -> Result<Frame> {
        self.index(location.pack, control)?;
        let indexes = self.indexes.borrow();
        let index = indexes[location.pack].as_ref().expect("index was acquired");
        let position = index
            .offsets
            .binary_search_by_key(&location.offset, |(offset, _)| *offset)
            .map_err(|_| invalid("delta base does not point to a physical entry"))?;
        let end = match index.offsets.get(position + 1) {
            Some((offset, _)) => *offset,
            None => {
                self.source.pack_size(location.pack)?
                    - self.identity.object_hash.len_in_bytes() as u64
            }
        };
        Ok(Frame {
            location,
            id: index
                .file
                .oid_at_index(index.offsets[position].1)
                .to_owned(),
            end,
        })
    }

    fn entry(&self, frame: Frame, control: &Control) -> Result<gix_pack::data::Entry> {
        let mut header = [0; 64];
        let count = usize::try_from((frame.end - frame.location.offset).min(header.len() as u64))?;
        if self.proofs.borrow()[frame.location.pack].is_some() {
            let bytes = self.authenticated_bytes(
                Frame {
                    end: frame.location.offset + count as u64,
                    ..frame
                },
                control,
            )?;
            header[..count].copy_from_slice(bytes.as_slice());
        } else {
            self.source.read_exact(
                frame.location.pack,
                frame.location.offset,
                &mut header[..count],
                control,
                &self.budget,
            )?;
        }
        Ok(gix_pack::data::Entry::from_bytes(
            &header[..count],
            frame.location.offset,
            self.identity.object_hash,
        )?)
    }

    fn delta_sizes(
        &self,
        frame: Frame,
        entry: &gix_pack::data::Entry,
        control: &Control,
    ) -> Result<(u64, u64)> {
        let _workspace = self
            .budget
            .reservation(gix_zlib::Workspace::ZLIB_RS_0_6_8.entry_inflate)?;
        let mut decoder = gix_zlib::Decompress::default();
        let mut output = [0; 32];
        let mut input = [0; 32 * 1024];
        let (mut offset, mut have, mut consumed, mut written) = (entry.data_offset, 0, 0, 0);
        loop {
            control.check()?;
            if consumed == have && offset < frame.end {
                have = usize::try_from((frame.end - offset).min(input.len() as u64))?;
                if self.proofs.borrow()[frame.location.pack].is_some() {
                    let bytes = self.authenticated_bytes(
                        Frame {
                            location: PackLocation {
                                pack: frame.location.pack,
                                offset,
                            },
                            end: offset + have as u64,
                            ..frame
                        },
                        control,
                    )?;
                    input[..have].copy_from_slice(bytes.as_slice());
                } else {
                    self.source.read_exact(
                        frame.location.pack,
                        offset,
                        &mut input[..have],
                        control,
                        &self.budget,
                    )?;
                }
                offset += have as u64;
                consumed = 0;
            }
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let status = decoder.decompress(
                &input[consumed..have],
                &mut output[written..],
                gix_zlib::FlushDecompress::None,
            )?;
            let read = usize::try_from(decoder.total_in() - before_in)?;
            let produced = usize::try_from(decoder.total_out() - before_out)?;
            consumed += read;
            written += produced;
            if written as u64 > entry.decompressed_size {
                return Err(invalid(
                    "delta prefix exceeds the physical instruction size",
                ));
            }
            if let Ok((base, prefix)) =
                gix_pack::data::delta::decode_header_size(&output[..written])
                && let Ok((result, _)) =
                    gix_pack::data::delta::decode_header_size(&output[prefix..written])
            {
                return Ok((base, result));
            }
            if written == output.len()
                || status == gix_zlib::Status::StreamEnd
                || (read == 0 && produced == 0)
            {
                return Err(invalid("invalid range-backed delta size prefix"));
            }
        }
    }

    fn prepare(
        &self,
        location: PackLocation,
        control: &Control,
        limits: ReadLimits,
        active: &mut [Option<PackLocation>; 65],
        depth: usize,
        authenticate: bool,
    ) -> Result<DecodePlan> {
        let result = self.prepare_source(location, control, limits, active, depth, authenticate);
        self.source_result(location.pack, control, result)
    }

    fn prepare_source(
        &self,
        location: PackLocation,
        control: &Control,
        limits: ReadLimits,
        active: &mut [Option<PackLocation>; 65],
        depth: usize,
        authenticate: bool,
    ) -> Result<DecodePlan> {
        control.check()?;
        if depth > limits.max_delta_depth || active[..depth].contains(&Some(location)) {
            return Err(invalid("range-backed delta depth or cycle limit exceeded"));
        }
        active[depth] = Some(location);
        if authenticate {
            self.authenticate_pack(location.pack, control)?;
        }
        let frame = self.frame(location, control)?;
        let entry = self.entry(frame, control)?;
        let size = usize::try_from(entry.decompressed_size)?;
        if entry.header.is_base() && size > limits.max_object_bytes {
            return Err(Box::new(super::ObjectSizeLimit {
                size: entry.decompressed_size,
                limit: limits.max_object_bytes,
            }));
        }
        if size > limits.max_decode_bytes {
            return Err(invalid("range-backed instruction byte budget exceeded"));
        }
        let plan = if let Some(kind) = entry.header.as_kind() {
            DecodePlan {
                header: gix_object::Header {
                    kind,
                    size: entry.decompressed_size,
                },
                max_object: size,
                expanded: size,
                depth: 0,
                instructions: 0,
                workspace: size,
            }
        } else {
            let (base_size, result_size) = self.delta_sizes(frame, &entry, control)?;
            let result_size = usize::try_from(result_size)?;
            if base_size > limits.max_object_bytes as u64 || result_size > limits.max_object_bytes {
                return Err(Box::new(super::ObjectSizeLimit {
                    size: base_size.max(result_size as u64),
                    limit: limits.max_object_bytes,
                }));
            }
            let base = self.base(location, &entry, control)?;
            let base = self.prepare(base, control, limits, active, depth + 1, authenticate)?;
            if base.header.size != base_size {
                return Err(invalid("range-backed delta base size mismatch"));
            }
            let max_object = base.max_object.max(result_size);
            let instructions = base
                .instructions
                .checked_add(size)
                .ok_or_else(|| invalid("range-backed delta instruction size overflow"))?;
            DecodePlan {
                header: gix_object::Header {
                    kind: base.header.kind,
                    size: result_size as u64,
                },
                max_object,
                expanded: base
                    .expanded
                    .checked_add(size)
                    .and_then(|n| n.checked_add(result_size))
                    .ok_or_else(|| invalid("range-backed expanded-byte count overflow"))?,
                depth: base.depth + 1,
                instructions,
                workspace: max_object
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(instructions))
                    .ok_or_else(|| invalid("range-backed delta workspace overflow"))?,
            }
        };
        active[depth] = None;
        plan.validate(limits)?;
        control.check()?;
        Ok(plan)
    }

    fn base(
        &self,
        location: PackLocation,
        entry: &gix_pack::data::Entry,
        control: &Control,
    ) -> Result<PackLocation> {
        match entry.header {
            gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                let offset = entry
                    .checked_base_pack_offset(base_distance)
                    .filter(|offset| *offset < location.offset)
                    .ok_or_else(|| invalid("range-backed OFS base is not earlier in the pack"))?;
                let base = PackLocation {
                    pack: location.pack,
                    offset,
                };
                self.frame(base, control)?;
                Ok(base)
            }
            gix_pack::data::entry::Header::RefDelta { base_id } => self
                .locate(&base_id, control)?
                .ok_or_else(|| invalid("range-backed REF base is missing")),
            _ => Err(invalid("base requested for a non-delta object")),
        }
    }

    fn inflate(
        &self,
        frame: Frame,
        entry: &gix_pack::data::Entry,
        control: &Control,
        limits: ReadLimits,
    ) -> Result<CapacityBuffer> {
        let size = usize::try_from(entry.decompressed_size)?;
        if size > limits.max_decode_bytes {
            return Err(invalid("range-backed inflate byte limit exceeded"));
        }
        let length = usize::try_from(frame.end - frame.location.offset)?;
        let max_compressed = limits
            .max_decode_bytes
            .checked_add(limits.max_decode_bytes / 1000)
            .and_then(|n| n.checked_add(65536))
            .ok_or_else(|| invalid("range-backed compressed-size limit overflow"))?;
        if length > max_compressed {
            return Err(invalid("range-backed physical entry byte limit exceeded"));
        }
        let mut input = self.budget.buffer(length)?;
        input.resize_controlled(length, 0, control)?;
        self.source.read_exact(
            frame.location.pack,
            frame.location.offset,
            input.as_mut_slice(),
            control,
            &self.budget,
        )?;
        // The complete index has a catalog SHA-256 and its native checksum. The
        // CRC protects a range before inflation; the reconstructed object hash
        // is still mandatory, including when an index has no CRC (version 1).
        let expected = {
            let indexes = self.indexes.borrow();
            let index = indexes[frame.location.pack]
                .as_ref()
                .expect("index was acquired");
            index
                .file
                .lookup(frame.id)
                .and_then(|position| index.file.crc32_at_index(position))
        };
        if let Some(expected) = expected {
            let mut crc = crc32fast::Hasher::new();
            for chunk in input.as_slice().chunks(32 * 1024) {
                control.check()?;
                crc.update(chunk);
            }
            if crc.finalize() != expected {
                return Err(invalid("range-backed entry CRC mismatch"));
            }
        }
        let actual = gix_pack::data::Entry::from_bytes(
            input.as_slice(),
            frame.location.offset,
            self.identity.object_hash,
        )?;
        if &actual != entry {
            return Err(invalid("immutable range-backed entry header changed"));
        }
        let mut output = self.budget.buffer(size)?;
        output.resize_controlled(size, 0, control)?;
        let _workspace = self
            .budget
            .reservation(gix_zlib::Workspace::ZLIB_RS_0_6_8.entry_inflate)?;
        let header = usize::try_from(entry.data_offset - frame.location.offset)?;
        let (status, consumed, produced) = gix_zlib::Inflate::default().once_controlled(
            &input.as_slice()[header..],
            output.as_mut_slice(),
            &|| check(control),
        )?;
        if status != gix_zlib::Status::StreamEnd || produced != size || consumed != length - header
        {
            return Err(invalid("range-backed physical entry framing mismatch"));
        }
        control.check()?;
        Ok(output)
    }

    fn decode_at(
        &self,
        location: PackLocation,
        control: &Control,
        limits: ReadLimits,
        active: &mut [Option<PackLocation>; 65],
        depth: usize,
    ) -> Result<(Object, DecodePlan)> {
        let result = self.decode_source(location, control, limits, active, depth);
        self.source_result(location.pack, control, result)
    }

    fn decode_source(
        &self,
        location: PackLocation,
        control: &Control,
        limits: ReadLimits,
        active: &mut [Option<PackLocation>; 65],
        depth: usize,
    ) -> Result<(Object, DecodePlan)> {
        control.check()?;
        if depth > limits.max_delta_depth || active[..depth].contains(&Some(location)) {
            return Err(invalid("range-backed delta depth or cycle limit exceeded"));
        }
        let frame = self.frame(location, control)?;
        let key = Key {
            pack: u32::try_from(location.pack)?,
            offset: location.offset,
        };
        if let Some(pin) = self.runtime.pin(key, frame.id, &|| check(control))? {
            pin.plan().validate(limits)?;
            let plan = pin.plan();
            let object = VerifiedObject::from_cached(pin).into_object(control, &self.budget)?;
            return Ok((object, plan));
        }
        active[depth] = Some(location);
        let entry = self.entry(frame, control)?;
        let bytes = self.inflate(frame, &entry, control, limits)?;
        let (object, plan) = if let Some(kind) = entry.header.as_kind() {
            let size = bytes.as_slice().len();
            (
                Object { kind, data: bytes },
                DecodePlan {
                    header: gix_object::Header {
                        kind,
                        size: size as u64,
                    },
                    max_object: size,
                    expanded: size,
                    depth: 0,
                    instructions: 0,
                    workspace: size,
                },
            )
        } else {
            let (base_size, prefix) = gix_pack::data::delta::decode_header_size(bytes.as_slice())?;
            let (result_size, _) =
                gix_pack::data::delta::decode_header_size(&bytes.as_slice()[prefix..])?;
            let size = usize::try_from(result_size)?;
            if size > limits.max_object_bytes || base_size > limits.max_object_bytes as u64 {
                return Err(Box::new(super::ObjectSizeLimit {
                    size: base_size.max(size as u64),
                    limit: limits.max_object_bytes,
                }));
            }
            let base = self.base(location, &entry, control)?;
            let (base, base_plan) = self.decode_at(base, control, limits, active, depth + 1)?;
            if base.data.as_slice().len() as u64 != base_size {
                return Err(invalid("range-backed delta base size mismatch"));
            }
            let mut data = self.budget.buffer(size)?;
            data.resize_controlled(size, 0, control)?;
            gix_pack::data::delta::reconstruct_controlled(
                base.data.as_slice(),
                data.as_mut_slice(),
                bytes.as_slice(),
                &|| check(control),
            )?;
            let max_object = base_plan.max_object.max(size);
            let instructions = base_plan
                .instructions
                .checked_add(bytes.as_slice().len())
                .ok_or_else(|| invalid("range-backed delta instruction size overflow"))?;
            let plan = DecodePlan {
                header: gix_object::Header {
                    kind: base.kind,
                    size: result_size,
                },
                max_object,
                expanded: base_plan
                    .expanded
                    .checked_add(bytes.as_slice().len())
                    .and_then(|n| n.checked_add(size))
                    .ok_or_else(|| invalid("range-backed expanded-byte count overflow"))?,
                depth: base_plan.depth + 1,
                instructions,
                workspace: max_object
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(instructions))
                    .ok_or_else(|| invalid("range-backed delta workspace overflow"))?,
            };
            (
                Object {
                    kind: base.kind,
                    data,
                },
                plan,
            )
        };
        plan.validate(limits)?;
        super::verify_hash_with(&frame.id, object.kind, object.data.as_slice(), &|| {
            check(control)
        })?;
        self.runtime.note_decode();
        self.runtime.store_verified(
            key,
            frame.id,
            plan,
            usize::try_from(frame.end - entry.data_offset)?,
            object.data.as_slice(),
            &|| check(control),
        )?;
        self.runtime.remember(key, frame.id, plan, control)?;
        active[depth] = None;
        control.check()?;
        Ok((object, plan))
    }

    /// Enumerate every catalog index entry, including unreachable objects. The
    /// callback owns deduplication/admission if it retains IDs. No index borrow
    /// spans the callback, so it can resolve objects through this same provider.
    pub fn for_each_object(
        &self,
        control: &Control,
        mut visit: impl FnMut(ObjectId) -> Result<()>,
    ) -> Result<()> {
        for pack in 0..self.source.pack_count() {
            self.index(pack, control)?;
            let count = self.indexes.borrow()[pack]
                .as_ref()
                .expect("index was acquired")
                .file
                .num_objects();
            for position in 0..count {
                control.check()?;
                let id = self.indexes.borrow()[pack]
                    .as_ref()
                    .expect("index was acquired")
                    .file
                    .oid_at_index(position)
                    .to_owned();
                visit(id)?;
            }
        }
        control.check()?;
        Ok(())
    }
}

impl<S: PackSource> VerifiedObjectProvider for RangePackSet<S> {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &oid, control: &Control) -> Result<Option<gix_object::Header>> {
        let Some(location) = self.locate(id, control)? else {
            return Ok(None);
        };
        let key = Key {
            pack: u32::try_from(location.pack)?,
            offset: location.offset,
        };
        if let Some(plan) = self.runtime.memo(key, id.to_owned(), control)? {
            plan.validate(self.limits)?;
            return Ok(Some(plan.header));
        }
        self.prepare(location, control, self.limits, &mut [None; 65], 0, false)
            .map(|plan| Some(plan.header))
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        self.verified_object(id, control, budget, limits)?
            .map(|object| object.into_object(control, budget))
            .transpose()
    }
    fn verified_object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        control.check()?;
        limits.validate()?;
        if !budget.shares_with(&self.budget)
            || limits.max_decode_bytes > self.limits.max_decode_bytes
        {
            return Err(invalid(
                "range-backed provider budget/limit identity mismatch",
            ));
        }
        let Some(location) = self.locate(id, control)? else {
            return Ok(None);
        };
        let key = Key {
            pack: u32::try_from(location.pack)?,
            offset: location.offset,
        };
        if let Some(pin) = self.runtime.pin(key, id.to_owned(), &|| check(control))? {
            pin.plan().validate(limits)?;
            return Ok(Some(VerifiedObject::from_cached(pin)));
        }
        self.prepare(location, control, limits, &mut [None; 65], 0, false)?;
        let (object, plan) = self.decode_at(location, control, limits, &mut [None; 65], 0)?;
        Ok(Some(VerifiedObject::from_pack_verified(
            id,
            object.kind,
            object.data,
            plan,
        )))
    }
    fn pack_location(&self, id: &oid, control: &Control) -> Result<Option<PackLocation>> {
        self.locate(id, control)
    }
    fn packed_object(&self, id: &oid, control: &Control) -> Result<Option<PackedObject>> {
        self.packed_object(id, control)
    }
}

fn check(control: &Control) -> gix_error::Result {
    control
        .check()
        .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
}

// Small standard-library sorts followed by controlled merges avoid an opaque
// whole-index sort between cancellation polls. Both full allocations are charged.
fn sort_offsets(
    values: &mut [(u64, u32)],
    control: &Control,
    budget: &CapacityBudget,
) -> Result<()> {
    const RUN: usize = 1024;
    for chunk in values.chunks_mut(RUN) {
        control.check()?;
        chunk.sort_unstable_by_key(|(offset, _)| *offset);
    }
    let mut capacity = budget.reservation(std::mem::size_of_val(values))?;
    let mut temporary = Vec::new();
    temporary.try_reserve_exact(values.len())?;
    capacity.resize(temporary.capacity() * std::mem::size_of::<(u64, u32)>())?;
    for chunk in values.chunks(RUN) {
        control.check()?;
        temporary.extend_from_slice(chunk);
    }
    let mut width = RUN;
    while width < values.len() {
        for start in (0..values.len()).step_by(width * 2) {
            let middle = (start + width).min(values.len());
            let end = (middle + width).min(values.len());
            let (mut left, mut right) = (start, middle);
            for (offset, output) in values[start..end].iter_mut().enumerate() {
                if offset % 1024 == 0 {
                    control.check()?;
                }
                if left < middle && (right == end || temporary[left].0 <= temporary[right].0) {
                    *output = temporary[left];
                    left += 1;
                } else {
                    *output = temporary[right];
                    right += 1;
                }
            }
        }
        for (source, destination) in values.chunks(RUN).zip(temporary.chunks_mut(RUN)) {
            control.check()?;
            destination.copy_from_slice(source);
        }
        width = width
            .checked_mul(2)
            .ok_or_else(|| invalid("index sort width overflow"))?;
    }
    control.check()?;
    Ok(())
}
