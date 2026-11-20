//! Receive-only inventory of an immutable, self-contained offset-delta pack.
//! Reuses the admitted pack decoder and a short-lived, admitted window of verified bases.
use crate::{
    CapacityBudget, CapacityBuffer, Control, Object, ReadLimits, SnapshotIdentity,
    VerifiedObjectProvider,
};
use crate::{
    objects::{Result, invalid},
    storage::ChargedVec,
};
use gix_hash::{ObjectId, oid};
use gix_pack as pack;
use std::{
    cell::{Cell, RefCell},
    io::Write,
};

#[path = "packed_forest.rs"]
mod forest;

const HASH: gix_hash::Kind = gix_hash::Kind::Sha1;
const QUANTUM: usize = 32 * 1024;
// Bound controlled decoder work in addition to the semantic and absolute deadlines.
// Each poll spans at most one decoder quantum (or one metadata step), not one object.
const DECODER_POLLS: u64 = 2_000_000_000;
const CACHE_SLOTS: usize = 32;
const CACHE_BYTES: usize = 384 * 1024 * 1024;
const CACHE_MIN: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InventoryFailure {
    DecoderCheckpoints,
    LogicalBytes,
    ChainDepth,
    ChainWorkspace,
    ObjectBytes,
}

impl std::fmt::Display for InventoryFailure {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.write_str(match self {
            Self::DecoderCheckpoints => "packed receive decoder checkpoint limit",
            Self::LogicalBytes => "packed receive logical byte limit",
            Self::ChainDepth => "packed receive chain depth limit",
            Self::ChainWorkspace => "packed receive chain workspace limit",
            Self::ObjectBytes => "packed receive object byte limit",
        })
    }
}
impl std::error::Error for InventoryFailure {}

#[derive(Clone, Copy, Default, Debug)]
pub struct ReuseStatistics {
    pub hits: u64,
    /// Ancestor result and instruction bytes not materialized by the chain decoder.
    pub avoided_ancestor_bytes: u64,
}

/// Closed physical-inventory telemetry; never includes object IDs or bytes.
#[derive(Clone, Copy)]
pub enum Progress {
    Checksum,
    Entries,
    /// Exact framing, CRC and complete chain plans succeeded; reconstruction starts.
    Resolving {
        roots: usize,
        lanes: usize,
        largest_root_bytes: u64,
    },
    EntryBatch {
        objects: u64,
        logical_bytes: u64,
        checkpoints: u64,
    },
    Sort,
    /// Increment for one verified entry, emitted even if a later entry fails.
    Reuse(ReuseStatistics),
}

pub struct Packed<'a> {
    identity: &'a SnapshotIdentity,
    file: pack::data::File<&'a [u8]>,
    physical: ChargedVec<Record>,
    index: ChargedVec<(ObjectId, u32, u64)>,
    hash: ObjectId,
    logical_bytes: u64,
    decoder: RefCell<Decoder<'a>>,
    polls: Cell<u64>,
    reuse: ReuseStatistics,
    frontier_peak: usize,
    exhausted: Cell<bool>,
    _workspace: pack::memory::Reservation<'a>,
}

#[derive(Clone, Copy)]
struct Record {
    id: ObjectId,
    offset: u64,
    kind: gix_object::Kind,
    size: u64,
    plan: Plan,
}

#[derive(Clone, Copy)]
struct Plan {
    depth: usize,
    max_object: usize,
    instructions: usize,
    expanded: u64,
}

impl Plan {
    fn full(size: usize, limits: ReadLimits) -> Result<Self> {
        if size > limits.max_object_bytes {
            return Err(InventoryFailure::ObjectBytes.into());
        }
        if size > limits.max_decode_bytes {
            return Err(InventoryFailure::ChainWorkspace.into());
        }
        Ok(Self {
            depth: 0,
            max_object: size,
            instructions: 0,
            expanded: size as u64,
        })
    }

    fn child(self, size: usize, instructions: usize, limits: ReadLimits) -> Result<Self> {
        if size > limits.max_object_bytes {
            return Err(InventoryFailure::ObjectBytes.into());
        }
        let depth = self.depth + 1;
        if depth > limits.max_delta_depth {
            return Err(InventoryFailure::ChainDepth.into());
        }
        let max_object = self.max_object.max(size);
        let instructions = self
            .instructions
            .checked_add(instructions)
            .ok_or(InventoryFailure::ChainWorkspace)?;
        if max_object
            .checked_mul(2)
            .and_then(|n| n.checked_add(instructions))
            .is_none_or(|n| n > limits.max_decode_bytes)
        {
            return Err(InventoryFailure::ChainWorkspace.into());
        }
        Ok(Self {
            depth,
            max_object,
            instructions,
            expanded: self
                .expanded
                .checked_add((instructions - self.instructions) as u64)
                .and_then(|n| n.checked_add(size as u64))
                .ok_or(InventoryFailure::LogicalBytes)?,
        })
    }
}

struct BaseWindow {
    slots: [Option<CachedBase>; CACHE_SLOTS],
    bytes: usize,
    clock: u64,
}

struct CachedBase {
    offset: u64,
    last_used: u64,
    body: CapacityBuffer,
}

impl BaseWindow {
    fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
            bytes: 0,
            clock: 0,
        }
    }

    fn get(&mut self, offset: u64) -> Option<&[u8]> {
        let base = self
            .slots
            .iter_mut()
            .flatten()
            .find(|base| base.offset == offset)?;
        self.clock += 1;
        base.last_used = self.clock;
        Some(base.body.as_slice())
    }

    fn room(&mut self, size: usize) -> Option<usize> {
        if !(CACHE_MIN..=CACHE_BYTES).contains(&size) {
            return None;
        }
        loop {
            if self.bytes + size <= CACHE_BYTES
                && let Some(slot) = self.slots.iter().position(Option::is_none)
            {
                return Some(slot);
            }
            let victim = self
                .slots
                .iter_mut()
                .filter(|slot| slot.is_some())
                .min_by_key(|slot| slot.as_ref().map(|base| base.last_used))?;
            self.bytes -= victim.take()?.body.capacity();
        }
    }

    fn insert(&mut self, slot: usize, offset: u64, body: CapacityBuffer) {
        if self.bytes + body.capacity() <= CACHE_BYTES {
            self.bytes += body.capacity();
            self.clock += 1;
            self.slots[slot] = Some(CachedBase {
                offset,
                last_used: self.clock,
                body,
            });
        }
    }
}

struct Decoder<'a> {
    decoded: pack::memory::Buffer<'a, u8>,
    inflate: gix_zlib::Inflate,
}

fn checked(control: &Control) -> gix_error::Result {
    control
        .check()
        .map_err(|e| gix_error::Error::from_boxed(Box::new(e)))
}

impl<'a> Packed<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "explicit source, control and decoder admission limits"
    )]
    pub fn open(
        bytes: &'a [u8],
        identity: &'a SnapshotIdentity,
        control: &Control,
        budget: &CapacityBudget,
        decoder_budget: &'a dyn pack::memory::Budget,
        limits: ReadLimits,
        receive: super::ReceiveLimits,
        progress: impl FnMut(Progress),
    ) -> Result<Self> {
        Self::open_with_reuse(
            bytes,
            identity,
            control,
            budget,
            decoder_budget,
            limits,
            receive,
            false,
            progress,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "explicit limits and reuse selection"
    )]
    pub fn open_with_reuse(
        bytes: &'a [u8],
        identity: &'a SnapshotIdentity,
        control: &Control,
        budget: &CapacityBudget,
        decoder_budget: &'a dyn pack::memory::Budget,
        limits: ReadLimits,
        receive: super::ReceiveLimits,
        reuse: bool,
        mut progress: impl FnMut(Progress),
    ) -> Result<Self> {
        Self::open_mode(
            bytes,
            identity,
            control,
            budget,
            decoder_budget,
            limits,
            receive,
            reuse,
            false,
            &mut progress,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "explicit admission and format limits"
    )]
    pub fn open_forest(
        bytes: &'a [u8],
        identity: &'a SnapshotIdentity,
        control: &Control,
        budget: &CapacityBudget,
        decoder_budget: &'a dyn pack::memory::Budget,
        limits: ReadLimits,
        receive: super::ReceiveLimits,
        mut progress: impl FnMut(Progress),
    ) -> Result<Self> {
        Self::open_mode(
            bytes,
            identity,
            control,
            budget,
            decoder_budget,
            limits,
            receive,
            false,
            true,
            &mut progress,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "shared admission and input checks for all inventory modes"
    )]
    fn open_mode(
        bytes: &'a [u8],
        identity: &'a SnapshotIdentity,
        control: &Control,
        budget: &CapacityBudget,
        decoder_budget: &'a dyn pack::memory::Budget,
        limits: ReadLimits,
        receive: super::ReceiveLimits,
        reuse: bool,
        use_forest: bool,
        progress: &mut impl FnMut(Progress),
    ) -> Result<Self> {
        control.check()?;
        limits.validate()?;
        if identity.object_hash != HASH {
            return Err(invalid("packed receive requires SHA-1"));
        }
        let file = pack::data::File::from_data(bytes, Default::default(), HASH)?
            .with_alloc_limit_bytes(Some(limits.max_decode_bytes));
        let count = file.num_objects() as usize;
        let end = bytes
            .len()
            .checked_sub(20)
            .ok_or_else(|| invalid("truncated pack"))?;
        if count > receive.max_incoming_objects
            || count > limits.max_graph_objects
            || count > receive.max_object_reads
            || count > end.saturating_sub(12)
        {
            return Err(invalid(
                "incoming packed receive object count exceeds bounds",
            ));
        }
        progress(Progress::Checksum);
        let mut hash = gix_hash::hasher(HASH);
        for chunk in bytes[..end].chunks(QUANTUM) {
            control.check()?;
            hash.update(chunk);
        }
        let hash = hash.try_finalize()?;
        if hash.as_slice() != &bytes[end..] {
            return Err(invalid("packed receive checksum mismatch"));
        }
        progress(Progress::Entries);
        let workspace = pack::memory::Reservation::new(
            decoder_budget,
            gix_zlib::Workspace::ZLIB_RS_0_6_8.entry_inflate,
        )?;
        let mut packed = Self {
            identity,
            file,
            hash,
            logical_bytes: 0,
            physical: ChargedVec::new(budget.clone(), count)?,
            index: ChargedVec::new(budget.clone(), count)?,
            decoder: RefCell::new(Decoder {
                decoded: pack::memory::Buffer::new(decoder_budget),
                inflate: gix_zlib::Inflate::default(),
            }),
            polls: Cell::new(DECODER_POLLS),
            exhausted: Cell::new(false),
            reuse: ReuseStatistics::default(),
            frontier_peak: 0,
            _workspace: workspace,
        };
        if use_forest {
            forest::inventory(
                &mut packed,
                bytes,
                end,
                count,
                control,
                budget,
                limits,
                receive,
                progress,
            )?;
            return packed.finish_inventory(budget, control, progress);
        }
        let mut framing = budget.buffer(0)?;
        let mut offset = 12usize;
        let mut previous = (0, 0);
        let _window_metadata = budget.reservation(if reuse {
            std::mem::size_of::<BaseWindow>()
        } else {
            0
        })?;
        let mut window = reuse.then(BaseWindow::new);
        for ordinal in 1..=count {
            control.check()?;
            if offset >= end {
                return Err(invalid("truncated physical packed receive inventory"));
            }
            let entry = packed.file.entry(offset as u64)?;
            let base = match &entry.header {
                pack::data::entry::Header::RefDelta { .. } => {
                    return Err(invalid(
                        "large bootstrap requires full objects or OFS_DELTA",
                    ));
                }
                pack::data::entry::Header::OfsDelta { base_distance } => {
                    let base = entry
                        .checked_base_pack_offset(*base_distance)
                        .ok_or_else(|| invalid("invalid packed receive OFS base"))?;
                    let physical = packed.physical.as_slice();
                    Some(
                        physical[physical.binary_search_by_key(&base, |r| r.offset).map_err(
                            |_| invalid("OFS base is not a prior physical entry start"),
                        )?],
                    )
                }
                _ => None,
            };
            let data_offset = entry.data_offset;
            let start = usize::try_from(data_offset)?;
            if start >= end {
                return Err(invalid("packed receive entry header overlaps checksum"));
            }
            let mut decoder = packed.decoder.borrow_mut();
            let mut fast = None;
            let (plan, declared_size, mut consumed) = if let Some(base) = base {
                let size = usize::try_from(entry.decompressed_size)?;
                if size > limits.max_decode_bytes {
                    return Err(InventoryFailure::ChainWorkspace.into());
                }
                if framing.capacity() < size {
                    framing = budget.buffer(size)?;
                }
                framing.resize_controlled(size, 0, control)?;
                let (_, consumed, _) = packed.decoder_result(decoder.inflate.once_controlled(
                    &bytes[start..end],
                    framing.as_mut_slice(),
                    &|| packed.tick(control),
                ))?;
                let (base_size, prefix) =
                    pack::data::delta::decode_header_size(framing.as_slice())?;
                let (result_size, _) =
                    pack::data::delta::decode_header_size(&framing.as_slice()[prefix..])?;
                if base_size != base.size {
                    return Err(invalid("packed receive delta base size mismatch"));
                }
                let result_size = usize::try_from(result_size)?;
                let plan = base.plan.child(result_size, size, limits)?;
                if let Some(body) = window.as_mut().and_then(|cache| cache.get(base.offset)) {
                    let mut result = budget.buffer(result_size)?;
                    for start in (0..result_size).step_by(QUANTUM) {
                        packed.decoder_result(packed.tick(control))?;
                        result.resize(result_size.min(start + QUANTUM), 0)?;
                    }
                    packed.decoder_result(pack::data::delta::reconstruct_controlled(
                        body,
                        result.as_mut_slice(),
                        framing.as_slice(),
                        &|| packed.tick(control),
                    ))?;
                    fast = Some((base.kind, result));
                }
                (plan, result_size, consumed)
            } else {
                let size = usize::try_from(entry.decompressed_size)?;
                (Plan::full(size, limits)?, size, 0)
            };
            let (kind, decoded) = if let Some((kind, ref body)) = fast {
                (kind, body.as_slice())
            } else {
                let outcome = packed.decode(entry, &mut decoder, control, limits)?;
                if base.is_none() {
                    consumed = outcome.compressed_size;
                }
                (outcome.kind, &decoder.decoded[..])
            };
            if decoded.len() != declared_size || base.is_some_and(|r| r.kind != kind) {
                return Err(invalid(
                    "packed receive decoded object differs from physical plan",
                ));
            }
            let logical_bytes = packed
                .logical_bytes
                .checked_add(decoded.len() as u64)
                .filter(|total| *total <= receive.max_decoded_bytes)
                .ok_or(InventoryFailure::LogicalBytes)?;
            let mut object_hash = gix_hash::hasher(HASH);
            object_hash.update(&gix_object::encode::loose_header(
                kind,
                decoded.len() as u64,
            ));
            for chunk in decoded.chunks(QUANTUM) {
                control.check()?;
                object_hash.update(chunk);
            }
            let id = object_hash.try_finalize()?;
            let next = start
                .checked_add(consumed)
                .filter(|next| consumed > 0 && *next > offset && *next <= end)
                .ok_or_else(|| invalid("packed receive entry overlaps checksum"))?;
            let mut crc = crc32fast::Hasher::new();
            for chunk in bytes[offset..next].chunks(QUANTUM) {
                control.check()?;
                crc.update(chunk);
            }
            let hit = fast.is_some();
            if let Some(cache) = &mut window
                && let Some(slot) = cache.room(declared_size)
            {
                let body = if let Some((_, body)) = fast {
                    body
                } else {
                    let mut body = budget.buffer(declared_size)?;
                    for chunk in decoded.chunks(QUANTUM) {
                        packed.decoder_result(packed.tick(control))?;
                        body.extend_from_slice(chunk)?;
                    }
                    body
                };
                cache.insert(slot, offset as u64, body);
            }
            drop(decoder);
            packed.logical_bytes = logical_bytes;
            packed.physical.push(
                Record {
                    id,
                    offset: offset as u64,
                    kind,
                    size: declared_size as u64,
                    plan,
                },
                control,
            )?;
            packed
                .index
                .push((id, crc.finalize(), offset as u64), control)?;
            if hit && let Some(base) = base {
                packed.reuse.hits += 1;
                packed.reuse.avoided_ancestor_bytes += base.plan.expanded;
                progress(Progress::Reuse(ReuseStatistics {
                    hits: 1,
                    avoided_ancestor_bytes: base.plan.expanded,
                }));
            }
            offset = next;
            if ordinal % 256 == 0 || ordinal == count {
                let (_, logical, checkpoints) = packed.statistics();
                progress(Progress::EntryBatch {
                    objects: ((ordinal - 1) % 256 + 1) as u64,
                    logical_bytes: logical - previous.0,
                    checkpoints: checkpoints - previous.1,
                });
                previous = (logical, checkpoints);
            }
        }
        if offset != end {
            return Err(invalid("packed receive count or trailing bytes mismatch"));
        }
        packed.finish_inventory(budget, control, progress)
    }

    fn finish_inventory(
        mut self,
        budget: &CapacityBudget,
        control: &Control,
        progress: &mut impl FnMut(Progress),
    ) -> Result<Self> {
        progress(Progress::Sort);
        crate::storage::sort_controlled(self.index.as_mut_slice(), budget, control, |a, b| {
            a.0.cmp(&b.0)
        })?;
        for pair in self.index.as_slice().windows(2) {
            control.check()?;
            if pair[0].0 == pair[1].0 {
                return Err(invalid(
                    "duplicate physical object ID in large packed receive",
                ));
            }
        }
        Ok(self)
    }

    fn tick(&self, control: &Control) -> gix_error::Result {
        checked(control)?;
        let remaining = self.polls.get().checked_sub(1).ok_or_else(|| {
            self.exhausted.set(true);
            gix_error::Error::from_boxed(Box::new(InventoryFailure::DecoderCheckpoints))
        })?;
        self.polls.set(remaining);
        Ok(())
    }

    fn decoder_result<T>(&self, result: gix_error::Result<T>) -> Result<T> {
        match result {
            Err(_) if self.exhausted.get() => Err(InventoryFailure::DecoderCheckpoints.into()),
            other => Ok(other?),
        }
    }

    pub fn reuse_statistics(&self) -> ReuseStatistics {
        self.reuse
    }

    /// Largest charged branched frontier in one forest lane (excludes its
    /// transient parent, result and instruction buffers).
    pub fn frontier_peak(&self) -> usize {
        self.frontier_peak
    }

    /// Physical pack order, irrespective of reconstruction scheduling.
    pub fn physical_ids(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.physical.as_slice().iter().map(|record| record.id)
    }

    fn decode(
        &self,
        entry: pack::data::Entry,
        decoder: &mut Decoder<'a>,
        control: &Control,
        limits: ReadLimits,
    ) -> Result<pack::data::decode::entry::Outcome> {
        let outcome = self.decoder_result(self.file.decode_entry_admitted(
            entry,
            &mut decoder.decoded,
            &mut decoder.inflate,
            &|_, _| Ok(None),
            pack::data::decode::Limits {
                max_delta_depth: limits.max_delta_depth,
                max_delta_bytes: limits.max_decode_bytes as u64,
            },
            &|| self.tick(control),
        ))?;
        if decoder.decoded.len() > limits.max_object_bytes {
            return Err(invalid("packed receive object size limit exceeded"));
        }
        Ok(outcome)
    }

    fn at(
        &self,
        offset: u64,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Object> {
        let mut decoder = self.decoder.borrow_mut();
        let outcome = self.decode(self.file.entry(offset)?, &mut decoder, control, limits)?;
        let mut data = budget.buffer(decoder.decoded.len())?;
        for chunk in decoder.decoded.chunks(QUANTUM) {
            control.check()?;
            data.extend_from_slice(chunk)?;
        }
        Ok(Object {
            kind: outcome.kind,
            data,
        })
    }

    pub fn hash(&self) -> ObjectId {
        self.hash
    }

    pub fn write_index(&self, output: &mut dyn Write, control: &Control) -> Result<()> {
        pack::index::write::write_sorted_entries_controlled(
            output,
            self.index.as_slice(),
            &self.hash,
            HASH,
            &|| checked(control),
        )?;
        Ok(())
    }

    pub fn statistics(&self) -> (usize, u64, u64) {
        (
            self.physical.as_slice().len(),
            self.logical_bytes,
            DECODER_POLLS - self.polls.get(),
        )
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (ObjectId, gix_object::Kind)> + '_ {
        self.physical
            .as_slice()
            .iter()
            .map(|entry| (entry.id, entry.kind))
    }

    fn record(&self, id: &oid) -> Option<Record> {
        let index = self.index.as_slice();
        let offset = index[index
            .binary_search_by(|item| item.0.as_slice().cmp(id.as_bytes()))
            .ok()?]
        .2;
        let records = self.physical.as_slice();
        let record = records[records.binary_search_by_key(&offset, |r| r.offset).ok()?];
        (record.id.as_slice() == id.as_bytes()).then_some(record)
    }
}

impl VerifiedObjectProvider for &Packed<'_> {
    fn identity(&self) -> &SnapshotIdentity {
        self.identity
    }

    fn header(&self, id: &oid, control: &Control) -> Result<Option<gix_object::Header>> {
        control.check()?;
        Ok(self.record(id).map(|r| gix_object::Header {
            kind: r.kind,
            size: r.size,
        }))
    }

    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        control.check()?;
        self.record(id)
            .map(|r| self.at(r.offset, control, budget, limits))
            .transpose()
    }
}
