//! Explicit pack/index pairs and controlled cross-pack delta decoding. Pack
//! mappings are dropped before the caller's immutable source/pin guard.

use std::path::PathBuf;
use std::sync::Arc;

use gix_hash::oid;

use crate::objects::{
    AdmittedVec, Object, Result, SnapshotIdentity, VerifiedObject, VerifiedObjectProvider, invalid,
};
use crate::{CapacityBudget, Control, ReadLimits};

#[path = "pack_cache.rs"]
mod acceleration;
pub use acceleration::{CachedObject, DecoderCacheStats, DecoderOptions};
use acceleration::{EntryCache, Key, Runtime, Workspace};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackLocation {
    pub pack: usize,
    pub offset: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct DecodePlan {
    header: gix_object::Header,
    max_object: usize,
    expanded: usize,
    depth: usize,
    instructions: usize,
    workspace: usize,
}

impl DecodePlan {
    pub(crate) fn validate(self, limits: ReadLimits) -> Result<()> {
        if self.max_object > limits.max_object_bytes {
            return Err(invalid("delta object byte limit exceeded"));
        }
        if self.expanded > limits.max_decode_bytes {
            return Err(invalid("delta decoded-byte budget exceeded"));
        }
        if self.depth > limits.max_delta_depth {
            return Err(invalid("delta depth limit exceeded"));
        }
        if self.workspace > limits.max_decode_bytes {
            return Err(invalid("decode workspace byte budget exceeded"));
        }
        Ok(())
    }
}

pub struct PackSet<G> {
    bundles: Vec<gix_pack::Bundle>,
    // Fields drop in declaration order: all mappings precede the source guard.
    _source_guard: G,
    limits: ReadLimits,
    runtime: Option<Arc<Runtime>>,
}

struct Prepared {
    plans: Vec<(PackLocation, DecodePlan)>,
    active: Vec<PackLocation>,
    _metadata: crate::CapacityReservation,
}

impl Prepared {
    fn new(budget: &CapacityBudget, depth: usize) -> Result<Self> {
        let count = depth
            .checked_add(1)
            .ok_or_else(|| invalid("delta depth overflow"))?;
        let vendor = count
            .checked_next_power_of_two()
            .and_then(|n| n.checked_mul(128))
            .ok_or_else(|| invalid("delta metadata allocation overflow"))?;
        let bytes = count
            .checked_mul(
                std::mem::size_of::<(PackLocation, DecodePlan)>()
                    + std::mem::size_of::<PackLocation>(),
            )
            .and_then(|n| n.checked_add(vendor))
            .ok_or_else(|| invalid("delta metadata allocation overflow"))?;
        let mut metadata = budget.reservation(bytes)?;
        let mut plans = Vec::new();
        let mut active = Vec::new();
        plans.try_reserve_exact(count)?;
        active.try_reserve_exact(count)?;
        metadata.resize(
            plans.capacity() * std::mem::size_of::<(PackLocation, DecodePlan)>()
                + active.capacity() * std::mem::size_of::<PackLocation>()
                + vendor,
        )?;
        Ok(Self {
            plans,
            active,
            _metadata: metadata,
        })
    }
    fn plan(&self, location: PackLocation) -> DecodePlan {
        self.plans
            .iter()
            .find(|(at, _)| *at == location)
            .expect("preflight covered each resolver entry")
            .1
    }
}

impl<G> PackSet<G> {
    /// `indexes` comes from the verified catalog, never config/discovery.
    /// Symlinks are intentionally followed while the caller's pins are held.
    pub fn open(
        indexes: impl IntoIterator<Item = PathBuf>,
        source_guard: G,
        hash: gix_hash::Kind,
        control: &Control,
        limits: ReadLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let mut set = Self {
            bundles: Vec::new(),
            _source_guard: source_guard,
            limits,
            runtime: None,
        };
        for path in indexes {
            control.check()?;
            if set.bundles.len() >= limits.max_graph_objects {
                return Err(invalid("pack count limit exceeded"));
            }
            let pack_id = u32::try_from(set.bundles.len())
                .map_err(|_| invalid("snapshot pack ID space exhausted"))?;
            let mut bundle = gix_pack::Bundle::at(path, hash)?;
            // Never use the library's path CRC as cache identity. Ordinals are
            // snapshot-local, checked, immutable, and never reused in this set.
            bundle.pack.id = pack_id;
            bundle.pack = bundle
                .pack
                .with_alloc_limit_bytes(Some(limits.max_decode_bytes));
            set.bundles.push(bundle);
        }
        control.check()?;
        Ok(set)
    }

    /// Explicit, snapshot-owned acceleration. Every later decode must use a
    /// clone of this exact budget, not another budget with an equal limit.
    pub fn open_with_options(
        indexes: impl IntoIterator<Item = PathBuf>,
        source_guard: G,
        hash: gix_hash::Kind,
        control: &Control,
        limits: ReadLimits,
        options: DecoderOptions,
        budget: CapacityBudget,
    ) -> Result<Self> {
        let mut set = Self::open(indexes, source_guard, hash, control, limits)?;
        set.runtime = Some(Runtime::new(options, budget, control)?);
        Ok(set)
    }

    /// The exact identity supplied to the maintained decoder for this source pack.
    pub fn source_pack_id(&self, location: PackLocation) -> Option<u32> {
        self.bundles.get(location.pack).map(|bundle| bundle.pack.id)
    }

    pub fn locate(&self, id: &oid, control: &Control) -> Result<Option<PackLocation>> {
        for (pack, bundle) in self.bundles.iter().enumerate() {
            control.check()?;
            if let Some(index) = bundle.index.lookup(id) {
                return Ok(Some(PackLocation {
                    pack,
                    offset: bundle.index.pack_offset_at_index(index),
                }));
            }
        }
        control.check()?;
        Ok(None)
    }

    fn check_call(
        &self,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<()> {
        control.check()?;
        limits.validate()?;
        if limits.max_decode_bytes > self.limits.max_decode_bytes {
            return Err(invalid("decode limits exceed opened pack admission"));
        }
        if self
            .runtime
            .as_ref()
            .is_some_and(|runtime| !runtime.budget.shares_with(budget))
        {
            return Err(invalid(
                "decoder acceleration requires its original shared capacity budget",
            ));
        }
        Ok(())
    }

    fn key(&self, location: PackLocation) -> Result<Key> {
        let bundle = &self.bundles[location.pack];
        Ok(Key {
            pack: bundle.pack.id,
            offset: bundle.pack.entry(location.offset)?.data_offset,
        })
    }

    pub fn cache_stats(&self, control: &Control) -> Result<DecoderCacheStats> {
        control.check()?;
        self.runtime
            .as_ref()
            .map_or(Ok(DecoderCacheStats::default()), |runtime| {
                runtime.stats(control)
            })
    }

    /// Pin only an already verified decoded entry. A tiny borrowed slice keeps
    /// its entire backing allocation and admission alive, even after eviction.
    pub fn cached_object(
        &self,
        id: &oid,
        control: &Control,
        limits: ReadLimits,
    ) -> Result<Option<CachedObject>> {
        control.check()?;
        limits.validate()?;
        if limits.max_decode_bytes > self.limits.max_decode_bytes {
            return Err(invalid("decode limits exceed opened pack admission"));
        }
        let Some(runtime) = &self.runtime else {
            return Ok(None);
        };
        let Some(location) = self.locate(id, control)? else {
            return Ok(None);
        };
        let check = || {
            control
                .check()
                .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
        };
        let entry = runtime.pin(self.key(location)?, id.to_owned(), &check)?;
        if let Some(entry) = &entry {
            entry.plan().validate(limits)?;
        }
        Ok(entry)
    }

    /// Only verified headers are memoized. A cold header read uses the same
    /// controlled, actual-size workspace and identity checks as a full read.
    pub fn header(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<gix_object::Header>> {
        self.check_call(control, budget, limits)?;
        let Some(location) = self.locate(id, control)? else {
            return Ok(None);
        };
        if let Some(runtime) = &self.runtime
            && let Some(plan) = runtime.memo(self.key(location)?, id.to_owned(), control)?
        {
            plan.validate(limits)?;
            return Ok(Some(plan.header));
        }
        self.decode_verified(id, control, budget, limits)
            .map(|object| object.map(|object| object.header()))
    }

    /// Preflight accounts the full chain even on cache hits. Workspace size is
    /// derived from actual entries, not the configured 256 MiB decode ceiling.
    pub fn decode(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        self.decode_verified(id, control, budget, limits)?
            .map(|object| object.into_object(control, budget))
            .transpose()
    }

    /// Resolve this snapshot's physical membership and limits, then return an
    /// immutable value whose exact identity/bytes have already been verified.
    pub fn decode_verified(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        self.check_call(control, budget, limits)?;
        let Some(location) = self.locate(id, control)? else {
            return Ok(None);
        };
        if let Some(runtime) = &self.runtime {
            let check = || {
                control
                    .check()
                    .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
            };
            if let Some(pin) = runtime.pin(self.key(location)?, id.to_owned(), &check)? {
                pin.plan().validate(limits)?;
                check()?;
                return Ok(Some(VerifiedObject::from_cached(pin)));
            }
        }
        let mut prepared = Prepared::new(budget, limits.max_delta_depth)?;
        let plan = self.prepare(location, control, budget, limits, &mut prepared)?;
        if plan.workspace > self.limits.max_decode_bytes {
            return Err(invalid("decode workspace exceeds opened pack admission"));
        }
        let mut out = Workspace::new(self.runtime.as_ref(), budget, plan.workspace, control)?;
        let kind = self.decode_at(
            location,
            id,
            out.buffer().vec_mut(),
            control,
            limits,
            &prepared,
        );
        out.buffer().reconcile()?;
        let kind = kind?;
        if let Some(runtime) = &self.runtime {
            let check = || {
                control
                    .check()
                    .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
            };
            if let Some(pin) = runtime.pin(self.key(location)?, id.to_owned(), &check)? {
                plan.validate(limits)?;
                return Ok(Some(VerifiedObject::from_cached(pin)));
            }
        }
        let mut data = budget.buffer(out.bytes().len())?;
        for chunk in out.bytes().chunks(32 * 1024) {
            control.check()?;
            let start = data.as_slice().len();
            data.resize(start + chunk.len(), 0)?;
            data.as_mut_slice()[start..].copy_from_slice(chunk);
        }
        control.check()?;
        Ok(Some(VerifiedObject::from_pack_verified(
            id, kind, data, plan,
        )))
    }

    fn prepare(
        &self,
        location: PackLocation,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
        prepared: &mut Prepared,
    ) -> Result<DecodePlan> {
        control.check()?;
        if prepared.active.contains(&location) {
            return Err(invalid("cross-pack delta cycle"));
        }
        if prepared.active.len() > limits.max_delta_depth {
            return Err(invalid("delta depth limit exceeded"));
        }
        prepared.active.push(location);
        let result = (|| {
            let bundle = &self.bundles[location.pack];
            let entry = bundle.pack.entry(location.offset)?;
            let size = usize::try_from(entry.decompressed_size)?;
            if size > limits.max_decode_bytes {
                return Err(invalid("delta decoded-byte budget exceeded"));
            }
            let mut plan = if let Some(kind) = entry.header.as_kind() {
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
                let mut instructions = AdmittedVec::new(budget, size)?;
                instructions.resize(size, control)?;
                let source = bundle
                    .pack
                    .entry_slice(entry.data_offset..bundle.pack.pack_end() as u64)
                    .ok_or_else(|| invalid("truncated delta instruction stream"))?;
                let check = || {
                    control
                        .check()
                        .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
                };
                let (status, _, produced) = gix_zlib::Inflate::default().once_controlled(
                    source,
                    instructions.as_mut_slice(),
                    &check,
                )?;
                if status != gix_zlib::Status::StreamEnd || produced != size {
                    return Err(invalid("delta instruction stream size mismatch"));
                }
                let (base_size, offset) =
                    gix_pack::data::delta::decode_header_size(instructions.as_slice())?;
                let (result_size, _) =
                    gix_pack::data::delta::decode_header_size(&instructions.as_slice()[offset..])?;
                let result_size = usize::try_from(result_size)?;
                if base_size > limits.max_object_bytes as u64
                    || result_size > limits.max_object_bytes
                {
                    return Err(invalid("delta object byte limit exceeded"));
                }
                // No instruction storage survives recursive preflight.
                drop(instructions);
                let base = match entry.header {
                    gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                        let offset = entry
                            .checked_base_pack_offset(base_distance)
                            .ok_or_else(|| invalid("invalid OFS delta offset"))?;
                        if offset >= location.offset {
                            return Err(invalid("OFS delta cycle"));
                        }
                        PackLocation {
                            pack: location.pack,
                            offset,
                        }
                    }
                    gix_pack::data::entry::Header::RefDelta { base_id } => self
                        .locate(base_id.as_ref(), control)?
                        .ok_or_else(|| invalid("missing REF delta base"))?,
                    _ => unreachable!("non-delta kinds handled above"),
                };
                let base = self.prepare(base, control, budget, limits, prepared)?;
                if base.header.size != base_size {
                    return Err(invalid("delta base size mismatch"));
                }
                let instructions = base
                    .instructions
                    .checked_add(size)
                    .ok_or_else(|| invalid("delta instruction size overflow"))?;
                let max_object = base.max_object.max(result_size);
                let workspace = max_object
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(instructions))
                    .ok_or_else(|| invalid("decode workspace overflow"))?;
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
                        .ok_or_else(|| invalid("delta decoded-byte budget exceeded"))?,
                    depth: base
                        .depth
                        .checked_add(1)
                        .ok_or_else(|| invalid("delta depth overflow"))?,
                    instructions,
                    workspace,
                }
            };
            plan.validate(limits)?;
            // An empty object still needs no output backing.
            plan.workspace = plan.workspace.max(plan.header.size as usize);
            prepared.plans.push((location, plan));
            Ok(plan)
        })();
        prepared.active.pop();
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_at(
        &self,
        location: PackLocation,
        id: &oid,
        out: &mut Vec<u8>,
        control: &Control,
        limits: ReadLimits,
        prepared: &Prepared,
    ) -> Result<gix_object::Kind> {
        control.check()?;
        let plan = prepared.plan(location);
        let bundle = &self.bundles[location.pack];
        let entry = bundle.pack.entry(location.offset)?;
        let check = || {
            control
                .check()
                .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
        };
        let key = Key {
            pack: bundle.pack.id,
            offset: entry.data_offset,
        };
        if let Some(runtime) = &self.runtime {
            if let Some(pin) = runtime.pin(key, id.to_owned(), &check)? {
                out.clear();
                if pin.as_slice().len() > out.capacity() {
                    return Err(invalid("cached object exceeds admitted workspace"));
                }
                for chunk in pin.as_slice().chunks(32 * 1024) {
                    check()?;
                    out.extend_from_slice(chunk);
                }
                check()?;
                return Ok(pin.header().kind);
            }
            runtime.note_decode();
        }
        let capacity = out.capacity();
        let mut cache = EntryCache {
            runtime: self.runtime.clone(),
            key,
            plan,
        };
        let outcome = bundle.pack.decode_entry_controlled(
            entry,
            out,
            &mut Default::default(),
            &|base, buffer| {
                check()?;
                let Some(location) = self
                    .locate(base, control)
                    .map_err(gix_error::Error::from_boxed)?
                else {
                    return Ok(None);
                };
                let kind = self
                    .decode_at(location, base, buffer, control, limits, prepared)
                    .map_err(gix_error::Error::from_boxed)?;
                Ok(Some(
                    gix_pack::data::decode::entry::ResolvedBase::OutOfPack {
                        kind,
                        end: buffer.len(),
                    },
                ))
            },
            &mut cache,
            gix_pack::data::decode::Limits {
                max_delta_depth: limits.max_delta_depth,
                max_delta_bytes: limits.max_decode_bytes as u64,
            },
            &check,
        )?;
        if out.capacity() != capacity {
            return Err(invalid("controlled decoder grew beyond admitted capacity"));
        }
        if outcome.kind != plan.header.kind || out.len() as u64 != plan.header.size {
            return Err(invalid("decoded object disagrees with preflight header"));
        }
        verify_hash_with(id, outcome.kind, out, &check)?;
        if let Some(runtime) = &self.runtime {
            runtime.store_verified(
                key,
                id.to_owned(),
                plan,
                outcome.compressed_size,
                out,
                &check,
            )?;
            runtime.remember(key, id.to_owned(), plan, control)?;
        }
        control.check()?;
        Ok(outcome.kind)
    }
}

fn verify_hash_with(
    id: &oid,
    kind: gix_object::Kind,
    bytes: &[u8],
    check: &dyn Fn() -> gix_error::Result,
) -> gix_error::Result {
    check()?;
    let mut hash = gix_hash::hasher(id.kind());
    hash.update(&gix_object::encode::loose_header(kind, bytes.len() as u64));
    for chunk in bytes.chunks(32 * 1024) {
        check()?;
        hash.update(chunk);
    }
    if hash.try_finalize()?.as_ref() != id {
        return Err(gix_error::corruption("physical pack object hash mismatch").into());
    }
    check()?;
    Ok(())
}

pub struct PackProvider<G> {
    identity: SnapshotIdentity,
    packs: PackSet<G>,
    capacity: CapacityBudget,
    limits: ReadLimits,
}

impl<G> PackProvider<G> {
    pub fn new(
        identity: SnapshotIdentity,
        packs: PackSet<G>,
        capacity: CapacityBudget,
        limits: ReadLimits,
    ) -> Self {
        Self {
            identity,
            packs,
            capacity,
            limits,
        }
    }
}

impl<G> VerifiedObjectProvider for PackProvider<G> {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &oid, control: &Control) -> Result<Option<gix_object::Header>> {
        self.packs.header(id, control, &self.capacity, self.limits)
    }
    fn object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        self.packs.decode(id, control, budget, limits)
    }
    fn verified_object(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        self.packs.decode_verified(id, control, budget, limits)
    }
    fn pack_location(&self, id: &oid, control: &Control) -> Result<Option<PackLocation>> {
        self.packs.locate(id, control)
    }
}
