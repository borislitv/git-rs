//! Explicit pack/index pairs and controlled cross-pack delta decoding. Pack
//! mappings are dropped before the caller's immutable source/pin guard.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;

use gix_hash::oid;

use crate::objects::{
    AdmittedVec, Object, Result, SnapshotIdentity, VerifiedObjectProvider, invalid,
};
use crate::{CapacityBudget, Control, ReadLimits};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackLocation {
    pub pack: usize,
    pub offset: u64,
}

pub struct PackSet<G> {
    bundles: Vec<gix_pack::Bundle>,
    // Fields drop in declaration order: all mappings precede the source guard.
    _source_guard: G,
    limits: ReadLimits,
}

struct DecodeState {
    active: RefCell<Vec<PackLocation>>,
    deltas: Cell<usize>,
    bytes: Cell<usize>,
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
        };
        for path in indexes {
            control.check()?;
            if set.bundles.len() >= limits.max_graph_objects {
                return Err(invalid("pack count limit exceeded"));
            }
            let mut bundle = gix_pack::Bundle::at(path, hash)?;
            bundle.pack = bundle
                .pack
                .with_alloc_limit_bytes(Some(limits.max_decode_bytes));
            set.bundles.push(bundle);
        }
        control.check()?;
        Ok(set)
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

    /// Decode with one shared cross-pack depth/byte budget. No cache is used, so
    /// path-derived CRC pack IDs cannot alias cached bases. The raw Vec is
    /// preallocated to the file's admitted allocation ceiling: the decoder may
    /// resize its length but cannot allocate a larger output buffer. Actual
    /// retained capacity (not length) is charged until the Vec is freed.
    pub fn decode(
        &self,
        id: &oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<Object>> {
        control.check()?;
        limits.validate()?;
        let Some(location) = self.locate(id, control)? else {
            return Ok(None);
        };
        if limits.max_decode_bytes > self.limits.max_decode_bytes || limits.max_delta_depth == 0 {
            return Err(invalid("decode limits exceed opened pack admission"));
        }
        let mut out = AdmittedVec::new(budget, self.limits.max_decode_bytes)?;
        // Delta records are at most 128 bytes each (the current maintained
        // decoder uses 56 on 64-bit hosts), and its spilled SmallVec grows to a
        // power of two. Active resolver entries use a second bounded Vec.
        let records = limits
            .max_delta_depth
            .checked_next_power_of_two()
            .and_then(|n| n.checked_mul(128 + std::mem::size_of::<PackLocation>()))
            .ok_or_else(|| invalid("delta metadata allocation overflow"))?;
        let _metadata = budget.reservation(records)?;
        let state = DecodeState {
            active: RefCell::new(Vec::new()),
            deltas: Cell::new(0),
            bytes: Cell::new(0),
        };
        state.active.borrow_mut().try_reserve_exact(
            limits
                .max_delta_depth
                .checked_add(1)
                .ok_or_else(|| invalid("delta depth overflow"))?,
        )?;
        let result = self.decode_at(location, out.vec_mut(), control, budget, limits, &state);
        out.reconcile()?;
        let kind = result?;
        verify_hash(id, kind, out.as_slice(), control)?;
        let mut data = budget.buffer(out.as_slice().len())?;
        for chunk in out.as_slice().chunks(32 * 1024) {
            control.check()?;
            let start = data.as_slice().len();
            data.resize(start + chunk.len(), 0)?;
            data.as_mut_slice()[start..].copy_from_slice(chunk);
        }
        control.check()?;
        Ok(Some(Object { kind, data }))
    }

    fn decode_at(
        &self,
        location: PackLocation,
        out: &mut Vec<u8>,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
        state: &DecodeState,
    ) -> Result<gix_object::Kind> {
        control.check()?;
        {
            let mut active = state.active.borrow_mut();
            if active.contains(&location) {
                return Err(invalid("cross-pack delta cycle"));
            }
            if active.len() > limits.max_delta_depth {
                return Err(invalid("cross-pack delta depth limit exceeded"));
            }
            active.push(location);
        }
        let result = (|| {
            let bundle = &self.bundles[location.pack];
            let entry = bundle.pack.entry(location.offset)?;
            // Walk maintained entry headers before inflate. This accounts local
            // OFS chains as well as REF chains, across every resolver invocation.
            let mut cursor = entry.clone();
            loop {
                control.check()?;
                let bytes = state
                    .bytes
                    .get()
                    .checked_add(usize::try_from(cursor.decompressed_size)?)
                    .ok_or_else(|| invalid("delta decoded-byte budget exceeded"))?;
                if bytes > limits.max_decode_bytes {
                    return Err(invalid("delta decoded-byte budget exceeded"));
                }
                state.bytes.set(bytes);
                if !cursor.header.is_delta() {
                    if cursor.decompressed_size > limits.max_object_bytes as u64 {
                        return Err(invalid("base object byte limit exceeded"));
                    }
                    break;
                }
                let depth = state
                    .deltas
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| invalid("delta depth limit exceeded"))?;
                if depth > limits.max_delta_depth {
                    return Err(invalid("delta depth limit exceeded"));
                }
                state.deltas.set(depth);
                // Account reconstructed intermediate objects too, not only the
                // tiny instruction streams of an expansion/copy bomb. Decode
                // headers with maintained primitives before applying deltas.
                let mut instructions =
                    AdmittedVec::new(budget, usize::try_from(cursor.decompressed_size)?)?;
                instructions.resize(usize::try_from(cursor.decompressed_size)?, control)?;
                let source = bundle
                    .pack
                    .entry_slice(cursor.data_offset..bundle.pack.pack_end() as u64)
                    .ok_or_else(|| invalid("truncated delta instruction stream"))?;
                let check = || {
                    control
                        .check()
                        .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
                };
                gix_zlib::Inflate::default().once_controlled(
                    source,
                    instructions.as_mut_slice(),
                    &check,
                )?;
                let (base_size, offset) =
                    gix_pack::data::delta::decode_header_size(instructions.as_slice())?;
                let (result_size, _) =
                    gix_pack::data::delta::decode_header_size(&instructions.as_slice()[offset..])?;
                if base_size > limits.max_object_bytes as u64
                    || result_size > limits.max_object_bytes as u64
                {
                    return Err(invalid("delta object byte limit exceeded"));
                }
                let bytes = state
                    .bytes
                    .get()
                    .checked_add(usize::try_from(result_size)?)
                    .ok_or_else(|| invalid("delta decoded-byte budget exceeded"))?;
                if bytes > limits.max_decode_bytes {
                    return Err(invalid("delta decoded-byte budget exceeded"));
                }
                state.bytes.set(bytes);
                match cursor.header {
                    gix_pack::data::entry::Header::OfsDelta { base_distance } => {
                        let offset = cursor
                            .checked_base_pack_offset(base_distance)
                            .ok_or_else(|| invalid("invalid OFS delta offset"))?;
                        if offset >= cursor.pack_offset() {
                            return Err(invalid("OFS delta cycle"));
                        }
                        cursor = bundle.pack.entry(offset)?;
                    }
                    _ => break,
                }
            }
            let check = || {
                control
                    .check()
                    .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))
            };
            let capacity = out.capacity();
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
                        .decode_at(location, buffer, control, budget, limits, state)
                        .map_err(gix_error::Error::from_boxed)?;
                    verify_hash(base, kind, buffer, control)
                        .map_err(gix_error::Error::from_boxed)?;
                    Ok(Some(
                        gix_pack::data::decode::entry::ResolvedBase::OutOfPack {
                            kind,
                            end: buffer.len(),
                        },
                    ))
                },
                &mut gix_pack::cache::Never,
                gix_pack::data::decode::Limits {
                    max_delta_depth: limits.max_delta_depth,
                    max_delta_bytes: limits.max_decode_bytes as u64,
                },
                &check,
            )?;
            if out.capacity() != capacity {
                return Err(invalid("controlled decoder grew beyond admitted capacity"));
            }
            if out.len() > limits.max_object_bytes {
                return Err(invalid("object byte limit exceeded"));
            }
            control.check()?;
            Ok(outcome.kind)
        })();
        state.active.borrow_mut().pop();
        if result.is_err() {
            out.clear();
        }
        result
    }
}

fn verify_hash(id: &oid, kind: gix_object::Kind, bytes: &[u8], control: &Control) -> Result<()> {
    let mut hash = gix_hash::hasher(id.kind());
    hash.update(&gix_object::encode::loose_header(kind, bytes.len() as u64));
    for chunk in bytes.chunks(32 * 1024) {
        control.check()?;
        hash.update(chunk);
    }
    if hash.try_finalize()?.as_ref() != id {
        return Err(invalid("physical pack object hash mismatch"));
    }
    control.check()?;
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
        self.packs
            .decode(id, control, &self.capacity, self.limits)
            .map(|object| {
                object.map(|object| gix_object::Header {
                    kind: object.kind,
                    size: object.data.as_slice().len() as u64,
                })
            })
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
    fn pack_location(&self, id: &oid, control: &Control) -> Result<Option<PackLocation>> {
        self.packs.locate(id, control)
    }
}
