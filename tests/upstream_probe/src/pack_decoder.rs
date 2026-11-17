//! Stored-entry proof only. The caller owns the authenticated catalog and the
//! immutable source hold. This module neither authenticates whole packs nor
//! accepts a receive stream, writes an index, or grants source authority.
#![forbid(unsafe_code)]

use gix_error::{ErrorExt, Result, ResultExt, allocation_limit, bail, corruption};
use gix_hash::{Kind, ObjectId};
use gix_pack::{
    cache,
    data::{self, Entry, File, decode, entry::Header},
};

const START: u64 = data::header::SIZE as u64;
// Enough for the public parser's u64 size and OFS fields or a REF OID.
const HEADER_PROBE: usize = 64;
const MAX_DEPTH: usize = 64;

/// Original metadata from an authenticated v2 index, in physical order.
#[derive(Clone, Copy, Debug)]
pub struct IndexedEntry {
    pub offset: u64,
    pub oid: ObjectId,
    pub crc32: u32,
}

pub struct Pack<'a> {
    /// The original trailer's offset, not the total pack length.
    pub entries_end: u64,
    pub entries: &'a [IndexedEntry],
}

/// Fill exactly `out.len()` bytes or return an error, under the caller's hold
/// and deadline. The adapter caps requests, not allocations inside the source.
pub trait RangeSource {
    fn read_exact(&mut self, pack: usize, offset: u64, out: &mut [u8]) -> Result;
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_packs: usize,
    pub max_catalog_entries: usize,
    pub max_entry_bytes: usize,
    /// Cumulative bytes requested, including header probes.
    pub max_source_bytes: usize,
    pub max_depth: usize,
    pub max_object_bytes: usize,
    /// Full base + each delta's inflated instructions and reconstructed result.
    pub max_expanded_bytes: usize,
    /// Overlapping original, synthetic, base and decoder Vec capacities. Not an
    /// RSS bound: excludes borrowed catalog, fixed stack and library workspace.
    pub max_live_buffer_bytes: usize,
}

#[derive(Default, Debug)]
pub struct Stats {
    pub source_reads: usize,
    pub source_bytes: usize,
    pub depth: usize,
    pub expanded_bytes: usize,
    pub peak_buffer_capacity: usize,
}

#[derive(Debug)]
pub struct Decoded {
    pub kind: gix_object::Kind,
    pub bytes: Vec<u8>,
    pub stats: Stats,
}

pub struct PackEntryDecoder<'a> {
    hash: Kind,
    packs: &'a [Pack<'a>],
    limits: Limits,
}

/// (Pack ordinal, index position in physical order). Never a synthetic offset.
type Location = (usize, usize);

struct Step {
    location: Location,
    entry: Entry,
    base_id: Option<ObjectId>,
}

struct Verified {
    oid: ObjectId,
    kind: gix_object::Kind,
    bytes: Vec<u8>,
}

fn sum(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| allocation_limit("pack proof byte count overflow").raise())
}

impl<'a> PackEntryDecoder<'a> {
    pub fn new(hash: Kind, packs: &'a [Pack<'a>], limits: Limits) -> Result<Self> {
        if packs.len() > limits.max_packs || limits.max_depth > MAX_DEPTH {
            bail!(allocation_limit("pack count or depth profile exceeded"));
        }
        let mut count = 0;
        for pack in packs {
            count = sum(count, pack.entries.len())?;
            if count > limits.max_catalog_entries {
                bail!(allocation_limit("catalog entry count exceeded"));
            }
            if pack.entries_end > u64::MAX - hash.len_in_bytes() as u64
                || pack.entries.first().map_or(pack.entries_end, |e| e.offset) != START
                || pack.entries.windows(2).any(|p| p[0].offset >= p[1].offset)
                || pack
                    .entries
                    .iter()
                    .any(|e| e.offset >= pack.entries_end || e.oid.kind() != hash)
            {
                bail!(corruption("invalid original catalog offsets or hash kind"));
            }
        }
        Ok(Self {
            hash,
            packs,
            limits,
        })
    }

    /// Decode only the requested object's original chain. A missing requested
    /// OID is distinct from a missing dependency or an I/O failure.
    pub fn decode(&self, oid: ObjectId, source: &mut impl RangeSource) -> Result<Option<Decoded>> {
        if oid.kind() != self.hash {
            bail!(gix_error::validation(
                "requested object hash kind disagrees with catalog"
            ));
        }
        let Some(mut location) = self.locate(oid) else {
            return Ok(None);
        };
        let mut stats = Stats::default();
        let mut chain = [const { None::<Step> }; MAX_DEPTH + 1];
        loop {
            if chain[..stats.depth]
                .iter()
                .flatten()
                .any(|s| s.location == location)
            {
                bail!(corruption("original pack delta chain contains a cycle"));
            }
            let (indexed, length) = self.frame(location)?;
            let mut probe = [0; HEADER_PROBE];
            let probe_len = length.min(probe.len());
            self.read(
                source,
                location.0,
                indexed.offset,
                &mut probe[..probe_len],
                &mut stats,
            )?;
            let entry = Entry::from_bytes(&probe[..probe_len], indexed.offset, self.hash)?;
            if entry.header_size() >= length {
                bail!(corruption("original entry has no compressed payload"));
            }
            let next = self.base_location(location, &entry)?;
            chain[stats.depth] = Some(Step {
                location,
                entry,
                base_id: next.map(|(pack, pos)| self.packs[pack].entries[pos].oid),
            });
            let Some(base) = next else { break };
            if stats.depth == self.limits.max_depth {
                bail!(allocation_limit("original delta depth exceeded"));
            }
            stats.depth += 1;
            location = base;
        }

        let mut verified = None;
        let mut inflate = gix_zlib::Inflate::default();
        for step in chain[..=stats.depth].iter().rev().flatten() {
            verified = Some(self.decode_step(
                step,
                verified.as_ref(),
                source,
                &mut inflate,
                &mut stats,
            )?);
        }
        Ok(verified.map(|value| Decoded {
            kind: value.kind,
            bytes: value.bytes,
            stats,
        }))
    }

    fn locate(&self, oid: ObjectId) -> Option<Location> {
        self.packs.iter().enumerate().find_map(|(pack, meta)| {
            meta.entries
                .iter()
                .position(|e| e.oid == oid)
                .map(|pos| (pack, pos))
        })
    }

    fn base_location(&self, (pack, _): Location, entry: &Entry) -> Result<Option<Location>> {
        match entry.header {
            Header::OfsDelta { base_distance } => {
                let offset = entry
                    .checked_base_pack_offset(base_distance)
                    .ok_or_else(|| corruption("invalid original OFS distance").raise())?;
                let pos = self.packs[pack]
                    .entries
                    .binary_search_by_key(&offset, |e| e.offset)
                    .map_err(|_| corruption("original OFS base is not an indexed entry").raise())?;
                Ok(Some((pack, pos)))
            }
            Header::RefDelta { base_id } => self
                .locate(base_id)
                .map(Some)
                .ok_or_else(|| corruption("original REF base missing from held catalog").raise()),
            _ => Ok(None),
        }
    }

    fn frame(&self, (pack, pos): Location) -> Result<(IndexedEntry, usize)> {
        let meta = &self.packs[pack];
        let indexed = meta.entries[pos];
        let end = meta
            .entries
            .get(pos + 1)
            .map_or(meta.entries_end, |e| e.offset);
        let length = usize::try_from(end - indexed.offset).or_error()?;
        if length > self.limits.max_entry_bytes {
            bail!(allocation_limit("original entry range exceeds byte limit"));
        }
        Ok((indexed, length))
    }

    fn read(
        &self,
        source: &mut impl RangeSource,
        pack: usize,
        offset: u64,
        out: &mut [u8],
        stats: &mut Stats,
    ) -> Result {
        let total = sum(stats.source_bytes, out.len())?;
        if total > self.limits.max_source_bytes {
            bail!(allocation_limit("original source read budget exceeded"));
        }
        stats.source_bytes = total;
        stats.source_reads += 1;
        source.read_exact(pack, offset, out)
    }

    fn account(&self, live: usize, stats: &mut Stats) -> Result {
        if live > self.limits.max_live_buffer_bytes {
            bail!(allocation_limit("pack proof live buffer budget exceeded"));
        }
        stats.peak_buffer_capacity = stats.peak_buffer_capacity.max(live);
        Ok(())
    }

    fn buffer(&self, capacity: usize, already_live: usize, stats: &mut Stats) -> Result<Vec<u8>> {
        self.account(sum(already_live, capacity)?, stats)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(capacity).or_error()?;
        self.account(sum(already_live, bytes.capacity())?, stats)?;
        Ok(bytes)
    }

    fn decode_step(
        &self,
        step: &Step,
        base: Option<&Verified>,
        source: &mut impl RangeSource,
        inflate: &mut gix_zlib::Inflate,
        stats: &mut Stats,
    ) -> Result<Verified> {
        let (indexed, length) = self.frame(step.location)?;
        let base_capacity = base.map_or(0, |b| b.bytes.capacity());
        let mut original = self.buffer(length, base_capacity, stats)?;
        original.resize(length, 0);
        self.read(
            source,
            step.location.0,
            indexed.offset,
            &mut original,
            stats,
        )?;
        if crc32fast::hash(&original) != indexed.crc32 {
            bail!(corruption("original entry CRC mismatch"));
        }
        if Entry::from_bytes(&original, indexed.offset, self.hash)? != step.entry {
            bail!(corruption("original entry header changed after probe"));
        }

        let payload = &original[step.entry.header_size()..];
        let payload_len = payload.len();
        let size = step.entry.decompressed_size;
        let header = step
            .base_id
            .map_or(step.entry.header, |base_id| Header::RefDelta { base_id });
        let wrapped_len = sum(
            sum(data::header::SIZE, header.size(size))?,
            sum(payload_len, self.hash.len_in_bytes())?,
        )?;
        let mut wrapped =
            self.buffer(wrapped_len, sum(base_capacity, original.capacity())?, stats)?;
        wrapped.extend_from_slice(&data::header::encode(data::Version::V2, 1));
        header.write_to(size, &mut wrapped).or_error()?;
        wrapped.extend_from_slice(payload);
        drop(original);
        let mut hash = gix_hash::hasher(self.hash);
        hash.update(&wrapped);
        wrapped.extend_from_slice(hash.try_finalize()?.as_slice());

        let file = File::from_data(wrapped.as_slice(), Default::default(), self.hash)?;
        let entry = file.entry(START)?;
        let base_for = |id: &gix_hash::oid| base.filter(|b| b.oid.as_ref() == id);
        let probed = file.decode_header(entry.clone(), inflate, &|id| {
            Ok(
                base_for(id).map(|b| decode::header::ResolvedBase::OutOfPack {
                    kind: b.kind,
                    num_deltas: None,
                }),
            )
        })?;
        let object_size = usize::try_from(probed.object_size).or_error()?;
        let inflated = usize::try_from(size).or_error()?;
        let delta_size = if base.is_some() { inflated } else { 0 };
        let expanded = sum(stats.expanded_bytes, sum(delta_size, object_size)?)?;
        if object_size > self.limits.max_object_bytes || expanded > self.limits.max_expanded_bytes {
            bail!(allocation_limit(
                "original chain expansion or object size exceeded"
            ));
        }

        // A single delta needs two work regions plus its instructions at this
        // pin. Reserve once so no decoder Vec reallocation overlaps old backing.
        let workspace = match base {
            Some(b) => {
                let largest = b.bytes.len().max(object_size);
                sum(sum(largest, largest)?, delta_size)?
            }
            None => object_size,
        };
        let mut bytes = self.buffer(workspace, sum(base_capacity, wrapped.capacity())?, stats)?;
        let outcome = file.with_alloc_limit_bytes(Some(workspace)).decode_entry(
            entry,
            &mut bytes,
            inflate,
            &|id, out| {
                Ok(base_for(id).map(|b| {
                    out.extend_from_slice(&b.bytes);
                    decode::entry::ResolvedBase::OutOfPack {
                        kind: b.kind,
                        end: out.len(),
                    }
                }))
            },
            &mut cache::Never,
        )?;
        if outcome.compressed_size != payload_len {
            bail!(corruption(
                "decoder did not consume the exact original compressed span"
            ));
        }
        if outcome.kind != probed.kind
            || outcome.object_size != probed.object_size
            || bytes.len() != object_size
        {
            bail!(corruption("decoded object disagrees with header probe"));
        }
        if gix_object::compute_hash(self.hash, outcome.kind, &bytes)? != indexed.oid {
            bail!(corruption("original entry object ID mismatch"));
        }
        stats.expanded_bytes = expanded;
        Ok(Verified {
            oid: indexed.oid,
            kind: outcome.kind,
            bytes,
        })
    }
}
