use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
    path::PathBuf,
};

use gix_error::{Result, ResultExt, allocation_limit, bail, corruption, message};

use crate::{Bundle, cache, data, index};

/// Explicit request-owned bounds for RAM pack preparation. No environment or
/// process-global interrupt state is consulted. All limits are mandatory.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Unique input-pack identity within the supplied cache's namespace/lifetime.
    pub input_pack_id: data::Id,
    /// Maximum bytes retained from the input, including header and checksum.
    pub max_input_bytes: usize,
    /// Maximum normalized output pack bytes, including header and checksum.
    pub max_output_bytes: usize,
    /// Maximum encoded index bytes.
    pub max_index_bytes: usize,
    /// Maximum incoming object count, bounding all per-object metadata/maps.
    pub max_objects: u32,
    /// Maximum individual/combined decoder work allocation.
    pub max_decode_bytes: usize,
    /// Maximum resolution attempts across all passes, including forward refs.
    pub max_resolve_attempts: u64,
    /// Per-object delta chain limits.
    pub decode: data::decode::Limits,
    /// Compression for normalized full objects.
    pub compression: gix_zlib::Compression,
}

/// A completely prepared pack and index, not yet published anywhere. Thin and
/// forward-reference packs are normalized to full objects; every incoming entry
/// is decoded and hashed, including entries unreachable from any reference.
///
/// Successful structural decoding is not Git object semantic validation. Callers
/// retain authorization, typed object validation, source holds and publication.
pub struct Outcome {
    /// Normalized self-contained pack bytes, with no externally dependent deltas.
    pub pack: Vec<u8>,
    /// V2 index bytes for exactly this pack.
    pub index: Vec<u8>,
    /// Checksums and entry count for the prepared result.
    pub outcome: index::write::Outcome,
}

impl Bundle {
    /// Controlled RAM alternative to `write_to_directory`. No temporary files,
    /// static interrupt flags or nested workers are used. A source must impose
    /// its own I/O deadline; cancellation cannot preempt a blocking `Read` call.
    ///
    /// `resolve` writes a verified external base into its output and returns its
    /// kind, or `None` when absent. It must enforce the same request's admission,
    /// checks and source holds. Returned base bytes are additionally hash-checked.
    /// The caller owns aggregate admission: input, normalized output, one bounded
    /// compressed entry, decoder/cache capacities and count-bounded metadata can
    /// coexist. On error no pack/index outcome is returned or published.
    #[allow(clippy::too_many_arguments)]
    #[expect(clippy::type_complexity)]
    pub fn prepare_stream_controlled(
        input: &mut impl Read,
        object_hash: gix_hash::Kind,
        options: Options,
        resolve: &dyn Fn(&gix_hash::oid, &mut Vec<u8>) -> Result<Option<gix_object::Kind>>,
        cache: &mut dyn cache::DecodeEntry,
        check: &dyn Fn() -> Result,
    ) -> Result<Outcome> {
        check()?;
        let mut bytes = Vec::new();
        let mut chunk = [0; 32 * 1024];
        loop {
            check()?;
            let n = match input.read(&mut chunk) {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    check()?;
                    continue;
                }
                result => result.or_raise(|| message("Could not read input pack"))?,
            };
            check()?;
            if n == 0 {
                break;
            }
            append_bounded(&mut bytes, &chunk[..n], options.max_input_bytes)?;
        }
        let mut file = data::File::from_data(bytes.as_slice(), PathBuf::new(), object_hash)?
            .with_alloc_limit_bytes(Some(options.max_decode_bytes));
        file.id = options.input_pack_id;
        if file.num_objects() > options.max_objects {
            bail!(allocation_limit("Input pack exceeds object count limit"));
        }
        let checksum_start = bytes.len() - object_hash.len_in_bytes();
        let mut hash = gix_hash::hasher(object_hash);
        for chunk in bytes[..checksum_start].chunks(32 * 1024) {
            check()?;
            hash.update(chunk);
        }
        hash.try_finalize()?
            .verify(gix_hash::oid::from_bytes_unchecked(&bytes[checksum_start..]))?;
        let count = file.num_objects() as usize;
        if count > checksum_start.saturating_sub(data::header::SIZE) {
            bail!(corruption("Pack object count cannot fit in its entry bytes"));
        }
        let metadata_bytes = count
            .checked_mul(std::mem::size_of::<(data::Entry, bool)>())
            .ok_or_else(|| gix_error::Error::from(allocation_limit("Pack metadata size overflows")))?;
        if metadata_bytes > options.max_decode_bytes {
            bail!(allocation_limit("Pack metadata exceeds allocation limit"));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .or_raise(|| gix_error::allocation_failure("Could not reserve pack metadata"))?;
        let mut known = BTreeMap::new();
        let mut cursor = data::header::SIZE;
        let mut decoded = Vec::new();
        let mut inflate = gix_zlib::Inflate::default();
        // Find and validate every stream boundary before resolving any deltas.
        for _ in 0..count {
            check()?;
            let entry = file.entry(cursor as u64)?;
            let size = usize::try_from(entry.decompressed_size)
                .map_err(|_| allocation_limit("Pack entry exceeds address space"))
                .or_error()?;
            resize(&mut decoded, size, options.max_decode_bytes, check)?;
            let start = usize::try_from(entry.data_offset)
                .map_err(|_| allocation_limit("Pack offset exceeds address space"))
                .or_error()?;
            if start > checksum_start {
                bail!(corruption("Pack entry overlaps trailer"));
            }
            let (_, consumed, _) = inflate.once_controlled(&bytes[start..checksum_start], &mut decoded, check)?;
            cursor = start
                .checked_add(consumed)
                .ok_or_else(|| gix_error::Error::from(corruption("Pack entry offset overflows")))?;
            if let Some(kind) = entry.header.as_kind() {
                let id = hash_object(object_hash, kind, &decoded, check)?;
                known.entry(id).or_insert_with(|| entry.clone());
            }
            entries.push((entry, false));
        }
        if cursor != checksum_start {
            bail!(corruption("Pack has trailing bytes or an incorrect object count"));
        }
        let mut state = Resolve {
            file: &file,
            options,
            entries,
            known,
            decoded,
            inflate,
            resolve,
            cache,
            check,
            cursor: 0,
            remaining: count,
            progress: false,
            attempts: 0,
            offset: data::header::SIZE as u64,
            metadata: BTreeMap::new(),
        };
        let input = std::iter::from_fn(|| state.next());
        let sink = Bounded {
            data: Vec::new(),
            limit: options.max_output_bytes,
        };
        let mut writer = data::output::bytes::FromEntriesIter::new_controlled(
            input,
            sink,
            count as u32,
            file.version(),
            object_hash,
            count,
            check,
        )?;
        while let Some(result) = writer.next_controlled(check) {
            result?;
        }
        let pack_hash = writer.digest().expect("a complete controlled writer has a digest");
        let pack = writer.into_write().data;
        let mut sorted = Vec::new();
        sorted
            .try_reserve_exact(count)
            .or_raise(|| gix_error::allocation_failure("Could not reserve index metadata"))?;
        for ((id, _), (crc, offset)) in state.metadata {
            check()?;
            sorted.push((id, crc, offset));
        }
        let mut index = Bounded {
            data: Vec::new(),
            limit: options.max_index_bytes,
        };
        let index_hash = index::encode::write_to_controlled(&mut index, &sorted, &pack_hash, object_hash, check)?;
        check()?;
        Ok(Outcome {
            pack,
            index: index.data,
            outcome: index::write::Outcome {
                index_version: index::Version::V2,
                index_hash,
                data_hash: pack_hash,
                num_objects: count as u32,
            },
        })
    }
}

struct Resolve<'a, T: crate::FileData> {
    file: &'a data::File<T>,
    options: Options,
    entries: Vec<(data::Entry, bool)>,
    known: BTreeMap<gix_hash::ObjectId, data::Entry>,
    decoded: Vec<u8>,
    inflate: gix_zlib::Inflate,
    #[allow(clippy::type_complexity)]
    resolve: &'a dyn Fn(&gix_hash::oid, &mut Vec<u8>) -> Result<Option<gix_object::Kind>>,
    cache: &'a mut dyn cache::DecodeEntry,
    check: &'a dyn Fn() -> Result,
    cursor: usize,
    remaining: usize,
    progress: bool,
    attempts: u64,
    offset: u64,
    metadata: BTreeMap<(gix_hash::ObjectId, u64), (u32, u64)>,
}

impl<T: crate::FileData> Resolve<'_, T> {
    fn next(&mut self) -> Option<Result<Vec<data::output::Entry>>> {
        if self.remaining == 0 {
            return None;
        }
        let result = self.next_entry();
        if result.is_err() {
            self.remaining = 0;
        }
        Some(result.map(|entry| vec![entry]))
    }

    fn next_entry(&mut self) -> Result<data::output::Entry> {
        loop {
            (self.check)()?;
            if self.cursor == self.entries.len() {
                if !self.progress {
                    bail!(corruption("Pack delta bases are missing or cyclic"));
                }
                self.cursor = 0;
                self.progress = false;
            }
            let current = self.cursor;
            self.cursor += 1;
            let (entry, done) = &self.entries[current];
            if *done {
                continue;
            }
            if self.attempts >= self.options.max_resolve_attempts {
                bail!(allocation_limit("Pack resolution attempt limit exceeded"));
            }
            self.attempts += 1;
            let resolver = |id: &gix_hash::oid, out: &mut Vec<u8>| {
                (self.check)()?;
                if let Some(entry) = self.known.get(id) {
                    return Ok(Some(data::decode::entry::ResolvedBase::InPack(entry.clone())));
                }
                let kind = (self.resolve)(id, out)?;
                (self.check)()?;
                let Some(kind) = kind else {
                    return Ok(None);
                };
                if out.len() > self.options.max_decode_bytes {
                    bail!(allocation_limit("External base exceeds decoder limit"));
                }
                hash_object(id.kind(), kind, out, self.check)?.verify(id)?;
                Ok(Some(data::decode::entry::ResolvedBase::OutOfPack {
                    kind,
                    end: out.len(),
                }))
            };
            let result = self.file.decode_entry_controlled(
                entry.clone(),
                &mut self.decoded,
                &mut self.inflate,
                &resolver,
                self.cache,
                self.options.decode,
                self.check,
            );
            let outcome = match result {
                Err(err) if err.downcast_any_ref::<data::decode::DeltaBaseUnresolved>().is_some() => continue,
                other => other?,
            };
            let id = hash_object(self.file.object_hash(), outcome.kind, &self.decoded, self.check)?;
            let count = data::output::Count {
                id,
                entry_pack_location: data::output::count::PackLocation::NotLookedUp,
            };
            let object = gix_object::Data {
                kind: outcome.kind,
                object_hash: id.kind(),
                data: &self.decoded,
            };
            let output = data::output::Entry::from_data_controlled(
                &count,
                &object,
                self.options.compression,
                self.options.max_output_bytes,
                self.check,
            )?;
            let header = output.to_entry_header(self.file.version(), |_| {
                unreachable!("normalized entries are full objects")
            });
            let mut header_bytes = [0; 32];
            let header_len = header
                .write_to(output.decompressed_size as u64, &mut header_bytes.as_mut_slice())
                .or_error()?;
            let mut crc = crc32fast::Hasher::new();
            crc.update(&header_bytes[..header_len]);
            for chunk in output.compressed_data.chunks(32 * 1024) {
                (self.check)()?;
                crc.update(chunk);
            }
            let next_offset = self
                .offset
                .checked_add(header_len as u64)
                .and_then(|n| n.checked_add(output.compressed_data.len() as u64))
                .ok_or_else(|| gix_error::Error::from(allocation_limit("Output pack offset overflows")))?;
            if next_offset > self.options.max_output_bytes.saturating_sub(id.kind().len_in_bytes()) as u64 {
                bail!(allocation_limit("Normalized pack exceeds output limit"));
            }
            self.metadata
                .insert((id, current as u64), (crc.finalize(), self.offset));
            self.known.entry(id).or_insert_with(|| entry.clone());
            self.offset = next_offset;
            self.entries[current].1 = true;
            self.remaining -= 1;
            self.progress = true;
            (self.check)()?;
            return Ok(output);
        }
    }
}

fn hash_object(
    hash: gix_hash::Kind,
    kind: gix_object::Kind,
    data: &[u8],
    check: &dyn Fn() -> Result,
) -> Result<gix_hash::ObjectId> {
    let mut hash = gix_hash::hasher(hash);
    hash.update(&gix_object::encode::loose_header(kind, data.len() as u64));
    for chunk in data.chunks(32 * 1024) {
        check()?;
        hash.update(chunk);
    }
    check()?;
    hash.try_finalize()
}

fn resize(out: &mut Vec<u8>, len: usize, limit: usize, check: &dyn Fn() -> Result) -> Result {
    check()?;
    if len > limit {
        bail!(allocation_limit("Pack entry exceeds decoder allocation limit"));
    }
    out.truncate(len);
    out.try_reserve_exact(len - out.len())
        .or_raise(|| gix_error::allocation_failure("Could not reserve decoder bytes"))?;
    while out.len() < len {
        check()?;
        out.resize(len.min(out.len() + 32 * 1024), 0);
    }
    check()
}

fn append_bounded(out: &mut Vec<u8>, bytes: &[u8], limit: usize) -> Result {
    if bytes.len() > limit.saturating_sub(out.len()) {
        bail!(allocation_limit("Pack byte limit exceeded"));
    }
    out.try_reserve_exact(bytes.len())
        .or_raise(|| gix_error::allocation_failure("Could not reserve pack bytes"))?;
    out.extend_from_slice(bytes);
    Ok(())
}

struct Bounded {
    data: Vec<u8>,
    limit: usize,
}
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        append_bounded(&mut self.data, bytes, self.limit).map_err(io::Error::other)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
