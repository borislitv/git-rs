//! Bounded authenticated complete units, never a suspended blame frontier.
use crate::{
    Result,
    derived_cache::{ArtifactKey, CacheConfig},
    invalid,
};
use git_rs_core::{CapacityBudget, CapacityBuffer, CapacityReservation, Control};
use gix::{ObjectId, objs::Kind};
use openssl::{
    hash::MessageDigest,
    pkey::{PKey, Private},
    sign::Signer,
};
use std::{fs, io::Read, mem::size_of, os::unix::fs::MetadataExt, path::Path};

pub(super) const WORK_BYTES: usize = 16 * 1024 * 1024;
pub(super) const UNIT_BYTES: usize = 4 * 1024 * 1024;
pub(super) const OBJECT_BYTES: usize = 1024 * 1024;
const RECORDS: usize = 3072;
pub(super) const MAX_MAPPINGS: usize = 2048;
const KEY_BYTES: usize = 112;
const RECORD_HEADER: usize = KEY_BYTES + 4 + 32;
const HEADER: usize = 184;
const TRAILER: usize = 32;
const MAGIC: &[u8; 8] = b"BLMSET01";

/// A checksum/valid partition is NOT proof of the canonical Myers tie choice.
/// Only a dedicated producer writer with this signing key may supply mappings.
/// The cache DB/S3 credentials alone cannot repair/re-sign a forged checkpoint.
/// Share this key only among the exact authorized producer instances; key
/// rotation safely makes old snapshots misses. Never expose it in Debug/logs.
#[derive(Clone)]
pub struct WriterProof {
    namespace: [u8; 36],
    writer: [u8; 36],
    key: PKey<Private>,
}
impl WriterProof {
    pub fn from_key(config: &CacheConfig, key: &[u8]) -> Result<Self> {
        config.validate()?;
        if key.len() != 32 {
            return Err(invalid(
                "checkpoint signing key must be exactly 32 random bytes",
            ));
        }
        Ok(Self {
            namespace: config.namespace.as_bytes().try_into()?,
            writer: config.owner.as_bytes().try_into()?,
            key: PKey::hmac(key)?,
        })
    }
    pub fn load(config: &CacheConfig, path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(invalid("checkpoint key path must be absolute"));
        }
        let uid = fs::metadata("/proc/self")?.uid();
        for ancestor in path.ancestors().skip(1) {
            let m = fs::symlink_metadata(ancestor)?;
            if !m.is_dir() || (m.uid() != 0 && m.uid() != uid) || m.mode() & 0o022 != 0 {
                return Err(invalid(
                    "checkpoint key ancestors must be protected real directories",
                ));
            }
        }
        let before = fs::symlink_metadata(path)?;
        if !before.is_file()
            || (before.uid() != 0 && before.uid() != uid)
            || before.mode() & 0o077 != 0
            || before.len() != 32
        {
            return Err(invalid(
                "checkpoint key must be a private owner-readable regular 32-byte file",
            ));
        }
        let mut file = fs::File::open(path)?;
        let opened = file.metadata()?;
        if (
            before.dev(),
            before.ino(),
            before.len(),
            before.mode(),
            before.uid(),
            before.ctime(),
            before.ctime_nsec(),
        ) != (
            opened.dev(),
            opened.ino(),
            opened.len(),
            opened.mode(),
            opened.uid(),
            opened.ctime(),
            opened.ctime_nsec(),
        ) {
            return Err(invalid("checkpoint key changed while opening"));
        }
        let mut key = [0; 32];
        if let Err(error) = file.read_exact(&mut key) {
            key.fill(0);
            return Err(error.into());
        }
        let after = file.metadata()?;
        if (
            opened.len(),
            opened.mtime(),
            opened.mtime_nsec(),
            opened.ctime(),
            opened.ctime_nsec(),
            opened.mode(),
            opened.uid(),
        ) != (
            after.len(),
            after.mtime(),
            after.mtime_nsec(),
            after.ctime(),
            after.ctime_nsec(),
            after.mode(),
            after.uid(),
        ) {
            key.fill(0);
            return Err(invalid("checkpoint key changed while reading"));
        }
        let proof = Self::from_key(config, &key);
        key.fill(0); // best effort only; not a claim of allocator/native zeroization
        proof
    }
    fn tag(&self, chunks: &[&[u8]], length: usize, control: &Control) -> Result<[u8; 32]> {
        let mut signer = Signer::new(MessageDigest::sha256(), &self.key)?;
        signer.update(b"git-rs/authorized-completed-work/v1\0")?;
        let mut left = length;
        for chunk in chunks {
            for part in chunk.chunks(32 * 1024) {
                control.check()?;
                let n = left.min(part.len());
                signer.update(&part[..n])?;
                left -= n;
                if left == 0 {
                    break;
                }
            }
            if left == 0 {
                break;
            }
        }
        if left != 0 {
            return Err(invalid("truncated checkpoint authentication input"));
        }
        let mut tag = [0; 32];
        if signer.sign(&mut tag)? != tag.len() {
            return Err(invalid("invalid checkpoint authentication tag size"));
        }
        control.check()?;
        Ok(tag)
    }
}

#[derive(Clone)]
pub(super) struct Scope {
    pub key: ArtifactKey,
    pub producer: [u8; 32],
    pub options: [u8; 32],
    pub writer: WriterProof,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnitKey {
    Object {
        oid: ObjectId,
        kind: Kind,
    },
    // Mapping wire v1 already embeds BOTH complete byte-line partitions. Do not
    // pretend its interner IDs are standalone reusable line/token artifacts.
    Mapping {
        before: ObjectId,
        after: ObjectId,
        tuning: u8,
        producer: [u8; 32],
        options: [u8; 32],
    },
}
impl UnitKey {
    fn wire(self) -> [u8; KEY_BYTES] {
        let mut key = [0; KEY_BYTES];
        match self {
            Self::Object { oid, kind } => {
                key[0] = 1;
                key[1] = kind_tag(kind);
                key[2..22].copy_from_slice(oid.as_bytes());
            }
            Self::Mapping {
                before,
                after,
                tuning,
                producer,
                options,
            } => {
                key[0] = 2;
                key[1..21].copy_from_slice(before.as_bytes());
                key[21..41].copy_from_slice(after.as_bytes());
                key[41] = tuning;
                key[42..74].copy_from_slice(&producer);
                key[74..106].copy_from_slice(&options);
            }
        }
        key
    }
    fn decode(bytes: [u8; KEY_BYTES], scope: &Scope) -> Result<Self> {
        let key = match bytes[0] {
            1 => Self::Object {
                oid: ObjectId::from(<[u8; 20]>::try_from(&bytes[2..22])?),
                kind: tag_kind(bytes[1])?,
            },
            2 => Self::Mapping {
                before: ObjectId::from(<[u8; 20]>::try_from(&bytes[1..21])?),
                after: ObjectId::from(<[u8; 20]>::try_from(&bytes[21..41])?),
                tuning: bytes[41],
                producer: bytes[42..74].try_into()?,
                options: bytes[74..106].try_into()?,
            },
            _ => return Err(invalid("unknown reusable unit")),
        };
        if key.wire() != bytes {
            return Err(invalid("noncanonical reusable unit identity"));
        }
        if let Self::Mapping {
            tuning,
            producer,
            options,
            ..
        } = key
            && (tuning & !31 != 0 || producer != scope.producer || options != scope.options)
        {
            return Err(invalid("foreign mapping producer/options"));
        }
        Ok(key)
    }
}
fn kind_tag(kind: Kind) -> u8 {
    match kind {
        Kind::Commit => 1,
        Kind::Tree => 2,
        Kind::Blob => 3,
        Kind::Tag => 4,
    }
}
fn tag_kind(tag: u8) -> Result<Kind> {
    match tag {
        1 => Ok(Kind::Commit),
        2 => Ok(Kind::Tree),
        3 => Ok(Kind::Blob),
        4 => Ok(Kind::Tag),
        _ => Err(invalid("unknown object kind")),
    }
}

struct Record {
    key: UnitKey,
    bytes: CapacityBuffer,
    imported: bool,
    used: bool,
}
/// Complete typed records only. Metadata/backings share the derivation budget.
/// Field ordering frees every record backing before refunding metadata capacity.
pub(crate) struct BatchSnapshot {
    records: Vec<Record>,
    _metadata: CapacityReservation,
    budget: CapacityBudget,
    occupied: usize,
    pub revision: u64,
    pub objects_reused: u64,
    pub mappings_inserted: u64,
    pub imported_objects_reused: u64,
    pub imported_mapping_loads: u64,
}
impl BatchSnapshot {
    pub fn new(budget: CapacityBudget) -> Result<Self> {
        let mut charge = budget.reservation(RECORDS * size_of::<Record>())?;
        let mut records = Vec::new();
        records.try_reserve_exact(RECORDS)?;
        charge.resize(records.capacity() * size_of::<Record>())?;
        Ok(Self {
            records,
            _metadata: charge,
            budget,
            occupied: HEADER + TRAILER,
            revision: 0,
            objects_reused: 0,
            mappings_inserted: 0,
            imported_objects_reused: 0,
            imported_mapping_loads: 0,
        })
    }
    pub fn budget(&self) -> &CapacityBudget {
        &self.budget
    }
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn mapping_count(&self) -> usize {
        self.records
            .iter()
            .filter(|r| matches!(r.key, UnitKey::Mapping { .. }))
            .count()
    }
    pub fn load(
        &mut self,
        key: UnitKey,
        into: &mut [u8],
        control: &Control,
    ) -> Result<Option<usize>> {
        control.check()?;
        let Some(record) = self.records.iter_mut().find(|record| record.key == key) else {
            return Ok(None);
        };
        if record.bytes.as_slice().len() > into.len() {
            return Ok(None);
        }
        copy(
            &mut into[..record.bytes.as_slice().len()],
            record.bytes.as_slice(),
            control,
        )?;
        let length = record.bytes.as_slice().len();
        record.used = true;
        if record.imported && matches!(key, UnitKey::Mapping { .. }) {
            self.imported_mapping_loads = self.imported_mapping_loads.saturating_add(1);
        }
        Ok(Some(length))
    }
    pub fn object(
        &mut self,
        oid: ObjectId,
        kind: Kind,
        control: &Control,
    ) -> Result<Option<git_rs_core::Object>> {
        control.check()?;
        let key = UnitKey::Object { oid, kind };
        let Some(record) = self.records.iter_mut().find(|record| record.key == key) else {
            return Ok(None);
        };
        let bytes = record.bytes.as_slice();
        let mut data = match self.budget.buffer(bytes.len()) {
            Ok(data) => data,
            Err(git_rs_core::CapacityError::Exhausted | git_rs_core::CapacityError::Allocation) => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        data.resize_controlled(bytes.len(), 0, control)?;
        copy(data.as_mut_slice(), bytes, control)?;
        self.objects_reused = self.objects_reused.saturating_add(1);
        if record.imported {
            self.imported_objects_reused = self.imported_objects_reused.saturating_add(1);
        }
        record.used = true;
        Ok(Some(git_rs_core::Object { kind, data }))
    }
    /// Optional storage policy: oversized units or full optional budget are skips.
    /// A successful retained copy is always charged by its ACTUAL capacity.
    pub fn insert(&mut self, key: UnitKey, bytes: &[u8], control: &Control) -> Result<bool> {
        control.check()?;
        let cap = if matches!(key, UnitKey::Object { .. }) {
            OBJECT_BYTES
        } else {
            UNIT_BYTES
        };
        if bytes.len() > cap {
            return Ok(false);
        }
        if let UnitKey::Object { oid, kind } = key {
            verify_object(oid, kind, bytes, control)?;
        }
        if let Some(index) = self.records.iter().position(|r| r.key == key) {
            if self.records[index].bytes.as_slice() == bytes {
                return Ok(false);
            }
            self.evict(index);
        }
        while self.records.len() >= RECORDS
            || self.occupied + RECORD_HEADER + bytes.len() > WORK_BYTES
            || (matches!(key, UnitKey::Mapping { .. }) && self.mapping_count() >= MAX_MAPPINGS)
        {
            // Graph/source metadata is streamed again before the mapping seam.
            // It must not evict imported mappings before their first use.
            let mapping = matches!(key, UnitKey::Mapping { .. });
            let victim = if mapping && self.mapping_count() >= MAX_MAPPINGS {
                self.records
                    .iter()
                    .position(|r| matches!(r.key, UnitKey::Mapping { .. }))
            } else {
                self.records
                    .iter()
                    .position(|r| {
                        matches!(r.key, UnitKey::Object { .. }) && (!r.imported || r.used)
                    })
                    .or_else(|| {
                        if mapping {
                            self.records
                                .iter()
                                .position(|r| matches!(r.key, UnitKey::Object { .. }))
                        } else {
                            None
                        }
                    })
                    .or(if mapping && !self.records.is_empty() {
                        Some(0)
                    } else {
                        None
                    })
            };
            let Some(victim) = victim else {
                return Ok(false);
            };
            self.evict(victim);
        }
        let mut stored = match self.budget.buffer(bytes.len()) {
            Ok(stored) => stored,
            Err(git_rs_core::CapacityError::Exhausted | git_rs_core::CapacityError::Allocation) => {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        if self.occupied + RECORD_HEADER + stored.capacity() > WORK_BYTES {
            return Ok(false);
        }
        stored.resize_controlled(bytes.len(), 0, control)?;
        copy(stored.as_mut_slice(), bytes, control)?;
        self.occupied += RECORD_HEADER + stored.capacity();
        self.records.push(Record {
            key,
            bytes: stored,
            imported: false,
            used: false,
        });
        if matches!(key, UnitKey::Mapping { .. }) {
            self.mappings_inserted = self.mappings_inserted.saturating_add(1);
        }
        self.revision = self.revision.saturating_add(1);
        Ok(true)
    }
    fn evict(&mut self, index: usize) {
        let record = self.records.remove(index);
        self.occupied -= RECORD_HEADER + record.bytes.capacity();
        drop(record); // free storage before its reservation is refunded
        self.revision = self.revision.saturating_add(1);
    }
    pub(super) fn encode(&self, scope: &Scope, control: &Control) -> Result<CapacityBuffer> {
        control.check()?;
        let length = HEADER
            + TRAILER
            + self
                .records
                .iter()
                .map(|r| RECORD_HEADER + r.bytes.as_slice().len())
                .sum::<usize>();
        if length > WORK_BYTES {
            return Err(invalid("workset frame exceeds bound"));
        }
        let mut frame = self.budget.buffer(length)?;
        frame.resize_controlled(length, 0, control)?;
        let out = frame.as_mut_slice();
        out[..8].copy_from_slice(MAGIC);
        out[8..12].copy_from_slice(&1u32.to_le_bytes());
        out[12..44].copy_from_slice(scope.key.digest());
        out[44..80].copy_from_slice(&scope.writer.namespace);
        out[80..116].copy_from_slice(&scope.writer.writer);
        out[116..148].copy_from_slice(&scope.producer);
        out[148..180].copy_from_slice(&scope.options);
        out[180..184].copy_from_slice(&(self.records.len() as u32).to_le_bytes());
        let mut offset = HEADER;
        for record in &self.records {
            control.check()?;
            let data = record.bytes.as_slice();
            out[offset..offset + KEY_BYTES].copy_from_slice(&record.key.wire());
            offset += KEY_BYTES;
            out[offset..offset + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
            offset += 4;
            out[offset..offset + 32].copy_from_slice(&digest(data, control)?);
            offset += 32;
            copy(&mut out[offset..offset + data.len()], data, control)?;
            offset += data.len();
        }
        let tag = scope.writer.tag(&[&out[..offset]], offset, control)?;
        out[offset..].copy_from_slice(&tag);
        Ok(frame)
    }
    /// Decode into a TEMPORARY owner. Caller MUST install only after the OUTER
    /// CompleteCheckpoint::with_chunks returns AND a fresh source fence passes.
    pub(super) fn decode(
        parts: &[&[u8]],
        scope: &Scope,
        budget: CapacityBudget,
        control: &Control,
    ) -> Result<Self> {
        control.check()?;
        if parts.len() > 64 {
            return Err(invalid("too many checkpoint frame parts"));
        }
        let mut reader = Reader::new(parts)?;
        if reader.remaining < HEADER + TRAILER || reader.remaining > WORK_BYTES {
            return Err(invalid("invalid workset frame size"));
        }
        let length = reader.remaining;
        let expected = scope.writer.tag(parts, length - TRAILER, control)?;
        let mut trailer = Reader::new(parts)?;
        trailer.skip(length - TRAILER)?;
        if !openssl::memcmp::eq(&trailer.array::<32>()?, &expected) {
            return Err(invalid("unauthorized checkpoint writer or repaired data"));
        }
        let header = reader.array::<HEADER>()?;
        if &header[..8] != MAGIC
            || header[8..12] != 1u32.to_le_bytes()
            || header[12..44] != scope.key.digest()[..]
            || header[44..80] != scope.writer.namespace
            || header[80..116] != scope.writer.writer
            || header[116..148] != scope.producer
            || header[148..180] != scope.options
        {
            return Err(invalid("foreign workset scope/profile"));
        }
        let count = u32::from_le_bytes(header[180..184].try_into()?) as usize;
        if count > RECORDS {
            return Err(invalid("too many reusable records"));
        }
        let mut batch = Self::new(budget)?;
        for _ in 0..count {
            control.check()?;
            let key = UnitKey::decode(reader.array()?, scope)?;
            if batch.records.iter().any(|r| r.key == key) {
                return Err(invalid("duplicate reusable identity"));
            }
            let length = u32::from_le_bytes(reader.array()?) as usize;
            let digest_expected = reader.array::<32>()?;
            let bound = if matches!(key, UnitKey::Object { .. }) {
                OBJECT_BYTES
            } else {
                UNIT_BYTES
            };
            if length > bound || length > reader.remaining.saturating_sub(TRAILER) {
                return Err(invalid("truncated or oversized complete unit"));
            }
            let mut data = batch.budget.buffer(length)?;
            data.resize_controlled(length, 0, control)?;
            for chunk in data.as_mut_slice().chunks_mut(32 * 1024) {
                control.check()?;
                reader.read(chunk)?;
            }
            if digest(data.as_slice(), control)? != digest_expected {
                return Err(invalid("unit digest mismatch"));
            }
            if let UnitKey::Object { oid, kind } = key {
                verify_object(oid, kind, data.as_slice(), control)?;
            }
            batch.occupied += RECORD_HEADER + data.capacity();
            if batch.occupied > WORK_BYTES {
                return Err(invalid("import backing exceeds workset bound"));
            }
            batch.records.push(Record {
                key,
                bytes: data,
                imported: true,
                used: false,
            });
        }
        if reader.remaining != TRAILER || batch.mapping_count() > MAX_MAPPINGS {
            return Err(invalid("incomplete/trailing workset records"));
        }
        control.check()?;
        Ok(batch)
    }
}

pub(crate) fn verify_object(
    oid: ObjectId,
    kind: Kind,
    bytes: &[u8],
    control: &Control,
) -> Result<()> {
    let mut hash = gix::hash::hasher(oid.kind());
    hash.update(&gix::objs::encode::loose_header(kind, bytes.len() as u64));
    for part in bytes.chunks(32 * 1024) {
        control.check()?;
        hash.update(part);
    }
    if hash.try_finalize()?.as_ref() != oid.as_ref() {
        return Err(invalid("reusable physical object OID mismatch"));
    }
    control.check()?;
    Ok(())
}
pub(super) fn digest(bytes: &[u8], control: &Control) -> Result<[u8; 32]> {
    let mut hash = openssl::sha::Sha256::new();
    for part in bytes.chunks(32 * 1024) {
        control.check()?;
        hash.update(part);
    }
    control.check()?;
    Ok(hash.finish())
}
fn copy(out: &mut [u8], bytes: &[u8], control: &Control) -> Result<()> {
    for (out, bytes) in out.chunks_mut(32 * 1024).zip(bytes.chunks(32 * 1024)) {
        control.check()?;
        out.copy_from_slice(bytes);
    }
    control.check()?;
    Ok(())
}
struct Reader<'a> {
    parts: &'a [&'a [u8]],
    part: usize,
    offset: usize,
    remaining: usize,
}
impl<'a> Reader<'a> {
    fn new(parts: &'a [&'a [u8]]) -> Result<Self> {
        let remaining = parts
            .iter()
            .try_fold(0usize, |n, p| n.checked_add(p.len()))
            .ok_or_else(|| invalid("checkpoint size overflow"))?;
        Ok(Self {
            parts,
            part: 0,
            offset: 0,
            remaining,
        })
    }
    fn skip(&mut self, mut length: usize) -> Result<()> {
        if length > self.remaining {
            return Err(invalid("truncated checkpoint"));
        }
        self.remaining -= length;
        while length != 0 {
            let n = length.min(self.parts[self.part].len() - self.offset);
            self.offset += n;
            length -= n;
            if self.offset == self.parts[self.part].len() {
                self.part += 1;
                self.offset = 0;
            }
        }
        Ok(())
    }
    fn read(&mut self, mut out: &mut [u8]) -> Result<()> {
        if out.len() > self.remaining {
            return Err(invalid("truncated checkpoint"));
        }
        while !out.is_empty() {
            let n = out.len().min(self.parts[self.part].len() - self.offset);
            out[..n].copy_from_slice(&self.parts[self.part][self.offset..self.offset + n]);
            self.skip(n)?;
            if n == 0 {
                self.part += 1;
                self.offset = 0;
            }
            out = &mut out[n..];
        }
        Ok(())
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0; N];
        self.read(&mut out)?;
        Ok(out)
    }
}

#[cfg(test)]
#[path = "blame_batch_tests.rs"]
mod tests;
