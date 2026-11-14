//! Small immutable physical fixtures. This adapter supplies bytes only: the
//! production RangePackSet remains responsible for indexes, deltas and hashes.
#![allow(dead_code)]

use git_rs::Result;
use git_rs_core::{
    CapacityBudget, CapacityBuffer, Control, ReadLimits, SnapshotIdentity, SnapshotSession,
    pack::{
        DecoderOptions,
        ranges::{PackSource, RangePackSet},
    },
};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicUsize, Ordering},
};

pub fn hash(bytes: &[u8]) -> Result<gix::ObjectId> {
    let mut hash = gix::hash::hasher(gix::hash::Kind::Sha1);
    hash.update(bytes);
    Ok(hash.try_finalize()?)
}

pub fn oid(kind: gix::objs::Kind, bytes: &[u8]) -> Result<gix::ObjectId> {
    Ok(gix::objs::compute_hash(gix::hash::Kind::Sha1, kind, bytes)?)
}

// Stored DEFLATE blocks make the few deliberately constructed delta instructions
// inspectable. Native compatibility fixtures use Git-generated packs instead.
pub fn compressed(bytes: &[u8]) -> Vec<u8> {
    let length = u16::try_from(bytes.len()).expect("small synthetic entry");
    let mut out = vec![0x78, 0x01, 0x01];
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&(!length).to_le_bytes());
    out.extend_from_slice(bytes);
    let (mut a, mut b) = (1u32, 0u32);
    for byte in bytes {
        a = (a + u32::from(*byte)) % 65521;
        b = (b + a) % 65521;
    }
    out.extend_from_slice(&((b << 16) | a).to_be_bytes());
    out
}

pub fn full(kind: gix::objs::Kind, bytes: &[u8]) -> Vec<u8> {
    let code = match kind {
        gix::objs::Kind::Commit => 1,
        gix::objs::Kind::Tree => 2,
        gix::objs::Kind::Blob => 3,
        gix::objs::Kind::Tag => 4,
    };
    let mut size = bytes.len();
    let mut first = (code << 4) | (size as u8 & 15);
    size >>= 4;
    if size != 0 {
        first |= 128;
    }
    let mut out = vec![first];
    while size != 0 {
        let mut byte = size as u8 & 127;
        size >>= 7;
        if size != 0 {
            byte |= 128;
        }
        out.push(byte);
    }
    out.extend_from_slice(&compressed(bytes));
    out
}

/// Tiny byte-oriented object graph, encoded as physical packs for public core
/// contracts. Object creation is a fixture primitive, not a traversal oracle.
#[derive(Default)]
pub struct Graph {
    entries: Vec<(gix::ObjectId, Vec<u8>)>,
}

impl Graph {
    pub fn object(&mut self, kind: gix::objs::Kind, bytes: &[u8]) -> Result<gix::ObjectId> {
        let id = oid(kind, bytes)?;
        self.entries.push((id, full(kind, bytes)));
        Ok(id)
    }

    pub fn tree(&mut self, name: &[u8], blob: gix::ObjectId) -> Result<gix::ObjectId> {
        let mut bytes = b"100644 ".to_vec();
        bytes.extend_from_slice(name);
        bytes.push(0);
        bytes.extend_from_slice(blob.as_slice());
        self.object(gix::objs::Kind::Tree, &bytes)
    }

    pub fn commit(
        &mut self,
        tree: gix::ObjectId,
        parents: &[gix::ObjectId],
        time: u64,
    ) -> Result<gix::ObjectId> {
        let mut bytes = format!("tree {tree}\n");
        for parent in parents {
            bytes.push_str(&format!("parent {parent}\n"));
        }
        bytes.push_str(&format!("author A <a@example.invalid> {time} +0000\ncommitter A <a@example.invalid> {time} +0000\n\nsynthetic\n"));
        self.object(gix::objs::Kind::Commit, bytes.as_bytes())
    }

    pub fn source(self) -> Result<Source> {
        Ok(Source::new(vec![Pack::new(&self.entries)?]))
    }
}

pub struct Pack {
    pub bytes: Vec<u8>,
    pub index: Vec<u8>,
    hash: gix::ObjectId,
    digest: [u8; 32],
    index_digest: [u8; 32],
}

impl Pack {
    pub fn from_bytes(bytes: Vec<u8>, index: Vec<u8>) -> Result<Self> {
        let trailer = bytes
            .len()
            .checked_sub(20)
            .ok_or_else(|| git_rs::invalid("fixture pack is truncated"))?;
        let hash = gix::ObjectId::from(<[u8; 20]>::try_from(&bytes[trailer..])?);
        let digest = openssl::sha::sha256(&bytes);
        let index_digest = openssl::sha::sha256(&index);
        Ok(Self {
            bytes,
            index,
            hash,
            digest,
            index_digest,
        })
    }

    pub fn new(entries: &[(gix::ObjectId, Vec<u8>)]) -> Result<Self> {
        let mut bytes = b"PACK\0\0\0\x02".to_vec();
        bytes.extend_from_slice(&u32::try_from(entries.len())?.to_be_bytes());
        let mut offsets = Vec::new();
        for (id, entry) in entries {
            offsets.push((*id, u32::try_from(bytes.len())?));
            bytes.extend_from_slice(entry);
        }
        let checksum = hash(&bytes)?;
        bytes.extend_from_slice(checksum.as_slice());
        offsets.sort_by_key(|(id, _)| *id);
        let mut index = Vec::new();
        for first in 0..256 {
            let count = offsets
                .iter()
                .filter(|(id, _)| usize::from(id.as_slice()[0]) <= first)
                .count();
            index.extend_from_slice(&u32::try_from(count)?.to_be_bytes());
        }
        for (id, offset) in offsets {
            index.extend_from_slice(&offset.to_be_bytes());
            index.extend_from_slice(id.as_slice());
        }
        index.extend_from_slice(checksum.as_slice());
        let checksum = hash(&index)?;
        index.extend_from_slice(checksum.as_slice());
        Self::from_bytes(bytes, index)
    }
}

#[derive(Default)]
pub struct Reads {
    pub total: AtomicUsize,
    pub largest: AtomicUsize,
}

#[derive(Clone)]
pub struct Source {
    packs: Arc<Vec<Pack>>,
    hold: Arc<()>,
    pub reads: Arc<Reads>,
}

impl Source {
    pub fn new(packs: Vec<Pack>) -> Self {
        Self {
            packs: Arc::new(packs),
            hold: Arc::new(()),
            reads: Arc::new(Reads::default()),
        }
    }

    pub fn witness(&self) -> Weak<()> {
        Arc::downgrade(&self.hold)
    }

    pub fn open(
        self,
        control: Control,
        budget: CapacityBudget,
        limits: ReadLimits,
    ) -> Result<SnapshotSession<RangePackSet<Self>>> {
        let identity = SnapshotIdentity {
            source: b"synthetic-range-contract".to_vec(),
            generation: b"one".to_vec(),
            object_hash: gix::hash::Kind::Sha1,
        };
        let provider = RangePackSet::new(
            self,
            identity.clone(),
            budget.clone(),
            limits,
            DecoderOptions::default(),
            &control,
        )?;
        SnapshotSession::open(provider, &identity, control, budget, limits)
    }

    fn pack(&self, pack: usize) -> Result<&Pack> {
        self.packs
            .get(pack)
            .ok_or_else(|| git_rs::invalid("fixture pack ordinal is absent"))
    }
}

impl PackSource for Source {
    fn pack_count(&self) -> usize {
        self.packs.len()
    }
    fn pack_size(&self, pack: usize) -> Result<u64> {
        Ok(self.pack(pack)?.bytes.len() as u64)
    }
    fn pack_hash(&self, pack: usize) -> Result<gix::ObjectId> {
        Ok(self.pack(pack)?.hash)
    }
    fn pack_digest(&self, pack: usize) -> Result<[u8; 32]> {
        Ok(self.pack(pack)?.digest)
    }

    fn index(
        &self,
        pack: usize,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<CapacityBuffer> {
        control.check()?;
        let pack = self.pack(pack)?;
        if openssl::sha::sha256(&pack.index) != pack.index_digest {
            return Err(git_rs::invalid("fixture catalog index digest mismatch"));
        }
        let mut output = budget.buffer(pack.index.len())?;
        for chunk in pack.index.chunks(32 * 1024) {
            output.extend_controlled(chunk, pack.index.len(), control)?;
        }
        Ok(output)
    }

    fn read_exact(
        &self,
        pack: usize,
        offset: u64,
        output: &mut [u8],
        control: &Control,
        _: &CapacityBudget,
    ) -> Result<()> {
        control.check()?;
        let start = usize::try_from(offset)?;
        let end = start
            .checked_add(output.len())
            .ok_or_else(|| git_rs::invalid("fixture range overflow"))?;
        let input = self
            .pack(pack)?
            .bytes
            .get(start..end)
            .ok_or_else(|| git_rs::invalid("fixture range is truncated"))?;
        self.reads.total.fetch_add(input.len(), Ordering::Relaxed);
        self.reads.largest.fetch_max(input.len(), Ordering::Relaxed);
        for (input, output) in input.chunks(32 * 1024).zip(output.chunks_mut(32 * 1024)) {
            control.check()?;
            output.copy_from_slice(input);
        }
        control.check()?;
        Ok(())
    }
}
