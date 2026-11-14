//! Authorized catalog/source ownership and bounded immutable object-store ranges.
//! Serving owns Rust buffers; no bare repository, local index path or tmpfs is
//! part of this adapter's contract.

use crate::{Result, blob::Store, cache::Cache, invalid, metadata, process};
use git_rs_core::{
    CapacityBudget, CapacityBuffer, CapacityReservation, Control, ReadLimits, SnapshotIdentity,
    pack::{
        DecoderOptions,
        ranges::{PackSource, RangePackSet, SharedRange},
    },
};
use std::{cell::Cell, sync::Arc};

const MAX_PACKS: usize = 4096;
const MAX_INDEX_BYTES: u64 = 128 * 1024 * 1024;
const SOURCE_METADATA_BYTES: usize = 128 * 1024 * 1024;
const BLOCK_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) struct Source {
    pub snapshot: metadata::Snapshot,
    lease: metadata::Barrier,
    principal: String,
    job: Option<Arc<crate::maintenance::queue::Job>>,
    strict_generation: bool,
    pub operation: Arc<process::Operation>,
    _metadata: CapacityReservation,
}

impl Source {
    pub fn acquire(
        slug: &str,
        principal: &str,
        write: bool,
        operation: Arc<process::Operation>,
        budget: &CapacityBudget,
    ) -> Result<Arc<Self>> {
        crate::metrics::measure(
            crate::metrics::Metric::Phase(crate::metrics::Phase::Snapshot),
            || {
                let metadata = budget.reservation(SOURCE_METADATA_BYTES)?;
                let (snapshot, lease) = metadata::leased_snapshot(slug, principal, write)?;
                Self::held(snapshot, lease, principal, !write, operation, metadata)
            },
        )
    }

    /// Export an immutable held snapshot even while ordinary pushes advance refs.
    /// Current permission, storage epoch and live reader lease remain mandatory.
    pub fn acquire_snapshot(
        slug: &str,
        principal: &str,
        operation: Arc<process::Operation>,
        budget: &CapacityBudget,
    ) -> Result<Arc<Self>> {
        crate::metrics::measure(
            crate::metrics::Metric::Phase(crate::metrics::Phase::Snapshot),
            || {
                let metadata = budget.reservation(SOURCE_METADATA_BYTES)?;
                let (snapshot, lease) = metadata::leased_snapshot(slug, principal, false)?;
                Self::held(snapshot, lease, principal, false, operation, metadata)
            },
        )
    }

    pub(crate) fn held(
        snapshot: metadata::Snapshot,
        lease: metadata::Barrier,
        principal: &str,
        strict_generation: bool,
        operation: Arc<process::Operation>,
        metadata: CapacityReservation,
    ) -> Result<Arc<Self>> {
        Self::held_by(
            snapshot,
            lease,
            principal,
            strict_generation,
            operation,
            metadata,
            None,
        )
    }

    fn held_by(
        snapshot: metadata::Snapshot,
        lease: metadata::Barrier,
        principal: &str,
        strict_generation: bool,
        operation: Arc<process::Operation>,
        metadata: CapacityReservation,
        job: Option<Arc<crate::maintenance::queue::Job>>,
    ) -> Result<Arc<Self>> {
        // The leased metadata decoder already bounds the complete retained
        // snapshot. Do not impose the former blame-only aggregate-name quota on
        // supported upload/receive/backup catalogs (10,000 refs of 1,024 bytes).
        if snapshot.packs.len() > MAX_PACKS || snapshot.refs.len() > git_rs_core::MAX_REFS {
            return Err(invalid("source catalog working-set bound exceeded"));
        }
        for pack in &snapshot.packs {
            process::check_operation()?;
            crate::protocol::Oid::parse(pack.hash.as_bytes())?;
            if pack.pack_size < 32
                || pack.idx_size > MAX_INDEX_BYTES
                || pack.pack_digest.len() != 64
                || pack.idx_digest.len() != 64
            {
                return Err(invalid("invalid immutable source catalog entry"));
            }
        }
        operation.check()?;
        Ok(Arc::new(Self {
            snapshot,
            lease,
            principal: principal.to_owned(),
            strict_generation,
            job,
            operation,
            _metadata: metadata,
        }))
    }

    pub(crate) fn maintenance(
        job: Arc<crate::maintenance::queue::Job>,
        operation: Arc<process::Operation>,
        budget: &CapacityBudget,
    ) -> Result<Arc<Self>> {
        job.check()?;
        let capacity = budget.reservation(SOURCE_METADATA_BYTES)?;
        let lease = metadata::Barrier::acquire(&job.repo, false)?;
        let snapshot = metadata::held_snapshot(&job.repo, &lease)?;
        job.check()?;
        let source = Self::held_by(snapshot, lease, "", true, operation, capacity, Some(job))?;
        source.check()?;
        Ok(source)
    }

    pub fn check(&self) -> Result<()> {
        process::check_operation()?;
        self.operation.check()?;
        // Keep the actual cancellation owner, not an unrelated operation with
        // only a copied timestamp. A tighter caller may still shorten this check.
        let operation = process::Operation::current()
            .filter(|operation| operation.deadline() <= self.operation.deadline())
            .unwrap_or_else(|| self.operation.clone());
        operation.enter(|| {
            self.lease.check()?;
            if let Some(job) = &self.job { job.check()?; }
            let s = &self.snapshot;
            let generation = if self.strict_generation {
                format!("AND r.generation={}", s.generation)
            } else { String::new() };
            let permission = if self.job.is_none() { format!(
                " AND EXISTS(SELECT 1 FROM permissions p WHERE p.repo_id=r.id AND p.principal={})", metadata::text(&self.principal))
            } else { String::new() };
            let found = metadata::sql(&format!(
                "SELECT 1 FROM repositories r WHERE r.id={}::uuid {generation} AND r.storage_epoch={}{permission};",
                metadata::text(&s.id), s.storage_epoch,
            ))?;
            if found.trim() != "1" { return Err(invalid("source authority changed")); }
            self.operation.check()?;
            process::check_operation()
        })
    }

    pub fn lease(&self) -> &metadata::Barrier {
        &self.lease
    }

    pub fn identity(&self) -> SnapshotIdentity {
        let mut generation = self.snapshot.generation.to_be_bytes().to_vec();
        generation.extend_from_slice(&self.snapshot.storage_epoch.to_be_bytes());
        SnapshotIdentity {
            source: self.snapshot.id.as_bytes().to_vec(),
            generation,
            object_hash: gix::hash::Kind::Sha1,
        }
    }
}

pub(crate) struct RangeStore {
    store: Store,
    cache: Arc<Cache>,
    index_bytes: Cell<u64>,
    // Source holds outlive the provider's loaded indexes and decoded values.
    source: Arc<Source>,
}

impl RangeStore {
    pub fn open(
        source: Arc<Source>,
        store: Store,
        cache: Arc<Cache>,
        budget: CapacityBudget,
        limits: ReadLimits,
        control: &Control,
    ) -> Result<RangePackSet<Self>> {
        let identity = source.identity();
        RangePackSet::new(
            Self {
                source,
                store,
                cache,
                index_bytes: Cell::new(0),
            },
            identity,
            budget,
            limits,
            DecoderOptions {
                cache_slots: 4096,
                cache_bytes: 32 * 1024 * 1024,
                max_cache_entry_bytes: 1024 * 1024,
                header_slots: 4096,
                retained_workspace_bytes: 0,
            },
            control,
        )
    }

    fn pack(&self, pack: usize) -> Result<&metadata::Pack> {
        self.source
            .snapshot
            .packs
            .get(pack)
            .ok_or_else(|| invalid("source pack ordinal is out of bounds"))
    }
}

impl PackSource for RangeStore {
    fn pack_count(&self) -> usize {
        self.source.snapshot.packs.len()
    }
    fn discard_cached(&self, ordinal: usize) {
        if let Some(pack) = self.source.snapshot.packs.get(ordinal) {
            for key in [&pack.pack_key, &pack.idx_key] {
                self.cache
                    .invalidate_prefix(&format!("{}:{key}:", self.source.snapshot.id));
            }
        }
    }
    fn pack_size(&self, pack: usize) -> Result<u64> {
        Ok(self.pack(pack)?.pack_size)
    }
    fn pack_hash(&self, pack: usize) -> Result<gix::ObjectId> {
        Ok(gix::ObjectId::from(
            crate::protocol::Oid::parse(self.pack(pack)?.hash.as_bytes())?.0,
        ))
    }
    fn pack_digest(&self, pack: usize) -> Result<[u8; 32]> {
        crate::protocol::unhex(self.pack(pack)?.pack_digest.as_bytes())?
            .try_into()
            .map_err(|_| invalid("source pack digest length is invalid"))
    }
    fn read_shared(
        &self,
        ordinal: usize,
        offset: u64,
        length: usize,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<SharedRange> {
        control.check()?;
        self.source.operation.check()?;
        let pack = self.pack(ordinal)?;
        if length == 0
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > pack.pack_size)
        {
            return Err(invalid("shared source range exceeds catalog pack size"));
        }
        let key = format!(
            "{}:{}:{}:{}:{offset}:{length}",
            self.source.snapshot.id, pack.pack_key, pack.pack_digest, pack.pack_size
        );
        if let Some(bytes) = self
            .cache
            .get_or_load_controlled(&key, length, control, |bytes| {
                self.store
                    .source_range(&pack.pack_key, pack.pack_size, offset, bytes, control)
            })?
        {
            return Ok(SharedRange::new(bytes));
        }
        let mut bytes = budget.buffer(length)?;
        bytes.resize_controlled(length, 0, control)?;
        self.store.source_range(
            &pack.pack_key,
            pack.pack_size,
            offset,
            bytes.as_mut_slice(),
            control,
        )?;
        Ok(SharedRange::new(Arc::new(bytes)))
    }
    fn index(
        &self,
        ordinal: usize,
        control: &Control,
        budget: &CapacityBudget,
    ) -> Result<CapacityBuffer> {
        control.check()?;
        self.source.operation.check()?;
        let pack = self.pack(ordinal)?;
        let next = self
            .index_bytes
            .get()
            .checked_add(pack.idx_size)
            .filter(|total| *total <= MAX_INDEX_BYTES)
            .ok_or_else(|| invalid("source index working-set byte limit exceeded"))?;
        let size = usize::try_from(pack.idx_size)?;
        let key = format!(
            "{}:{}:{}:{}",
            self.source.snapshot.id, pack.idx_key, pack.idx_digest, pack.idx_size
        );
        let cached = self
            .cache
            .get_or_load_controlled(&key, size, control, |output| {
                self.store
                    .source_into(&pack.idx_key, &pack.idx_digest, output, control)
            })?;
        let bytes = if let Some(cached) = cached {
            let mut bytes = budget.buffer(size)?;
            for chunk in cached.chunks(32 * 1024) {
                control.check()?;
                let start = bytes.as_slice().len();
                bytes.resize(start + chunk.len(), 0)?;
                bytes.as_mut_slice()[start..].copy_from_slice(chunk);
            }
            bytes
        } else {
            self.store.source_bytes(
                &pack.idx_key,
                pack.idx_size,
                &pack.idx_digest,
                control,
                budget,
            )?
        };
        self.index_bytes.set(next);
        control.check()?;
        Ok(bytes)
    }
    fn read_exact(
        &self,
        ordinal: usize,
        mut offset: u64,
        mut output: &mut [u8],
        control: &Control,
        _budget: &CapacityBudget,
    ) -> Result<()> {
        let pack = self.pack(ordinal)?;
        if offset
            .checked_add(output.len() as u64)
            .is_none_or(|end| end > pack.pack_size)
        {
            return Err(invalid("source range exceeds catalog pack size"));
        }
        while !output.is_empty() {
            control.check()?;
            self.source.operation.check()?;
            let start = offset / BLOCK_BYTES * BLOCK_BYTES;
            let length = (pack.pack_size - start).min(BLOCK_BYTES) as usize;
            let key = format!(
                "{}:{}:{}:{}:{start}:{length}",
                self.source.snapshot.id, pack.pack_key, pack.pack_digest, pack.pack_size
            );
            let cached = self
                .cache
                .get_or_load_controlled(&key, length, control, |bytes| {
                    self.store
                        .source_range(&pack.pack_key, pack.pack_size, start, bytes, control)
                })?;
            let within = usize::try_from(offset - start)?;
            let count = output.len().min(length - within);
            let (destination, rest) = output.split_at_mut(count);
            if let Some(bytes) = cached {
                for (destination, source) in destination
                    .chunks_mut(32 * 1024)
                    .zip(bytes[within..within + count].chunks(32 * 1024))
                {
                    control.check()?;
                    destination.copy_from_slice(source);
                }
            } else {
                // Bypass fills only the already-admitted destination; it cannot
                // allocate a second complete pack or an uncharged block buffer.
                self.store.source_range(
                    &pack.pack_key,
                    pack.pack_size,
                    offset,
                    destination,
                    control,
                )?;
            }
            output = rest;
            offset += count as u64;
        }
        control.check()?;
        Ok(())
    }
}
