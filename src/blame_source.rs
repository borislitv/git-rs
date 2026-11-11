//! Owned source authority and demand-driven pack acquisition for core blame.
//! Network I/O is performed on the admitted job thread, never under a mutex.

use crate::{Result, blob::Store, cache::Cache, invalid, metadata, process, scratch::Scratch};
use git_rs_core::{
    CapacityBudget, CapacityReservation, Control, Object, ReadLimits, SnapshotIdentity,
    VerifiedObjectProvider,
    objects::VerifiedObject,
    pack::{DecoderOptions, PackLocation, PackSet},
};
use gix::odb::pack;
use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

// Independent, finite source working-set bounds, not a claim that every legal
// repository fits one request. Index discovery never downloads unrelated packs.
const MAX_INDEXES: usize = 4096;
const MAX_INDEX_BYTES: u64 = 128 * 1024 * 1024;
const MAX_SELECTED_BYTES: u64 = 12 * 1024 * 1024 * 1024;

pub(crate) struct Source {
    pub snapshot: metadata::Snapshot,
    workspace: Mutex<Scratch>,
    directory: PathBuf,
    barrier: metadata::Barrier,
    principal: String,
    pub operation: Arc<process::Operation>,
}

impl Source {
    pub fn acquire(
        root: &Path,
        threads: u8,
        slug: &str,
        principal: &str,
        operation: Arc<process::Operation>,
    ) -> Result<Arc<Self>> {
        let (snapshot, barrier) = metadata::leased_snapshot(slug, principal, false)?;
        // Streaming already has metadata's <=128 MiB bound and original deadline.
        if snapshot.packs.len() > MAX_INDEXES
            || snapshot.refs.len() > 4096
            || snapshot
                .refs
                .keys()
                .try_fold(0usize, |size, name| {
                    size.checked_add(name.len())
                        .filter(|size| *size <= 1024 * 1024)
                })
                .is_none()
        {
            return Err(invalid("blame source catalog bound exceeded"));
        }
        let workspace = Scratch::new(root, threads)?;
        let directory = workspace.path.join("objects/pack");
        Ok(Arc::new(Self {
            snapshot,
            workspace: Mutex::new(workspace),
            directory,
            barrier,
            principal: principal.to_owned(),
            operation,
        }))
    }

    /// Batch-boundary authority, not a callback in graph/delta/line loops.
    /// Generation also fences grants/ref changes; storage epoch fences truncation.
    pub fn check(&self) -> Result<()> {
        process::check_operation()?;
        self.operation.check()?;
        let deadline = process::Operation::current()
            .map_or(self.operation.deadline(), |operation| {
                operation.deadline().min(self.operation.deadline())
            });
        process::Operation::until(deadline).enter(|| {
            self.barrier.check()?;
            let s = &self.snapshot;
            let found = metadata::sql(&format!(
                "SELECT 1 FROM repositories r JOIN permissions p ON p.repo_id=r.id WHERE r.id={}::uuid AND r.generation={} AND r.storage_epoch={} AND p.principal={};",
                metadata::text(&s.id), s.generation, s.storage_epoch, metadata::text(&self.principal)))?;
            if found.trim() != "1" { return Err(invalid("blame source authority changed")); }
            process::check_operation()
        })
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

    fn fetch(&self, store: &Store, cache: &Cache, ordinal: usize, index: bool) -> Result<PathBuf> {
        self.operation.check()?;
        let pack = &self.snapshot.packs[ordinal];
        // Catalog strings never become path components without validation.
        crate::protocol::Oid::parse(pack.hash.as_bytes())?;
        let (key, digest, size, extension) = if index {
            (&pack.idx_key, &pack.idx_digest, pack.idx_size, "idx")
        } else {
            (&pack.pack_key, &pack.pack_digest, pack.pack_size, "pack")
        };
        let path = self
            .directory
            .join(format!("pack-{}.{}", pack.hash, extension));
        let pin = store.materialize(&self.snapshot.id, key, digest, size, &path, cache)?;
        self.workspace
            .lock()
            .map_err(|_| invalid("source workspace poisoned"))?
            .retain(pin);
        Ok(path)
    }
}

struct State {
    indexes: Vec<Option<pack::index::File>>,
    index_bytes: u64,
    selected_bytes: u64,
    selected: Vec<(usize, pack::Bundle)>,
    decoder: Option<PackSet<()>>,
}

/// Single-job adapter. Interior borrows are never shared across threads or
/// retained across acquisition; only the source/pin owner escapes with output.
pub(crate) struct LazySnapshot {
    // Mappings and decoder caches must drop before the last source/pin guard.
    state: RefCell<State>,
    work: Option<crate::blame::memo::WorkSet>,
    store: Store,
    cache: Arc<Cache>,
    identity: SnapshotIdentity,
    budget: CapacityBudget,
    limits: ReadLimits,
    _metadata: CapacityReservation,
    source: Arc<Source>,
}

impl LazySnapshot {
    pub fn new(
        source: Arc<Source>,
        store: Store,
        cache: Arc<Cache>,
        budget: CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Self> {
        let count = source.snapshot.packs.len();
        if count > MAX_INDEXES {
            return Err(invalid("blame index discovery count exceeded"));
        }
        if limits.max_delta_depth > 64 {
            return Err(invalid("blame delta depth exceeds supported bound"));
        }
        // Includes index/set metadata, owned paths and allocation growth. Mapped
        // immutable file bytes have the separate source bounds above.
        let metadata = budget.reservation(count.saturating_mul(8192))?;
        let mut indexes = Vec::new();
        indexes.try_reserve_exact(count)?;
        indexes.resize_with(count, || None);
        let mut selected = Vec::new();
        selected.try_reserve_exact(count)?;
        let identity = source.identity();
        Ok(Self {
            source,
            store,
            cache,
            identity,
            budget,
            limits,
            state: RefCell::new(State {
                indexes,
                selected,
                index_bytes: 0,
                selected_bytes: 0,
                decoder: None,
            }),
            _metadata: metadata,
            work: None,
        })
    }

    pub fn with_workset(mut self, work: Option<crate::blame::memo::WorkSet>) -> Self {
        self.work = work;
        self
    }

    fn locate(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<PackLocation>> {
        control.check()?;
        // Match PackSet's selected-pack order even for duplicated physical IDs.
        {
            let state = self.state.borrow();
            for (pack, (_, bundle)) in state.selected.iter().enumerate() {
                control.check()?;
                if let Some(index) = bundle.index.lookup(id) {
                    return Ok(Some(PackLocation {
                        pack,
                        offset: bundle.index.pack_offset_at_index(index),
                    }));
                }
            }
        }
        let count = self.source.snapshot.packs.len();
        for ordinal in 0..count {
            control.check()?;
            if self.state.borrow().indexes[ordinal].is_none() {
                let bytes = self.source.snapshot.packs[ordinal].idx_size;
                let next = self
                    .state
                    .borrow()
                    .index_bytes
                    .checked_add(bytes)
                    .filter(|n| *n <= MAX_INDEX_BYTES)
                    .ok_or_else(|| invalid("blame index discovery byte limit exceeded"))?;
                let path = self.source.fetch(&self.store, &self.cache, ordinal, true)?;
                let index = pack::index::File::at(path, gix::hash::Kind::Sha1)?;
                let mut state = self.state.borrow_mut();
                state.indexes[ordinal] = Some(index);
                state.index_bytes = next;
            }
            let offset = {
                let state = self.state.borrow();
                let index = state.indexes[ordinal].as_ref().expect("discovered index");
                index
                    .lookup(id)
                    .map(|position| index.pack_offset_at_index(position))
            };
            if let Some(offset) = offset {
                let bytes = self.source.snapshot.packs[ordinal].pack_size;
                let next = self
                    .state
                    .borrow()
                    .selected_bytes
                    .checked_add(bytes)
                    .filter(|n| *n <= MAX_SELECTED_BYTES)
                    .ok_or_else(|| invalid("blame selected-pack byte limit exceeded"))?;
                let path = self
                    .source
                    .fetch(&self.store, &self.cache, ordinal, false)?;
                let bundle = pack::Bundle::at(path.with_extension("idx"), gix::hash::Kind::Sha1)?;
                let mut state = self.state.borrow_mut();
                let pack = state.selected.len();
                state.selected.push((ordinal, bundle));
                state.selected_bytes = next;
                // Old cache and mappings are destroyed before ordinal IDs are
                // re-created. Existing IDs stay stable; appended IDs are unique.
                state.decoder = None;
                return Ok(Some(PackLocation { pack, offset }));
            }
        }
        Ok(None)
    }

    /// Acquire exactly the requested object's REF/OFS dependency chain before
    /// entering PackSet. No recursive I/O callback or network-held decoder lock.
    /// Also obtains a header without inflating a blob merely to reject its size.
    fn prepare(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<gix::objs::Header>> {
        let Some(mut location) = self.locate(id, control)? else {
            return Ok(None);
        };
        let mut visited = [None; 65];
        let mut expected_base = None;
        let mut target_size = None;
        for depth in 0..=self.limits.max_delta_depth {
            control.check()?;
            if visited[..depth].contains(&Some(location)) {
                return Err(invalid("cross-pack delta cycle"));
            }
            visited[depth] = Some(location);
            let entry = self.state.borrow().selected[location.pack]
                .1
                .pack
                .entry(location.offset)?;
            if let Some(kind) = entry.header.as_kind() {
                if expected_base.is_some_and(|size| size != entry.decompressed_size) {
                    return Err(invalid("delta base size mismatch"));
                }
                return Ok(Some(gix::objs::Header {
                    kind,
                    size: target_size.unwrap_or(entry.decompressed_size),
                }));
            }
            if entry.decompressed_size > self.limits.max_decode_bytes as u64 {
                return Err(invalid("delta instruction byte limit exceeded"));
            }
            let (base, result) = {
                let state = self.state.borrow();
                delta_sizes(&state.selected[location.pack].1.pack, &entry, control)?
            };
            if expected_base.is_some_and(|size| size != result) {
                return Err(invalid("delta base size mismatch"));
            }
            target_size.get_or_insert(result);
            expected_base = Some(base);
            location = match entry.header {
                pack::data::entry::Header::OfsDelta { base_distance } => PackLocation {
                    pack: location.pack,
                    offset: entry
                        .checked_base_pack_offset(base_distance)
                        .ok_or_else(|| invalid("invalid OFS delta offset"))?,
                },
                pack::data::entry::Header::RefDelta { base_id } => self
                    .locate(&base_id, control)?
                    .ok_or_else(|| invalid("missing REF delta base"))?,
                _ => unreachable!("non-delta header handled above"),
            };
        }
        Err(invalid("delta depth limit exceeded"))
    }

    fn decoder(&self, control: &Control) -> Result<()> {
        if self.state.borrow().decoder.is_some() {
            return Ok(());
        }
        let indexes = self
            .state
            .borrow()
            .selected
            .iter()
            .map(|(ordinal, _)| {
                self.source.directory.join(format!(
                    "pack-{}.idx",
                    self.source.snapshot.packs[*ordinal].hash
                ))
            })
            .collect::<Vec<_>>();
        let decoder = PackSet::open_with_options(
            indexes,
            (),
            self.identity.object_hash,
            control,
            self.limits,
            DecoderOptions {
                cache_slots: 4096,
                cache_bytes: 32 * 1024 * 1024,
                max_cache_entry_bytes: 1024 * 1024,
                header_slots: 4096,
                retained_workspace_bytes: 16 * 1024 * 1024,
            },
            self.budget.clone(),
        )?;
        self.state.borrow_mut().decoder = Some(decoder);
        Ok(())
    }
}

impl VerifiedObjectProvider for LazySnapshot {
    fn identity(&self) -> &SnapshotIdentity {
        &self.identity
    }
    fn header(&self, id: &gix::hash::oid, control: &Control) -> Result<Option<gix::objs::Header>> {
        if let Some(decoder) = &self.state.borrow().decoder
            && let Some(object) = decoder.cached_object(id, control, self.limits)?
        {
            return Ok(Some(object.header()));
        }
        self.prepare(id, control)
    }
    fn object(
        &self,
        id: &gix::hash::oid,
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
        id: &gix::hash::oid,
        control: &Control,
        budget: &CapacityBudget,
        limits: ReadLimits,
    ) -> Result<Option<VerifiedObject>> {
        if !budget.shares_with(&self.budget) {
            return Err(invalid("source budget identity mismatch"));
        }
        let Some(header) = self.prepare(id, control)? else {
            return Ok(None);
        };
        // ObjectUnit carries bytes, not a decoder-chain plan. Preserve the hook
        // for the job's selected limits; stricter decode requests must use the
        // physical decoder's complete-chain validation rather than infer a plan.
        if let Some(work) = &self.work
            && limits.max_delta_depth == self.limits.max_delta_depth
            && limits.max_decode_bytes == self.limits.max_decode_bytes
        {
            let cached = work
                .lock()
                .map_err(|_| invalid("blame workset poisoned"))?
                .object(id.to_owned(), header.kind, control)?;
            if let Some(object) = cached {
                if object.kind != header.kind || object.data.as_slice().len() as u64 != header.size
                {
                    return Err(invalid("reusable source header mismatch"));
                }
                return VerifiedObject::verify(id, object, control, budget, limits).map(Some);
            }
        }
        self.decoder(control)?;
        let object = self
            .state
            .borrow()
            .decoder
            .as_ref()
            .expect("prepared decoder")
            .decode_verified(id, control, budget, limits)?;
        // Decoder/source I/O has completed and its borrow has been released.
        if let (Some(work), Some(object)) = (&self.work, &object) {
            work.lock()
                .map_err(|_| invalid("blame workset poisoned"))?
                .insert(
                    crate::blame::batch::UnitKey::Object {
                        oid: id.to_owned(),
                        kind: object.header().kind,
                    },
                    object.as_slice(),
                    control,
                )?;
        }
        Ok(object)
    }
    fn pack_location(
        &self,
        id: &gix::hash::oid,
        control: &Control,
    ) -> Result<Option<PackLocation>> {
        self.locate(id, control)
    }
}

fn delta_sizes(
    file: &pack::data::File,
    entry: &pack::data::Entry,
    control: &Control,
) -> Result<(u64, u64)> {
    let input = file
        .entry_slice(entry.data_offset..file.pack_end() as u64)
        .ok_or_else(|| invalid("truncated delta stream"))?;
    let mut decoder = gix::zlib::Decompress::default();
    let mut prefix = [0u8; 32];
    let (mut read, mut written) = (0usize, 0usize);
    loop {
        control.check()?;
        let before_in = decoder.total_in();
        let before_out = decoder.total_out();
        let end = input.len().min(read.saturating_add(32 * 1024));
        let status = decoder.decompress(
            &input[read..end],
            &mut prefix[written..],
            gix::zlib::FlushDecompress::None,
        )?;
        let consumed = usize::try_from(decoder.total_in() - before_in)?;
        let produced = usize::try_from(decoder.total_out() - before_out)?;
        read += consumed;
        written += produced;
        if written as u64 > entry.decompressed_size {
            return Err(invalid("delta header exceeds declared instruction size"));
        }
        if let Ok((base, n)) = pack::data::delta::decode_header_size(&prefix[..written])
            && let Ok((result, _)) = pack::data::delta::decode_header_size(&prefix[n..written])
        {
            return Ok((base, result));
        }
        if written == prefix.len()
            || status == gix::zlib::Status::StreamEnd
            || (consumed == 0 && produced == 0)
        {
            return Err(invalid("invalid delta size header"));
        }
    }
}
