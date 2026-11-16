//! Snapshot-owned optional acceleration. Locks protect slot metadata only, never
//! inflate, hashing, copying, callbacks or recursive object resolution.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use gix_hash::ObjectId;
use gix_object::{Header, Kind};

use super::DecodePlan;
use crate::executor::{CapacityError, CapacityReservation};
use crate::objects::{AdmittedVec, Result, VerifiedObject, invalid};
use crate::{CapacityBudget, CapacityBuffer, Control};

/// Existing `PackSet::open` reserves no acceleration memory. Explicit options
/// admit runtime metadata even when every optional table is disabled. Entry
/// bytes are actual backing capacities, including pinned entries; slot/owner
/// metadata is separately admitted to the same CapacityBudget. Workspace
/// retention is a backing-capacity limit independent of cache entry quota.
#[derive(Clone, Copy, Debug, Default)]
pub struct DecoderOptions {
    pub cache_slots: usize,
    pub cache_bytes: usize,
    pub max_cache_entry_bytes: usize,
    pub header_slots: usize,
    pub retained_workspace_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DecoderCacheStats {
    pub entries: usize,
    pub pinned_entries: usize,
    pub entry_bytes: usize,
    pub header_entries: usize,
    pub workspace_capacity: usize,
    pub hits: u64,
    pub header_hits: u64,
    pub decodes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key {
    pub pack: u32,
    pub offset: u64,
}

struct Quota {
    used: AtomicUsize,
    limit: usize,
}
struct QuotaPermit {
    quota: Arc<Quota>,
    bytes: usize,
}
impl Quota {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<QuotaPermit> {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            if bytes > self.limit.checked_sub(used)? {
                return None;
            }
            match self.used.compare_exchange_weak(
                used,
                used + bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(QuotaPermit {
                        quota: self.clone(),
                        bytes,
                    });
                }
                Err(next) => used = next,
            }
        }
    }
}
impl Drop for QuotaPermit {
    fn drop(&mut self) {
        self.quota.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
impl QuotaPermit {
    fn grow_to(&mut self, bytes: usize) -> bool {
        if bytes <= self.bytes {
            return true;
        }
        let Some(mut extra) = self.quota.reserve(bytes - self.bytes) else {
            return false;
        };
        self.bytes += extra.bytes;
        // Transfer the charge, not the Arc: the temporary permit must still be
        // dropped so its reference to the quota owner is released.
        extra.bytes = 0;
        true
    }
}

pub(super) struct Entry {
    pub key: Key,
    pub id: ObjectId,
    pub plan: DecodePlan,
    pub compressed: usize,
    // Byte storage is freed before its quota permit. CapacityBuffer itself
    // frees backing allocation before returning CapacityBudget admission.
    pub data: CapacityBuffer,
    _quota: QuotaPermit,
    // Candidates and pinned entries can coexist with every indexed slot. Their
    // ownership metadata must therefore be charged per allocation, not per slot.
    _metadata: CapacityReservation,
}

struct EntryAdmission {
    quota: QuotaPermit,
    metadata: CapacityReservation,
}

// Actual backing precedes its optional cache charges on every failure path.
struct EntryStorage {
    data: CapacityBuffer,
    admission: EntryAdmission,
}

#[derive(Clone, Copy)]
struct Memo {
    key: Key,
    id: ObjectId,
    plan: DecodePlan,
}
struct Tables {
    entries: Vec<Option<Arc<Entry>>>,
    headers: Vec<Option<Memo>>,
    // Sorted slot metadata only, not additional object/header ownership. Header
    // duplicates retain their individual slots; lowest slot wins as in the scan.
    entry_index: Vec<(Key, usize)>,
    header_index: Vec<(Key, ObjectId, usize)>,
    workspace: Option<AdmittedVec>,
    entry_cursor: usize,
    header_cursor: usize,
    hits: u64,
    header_hits: u64,
    decodes: u64,
}

pub(super) struct Runtime {
    tables: Mutex<Tables>,
    pub budget: CapacityBudget,
    options: DecoderOptions,
    quota: Arc<Quota>,
    // Fixed slot arrays/indexes and runtime/Arc metadata. CachedObject keeps this owner
    // alive while any bytes are pinned; Entry owns its separate metadata charge.
    _metadata: CapacityReservation,
}

/// A decoded immutable cache view. Even a one-byte borrowed slice pins the full
/// entry and its snapshot-cache metadata. It never exposes mutable bytes.
#[derive(Clone)]
pub struct CachedObject {
    entry: Arc<Entry>,
    _owner: Arc<Runtime>,
}
impl CachedObject {
    pub(crate) fn shares_budget(&self, budget: &CapacityBudget) -> bool {
        self._owner.budget.shares_with(budget)
    }
    pub(crate) fn plan(&self) -> DecodePlan {
        self.entry.plan
    }
    pub fn id(&self) -> ObjectId {
        self.entry.id
    }
    pub fn header(&self) -> Header {
        self.entry.plan.header
    }
    pub fn as_slice(&self) -> &[u8] {
        self.entry.data.as_slice()
    }
    pub fn capacity(&self) -> usize {
        self.entry.data.capacity()
    }
}

impl Runtime {
    pub fn new(
        options: DecoderOptions,
        budget: CapacityBudget,
        control: &Control,
    ) -> Result<Arc<Self>> {
        control.check()?;
        if options.cache_slots > 4096 || options.header_slots > 4096 {
            return Err(invalid(
                "decoder cache supports at most 4096 slots per table",
            ));
        }
        if (options.cache_slots == 0) != (options.cache_bytes == 0)
            || (options.cache_slots != 0
                && (options.max_cache_entry_bytes == 0
                    || options.max_cache_entry_bytes > options.cache_bytes))
        {
            return Err(invalid("inconsistent decoder cache quota options"));
        }
        let metadata_bytes = |entries, headers, entry_index, header_index| {
            [
                (entries, std::mem::size_of::<Option<Arc<Entry>>>()),
                (headers, std::mem::size_of::<Option<Memo>>()),
                (entry_index, std::mem::size_of::<(Key, usize)>()),
                (header_index, std::mem::size_of::<(Key, ObjectId, usize)>()),
            ]
            .into_iter()
            .try_fold(
                std::mem::size_of::<Self>() + std::mem::size_of::<Quota>() + 64,
                |total, (capacity, size): (usize, usize)| {
                    capacity
                        .checked_mul(size)
                        .and_then(|bytes| total.checked_add(bytes))
                        .ok_or_else(|| invalid("decoder metadata overflow"))
                },
            )
        };
        // Admit all four allocations before reserving. Locals declared after
        // this lease, and Runtime.tables before _metadata, free backing first on
        // every failure/drop path. Reconcile actual capacities before use.
        let mut metadata = budget.reservation(metadata_bytes(
            options.cache_slots,
            options.header_slots,
            options.cache_slots,
            options.header_slots,
        )?)?;
        let mut entries = Vec::new();
        entries.try_reserve_exact(options.cache_slots)?;
        control.check()?;
        let mut headers = Vec::new();
        headers.try_reserve_exact(options.header_slots)?;
        control.check()?;
        let mut entry_index = Vec::new();
        entry_index.try_reserve_exact(options.cache_slots)?;
        control.check()?;
        let mut header_index = Vec::new();
        header_index.try_reserve_exact(options.header_slots)?;
        metadata.resize(metadata_bytes(
            entries.capacity(),
            headers.capacity(),
            entry_index.capacity(),
            header_index.capacity(),
        )?)?;
        for n in 0..options.cache_slots {
            if n % 32 == 0 {
                control.check()?
            }
            entries.push(None);
        }
        for n in 0..options.header_slots {
            if n % 32 == 0 {
                control.check()?
            }
            headers.push(None);
        }
        control.check()?;
        Ok(Arc::new(Self {
            tables: Mutex::new(Tables {
                entries,
                headers,
                entry_index,
                header_index,
                workspace: None,
                entry_cursor: 0,
                header_cursor: 0,
                hits: 0,
                header_hits: 0,
                decodes: 0,
            }),
            quota: Arc::new(Quota {
                used: AtomicUsize::new(0),
                limit: options.cache_bytes,
            }),
            budget,
            options,
            _metadata: metadata,
        }))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Tables> {
        self.tables
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub fn lookup(
        &self,
        key: Key,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<Option<Arc<Entry>>> {
        check()?;
        let found = {
            let tables = self.lock();
            // <=13 fixed-key comparisons at the constructor's 4096-slot cap.
            tables
                .entry_index
                .binary_search_by_key(&key, |&(key, _)| key)
                .ok()
                .and_then(|position| tables.entries[tables.entry_index[position].1].as_ref())
                .filter(|entry| entry.key == key)
                .cloned()
        };
        check()?;
        Ok(found)
    }

    pub fn memo(&self, key: Key, id: ObjectId, control: &Control) -> Result<Option<DecodePlan>> {
        control.check()?;
        let plan = {
            let mut tables = self.lock();
            let position = tables
                .header_index
                .binary_search(&(key, id, 0))
                .unwrap_or_else(|position| position);
            let plan = tables
                .header_index
                .get(position)
                .and_then(|&(k, oid, slot)| {
                    (k == key && oid == id)
                        .then(|| tables.headers[slot])
                        .flatten()
                        .filter(|memo| memo.key == key && memo.id == id)
                        .map(|memo| memo.plan)
                });
            if plan.is_some() {
                tables.header_hits = tables.header_hits.saturating_add(1);
            }
            plan
        };
        control.check()?;
        Ok(plan)
    }

    pub fn remember(
        &self,
        key: Key,
        id: ObjectId,
        plan: DecodePlan,
        control: &Control,
    ) -> Result<()> {
        control.check()?;
        if self.options.header_slots == 0 {
            return Ok(());
        }
        {
            let mut tables = self.lock();
            let slot = tables.header_cursor;
            if let Some(old) = tables.headers[slot] {
                let position = tables
                    .header_index
                    .binary_search(&(old.key, old.id, slot))
                    .expect("occupied header slot is indexed");
                tables.header_index.remove(position);
            }
            let position = tables
                .header_index
                .binary_search(&(key, id, slot))
                .expect_err("overwritten header slot was removed");
            // Atomic metadata shifts are bounded by 4096 items per removal or
            // insertion, not the 13-comparison lookup bound. No allocation,
            // external callback or owner destruction while holding this lock.
            assert!(tables.header_index.len() < tables.header_index.capacity());
            tables.header_index.insert(position, (key, id, slot));
            tables.headers[slot] = Some(Memo { key, id, plan });
            tables.header_cursor = (slot + 1) % tables.headers.len();
        }
        control.check()?;
        Ok(())
    }

    fn evict_one(&self, check: &dyn Fn() -> gix_error::Result) -> gix_error::Result<bool> {
        for _ in 0..self.options.cache_slots {
            check()?;
            let removed = {
                let mut tables = self.lock();
                let index = tables.entry_cursor;
                tables.entry_cursor = (index + 1) % tables.entries.len();
                if tables.entries[index]
                    .as_ref()
                    .is_some_and(|entry| Arc::strong_count(entry) == 1)
                {
                    let key = tables.entries[index].as_ref().expect("occupied victim").key;
                    let position = tables
                        .entry_index
                        .binary_search_by_key(&key, |&(key, _)| key)
                        .expect("occupied entry slot is indexed");
                    tables.entry_index.remove(position);
                    tables.entries[index].take()
                } else {
                    None
                }
            };
            if removed.is_some() {
                drop(removed);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Publish bytes only after the caller verified their physical object hash.
    /// Entry storage is private and immutable once wrapped in Arc, so every
    /// later pin/copy can reuse that proof without hashing the same bytes again.
    pub fn store_verified(
        &self,
        key: Key,
        id: ObjectId,
        plan: DecodePlan,
        compressed: usize,
        bytes: &[u8],
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result {
        check()?;
        if self.options.cache_slots == 0 || bytes.len() > self.options.max_cache_entry_bytes {
            return Ok(());
        }
        if let Some(existing) = self.lookup(key, check)? {
            if existing.id != id {
                return Err(gix_error::corruption("decoder cache object identity mismatch").into());
            }
            return Ok(());
        }
        let Some(mut storage) = self.entry_storage(bytes.len(), check)? else {
            return Ok(());
        };
        Self::copy_entry(bytes, &mut storage.data, check)?;
        let candidate = Self::entry(key, id, plan, compressed, storage);
        self.publish(&candidate, check)?;
        Ok(())
    }

    fn entry_admission(
        &self,
        bytes: usize,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<Option<EntryAdmission>> {
        check()?;
        if self.options.cache_slots == 0 || bytes > self.options.max_cache_entry_bytes {
            return Ok(None);
        }
        let mut permit = self.quota.reserve(bytes);
        for _ in 0..self.options.cache_slots {
            if permit.is_some() {
                break;
            }
            if !self.evict_one(check)? {
                break;
            }
            permit = self.quota.reserve(bytes);
        }
        let Some(permit) = permit else {
            check()?;
            return Ok(None);
        };
        let metadata = match self
            .budget
            .reservation(std::mem::size_of::<Entry>() + 4 * std::mem::size_of::<usize>())
        {
            Ok(metadata) => metadata,
            Err(CapacityError::Stopped(reason)) => {
                return Err(gix_error::Error::from_boxed(Box::new(reason)));
            }
            Err(_) => {
                check()?;
                return Ok(None);
            }
        };
        check()?;
        Ok(Some(EntryAdmission {
            quota: permit,
            metadata,
        }))
    }

    fn entry_storage(
        &self,
        bytes: usize,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<Option<EntryStorage>> {
        let Some(mut admission) = self.entry_admission(bytes, check)? else {
            return Ok(None);
        };
        let data = match self.budget.buffer(bytes) {
            Ok(data) => data,
            Err(CapacityError::Stopped(reason)) => {
                return Err(gix_error::Error::from_boxed(Box::new(reason)));
            }
            Err(_) => {
                check()?;
                return Ok(None);
            }
        };
        if data.capacity() > self.options.max_cache_entry_bytes {
            check()?;
            return Ok(None);
        }
        if !admission.quota.grow_to(data.capacity()) {
            check()?;
            return Ok(None);
        }
        check()?;
        Ok(Some(EntryStorage { data, admission }))
    }

    fn copy_entry(
        bytes: &[u8],
        data: &mut CapacityBuffer,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result {
        for chunk in bytes.chunks(32 * 1024) {
            check()?;
            data.extend_from_slice(chunk)
                .map_err(|error| gix_error::Error::from_boxed(Box::new(error)))?;
        }
        check()?;
        Ok(())
    }

    fn entry(
        key: Key,
        id: ObjectId,
        plan: DecodePlan,
        compressed: usize,
        storage: EntryStorage,
    ) -> Arc<Entry> {
        Arc::new(Entry {
            key,
            id,
            plan,
            compressed,
            data: storage.data,
            _quota: storage.admission.quota,
            _metadata: storage.admission.metadata,
        })
    }

    fn publish(
        &self,
        candidate: &Arc<Entry>,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<bool> {
        let key = candidate.key;
        // Races can consume the optional slot after admission. Skip publication
        // rather than retaining unindexed storage or evicting pinned entries.
        for _ in 0..=self.options.cache_slots {
            check()?;
            {
                let mut tables = self.lock();
                if tables
                    .entry_index
                    .binary_search_by_key(&key, |&(key, _)| key)
                    .is_ok()
                {
                    return Ok(false);
                }
                if let Some(slot) = tables.entries.iter().position(|slot| slot.is_none()) {
                    let position = tables
                        .entry_index
                        .binary_search_by_key(&key, |&(key, _)| key)
                        .expect_err("new entry key is absent");
                    assert!(tables.entry_index.len() < tables.entry_index.capacity());
                    tables.entry_index.insert(position, (key, slot));
                    tables.entries[slot] = Some(candidate.clone());
                    return Ok(true);
                }
            }
            if !self.evict_one(check)? {
                return Ok(false);
            }
        }
        Ok(false)
    }

    /// Keep mandatory result backing owned through later mutable conversions.
    /// A cache pin would force into_object to admit another mandatory body, so
    /// target caching must instead admit its own independently optional copy.
    pub fn returned_verified(
        &self,
        key: Key,
        id: ObjectId,
        plan: DecodePlan,
        compressed: usize,
        data: CapacityBuffer,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<VerifiedObject> {
        self.store_verified(key, id, plan, compressed, data.as_slice(), check)?;
        check()?;
        Ok(VerifiedObject::from_pack_verified(
            id.as_ref(),
            plan.header.kind,
            data,
            plan,
        ))
    }

    /// A bounded, advisory eligibility check before optional intermediate hashing.
    /// Publication still admits actual backing and rechecks races in store_verified.
    fn may_store_intermediate(
        &self,
        key: Key,
        bytes: usize,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<bool> {
        check()?;
        if self.options.cache_slots == 0 || bytes > self.options.max_cache_entry_bytes {
            return Ok(false);
        }
        let eligible = {
            let tables = self.lock();
            if tables
                .entry_index
                .binary_search_by_key(&key, |&(key, _)| key)
                .is_ok()
            {
                false
            } else {
                let mut available = self
                    .quota
                    .limit
                    .saturating_sub(self.quota.used.load(Ordering::Acquire));
                let mut slot_available = false;
                // At most 4096 metadata-only inspections, with no owners dropped.
                for entry in &tables.entries {
                    match entry {
                        None => slot_available = true,
                        Some(entry) if Arc::strong_count(entry) == 1 => {
                            slot_available = true;
                            available = available.saturating_add(entry.data.capacity());
                        }
                        Some(_) => {}
                    }
                }
                slot_available && bytes <= available
            }
        };
        check()?;
        Ok(eligible)
    }

    pub fn note_decode(&self) {
        let mut tables = self.lock();
        tables.decodes = tables.decodes.saturating_add(1);
    }

    fn note_hit(&self) {
        let mut tables = self.lock();
        tables.hits = tables.hits.saturating_add(1);
    }

    pub fn pin(
        self: &Arc<Self>,
        key: Key,
        id: ObjectId,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<Option<CachedObject>> {
        let Some(entry) = self.lookup(key, check)? else {
            return Ok(None);
        };
        if entry.id != id {
            return Err(gix_error::corruption("decoder cache object identity mismatch").into());
        }
        // Only store_verified can create an Entry; no mutable storage escapes
        // publication. Keep the identity and stop checks, not a redundant hash.
        check()?;
        self.note_hit();
        Ok(Some(CachedObject {
            entry,
            _owner: self.clone(),
        }))
    }

    pub fn stats(&self, control: &Control) -> Result<DecoderCacheStats> {
        control.check()?;
        let tables = self.lock();
        // Both fixed arrays have a constructor-enforced 4096-slot bound. No
        // user callbacks, allocation or byte scans run while their lock is held.
        Ok(DecoderCacheStats {
            entries: tables.entries.iter().flatten().count(),
            pinned_entries: tables
                .entries
                .iter()
                .flatten()
                .filter(|entry| Arc::strong_count(entry) > 1)
                .count(),
            entry_bytes: self.quota.used.load(Ordering::Acquire),
            header_entries: tables.headers.iter().flatten().count(),
            workspace_capacity: tables.workspace.as_ref().map_or(0, AdmittedVec::capacity),
            hits: tables.hits,
            header_hits: tables.header_hits,
            decodes: tables.decodes,
        })
    }

    fn take_workspace(&self) -> Option<AdmittedVec> {
        self.lock().workspace.take()
    }
    fn return_workspace(&self, mut buffer: AdmittedVec) {
        if buffer.capacity() > self.options.retained_workspace_bytes {
            return;
        }
        let old = {
            let mut tables = self.lock();
            if let Some(old) = &mut tables.workspace {
                if old.capacity() < buffer.capacity() {
                    std::mem::swap(old, &mut buffer);
                }
                Some(buffer)
            } else {
                tables.workspace = Some(buffer);
                None
            }
        };
        drop(old);
    }
}

pub(super) struct Workspace {
    buffer: Option<AdmittedVec>,
    runtime: Option<Arc<Runtime>>,
}
impl Workspace {
    pub fn new(
        runtime: Option<&Arc<Runtime>>,
        budget: &CapacityBudget,
        needed: usize,
        control: &Control,
    ) -> Result<Self> {
        control.check()?;
        let mut owner = Self {
            buffer: runtime.and_then(|runtime| runtime.take_workspace()),
            runtime: runtime.cloned(),
        };
        if owner
            .buffer
            .as_ref()
            .is_none_or(|buffer| buffer.capacity() < needed)
        {
            let replacement = AdmittedVec::new(budget, needed)?;
            owner.buffer = Some(replacement);
        }
        owner.buffer().resize(0, control)?;
        Ok(owner)
    }
    pub fn buffer(&mut self) -> &mut AdmittedVec {
        self.buffer
            .as_mut()
            .expect("workspace always has an admitted buffer")
    }
    pub fn bytes(&self) -> &[u8] {
        self.buffer
            .as_ref()
            .expect("workspace always has an admitted buffer")
            .as_slice()
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        if let (Some(runtime), Some(buffer)) = (&self.runtime, self.buffer.take()) {
            runtime.return_workspace(buffer);
        }
    }
}

pub(super) struct EntryCache<'a> {
    pub runtime: Option<Arc<Runtime>>,
    pub key: Key,
    pub plan: DecodePlan,
    pub bundle: &'a gix_pack::Bundle,
    pub prepared: &'a super::Prepared,
}
impl gix_pack::cache::DecodeEntry for EntryCache<'_> {
    fn cache_intermediate_objects(&self) -> bool {
        self.runtime.is_some()
    }
    fn get_controlled(
        &mut self,
        pack_id: u32,
        offset: u64,
        out: &mut Vec<u8>,
        limit: usize,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result<Option<(Kind, usize)>> {
        check()?;
        let Some(runtime) = &self.runtime else {
            return Ok(None);
        };
        let Some(entry) = runtime.lookup(
            Key {
                pack: pack_id,
                offset,
            },
            check,
        )?
        else {
            return Ok(None);
        };
        let bytes = entry.data.as_slice();
        if bytes.len() > limit || bytes.len() > out.capacity() {
            return Err(
                gix_error::corruption("cached base exceeds admitted decode workspace").into(),
            );
        }
        runtime.note_hit();
        out.clear();
        for chunk in bytes.chunks(32 * 1024) {
            check()?;
            out.extend_from_slice(chunk);
        }
        check()?;
        Ok(Some((entry.plan.header.kind, entry.compressed)))
    }
    fn put_controlled(
        &mut self,
        pack_id: u32,
        offset: u64,
        data: &[u8],
        kind: Kind,
        compressed: usize,
        check: &dyn Fn() -> gix_error::Result,
    ) -> gix_error::Result {
        check()?;
        if self.key
            != (Key {
                pack: pack_id,
                offset,
            })
        {
            let Some(runtime) = &self.runtime else {
                return Ok(());
            };
            let key = Key {
                pack: pack_id,
                offset,
            };
            if pack_id != self.bundle.pack.id
                || !runtime.may_store_intermediate(key, data.len(), check)?
            {
                return Ok(());
            }
            // Prepared metadata is bounded by admitted chain depth. Bind the
            // decoder's data offset to this physical entry and its original full
            // chain plan, not the requested target's resource summary.
            for prepared in &self.prepared.plans {
                check()?;
                if prepared.key != key || !prepared.is_ofs_delta {
                    continue;
                }
                if kind != prepared.plan.header.kind
                    || data.len() as u64 != prepared.plan.header.size
                {
                    return Err(gix_error::corruption(
                        "decoded intermediate disagrees with its preflight header",
                    )
                    .into());
                }
                // First protect the root's mandatory returned backing. Then
                // admit this optional entry's actual allocation and metadata
                // before hashing: repeated global-admission misses do no hash
                // work and cannot consume the caller's result capacity.
                if !self.prepared.protect_output(&runtime.budget, check)? {
                    return Ok(());
                }
                let Some(mut storage) = runtime.entry_storage(data.len(), check)? else {
                    return Ok(());
                };
                let id =
                    super::hash_object_with(self.bundle.index.object_hash(), kind, data, check)?;
                // No reverse index or caller-provided proof is needed: only a
                // digest indexed at this exact physical location may be sealed.
                let Some(position) = self.bundle.index.lookup(id.as_ref()) else {
                    return Ok(());
                };
                if self.bundle.index.pack_offset_at_index(position) != prepared.location.offset {
                    return Ok(());
                }
                check()?;
                Runtime::copy_entry(data, &mut storage.data, check)?;
                let candidate = Runtime::entry(key, id, prepared.plan, compressed, storage);
                runtime.publish(&candidate, check)?;
                return Ok(());
            }
            return Ok(());
        }
        if kind != self.plan.header.kind || data.len() as u64 != self.plan.header.size {
            return Err(gix_error::corruption(
                "decoded object disagrees with its preflight header",
            )
            .into());
        }
        // The maintained decoder may call this before it returns its final
        // outcome. Publication belongs after decode_at validates that outcome
        // and verifies the object hash, including when optional admission fails.
        Ok(())
    }
    // Legacy entrypoints intentionally never perform hidden uncontrolled work.
    fn get(&mut self, _: u32, _: u64, _: &mut Vec<u8>) -> Option<(Kind, usize)> {
        None
    }
    fn put(&mut self, _: u32, _: u64, _: &[u8], _: Kind, _: usize) {}
}
