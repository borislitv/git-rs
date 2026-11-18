//! Complete immutable-commit blame over the verified physical snapshot only.
//! Authorization, source leases and publication fencing stay with the caller.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex, OnceLock};

use gix_diff::blob::control::{Allocation, Allocator, Cancelled, Stage};
use gix_hash::{ObjectId, oid};
use gix_object::{Find, FindHeader, Header, Kind, commit::ref_iter::Token};
use hashbrown::HashTable;
use sha2::{Digest, Sha256};

use crate::objects::{MAX_METADATA_HEADER_BYTES, MAX_METADATA_OBJECT_BYTES, VerifiedObject};
use crate::storage::ChargedVec;
use crate::{
    CapacityBudget, CapacityBuffer, CapacityError, CapacityReservation, Control, ReadLimits,
    SnapshotSession, StopReason, VerifiedObjectProvider,
};

#[path = "blame_replacement.rs"]
mod replacement;
pub use replacement::{ReplacementBlame, ReplacementSeed};
#[path = "blame_completed.rs"]
mod completed;
pub use completed::{CompletedBlameMap, MAX_COMPLETED_MAP_BYTES};
pub use gix_blame::native::CompletedUse;

/// Owned request-local tuning API for consumers without a direct vendor dependency.
/// The tuning type defaults to all-off; `BlameOptions::default()` selects the
/// qualified NativeRootV1 profile explicitly.
pub use gix_blame::Tuning as BlameTuning;

pub const MAX_BLOB_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_BODY_BYTES: usize = 256 * 1024 * 1024;
const MAX_COMMITS: usize = 1_000_000;
// Match the pinned vendor's total-object bound, not its per-name metadata bound.
const MAX_TREE_BYTES: usize = 128 * 1024 * 1024;
// Fixed attribute/filter-selection metadata constructed by the pinned Platform
// constructor, never populated from repository configuration in the raw path.
// This conservative bootstrap charge is separate from dependency-owned data leases;
// it must be reviewed with any constructor/dependency update.
const PLATFORM_BOOTSTRAP_BYTES: usize = 16 * 1024;

/// Completed mappings are an optimization, never source or cache authority.
pub mod memo {
    pub use gix_blame::memo::{
        MappingKey, MappingOptions, MappingStatistics, UNIT_BYTES, WIRE_VERSION,
    };
    pub use gix_diff::blob::Algorithm;

    use super::BlameError;

    /// CPU/memory-only callbacks. Copies must use the original job's capacity
    /// budget. Load only authenticated trusted-producer records after the root
    /// cache's final authority fence; a matching key/checksum is not provenance.
    /// Optional storage failures are misses/skipped writes, not operation errors.
    pub trait MappingMemo {
        fn load(&mut self, key: &MappingKey, into: &mut [u8]) -> Result<Option<usize>, BlameError>;
        fn completed(&mut self, key: &MappingKey, encoded: &[u8]) -> Result<(), BlameError>;
    }

    /// Separate complete-edge callback, outside core locks. Root may checkpoint
    /// here using the original deadline and retained source holds. Optional
    /// checkpoint failures must not fail successful source computation.
    pub trait MappingBoundary {
        fn completed(&mut self, statistics: MappingStatistics) -> Result<(), BlameError>;
    }

    /// Caller selects reuse and hooks, but cannot supply producer/options bytes.
    /// Core binds those identities after runtime verification on every call.
    pub struct MemoryMappingRequest<'a> {
        pub reuse: bool,
        pub memo: &'a mut dyn MappingMemo,
        pub boundary: Option<&'a mut dyn MappingBoundary>,
    }
}

/// Vendor-observed work, not progress polls or checkpoint publication counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlameStatistics {
    pub commits_traversed: usize,
    /// Actual mapping work is observed even without hooks. `completed` counts
    /// delivered complete units, not persisted records; hook-free calls deliver
    /// no units. `edges_visited - hits` includes all mapping misses.
    pub mapping: memo::MappingStatistics,
}

#[derive(Clone, Copy, Debug)]
pub struct BlameRequest<'a> {
    pub commit: &'a oid,
    pub path: &'a [u8],
}

/// NativeRootV1: whole immutable file, Myers, whole-file rename following at
/// 50%, 1000 rename candidates, no empty-file rename or line-copy heuristics.
/// Tuning is immutable and cache-keyed even where it preserves attribution.
/// Metadata uses the qualified GNU libc iconv implementation. Unknown codecs or
/// malformed encoded input preserve the entire original message, as native Git.
/// The actual thread codeset is GNU C-locale ASCII, bound by BUILD_CONTRACT;
/// compatibility oracles run with LC_ALL=C, without changing the serving locale.
#[derive(Clone, Debug)]
pub struct BlameOptions {
    tuning: gix_blame::Tuning,
    canonical: [u8; 32],
}

impl BlameOptions {
    pub fn native_root(tuning: BlameTuning) -> Self {
        let mut canonical = [0; 32];
        canonical[..8].copy_from_slice(b"GRCBLM01");
        canonical[8] = 1; // NativeRootV1
        canonical[9] = 1; // Myers
        canonical[10] = 1; // --root, whole-file, immutable commit
        canonical[11] = 50; // integral rename percentage, not float formatting
        canonical[12..16].copy_from_slice(&1000u32.to_be_bytes());
        canonical[16] = u8::from(tuning.reuse_line_tokens)
            | (u8::from(tuning.retain_current_input_tokens) << 1)
            | (u8::from(tuning.reuse_unchanged_runs) << 2)
            | (u8::from(tuning.bounded_resource_cache) << 3)
            | (u8::from(tuning.index_changes) << 4);
        canonical[17] = 1; // fixed quotePath=true, UTF-8 metadata output
        canonical[18..22].copy_from_slice(&(MAX_COMMITS as u32).to_be_bytes());
        canonical[22..26].copy_from_slice(&(MAX_BLOB_BYTES as u32).to_be_bytes());
        canonical[26..30].copy_from_slice(&(MAX_BODY_BYTES as u32).to_be_bytes());
        canonical[30] = 2; // native iconv, atomic original-message fallback
        Self { tuning, canonical }
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    pub fn tuning(&self) -> BlameTuning {
        self.tuning
    }

    /// Identity for preparing authenticated root records. The serving entrypoint
    /// recomputes it; this value cannot override its verified producer binding.
    pub fn mapping_options(&self, reuse: bool) -> Result<memo::MappingOptions, BlameError> {
        let producer = ProducerVersion::verified()?;
        Ok(memo::MappingOptions {
            reuse,
            producer: Sha256::digest(producer.cache_id().as_bytes()).into(),
            options: Sha256::digest(self.canonical_bytes()).into(),
        })
    }

    /// Explicit profile, including enough bounded history for a 160000-commit
    /// full traversal. Physical graph APIs enforce the edge bound; blame uses
    /// the stricter of the session's commit bound and this profile's work cap,
    /// in addition to vendor parent/suspect bounds and the original deadline.
    pub fn read_limits(&self) -> ReadLimits {
        ReadLimits {
            max_object_bytes: MAX_BLOB_BYTES,
            max_metadata_bytes: MAX_METADATA_HEADER_BYTES,
            max_graph_objects: MAX_COMMITS,
            max_graph_edges: 8_000_000,
            ..ReadLimits::default()
        }
    }

    fn vendor_options(&self) -> gix_blame::Options {
        gix_blame::Options {
            diff_algorithm: gix_diff::blob::Algorithm::Myers,
            ranges: gix_blame::BlameRanges::WholeFile,
            since: None,
            rewrites: Some(gix_diff::Rewrites {
                copies: None,
                percentage: Some(0.5),
                limit: 1000,
                track_empty: false,
            }),
            debug_track_path: false,
        }
    }
}

impl Default for BlameOptions {
    fn default() -> Self {
        Self::native_root(BlameTuning {
            reuse_line_tokens: true,
            retain_current_input_tokens: true,
            reuse_unchanged_runs: true,
            bounded_resource_cache: true,
            index_changes: true,
        })
    }
}

/// Build identity, not merely a package version. Release builds must first pass
/// the pinned dependency source audit. Refreshing provenance or the lockfile
/// changes this identity and requires rebuilding before cache publication.
pub struct ProducerVersion {
    cache_id: String,
}

impl ProducerVersion {
    /// Required before cache lookup/publication or serving a complete artifact.
    /// Expensive native fingerprinting/module pinning is once-per-process;
    /// subsequent calls reject overrides and changed protected file identities.
    pub fn verified() -> Result<&'static Self, BlameError> {
        iconv_native::runtime::verify().map_err(BlameError::CodecRuntime)?;
        Ok(Self::current())
    }

    /// Pure build identity only. This is not a serving/cache authorization gate;
    /// all servable uses must first succeed through `verified()`.
    pub fn current() -> &'static Self {
        static CURRENT: OnceLock<ProducerVersion> = OnceLock::new();
        CURRENT.get_or_init(|| {
            let mut digest = Sha256::new();
            for source in [
                b"git-rs-core/native-root-v1/porcelain-v1;raw-togit;sha1;no-config;no-textconv;quote-path;utf8".as_slice(),
                env!("CARGO_PKG_VERSION").as_bytes(),
                env!("CARGO_PKG_RUST_VERSION").as_bytes(),
                std::env::consts::ARCH.as_bytes(), std::env::consts::OS.as_bytes(),
                include_bytes!("../Cargo.toml").as_slice(),
                include_bytes!("../../../Cargo.toml").as_slice(),
                include_bytes!("../../../Dockerfile").as_slice(),
                include_bytes!("../../../Cargo.lock").as_slice(),
                include_bytes!("../../../docs/gitoxide-source.json").as_slice(),
                include_bytes!("lib.rs").as_slice(), include_bytes!("blame.rs").as_slice(),
                include_bytes!("blame_replacement.rs").as_slice(),
                include_bytes!("blame_completed.rs").as_slice(),
                include_bytes!("porcelain.rs").as_slice(), include_bytes!("objects.rs").as_slice(),
                include_bytes!("graph.rs").as_slice(), include_bytes!("pack.rs").as_slice(),
                include_bytes!("pack_cache.rs").as_slice(), include_bytes!("control.rs").as_slice(),
                include_bytes!("executor.rs").as_slice(),
                iconv_native::CODEC_SOURCE_ID.as_bytes(),
                iconv_native::BUILD_CONTRACT.as_bytes(),
            ] {
                digest.update((source.len() as u64).to_be_bytes());
                digest.update(source);
            }
            let mut cache_id = String::from("git-rs-core/native-root-v1/porcelain-v1/");
            cache_id.reserve(64);
            for byte in digest.finalize().iter() {
                cache_id.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
                cache_id.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
            }
            Self { cache_id }
        })
    }
    pub fn cache_id(&self) -> &str {
        &self.cache_id
    }
}

/// Bounded typed errors: external error text is not accumulated in a job slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlameError {
    NotFound,
    BlobTooLarge,
    OutputTooLarge,
    Capacity(CapacityError),
    Stopped(StopReason),
    InvalidRequest,
    Corrupt(&'static str),
    UnsupportedContract,
    WorkLimit,
    CodecRuntime(iconv_native::runtime::Error),
}
impl std::fmt::Display for BlameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capacity(error) => std::fmt::Display::fmt(error, f),
            Self::Stopped(error) => std::fmt::Display::fmt(error, f),
            Self::Corrupt(reason) => write!(f, "invalid physical blame input: {reason}"),
            Self::CodecRuntime(error) => std::fmt::Display::fmt(error, f),
            _ => write!(f, "complete blame: {self:?}"),
        }
    }
}
impl std::error::Error for BlameError {}
impl From<StopReason> for BlameError {
    fn from(error: StopReason) -> Self {
        Self::Stopped(error)
    }
}
impl From<CapacityError> for BlameError {
    fn from(error: CapacityError) -> Self {
        match error {
            CapacityError::Stopped(reason) => Self::Stopped(reason),
            error => Self::Capacity(error),
        }
    }
}

pub(crate) fn source_error(mut error: &(dyn std::error::Error + 'static)) -> BlameError {
    loop {
        if let Some(reason) = error.downcast_ref::<StopReason>() {
            return (*reason).into();
        }
        if let Some(reason) = error.downcast_ref::<CapacityError>() {
            return (*reason).into();
        }
        // gix contexts retain an error tree, not just a native source chain.
        // Inspect it with the same typed recovery used by FindBridge::try_find.
        if let Some(context) = error.downcast_ref::<gix_error::Error>() {
            if let Some(reason) = context.downcast_any_ref::<StopReason>() {
                return (*reason).into();
            }
            if let Some(reason) = context.downcast_any_ref::<CapacityError>() {
                return (*reason).into();
            }
        }
        let Some(source) = error.source() else {
            break;
        };
        error = source;
    }
    BlameError::Corrupt("object provider failed")
}

#[derive(Default)]
struct Cause(Mutex<Option<BlameError>>);
impl Cause {
    fn record(&self, error: BlameError) {
        self.0.lock().unwrap().get_or_insert(error);
    }
    fn get(&self) -> Option<BlameError> {
        *self.0.lock().unwrap()
    }
}

struct CallbackBridge<'a, T: ?Sized> {
    callback: &'a mut T,
    control: &'a Control,
    cause: &'a Cause,
}
impl<T: ?Sized> CallbackBridge<'_, T> {
    fn invoke<R>(
        &mut self,
        call: impl FnOnce(&mut T) -> Result<R, BlameError>,
    ) -> Result<R, Cancelled> {
        let result = (|| {
            self.control.check()?;
            let value = call(self.callback)?;
            self.control.check()?;
            Ok(value)
        })();
        result.map_err(|error| {
            self.cause.record(error);
            Cancelled
        })
    }
}
impl gix_blame::memo::MappingMemo for CallbackBridge<'_, dyn memo::MappingMemo + '_> {
    fn load(
        &mut self,
        key: &memo::MappingKey,
        into: &mut [u8],
    ) -> Result<Option<usize>, Cancelled> {
        self.invoke(|callback| callback.load(key, into))
    }

    fn completed(&mut self, key: &memo::MappingKey, encoded: &[u8]) -> Result<(), Cancelled> {
        self.invoke(|callback| callback.completed(key, encoded))
    }
}
impl gix_blame::memo::MappingBoundary for CallbackBridge<'_, dyn memo::MappingBoundary + '_> {
    fn completed(&mut self, statistics: memo::MappingStatistics) -> Result<(), Cancelled> {
        self.invoke(|callback| callback.completed(statistics))
    }
}

struct BlameAllocator {
    budget: CapacityBudget,
    control: Control,
    cause: Arc<Cause>,
}
struct BlameAllocation {
    reservation: CapacityReservation,
    bytes: usize,
    control: Control,
    cause: Arc<Cause>,
}

impl Allocator for BlameAllocator {
    fn reserve(&self, bytes: usize) -> Result<Box<dyn Allocation>, Cancelled> {
        let result = (|| {
            self.control.check()?;
            Ok(Box::new(BlameAllocation {
                reservation: self.budget.reservation(bytes)?,
                bytes,
                control: self.control.clone(),
                cause: self.cause.clone(),
            }) as Box<dyn Allocation>)
        })();
        result.map_err(|error: BlameError| {
            self.cause.record(error);
            Cancelled
        })
    }
}
impl Allocation for BlameAllocation {
    fn resize(&mut self, bytes: usize) -> Result<(), Cancelled> {
        let result = (|| {
            // Cleanup/shrink must remain possible after cancellation.
            if bytes > self.bytes {
                self.control.check()?;
            }
            self.reservation.resize(bytes)?;
            self.bytes = bytes;
            Ok(())
        })();
        result.map_err(|error: BlameError| {
            self.cause.record(error);
            Cancelled
        })
    }
}

struct FindBridge<'a, P> {
    session: &'a SnapshotSession<P>,
    cause: Arc<Cause>,
}

/// No mutable bytes or weak handles escape this private raw input owner.
struct RawBlameBlob(VerifiedObject);
impl AsRef<[u8]> for RawBlameBlob {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

// Conservative std Arc allocation-layout bound: its strong/weak AtomicUsize
// counts plus the sized inline payload, with room for four counter widths and
// two payload-alignment units instead of the two counts and alignment padding.
// This includes the inline witness/handles, not its separately charged byte
// capacity. Arc unsizing/cloning does not allocate another control block; the
// outer RawBlob record is covered by the vendor's cache metadata admission.
// Allocator-private overhead/RSS is outside CapacityBudget's contract. Review
// this bound with Rust/Arc or RawBlameBlob layout changes (image Rust: 1.99.1).
const RAW_BLOB_METADATA_BYTES: usize = std::mem::size_of::<RawBlameBlob>()
    + 4 * std::mem::size_of::<std::sync::atomic::AtomicUsize>()
    + 2 * std::mem::align_of::<RawBlameBlob>();

struct RawBlobMetadata {
    tokens: ChargedVec<CapacityReservation>,
    control: Control,
}
impl Drop for RawBlobMetadata {
    fn drop(&mut self) {
        let mut released = 0usize;
        while let Some(token) = self.tokens.pop() {
            drop(token);
            released += 1;
            // A stop cannot skip cleanup. Unwind must not re-enter observers.
            if released.is_multiple_of(32) && !std::thread::panicking() {
                let _ = self.control.check();
            }
        }
        // ChargedVec frees its now-empty backing before its capacity lease.
    }
}

impl<P: VerifiedObjectProvider> FindBridge<'_, P> {
    fn raw_blob(
        &self,
        id: &oid,
        metadata: &mut RawBlobMetadata,
    ) -> gix_error::Result<Option<gix_diff::blob::raw::RawBlob>> {
        let result = (|| -> Result<_, BlameError> {
            self.session.control.check()?;
            // This session seam preserves physical membership, the complete
            // hash/collision witness, original decode plan and budget identity.
            // Moving the witness retains either owned bytes or the cache pin;
            // object()/into_object() would copy pinned bytes here.
            let Some(object) = self
                .session
                .verified_object(id)
                .map_err(|error| source_error(error.as_ref()))?
            else {
                return Ok(None);
            };
            if object.header().kind != Kind::Blob {
                return Err(BlameError::Corrupt("raw blame object is not a blob"));
            }
            if object.as_slice().len() > MAX_BLOB_BYTES {
                return Err(BlameError::BlobTooLarge);
            }
            let reservation = self.session.capacity.reservation(RAW_BLOB_METADATA_BYTES)?;
            // Complete every fallible ledger insertion BEFORE Arc allocation.
            // Tokens live outside the Arc, beyond even its final deallocation.
            metadata
                .tokens
                .push(reservation, &self.session.control)
                .map_err(|error| {
                    if error.is::<std::collections::TryReserveError>() {
                        BlameError::Capacity(CapacityError::Allocation)
                    } else {
                        source_error(error.as_ref())
                    }
                })?;
            let raw = gix_diff::blob::raw::RawBlob::new(
                object.id().to_owned(),
                Kind::Blob,
                Arc::new(RawBlameBlob(object)),
            );
            self.session.control.check()?;
            Ok(Some(raw))
        })();
        result.map_err(|error| {
            self.cause.record(error);
            gix_error::Error::from_boxed(Box::new(error))
        })
    }
}
impl<P: VerifiedObjectProvider> FindHeader for FindBridge<'_, P> {
    fn try_header(&self, id: &oid) -> gix_error::Result<Option<Header>> {
        let result = (|| {
            let header = self
                .session
                .header(id)
                .map_err(|e| source_error(e.as_ref()))?;
            if let Some(header) = header {
                if header.kind == Kind::Blob && header.size > MAX_BLOB_BYTES as u64 {
                    return Err(BlameError::BlobTooLarge);
                }
                if matches!(header.kind, Kind::Commit | Kind::Tag)
                    && header.size > MAX_METADATA_OBJECT_BYTES as u64
                {
                    return Err(BlameError::Corrupt("metadata object byte limit exceeded"));
                }
                if header.kind == Kind::Tree
                    && header.size > self.session.limits.max_object_bytes.min(MAX_TREE_BYTES) as u64
                {
                    return Err(BlameError::Corrupt("tree object byte limit exceeded"));
                }
            }
            Ok(header)
        })();
        result.map_err(|error: BlameError| {
            self.cause.record(error);
            gix_error::Error::from_boxed(Box::new(error))
        })
    }
}
impl<P: VerifiedObjectProvider> Find for FindBridge<'_, P> {
    fn try_find<'a>(
        &self,
        id: &oid,
        buffer: &'a mut Vec<u8>,
    ) -> gix_error::Result<Option<gix_object::Data<'a>>> {
        self.session.try_find(id, buffer).inspect_err(|error| {
            if let Some(reason) = error.downcast_any_ref::<StopReason>() {
                self.cause.record((*reason).into());
            } else if let Some(reason) = error.downcast_any_ref::<CapacityError>() {
                self.cause.record((*reason).into());
            } else {
                self.cause
                    .record(BlameError::Corrupt("verified object read failed"));
            }
        })
    }
}

struct CommitMetadata<T> {
    id: ObjectId,
    fields: T,
    first_row: usize,
}
struct Metadata<T = CapacityBuffer> {
    table: HashTable<CommitMetadata<T>>,
    _reservation: CapacityReservation,
    hash: RandomState,
    control: Control,
}
impl<T> Metadata<T> {
    fn new(count: usize, budget: &CapacityBudget, control: &Control) -> Result<Self, BlameError> {
        if count == 0 {
            return Ok(Self {
                table: HashTable::new(),
                _reservation: budget.reservation(0)?,
                hash: RandomState::new(),
                control: control.clone(),
            });
        }
        let buckets = count
            .max(4)
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(BlameError::Capacity(CapacityError::Exhausted))?;
        let upper = buckets
            .checked_mul(std::mem::size_of::<CommitMetadata<T>>() + 1)
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or(BlameError::Capacity(CapacityError::Exhausted))?;
        let mut reservation = budget.reservation(upper)?;
        let mut table = HashTable::new();
        table
            .try_reserve(count, |_| 0)
            .map_err(|_| CapacityError::Allocation)?;
        reservation.resize(table.allocation_size())?;
        Ok(Self {
            table,
            _reservation: reservation,
            hash: RandomState::new(),
            control: control.clone(),
        })
    }
    fn get(&self, id: &oid) -> Option<&CommitMetadata<T>> {
        self.table
            .find(self.hash.hash_one(id), |entry| entry.id.as_ref() == id)
    }
    fn insert(&mut self, id: ObjectId, first_row: usize, fields: T) {
        self.table.insert_unique(
            self.hash.hash_one(id.as_ref()),
            CommitMetadata {
                id,
                fields,
                first_row,
            },
            |entry| self.hash.hash_one(entry.id.as_ref()),
        );
    }
}
impl<T> Drop for Metadata<T> {
    fn drop(&mut self) {
        for (index, entry) in self.table.drain().enumerate() {
            drop(entry);
            if index % 32 == 0 && !std::thread::panicking() {
                let _ = self.control.check();
            }
        }
    }
}

/// Only constructed after complete attribution, trace validation and metadata
/// resolution. Its vendor backing leases and metadata remain admitted through
/// serialization or any other final consumer. Source authority is caller-owned.
pub struct CompleteBlame {
    admitted: gix_blame::AdmittedOutcome,
    target: ObjectId,
    path_digest: [u8; 32],
    live_scope: [u8; 32],
    completed_use: CompletedUse,
    metadata: Metadata,
    control: Control,
    capacity: CapacityBudget,
}
impl CompleteBlame {
    /// Whether this operation actually grafted complete attribution, independent of edge memo reuse.
    pub fn completed_use(&self) -> CompletedUse {
        self.completed_use
    }

    /// Signing/export gate for locally computed attribution. Rendering is still
    /// valid on a graft or mapping-memo hit, but it must not certify a new record.
    pub fn can_certify_local_attribution(&self) -> bool {
        !matches!(self.completed_use, CompletedUse::Hit { .. })
            && self.admitted.mapping_statistics().hits == 0
    }
    pub fn statistics(&self) -> BlameStatistics {
        BlameStatistics {
            commits_traversed: self.admitted.outcome().statistics.commits_traversed,
            mapping: self.admitted.mapping_statistics(),
        }
    }

    pub fn producer(&self) -> &'static ProducerVersion {
        ProducerVersion::current()
    }
    pub(crate) fn trace(&self) -> &[gix_blame::native::NativeOrigin] {
        self.admitted
            .native_trace()
            .expect("constructor requires native trace")
    }
    pub(crate) fn blob(&self) -> &[u8] {
        &self.admitted.outcome().blob
    }
    pub(crate) fn metadata(&self, id: &oid) -> Option<&[u8]> {
        self.metadata.get(id).map(|entry| entry.fields.as_slice())
    }
    pub(crate) fn uses_resources(&self, control: &Control, budget: &CapacityBudget) -> bool {
        self.capacity.shares_with(budget)
            && self.control.deadline() == control.deadline()
            && std::ptr::eq(
                self.control.cancellation_flag(),
                control.cancellation_flag(),
            )
    }
}
impl Drop for CompleteBlame {
    fn drop(&mut self) {
        self.admitted
            .clear_with_control(&mut |_| self.control.check().map_err(|_| Cancelled));
    }
}

impl<P: VerifiedObjectProvider> SnapshotSession<P> {
    /// Verify the physical object for a caller-selected published commit. This
    /// does not establish ref visibility, reachability, or source authorization.
    /// False declines the shortcut (including when the prefix exceeds the graph
    /// budget); source failures and errors parsing the needed tokens propagate.
    pub fn verify_published_blame_commit(&self, commit: &oid) -> crate::objects::Result<bool> {
        self.control.check()?;
        if self.limits.max_graph_objects < 1 {
            return Ok(false);
        }
        let Some(header) = self.header(commit)? else {
            return Ok(false);
        };
        if header.kind != Kind::Commit {
            return Ok(false);
        }
        // This is a reachability preflight, before the blame engine's typed
        // error boundary. Preserve physical source failures just as the full
        // reachability walk does; successful verification is still mandatory.
        let object = self
            .verified_object(commit)?
            .ok_or(BlameError::Corrupt("missing physical commit"))?;
        if object.header().kind != header.kind || object.header().size != header.size {
            return Err(BlameError::Corrupt("physical object header mismatch").into());
        }
        Ok(self.blame_commit_prefix(commit, &object)?.is_some())
    }

    /// Prove one unchanged byte-path edge for the fixed whole-file profile.
    /// Roots, merges, insufficient graph budget and ineligible paths return None.
    /// Missing referenced objects, malformed data and source failures are errors.
    /// Cache authentication and current reachability remain caller obligations.
    pub fn unchanged_blame_parent(
        &self,
        request: BlameRequest<'_>,
    ) -> Result<Option<ObjectId>, BlameError> {
        self.validate_blame_request(request)?;
        if self.limits.max_graph_objects < 2 || self.limits.max_graph_edges < 1 {
            return Ok(None);
        }
        let header = self.blame_commit_header(request.commit)?;
        let prefix = {
            let object = self.blame_commit(request.commit, header)?;
            self.blame_commit_prefix(request.commit, &object)?
        };
        let Some((child_tree, Some(parent))) = prefix else {
            return Ok(None);
        };
        let Some(child) = self.blame_path_entry(child_tree, request.path, true)? else {
            return Ok(None);
        };
        let header = self
            .header(&parent)
            .map_err(|e| source_error(e.as_ref()))?
            .ok_or(BlameError::Corrupt("missing physical parent commit"))?;
        if header.kind != Kind::Commit {
            return Err(BlameError::Corrupt("parent commit type mismatch"));
        }
        let parent_tree = {
            let object = self.blame_commit(&parent, header)?;
            gix_object::CommitRefIter::from_bytes(
                self.blame_commit_header_bytes(&object)?,
                parent.kind(),
            )
            .tree_id()
            .map_err(|_| BlameError::Corrupt("parent commit tree"))?
        };
        if self.blame_path_entry(parent_tree, request.path, true)? != Some(child) {
            return Ok(None);
        }
        // The same OID denotes the same physical blob in this snapshot. Verify
        // it once, after both independently resolved entries agree in full.
        let blob_header = self.blame_blob_header(&child.0)?;
        self.blame_object(&child.0, blob_header, "missing physical blob")?;
        self.control.check()?;
        Ok(Some(parent))
    }

    pub fn blame_complete(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
    ) -> Result<CompleteBlame, BlameError> {
        self.blame_complete_inner(request, options, None, None)
    }

    pub fn blame_complete_with_memo(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        memo: memo::MemoryMappingRequest<'_>,
    ) -> Result<CompleteBlame, BlameError> {
        self.blame_complete_inner(request, options, Some(memo), None)
    }

    fn blame_complete_inner(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        memo: Option<memo::MemoryMappingRequest<'_>>,
        completed: Option<([u8; 32], &CompletedBlameMap<'_>)>,
    ) -> Result<CompleteBlame, BlameError> {
        self.validate_blame_request(request)?;
        let scopes = completed::scopes(self, options)?;
        let scope = scopes.immutable;
        let seed = if let Some((namespace, seed)) = completed {
            seed.check_live(self, scopes.live, request)?;
            seed.applies(scope, namespace).then_some(seed)
        } else {
            None
        };
        self.blame_target(request)?;
        let cause = Arc::new(Cause::default());
        let allocator = Arc::new(BlameAllocator {
            budget: self.capacity.clone(),
            control: self.control.clone(),
            cause: cause.clone(),
        });
        let platform_metadata = self.capacity.reservation(PLATFORM_BOOTSTRAP_BYTES)?;
        // Declare before the platform: on every return/unwind all raw owners
        // (including final Arc control blocks) die before these metadata tokens.
        // The ledger retains reservations only, never blob bytes. Its Vec and
        // relocation overlap are themselves charged by ChargedVec. The maximum
        // follows Vec's addressable byte layout, not an arbitrary owner ceiling;
        // the existing shared budget bounds actual admission first.
        // Conservative first version: tokens accumulate for all acquisitions,
        // even after eviction. This can deny long histories earlier than exact
        // live-owner accounting; it never refunds still-allocated Arc metadata.
        let mut raw_metadata = RawBlobMetadata {
            tokens: ChargedVec::new(
                self.capacity.clone(),
                isize::MAX as usize / std::mem::size_of::<CapacityReservation>(),
            )
            .map_err(|error| source_error(error.as_ref()))?,
            control: self.control.clone(),
        };
        let mut platform = raw_platform(self.identity().object_hash);
        platform
            .validate_controlled_raw()
            .map_err(|_| BlameError::UnsupportedContract)?;
        let max_commits = self.limits.max_graph_objects.min(MAX_COMMITS);
        let mut cpu = self.control.cpu_poller();
        let mut previous_cpu_stage = None;
        let mut check = |event: gix_diff::blob::control::Checkpoint| {
            // Only the audited pure CPU loops are sampled: byte operations use
            // <=4096-byte chunks (offset copies <=1024 items), Common <=1024
            // tokens, Attribution bounded hunk metadata (<=128 suspects plus a
            // logarithmic prefix query) or <=4096 bytes of fixed-width origin
            // IDs. Explicit origin paths retain byte-comparison checks. Any
            // stage transition is strict. All
            // allocation, graph/source, output, trace, census, cleanup and
            // other/unknown stages remain strict; user hooks have their own
            // strict pre/post checks. No provider/I/O check is amortized.
            let bounded_cpu = matches!(
                event.stage,
                Stage::Attribution
                    | Stage::Tokenize
                    | Stage::Hash
                    | Stage::Equal
                    | Stage::Copy
                    | Stage::Common
            );
            let checked = if bounded_cpu && previous_cpu_stage == Some(event.stage) {
                cpu.checkpoint()
            } else {
                cpu.boundary()
            };
            previous_cpu_stage = bounded_cpu.then_some(event.stage);
            let result = checked.map_err(BlameError::from).and_then(|()| {
                if event.stage == Stage::Commit && event.processed >= max_commits {
                    Err(BlameError::WorkLimit)
                } else {
                    Ok(())
                }
            });
            result.map_err(|error| {
                cause.record(error);
                Cancelled
            })
        };
        let source = FindBridge {
            session: self,
            cause: cause.clone(),
        };
        let mut raw_blobs = |id: &oid| source.raw_blob(id, &mut raw_metadata);
        let result = if let Some(memo) = memo {
            let identities = options.mapping_options(memo.reuse)?;
            let mut memory = CallbackBridge {
                callback: memo.memo,
                control: &self.control,
                cause: &cause,
            };
            let mut boundary = memo.boundary.map(|callback| CallbackBridge {
                callback,
                control: &self.control,
                cause: &cause,
            });
            gix_blame::file_with_allocations_and_completed_attribution(
                &source,
                gix_blame::Start::Commit(request.commit.to_owned()),
                None,
                &mut platform,
                request.path.into(),
                options.vendor_options(),
                options.tuning,
                allocator,
                &mut check,
                scope,
                seed.map(CompletedBlameMap::as_vendor),
                Some(&mut raw_blobs),
                Some(gix_blame::memo::MappingRequest {
                    options: identities,
                    memo: &mut memory,
                    boundary: boundary
                        .as_mut()
                        .map(|callback| callback as &mut dyn gix_blame::memo::MappingBoundary),
                }),
            )
        } else {
            gix_blame::file_with_allocations_and_completed_attribution(
                &source,
                gix_blame::Start::Commit(request.commit.to_owned()),
                None,
                &mut platform,
                request.path.into(),
                options.vendor_options(),
                options.tuning,
                allocator,
                &mut check,
                scope,
                seed.map(CompletedBlameMap::as_vendor),
                Some(&mut raw_blobs),
                None,
            )
        };
        let admitted = result.map_err(|_| {
            cause
                .get()
                .unwrap_or(BlameError::Corrupt("blame or native origin trace failed"))
        })?;
        drop(platform);
        drop(raw_metadata);
        drop(platform_metadata);
        let completed_use = if completed.is_some() && seed.is_none() {
            CompletedUse::Miss
        } else {
            admitted.completed_use()
        };
        // Wrap before post-work validation so every failure uses polled output
        // cleanup, rather than silently dropping a large admitted trace at once.
        let mut complete = CompleteBlame {
            admitted,
            target: request.commit.to_owned(),
            path_digest: Sha256::digest(request.path).into(),
            live_scope: scopes.live,
            completed_use,
            metadata: Metadata::new(0, &self.capacity, &self.control)?,
            control: self.control.clone(),
            capacity: self.capacity.clone(),
        };
        let provenance = if matches!(completed_use, CompletedUse::Hit { .. }) {
            seed
        } else {
            None
        };
        let trace = complete
            .admitted
            .native_trace()
            .ok_or(BlameError::UnsupportedContract)?;
        if complete.blob().len() > MAX_BLOB_BYTES {
            return Err(BlameError::BlobTooLarge);
        }
        let mut metadata =
            Metadata::new(trace.len().min(max_commits), &self.capacity, &self.control)?;
        let mut expected = 0;
        for (index, row) in trace.iter().enumerate() {
            self.control.check()?;
            if row.final_range.start != expected
                || row.final_range.end <= expected
                || row.source_range.end.checked_sub(row.source_range.start) != Some(row.group_count)
                || row.final_range.end - row.final_range.start != row.group_count
                || usize::try_from(row.group_id).ok() != Some(index)
                || row.boundary
                || row.source_file_name.is_empty()
            {
                return Err(BlameError::Corrupt(
                    "incomplete or inconsistent native trace",
                ));
            }
            expected = row.final_range.end;
            if metadata.get(&row.commit_id).is_none() {
                if metadata.table.len() >= max_commits {
                    return Err(BlameError::WorkLimit);
                }
                let fields =
                    if let Some(fields) = provenance.and_then(|seed| seed.fields(&row.commit_id)) {
                        completed::copy_fields(self, fields)?
                    } else {
                        crate::porcelain::commit_metadata(self, &row.commit_id)?
                    };
                metadata.insert(row.commit_id, index, fields);
            }
        }
        let mut lines = 0u32;
        for chunk in complete.blob().chunks(32 * 1024) {
            self.control.check()?;
            lines = lines
                .checked_add(chunk.iter().filter(|byte| **byte == b'\n').count() as u32)
                .ok_or(BlameError::Corrupt("line count overflow"))?;
        }
        if complete.blob().last().is_some_and(|byte| *byte != b'\n') {
            lines += 1;
        }
        if expected != lines {
            return Err(BlameError::Corrupt(
                "native trace does not cover the complete file",
            ));
        }
        complete.metadata = metadata;
        self.control.check()?;
        ProducerVersion::verified()?;
        self.control.check()?;
        Ok(complete)
    }

    // Header-first literal resolution avoids inflating an oversized target just
    // to discover it violates the public blob bound. No virtual empty trees.
    fn validate_blame_request(&self, request: BlameRequest<'_>) -> Result<(), BlameError> {
        self.control.check()?;
        ProducerVersion::verified()?;
        if self.identity().object_hash != gix_hash::Kind::Sha1 {
            return Err(BlameError::UnsupportedContract);
        }
        if request.commit.kind() != self.identity().object_hash
            || request.commit.is_null()
            || request.path.is_empty()
            || request.path.len() > 4096
            || request.path.contains(&0)
            || request
                .path
                .split(|byte| *byte == b'/')
                .any(|part| part.is_empty() || part == b"." || part == b"..")
        {
            return Err(BlameError::InvalidRequest);
        }
        Ok(())
    }

    fn blame_commit_header(&self, commit: &oid) -> Result<Header, BlameError> {
        let header = self
            .header(commit)
            .map_err(|e| source_error(e.as_ref()))?
            .ok_or(BlameError::NotFound)?;
        if header.kind != Kind::Commit {
            return Err(BlameError::InvalidRequest);
        }
        Ok(header)
    }

    fn blame_object(
        &self,
        id: &oid,
        expected: Header,
        missing: &'static str,
    ) -> Result<VerifiedObject, BlameError> {
        let object = self
            .verified_object(id)
            .map_err(|e| source_error(e.as_ref()))?
            .ok_or(BlameError::Corrupt(missing))?;
        let actual = object.header();
        if actual.kind != expected.kind || actual.size != expected.size {
            return Err(BlameError::Corrupt("physical object header mismatch"));
        }
        Ok(object)
    }

    fn blame_commit(&self, id: &oid, header: Header) -> Result<VerifiedObject, BlameError> {
        if header.size > self.limits.max_object_bytes.min(MAX_METADATA_OBJECT_BYTES) as u64 {
            return Err(BlameError::Corrupt("commit object byte limit exceeded"));
        }
        self.blame_object(id, header, "missing physical commit")
    }

    fn blame_commit_header_bytes<'a>(
        &self,
        object: &'a VerifiedObject,
    ) -> Result<&'a [u8], BlameError> {
        let bytes = object.as_slice();
        let end = crate::objects::metadata_header_end(
            bytes,
            self.limits.max_metadata_bytes,
            &self.control,
        )
        .map_err(|e| source_error(e.as_ref()))?;
        Ok(&bytes[..end])
    }

    fn blame_commit_prefix(
        &self,
        id: &oid,
        object: &VerifiedObject,
    ) -> Result<Option<(ObjectId, Option<ObjectId>)>, BlameError> {
        let header = self.blame_commit_header_bytes(object)?;
        let mut tree = None;
        let mut only_parent = None;
        let mut edges = 0;
        // As in the graph walk, use fallible tokens through the first author:
        // parent_ids() hides parser errors, and receive validates later fields.
        for token in gix_object::CommitRefIter::from_bytes(header, id.kind()) {
            self.control.check()?;
            match token.map_err(|_| BlameError::Corrupt("invalid commit prefix"))? {
                Token::Tree { id } => tree = Some(id),
                Token::Parent { id } => {
                    if edges == self.limits.max_graph_edges || id.is_null() {
                        return Ok(None);
                    }
                    edges += 1;
                    only_parent = (edges == 1).then_some(id);
                }
                Token::Author { .. } => {
                    return Ok(tree.map(|tree| (tree, only_parent)));
                }
                _ => {}
            }
        }
        self.control.check()?;
        Ok(None)
    }

    fn blame_target(&self, request: BlameRequest<'_>) -> Result<(), BlameError> {
        let header = self.blame_commit_header(request.commit)?;
        let commit = self.blame_commit(request.commit, header)?;
        let tree = gix_object::CommitRefIter::from_bytes(commit.as_slice(), request.commit.kind())
            .tree_id()
            .map_err(|_| BlameError::Corrupt("commit tree"))?;
        drop(commit);
        let (id, _) = self
            .blame_path_entry(tree, request.path, false)?
            .ok_or(BlameError::NotFound)?;
        self.blame_blob_header(&id)?;
        Ok(())
    }

    fn blame_path_entry(
        &self,
        mut tree_id: ObjectId,
        path: &[u8],
        require_unique: bool,
    ) -> Result<Option<(ObjectId, u16)>, BlameError> {
        let mut components = path.split(|byte| *byte == b'/').peekable();
        while let Some(component) = components.next() {
            self.control.check()?;
            let header = self
                .header(&tree_id)
                .map_err(|e| source_error(e.as_ref()))?
                .ok_or(BlameError::Corrupt("missing physical tree"))?;
            if header.kind != Kind::Tree
                || header.size > self.limits.max_object_bytes.min(MAX_TREE_BYTES) as u64
            {
                return Err(BlameError::Corrupt("physical tree object limit or type"));
            }
            let tree = self.blame_object(&tree_id, header, "missing physical tree")?;
            let mut found = None;
            let mut ambiguous = false;
            for entry in gix_object::TreeRefIter::from_bytes(tree.as_slice(), tree_id.kind()) {
                self.control.check()?;
                let entry = entry.map_err(|_| BlameError::Corrupt("physical tree entry"))?;
                let filename: &[u8] = entry.filename.as_ref();
                if filename == component {
                    ambiguous |= found.is_some();
                    found = Some((entry.oid.to_owned(), entry.mode));
                }
            }
            if require_unique && ambiguous {
                return Err(BlameError::Corrupt("ambiguous physical tree path"));
            }
            let Some((id, mode)) = found else {
                return Ok(None);
            };
            if components.peek().is_none() {
                return Ok((mode.is_blob() || mode.is_link()).then(|| (id, mode.value())));
            }
            if !mode.is_tree() {
                return Ok(None);
            }
            tree_id = id;
        }
        Ok(None)
    }

    fn blame_blob_header(&self, id: &oid) -> Result<Header, BlameError> {
        let header = self
            .header(id)
            .map_err(|error| {
                // The provider may reject the blob's size during preflight.
                if error
                    .downcast_ref::<crate::pack::ObjectSizeLimit>()
                    .is_some_and(|size| {
                        size.limit == MAX_BLOB_BYTES && size.size > MAX_BLOB_BYTES as u64
                    })
                {
                    BlameError::BlobTooLarge
                } else {
                    source_error(error.as_ref())
                }
            })?
            .ok_or(BlameError::Corrupt("missing physical blob"))?;
        if header.kind != Kind::Blob {
            return Err(BlameError::Corrupt("blob type mismatch"));
        }
        if header.size > MAX_BLOB_BYTES as u64 {
            return Err(BlameError::BlobTooLarge);
        }
        Ok(header)
    }
}

fn raw_platform(hash: gix_hash::Kind) -> gix_diff::blob::Platform {
    let stack = gix_worktree::Stack::new(
        std::path::PathBuf::new(),
        gix_worktree::stack::State::AttributesStack(Default::default()),
        gix_glob::pattern::Case::Sensitive,
        Vec::new(),
        Vec::new(),
    );
    gix_diff::blob::Platform::new(
        Default::default(),
        gix_diff::blob::Pipeline::new(
            gix_diff::blob::pipeline::WorktreeRoots {
                old_root: None,
                new_root: None,
            },
            gix_filter::Pipeline::new(Default::default(), hash, Default::default()),
            Vec::new(),
            gix_diff::blob::pipeline::Options {
                large_file_threshold_bytes: 0,
                fs: Default::default(),
            },
        ),
        gix_diff::blob::pipeline::Mode::ToGit,
        stack,
    )
}
