//! Complete immutable-commit blame over the verified physical snapshot only.
//! Authorization, source leases and publication fencing stay with the caller.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex, OnceLock};

use gix_diff::blob::control::{Allocation, Allocator, Cancelled, Stage};
use gix_hash::{ObjectId, oid};
use gix_object::{Find, FindHeader, Header, Kind};
use hashbrown::HashTable;
use sha2::{Digest, Sha256};

use crate::{
    CapacityBudget, CapacityBuffer, CapacityError, CapacityReservation, Control, ReadLimits,
    SnapshotSession, StopReason, VerifiedObjectProvider,
};

/// Owned request-local tuning API for consumers without a direct vendor dependency.
/// The tuning type defaults to all-off; `BlameOptions::default()` selects the
/// qualified NativeRootV1 profile explicitly.
pub use gix_blame::Tuning as BlameTuning;

pub const MAX_BLOB_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_BODY_BYTES: usize = 256 * 1024 * 1024;
const MAX_COMMITS: usize = 1_000_000;
pub(crate) const MAX_COMMIT_BYTES: usize = 64 * 1024;
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
            max_metadata_bytes: MAX_COMMIT_BYTES,
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

pub(crate) fn source_error(error: &(dyn std::error::Error + 'static)) -> BlameError {
    if let Some(reason) = error.downcast_ref::<StopReason>() {
        return (*reason).into();
    }
    if let Some(reason) = error.downcast_ref::<CapacityError>() {
        return (*reason).into();
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
                if header.kind != Kind::Blob && header.size > MAX_COMMIT_BYTES as u64 {
                    return Err(BlameError::Corrupt("metadata byte limit exceeded"));
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

struct CommitMetadata {
    id: ObjectId,
    fields: CapacityBuffer,
}
struct Metadata {
    table: HashTable<CommitMetadata>,
    _reservation: CapacityReservation,
    hash: RandomState,
    control: Control,
}
impl Metadata {
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
            .checked_mul(std::mem::size_of::<CommitMetadata>() + 1)
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
    fn get(&self, id: &oid) -> Option<&[u8]> {
        self.table
            .find(self.hash.hash_one(id), |entry| entry.id.as_ref() == id)
            .map(|entry| entry.fields.as_slice())
    }
}
impl Drop for Metadata {
    fn drop(&mut self) {
        for (index, entry) in self.table.drain().enumerate() {
            drop(entry);
            if index % 32 == 0 {
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
    metadata: Metadata,
    control: Control,
    capacity: CapacityBudget,
}
impl CompleteBlame {
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
        self.metadata.get(id)
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
    pub fn blame_complete(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
    ) -> Result<CompleteBlame, BlameError> {
        self.blame_complete_inner(request, options, None)
    }

    pub fn blame_complete_with_memo(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        memo: memo::MemoryMappingRequest<'_>,
    ) -> Result<CompleteBlame, BlameError> {
        self.blame_complete_inner(request, options, Some(memo))
    }

    fn blame_complete_inner(
        &self,
        request: BlameRequest<'_>,
        options: &BlameOptions,
        memo: Option<memo::MemoryMappingRequest<'_>>,
    ) -> Result<CompleteBlame, BlameError> {
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
        self.blame_target(request)?;
        let cause = Arc::new(Cause::default());
        let allocator = Arc::new(BlameAllocator {
            budget: self.capacity.clone(),
            control: self.control.clone(),
            cause: cause.clone(),
        });
        let platform_metadata = self.capacity.reservation(PLATFORM_BOOTSTRAP_BYTES)?;
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
            gix_blame::file_with_allocations_trace_and_memo(
                source,
                gix_blame::Start::Commit(request.commit.to_owned()),
                None,
                &mut platform,
                request.path.into(),
                options.vendor_options(),
                options.tuning,
                allocator,
                &mut check,
                gix_blame::memo::MappingRequest {
                    options: identities,
                    memo: &mut memory,
                    boundary: boundary
                        .as_mut()
                        .map(|callback| callback as &mut dyn gix_blame::memo::MappingBoundary),
                },
            )
        } else {
            gix_blame::file_with_allocations_and_trace(
                source,
                gix_blame::Start::Commit(request.commit.to_owned()),
                None,
                &mut platform,
                request.path.into(),
                options.vendor_options(),
                options.tuning,
                allocator,
                &mut check,
            )
        };
        let admitted = result.map_err(|_| {
            cause
                .get()
                .unwrap_or(BlameError::Corrupt("blame or native origin trace failed"))
        })?;
        drop(platform);
        drop(platform_metadata);
        // Wrap before post-work validation so every failure uses polled output
        // cleanup, rather than silently dropping a large admitted trace at once.
        let mut complete = CompleteBlame {
            admitted,
            metadata: Metadata::new(0, &self.capacity, &self.control)?,
            control: self.control.clone(),
            capacity: self.capacity.clone(),
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
                let fields = crate::porcelain::commit_metadata(self, &row.commit_id)?;
                metadata.table.insert_unique(
                    metadata.hash.hash_one(row.commit_id),
                    CommitMetadata {
                        id: row.commit_id,
                        fields,
                    },
                    |entry| metadata.hash.hash_one(entry.id),
                );
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
    fn blame_target(&self, request: BlameRequest<'_>) -> Result<(), BlameError> {
        let header = self
            .header(request.commit)
            .map_err(|e| source_error(e.as_ref()))?
            .ok_or(BlameError::NotFound)?;
        if header.kind != Kind::Commit {
            return Err(BlameError::InvalidRequest);
        }
        if header.size > MAX_COMMIT_BYTES as u64 {
            return Err(BlameError::Corrupt("commit metadata limit"));
        }
        let commit = self
            .required(request.commit, Kind::Commit)
            .map_err(|e| source_error(e.as_ref()))?;
        let mut tree_id =
            gix_object::CommitRefIter::from_bytes(commit.data.as_slice(), request.commit.kind())
                .tree_id()
                .map_err(|_| BlameError::Corrupt("commit tree"))?;
        drop(commit);
        let mut components = request.path.split(|byte| *byte == b'/').peekable();
        while let Some(component) = components.next() {
            self.control.check()?;
            let header = self
                .header(&tree_id)
                .map_err(|e| source_error(e.as_ref()))?
                .ok_or(BlameError::Corrupt("missing physical tree"))?;
            if header.kind != Kind::Tree || header.size > MAX_COMMIT_BYTES as u64 {
                return Err(BlameError::Corrupt("physical tree metadata limit or type"));
            }
            let tree = self
                .required(&tree_id, Kind::Tree)
                .map_err(|e| source_error(e.as_ref()))?;
            let mut found = None;
            for entry in gix_object::TreeRefIter::from_bytes(tree.data.as_slice(), tree_id.kind()) {
                self.control.check()?;
                let entry = entry.map_err(|_| BlameError::Corrupt("physical tree entry"))?;
                let filename: &[u8] = entry.filename.as_ref();
                if filename == component {
                    found = Some((entry.oid.to_owned(), entry.mode));
                }
            }
            let (id, mode) = found.ok_or(BlameError::NotFound)?;
            if components.peek().is_none() {
                if !mode.is_blob() && !mode.is_link() {
                    return Err(BlameError::NotFound);
                }
                let header = self
                    .header(&id)
                    .map_err(|e| source_error(e.as_ref()))?
                    .ok_or(BlameError::Corrupt("missing physical blob"))?;
                if header.kind != Kind::Blob {
                    return Err(BlameError::Corrupt("blob type mismatch"));
                }
                if header.size > MAX_BLOB_BYTES as u64 {
                    return Err(BlameError::BlobTooLarge);
                }
                return Ok(());
            }
            if !mode.is_tree() {
                return Err(BlameError::NotFound);
            }
            tree_id = id;
        }
        Err(BlameError::NotFound)
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
