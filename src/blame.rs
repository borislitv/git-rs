//! Authenticated, immutable, complete Rust-core line-porcelain serving and reuse.
use crate::derived_cache::{
    Access, ArtifactKey, ArtifactKind, BlameArtifacts, CacheConfig, CompleteArtifact, DerivedCache,
    KeyParts, MAX_CHUNK_BYTES,
};
use crate::repository::blame_source::Source;
use crate::{Result, engine::Engine, invalid, process, protocol::Oid};
use git_rs_core::{
    CapacityBudget, Control,
    blame::{BlameError, BlameOptions, ProducerVersion},
    porcelain::CompletePorcelain,
};
use std::{
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

#[path = "blame_batch.rs"]
pub(crate) mod batch;
#[path = "blame_derive.rs"]
mod derive;
#[path = "blame_jobs.rs"]
mod jobs;
#[path = "blame_memo.rs"]
pub(crate) mod memo;
#[path = "blame_warm.rs"]
mod warm;
pub use derive::WorkStatistics;
pub use jobs::{Completed, JOB_BYTES, JobEvent, JobObserver, Jobs};
pub use warm::{WarmDisposition, WarmPolicy, WarmStatistics};

pub const MAX_BLOB: u64 = 128 * 1024 * 1024;
pub const MAX_OUTPUT: u64 = 256 * 1024 * 1024;
#[derive(Clone)]
pub struct Request {
    commit: Oid,
    path: Vec<u8>,
}
impl Request {
    pub fn commit(&self) -> Oid {
        self.commit
    }
    pub fn path(&self) -> &[u8] {
        &self.path
    }
    pub fn parse(commit: &str, query: &str) -> Result<Self> {
        if query.len() > 5 + 3 * 4096 {
            return Err(invalid("oversized encoded blame query"));
        }
        let commit = Oid::parse(commit.as_bytes())?;
        let encoded = query
            .strip_prefix("path=")
            .filter(|value| !value.contains('&'))
            .ok_or_else(|| invalid("blame requires exactly one path query"))?;
        if encoded.len() > 3 * 4096 {
            return Err(invalid("oversized encoded blame path"));
        }
        let mut cursor = encoded.as_bytes().iter();
        while let Some(byte) = cursor.next() {
            if *byte == b'%'
                && !(cursor.next().is_some_and(u8::is_ascii_hexdigit)
                    && cursor.next().is_some_and(u8::is_ascii_hexdigit))
            {
                return Err(invalid("invalid blame path encoding"));
            }
        }
        let path = percent_encoding::percent_decode_str(encoded).collect::<Vec<_>>();
        if commit == Oid::ZERO
            || path.is_empty()
            || path.len() > 4096
            || path.contains(&0)
            || path
                .split(|byte| *byte == b'/')
                .any(|part| part.is_empty() || part == b"." || part == b"..")
        {
            return Err(invalid("invalid immutable blame target"));
        }
        Ok(Self { commit, path })
    }
}
pub struct Blame {
    jobs: Jobs<Outcome>,
    cache: Option<DerivedCache>,
    observer: Option<Arc<dyn Observer>>,
    writer: Option<batch::WriterProof>,
    warming: Option<WarmPolicy>,
    options: BlameOptions,
    reuse: bool,
    body_lookup: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Preparing,
    SourceAcquired,
    CacheHit,
    Staging,
    Publishing,
    Complete,
    CheckpointDecoded,
    CheckpointImported,
    CheckpointInstalled,
    CheckpointPublished,
}
/// In-process observation outside source/cache/workset/scheduler locks. Callbacks
/// must honor the original control/deadline, including the statistics callback.
pub trait Observer: Send + Sync {
    fn observe(&self, phase: Phase, control: &Control) -> Result<()>;
    /// Actual completed core work, including independent warm jobs. Not progress.
    fn work_completed(&self, _warm: bool, _statistics: WorkStatistics, _control: &Control) {}
}
fn observe(observer: &Option<Arc<dyn Observer>>, phase: Phase, control: &Control) -> Result<()> {
    control.check()?;
    if let Some(observer) = observer {
        observer.observe(phase, control)?;
    }
    control.check()?;
    Ok(())
}
impl Default for Blame {
    fn default() -> Self {
        Self::new(1, 0).expect("start bounded core blame executor")
    }
}
impl Blame {
    pub fn admitted(&self) -> usize {
        self.jobs.admitted()
    }
    pub fn waiters(&self) -> usize {
        self.jobs.waiters()
    }
    pub fn warming_statistics(&self) -> Option<WarmStatistics> {
        self.warming.as_ref().map(WarmPolicy::statistics)
    }
    pub fn new(concurrency: usize, waiters: usize) -> Result<Self> {
        ProducerVersion::verified()?;
        Ok(Self {
            jobs: Jobs::new(concurrency, waiters)?,
            cache: None,
            observer: None,
            writer: None,
            warming: None,
            options: BlameOptions::default(),
            reuse: true,
            body_lookup: true,
        })
    }
    pub fn with_capacity(self, concurrency: usize, waiters: usize) -> Result<Self> {
        ProducerVersion::verified()?;
        Ok(Self {
            jobs: Jobs::new(concurrency, waiters)?,
            cache: self.cache,
            observer: self.observer,
            writer: self.writer,
            warming: self.warming,
            options: self.options,
            reuse: self.reuse,
            body_lookup: self.body_lookup,
        })
    }
    pub fn with_cache(mut self, cache: DerivedCache) -> Self {
        self.cache = Some(cache);
        self.writer = None; // never carry a signing namespace across cache changes
        self.warming = None;
        self
    }
    /// Configuration and signer are constructed together: callers cannot pair a
    /// proof for namespace A with a cache opened for namespace B.
    pub fn with_authenticated_cache(
        mut self,
        config: CacheConfig,
        key_file: &Path,
    ) -> Result<Self> {
        let writer = batch::WriterProof::load(&config, key_file)?;
        self.cache = Some(DerivedCache::open(config)?);
        self.writer = Some(writer);
        Ok(self)
    }
    /// Explicit in-process configuration/test counterpart; never an HTTP API.
    pub fn with_authenticated_cache_key(mut self, config: CacheConfig, key: &[u8]) -> Result<Self> {
        let writer = batch::WriterProof::from_key(&config, key)?;
        self.cache = Some(DerivedCache::open(config)?);
        self.writer = Some(writer);
        Ok(self)
    }
    pub fn with_warming(mut self, policy: WarmPolicy) -> Result<Self> {
        if self.cache.is_none() || self.writer.is_none() {
            return Err(invalid(
                "warming requires explicitly authenticated checkpoint cache",
            ));
        }
        self.warming = Some(policy);
        Ok(self)
    }
    pub fn with_observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
        self
    }
    /// In-process fixture configuration only. No production CLI/HTTP selector.
    /// Immutable after construction; ALWAYS bypasses ready-body lookup so the
    /// fixture measures core work rather than a ready final response.
    #[doc(hidden)]
    pub fn with_test_options(mut self, options: BlameOptions, reuse: bool) -> Self {
        self.options = options;
        self.reuse = reuse;
        self.body_lookup = false;
        self
    }
    fn environment(&self, engine: &Engine) -> derive::Environment {
        derive::Environment {
            root: engine.scratch_root.clone(),
            threads: engine.pack_threads,
            store: engine.store.clone(),
            files: engine.cache.clone(),
            derived: self.cache.clone(),
            writer: self.writer.clone(),
            observer: self.observer.clone(),
            options: self.options.clone(),
            reuse: self.reuse,
            body_lookup: self.body_lookup,
        }
    }
}
pub enum Response {
    Busy,
    NotFound,
    TooLarge,
    Complete(CompleteResponse),
}
// At most four admitted owners; inline statistics avoid another uncharged heap owner.
#[allow(clippy::large_enum_variant)]
enum Outcome {
    NotFound,
    TooLarge,
    Complete(Body),
}
struct Body {
    bytes: Bytes,
    source: Arc<Source>,
    work: Option<WorkStatistics>,
}
enum Bytes {
    Local(CompletePorcelain),
    Cached(CompleteArtifact),
}
pub struct CompleteResponse {
    owner: Arc<Completed<Outcome>>,
}
impl CompleteResponse {
    fn body(&self) -> &Body {
        match &**self.owner {
            Outcome::Complete(body) => body,
            _ => unreachable!("complete response constructor"),
        }
    }
    /// None on a final-body hit or when memo integration is disabled. Never count
    /// a ready final response as avoided fresh mapping computation.
    pub fn work_statistics(&self) -> Option<WorkStatistics> {
        self.body().work
    }
    pub fn len(&self) -> u64 {
        match &self.body().bytes {
            Bytes::Local(body) => body.len(),
            Bytes::Cached(body) => body.len() as u64,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn deadline(&self) -> Instant {
        self.body().source.operation.deadline()
    }
    pub fn check(&self) -> Result<()> {
        ProducerVersion::verified()?;
        self.body().source.check()
    }
    pub fn write_to(&self, output: &mut impl Write) -> Result<()> {
        self.check()?;
        let mut remaining = self.len();
        let mut tail = None;
        let mut consume = |chunks: &[&[u8]]| -> Result<()> {
            for chunk in chunks {
                process::check_operation()?;
                self.body().source.operation.check()?;
                let length = chunk.len().min(remaining.saturating_sub(1) as usize);
                for bytes in chunk[..length].chunks(32 * 1024) {
                    process::check_operation()?;
                    self.body().source.operation.check()?;
                    output.write_all(bytes)?;
                }
                if length < chunk.len() {
                    tail = chunk.last().copied();
                }
                remaining -= chunk.len() as u64;
            }
            Ok(())
        };
        match &self.body().bytes {
            Bytes::Local(body) => {
                for chunk in body.chunks() {
                    consume(&[chunk])?;
                }
            }
            Bytes::Cached(body) => body.with_chunks(consume)?,
        }
        // Preserve the source/runtime/cache pre-tail and post-flush fences.
        self.check()?;
        if let Some(byte) = tail {
            output.write_all(&[byte])?;
        }
        output.flush()?;
        self.check()
    }
}
impl Engine {
    pub fn blame(&self, slug: &str, principal: &str, query: &Request) -> Result<Response> {
        crate::auth::repository(slug)?;
        crate::auth::principal(principal)?;
        ProducerVersion::verified()?;
        let mut key = Vec::new();
        for field in [
            slug.as_bytes(),
            principal.as_bytes(),
            &query.commit.0,
            &query.path,
        ] {
            key.extend_from_slice(&(field.len() as u32).to_be_bytes());
            key.extend_from_slice(field);
        }
        let (slug, principal, query, environment) = (
            slug.to_owned(),
            principal.to_owned(),
            query.clone(),
            self.blame.environment(self),
        );
        let result = self
            .blame
            .jobs
            .run(key, self.operation_seconds, move |control, budget| {
                derive::derive(environment, slug, principal, query, None, control, budget)
            })?;
        let Some(owner) = result else {
            return Ok(Response::Busy);
        };
        Ok(match &**owner {
            Outcome::NotFound => Response::NotFound,
            Outcome::TooLarge => Response::TooLarge,
            Outcome::Complete(_) => {
                let response = CompleteResponse { owner };
                response.check()?;
                Response::Complete(response)
            }
        })
    }
    pub(crate) fn warm_after_confirmed(
        &self,
        slug: &str,
        principal: &str,
        snapshot: &crate::metadata::Snapshot,
        generation: u64,
        operation: &str,
        commands: &[crate::protocol::Update],
    ) -> WarmDisposition {
        self.blame.try_warm_confirmed(
            self.blame.environment(self),
            slug,
            principal,
            snapshot,
            generation,
            operation,
            commands,
        )
    }
}
struct SourceAccess {
    source: Arc<Source>,
    key: ArtifactKey,
}
impl Access for SourceAccess {
    fn check(&self, key: &ArtifactKey) -> Result<()> {
        if key != &self.key {
            return Err(invalid("blame cache authority scope mismatch"));
        }
        ProducerVersion::verified()?;
        self.source.check()
    }
}
fn artifact_key(
    source: &Source,
    principal: &str,
    query: &Request,
    options: &BlameOptions,
) -> Result<ArtifactKey> {
    ArtifactKey::new(KeyParts {
        repository: source.snapshot.id.as_bytes(),
        security: principal.as_bytes(),
        object_format: "sha1",
        kind: "complete-blame",
        inputs: &[
            &source.snapshot.generation.to_be_bytes(),
            &source.snapshot.storage_epoch.to_be_bytes(),
            &query.commit.0,
            &query.path,
        ],
        options: options.canonical_bytes(),
        representation: "native-root-line-porcelain-v1",
        producer: &memo::Profile::verified(options)?.id,
    })
}
fn optional_cache<T>(control: &Control, work: impl FnOnce() -> Result<T>) -> Result<Option<T>> {
    control.check()?;
    let deadline = control
        .deadline()
        .min(Instant::now() + Duration::from_secs(2));
    let result = process::Operation::until(deadline).enter(work);
    control.check()?; // an optional cache failure must not hide a real job stop
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if codec_runtime_failure(error.as_ref()) => Err(error),
        Err(_) => Ok(None),
    }
}
fn codec_runtime_failure(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if matches!(
            error.downcast_ref::<BlameError>(),
            Some(BlameError::CodecRuntime(_))
        ) || error.is::<git_rs_core::CodecRuntimeError>()
        {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}
fn publish_body(
    cache: &DerivedCache,
    build: &crate::derived_cache::BuildLease,
    body: &CompletePorcelain,
    control: &Control,
    budget: &CapacityBudget,
    observer: &Option<Arc<dyn Observer>>,
) -> Result<()> {
    observe(observer, Phase::Staging, control)?;
    ProducerVersion::verified()?;
    let artifacts = BlameArtifacts::new(cache);
    let mut chunks = Vec::with_capacity(64);
    let mut buffer = budget.buffer(MAX_CHUNK_BYTES)?;
    for mut bytes in body.chunks() {
        while !bytes.is_empty() {
            control.check()?;
            let start = buffer.as_slice().len();
            let length = bytes.len().min(MAX_CHUNK_BYTES - start).min(32 * 1024);
            let (part, rest) = bytes.split_at(length);
            buffer.resize(start + part.len(), 0)?;
            buffer.as_mut_slice()[start..].copy_from_slice(part);
            bytes = rest;
            if buffer.as_slice().len() == MAX_CHUNK_BYTES {
                let Some(chunk) = artifacts.stage_chunk(build, buffer.as_slice())? else {
                    return Ok(());
                };
                chunks.push(chunk);
                buffer.resize(0, 0)?;
            }
        }
    }
    if !buffer.as_slice().is_empty() {
        let Some(chunk) = artifacts.stage_chunk(build, buffer.as_slice())? else {
            return Ok(());
        };
        chunks.push(chunk);
    }
    observe(observer, Phase::Publishing, control)?;
    ProducerVersion::verified()?;
    let _ = artifacts.publish(build, ArtifactKind::CompleteBody, &chunks)?;
    Ok(())
}
