//! Memory adapters and batch checkpoints for core-owned mapping identities.
use super::{
    batch::{BatchSnapshot, Scope, UnitKey, WriterProof},
    optional_cache,
};
use crate::{
    Result,
    derived_cache::{
        Access, ArtifactKey, BlameArtifacts, BlameBuilds, BuildLease, BuildLimits, BuildLookup,
        CheckpointVersion, DerivedCache, KeyParts, MAX_CHUNK_BYTES,
    },
    invalid,
    repository::blame_source::Source,
};
use git_rs_core::blame::memo::{
    Algorithm, MappingBoundary, MappingKey, MappingMemo, MappingStatistics, MemoryMappingRequest,
};
use git_rs_core::{
    CapacityBudget, Control,
    blame::{BlameError, BlameOptions, ProducerVersion},
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(crate) type WorkSet = Arc<Mutex<BatchSnapshot>>;

/// The same pure clock boundary is used by production callbacks and tests.
pub(super) struct Cadence {
    last_attempt: Instant,
    last_insertions: u64,
}
impl Cadence {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            last_attempt: now,
            last_insertions: 0,
        }
    }
    pub(super) fn poll(&mut self, now: Instant, insertions: u64, final_batch: bool) -> bool {
        if !final_batch
            && insertions.saturating_sub(self.last_insertions) < 256
            && now.saturating_duration_since(self.last_attempt) < Duration::from_secs(5)
        {
            return false;
        }
        self.last_attempt = now;
        self.last_insertions = insertions;
        true
    }
}

pub(super) struct Profile {
    pub producer: [u8; 32],
    pub options: [u8; 32],
    pub id: String,
}
impl Profile {
    pub fn verified(options: &BlameOptions) -> Result<Self> {
        let core = ProducerVersion::verified()?;
        // Bind the exact new root codec/callback code, not just the core package
        // version. Core binds the committed manifest-referenced pins and source
        // inventory; that is not a claim that every possible build pin is released.
        let mut hash = openssl::sha::Sha256::new();
        for bytes in [
            b"git-rs/completed-native-root-units/v1".as_slice(),
            core.cache_id().as_bytes(),
            include_bytes!("blame_batch.rs").as_slice(),
            include_bytes!("blame_memo.rs").as_slice(),
            include_bytes!("blame_derive.rs").as_slice(),
            include_bytes!("blame.rs").as_slice(),
            include_bytes!("blame_source.rs").as_slice(),
            include_bytes!("blame_jobs.rs").as_slice(),
            include_bytes!("blame_warm.rs").as_slice(),
        ] {
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        let id = format!(
            "git-rs/completed-native-root-v1/{}",
            crate::protocol::hex(&hash.finish())
        );
        let verified = options.mapping_options(true)?;
        Ok(Self {
            producer: verified.producer,
            options: verified.options,
            id,
        })
    }
}

pub(super) fn checkpoint_key(
    source: &Source,
    principal: &str,
    query: &super::Request,
    options: &BlameOptions,
    profile: &Profile,
) -> Result<ArtifactKey> {
    ProducerVersion::verified()?;
    ArtifactKey::new(KeyParts {
        repository: source.snapshot.id.as_bytes(),
        security: principal.as_bytes(),
        object_format: "sha1",
        kind: "complete-blame-workset",
        inputs: &[
            &source.snapshot.generation.to_be_bytes(),
            &source.snapshot.storage_epoch.to_be_bytes(),
            &query.commit.0,
            &query.path,
        ],
        options: options.canonical_bytes(),
        representation: "authenticated-complete-units-v1",
        producer: &profile.id,
    })
}

type RootError = Box<dyn std::error::Error + Send + Sync>;
#[derive(Default)]
pub(super) struct CallbackFailure(Mutex<Option<RootError>>);
impl CallbackFailure {
    fn stop(&self, error: RootError) -> BlameError {
        let typed = error
            .downcast_ref::<BlameError>()
            .copied()
            .or_else(|| {
                error
                    .downcast_ref::<git_rs_core::StopReason>()
                    .copied()
                    .map(BlameError::from)
            })
            .or_else(|| {
                error
                    .downcast_ref::<git_rs_core::CapacityError>()
                    .copied()
                    .map(BlameError::from)
            })
            .unwrap_or(BlameError::Corrupt("root mapping callback failed"));
        let mut first = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if first.is_none() {
            *first = Some(error);
        }
        typed
    }
    pub fn take(&self) -> Option<RootError> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
    }
}

/// No cache/source I/O in either memory callback, including failure paths.
pub(super) struct Memory {
    pub work: WorkSet,
    pub control: Control,
    pub failure: Arc<CallbackFailure>,
    pub producer: [u8; 32],
    pub options: [u8; 32],
}
impl Memory {
    fn key(&self, key: &MappingKey) -> Option<UnitKey> {
        if key.algorithm != Algorithm::Myers
            || key.producer != self.producer
            || key.options != self.options
        {
            return None;
        }
        Some(UnitKey::Mapping {
            before: key.before,
            after: key.after,
            tuning: key.tuning,
            producer: key.producer,
            options: key.options,
        })
    }
}
impl MappingMemo for Memory {
    fn load(
        &mut self,
        key: &MappingKey,
        into: &mut [u8],
    ) -> std::result::Result<Option<usize>, BlameError> {
        self.control
            .check()
            .map_err(|e| self.failure.stop(e.into()))?;
        let Some(key) = self.key(key) else {
            return Ok(None);
        };
        self.work
            .lock()
            .map_err(|_| self.failure.stop(invalid("blame workset poisoned")))?
            .load(key, into, &self.control)
            .map_err(|e| self.failure.stop(e))
    }
    fn completed(
        &mut self,
        key: &MappingKey,
        encoded: &[u8],
    ) -> std::result::Result<(), BlameError> {
        self.control
            .check()
            .map_err(|e| self.failure.stop(e.into()))?;
        let Some(key) = self.key(key) else {
            return Ok(());
        };
        self.work
            .lock()
            .map_err(|_| self.failure.stop(invalid("blame workset poisoned")))?
            .insert(key, encoded, &self.control)
            .map_err(|e| self.failure.stop(e))?;
        Ok(())
    }
}

pub(super) struct Checkpoints {
    pub work: WorkSet,
    scope: Scope,
    cache: DerivedCache,
    source: Arc<Source>,
    control: Control,
    build: Option<Arc<BuildLease>>,
    version: Option<CheckpointVersion>,
    failure: Arc<CallbackFailure>,
    saved_revision: u64,
    cadence: Cadence,
    pub statistics: MappingStatistics,
    observer: Option<Arc<dyn super::Observer>>,
    budget: CapacityBudget,
}
impl Checkpoints {
    /// Exactly ONE checkpoint lookup/import attempt per derivation, outside vendor
    /// callbacks. Neither candidate nor version is installed before both fences.
    #[allow(clippy::too_many_arguments)] // Explicit ownership/authority inputs at one boundary.
    pub fn open(
        cache: DerivedCache,
        writer: WriterProof,
        key: ArtifactKey,
        profile: &Profile,
        source: Arc<Source>,
        access: Arc<dyn Access>,
        work: WorkSet,
        budget: &CapacityBudget,
        control: &Control,
        observer: Option<Arc<dyn super::Observer>>,
        reuse: bool,
    ) -> Result<Self> {
        let scope = Scope {
            key: key.clone(),
            producer: profile.producer,
            options: profile.options,
            writer,
        };
        if !work
            .lock()
            .map_err(|_| invalid("blame workset poisoned"))?
            .budget()
            .shares_with(budget)
        {
            return Err(invalid("workset job budget mismatch"));
        }
        let mut imported = None;
        let mut version = None;
        let checkpoint = if reuse {
            optional_cache(control, || {
                BlameArtifacts::new(&cache).get_checkpoint_verified(&key, access.clone())
            })?
        } else {
            None
        };
        if let Some(Some(checkpoint)) = checkpoint {
            let candidate = optional_cache(control, || {
                checkpoint.with_chunks(|parts| {
                    // Return temporary owned data to OUTER with_chunks. It drops
                    // on a post-consumption cache/source fence failure.
                    let candidate = BatchSnapshot::decode(parts, &scope, budget.clone(), control)?;
                    super::observe(&observer, super::Phase::CheckpointDecoded, control)?;
                    Ok(candidate)
                })
            })?;
            if candidate.is_some() {
                super::observe(&observer, super::Phase::CheckpointImported, control)?;
            }
            source.check()?; // failure drops candidate, never mutates live state
            if let Some(candidate) = candidate {
                imported = Some(candidate);
                version = Some(checkpoint.version().clone());
            }
        }
        source.check()?;
        if let Some(candidate) = imported {
            *work.lock().map_err(|_| invalid("blame workset poisoned"))? = candidate;
            super::observe(&observer, super::Phase::CheckpointInstalled, control)?;
        }
        let build = if !reuse || control.remaining().as_secs() == 0 {
            None
        } else {
            optional_cache(control, || {
                BlameBuilds::new(&cache).claim(
                    &key,
                    access,
                    BuildLimits {
                        hard_seconds: control.remaining().as_secs().min(3600) as i32,
                        lease_seconds: control.remaining().as_secs().min(15) as i32,
                    },
                )
            })?
            .and_then(|claim| match claim {
                BuildLookup::Claimed(build) => Some(Arc::new(build)),
                BuildLookup::Pending => None,
            })
        };
        Ok(Self {
            work,
            scope,
            cache,
            source,
            control: control.clone(),
            build,
            version,
            failure: Arc::new(CallbackFailure::default()),
            saved_revision: 0,
            cadence: Cadence::new(Instant::now()),
            statistics: MappingStatistics::default(),
            observer,
            budget: budget.clone(),
        })
    }
    pub fn memory(&self) -> Memory {
        Memory {
            work: self.work.clone(),
            control: self.control.clone(),
            failure: self.failure.clone(),
            producer: self.scope.producer,
            options: self.scope.options,
        }
    }
    pub fn build(&self) -> Option<Arc<BuildLease>> {
        self.build.clone()
    }
    pub fn take_failure(&self) -> Option<RootError> {
        self.failure.take()
    }
    pub fn request<'a>(
        &'a mut self,
        memory: &'a mut Memory,
        reuse: bool,
    ) -> MemoryMappingRequest<'a> {
        MemoryMappingRequest {
            reuse,
            memo: memory,
            boundary: Some(self),
        }
    }
    pub fn flush(&mut self, final_batch: bool) -> Result<()> {
        self.control.check()?;
        let inserted = self
            .work
            .lock()
            .map_err(|_| invalid("blame workset poisoned"))?
            .mappings_inserted;
        if !self.cadence.poll(Instant::now(), inserted, final_batch) {
            return Ok(());
        }
        let Some(build) = self.build.clone() else {
            return Ok(());
        };
        ProducerVersion::verified()?;
        self.source.check()?;
        let snapshot = optional_cache(&self.control, || {
            let batch = self
                .work
                .lock()
                .map_err(|_| invalid("blame workset poisoned"))?;
            if batch.revision == self.saved_revision {
                return Ok(None);
            }
            Ok(Some((
                batch.encode(&self.scope, &self.control)?,
                batch.revision,
            )))
        })?; // NO workset/decoder/flight borrow survives publication I/O.
        let snapshot = match snapshot {
            Some(Some(snapshot)) => snapshot,
            Some(None) => return Ok(()),
            // Encoding is optional too: an exhausted copy budget must not turn
            // otherwise correct attribution into a failed source operation.
            None => {
                self.build = None;
                return Ok(());
            }
        };
        let publication = optional_cache(&self.control, || {
            ProducerVersion::verified()?;
            let artifacts = BlameArtifacts::new(&self.cache);
            let count = snapshot.0.as_slice().len().div_ceil(MAX_CHUNK_BYTES);
            let mut metadata = self
                .budget
                .reservation(count * std::mem::size_of::<crate::derived_cache::StagedChunk>())?;
            let mut staged = Vec::new();
            staged.try_reserve_exact(count)?;
            metadata.resize(
                staged.capacity() * std::mem::size_of::<crate::derived_cache::StagedChunk>(),
            )?;
            for bytes in snapshot.0.as_slice().chunks(MAX_CHUNK_BYTES) {
                self.control.check()?;
                let Some(chunk) = artifacts.stage_chunk(&build, bytes)? else {
                    return Ok(None);
                };
                staged.push(chunk);
            }
            self.source.check()?;
            ProducerVersion::verified()?;
            artifacts.replace_checkpoint(&build, self.version.as_ref(), &staged)
        })?;
        self.source.check()?;
        match publication {
            Some(Some(version)) => {
                self.version = Some(version);
                self.saved_revision = snapshot.1;
                super::observe(
                    &self.observer,
                    super::Phase::CheckpointPublished,
                    &self.control,
                )?;
            }
            // Failback once, not SQL on every subsequent complete edge. A future
            // derivation may try again under a fresh independent cache claim.
            _ => self.build = None,
        }
        Ok(())
    }
}
impl MappingBoundary for Checkpoints {
    fn completed(&mut self, statistics: MappingStatistics) -> std::result::Result<(), BlameError> {
        self.statistics = statistics;
        self.flush(false).map_err(|error| self.failure.stop(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn production_cadence_obeys_exact_count_or_clock_and_resets_without_sleeping() {
        let start = Instant::now();
        let mut cadence = Cadence::new(start);
        assert!(!cadence.poll(start + Duration::from_millis(4999), 255, false));
        assert!(cadence.poll(start + Duration::from_secs(5), 255, false));
        assert!(!cadence.poll(start + Duration::from_secs(5), 510, false));
        assert!(cadence.poll(start + Duration::from_secs(5), 511, false));
        assert!(!cadence.poll(start + Duration::from_millis(9999), 511, false));
        assert!(cadence.poll(start + Duration::from_secs(10), 511, false));
        assert!(cadence.poll(start + Duration::from_secs(10), 511, true));
    }

    #[test]
    fn callback_copies_share_job_capacity_and_remain_owned_through_cancellation() {
        let budget = CapacityBudget::new(8 * 1024 * 1024);
        let control = Control::new(Instant::now() + Duration::from_secs(5));
        let options = BlameOptions::default().mapping_options(true).unwrap();
        let work = Arc::new(Mutex::new(BatchSnapshot::new(budget.clone()).unwrap()));
        let base = budget.used();
        let mut memory = Memory {
            work: work.clone(),
            control: control.clone(),
            failure: Arc::new(CallbackFailure::default()),
            producer: options.producer,
            options: options.options,
        };
        let key = MappingKey {
            before: gix::ObjectId::from([1; 20]),
            after: gix::ObjectId::from([2; 20]),
            algorithm: Algorithm::Myers,
            tuning: 0,
            producer: options.producer,
            options: options.options,
        };
        // Opaque callback-store fixture, not a claim of canonical vendor mapping.
        let bytes = [9; 32768];
        memory.completed(&key, &bytes).unwrap();
        assert!(budget.used() >= base + bytes.len());
        let mut into = budget.buffer(bytes.len()).unwrap();
        into.resize(bytes.len(), 0).unwrap();
        assert_eq!(
            memory.load(&key, into.as_mut_slice()).unwrap(),
            Some(bytes.len())
        );
        assert_eq!(into.as_slice(), bytes);
        let retained = budget.used();
        control.cancel();
        assert!(matches!(
            memory.load(&key, into.as_mut_slice()),
            Err(BlameError::Stopped(_))
        ));
        assert_eq!(
            budget.used(),
            retained,
            "live callback copies cannot be refunded on stop"
        );
        drop(memory);
        drop(work);
        assert_eq!(budget.used(), into.capacity());
        drop(into);
        assert_eq!(budget.used(), 0);
    }
}
