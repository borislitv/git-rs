//! All-catalog compaction, not reachability collection. Build on a committed
//! reader hold, release that hold, then drain readers with the existing barrier.

use super::{
    Outcome,
    queue::{self, Job},
};
use crate::{
    Result,
    blob::{Store, UploadPlan},
    cache::Cache,
    cpu_jobs::{CpuJob, CpuJobs},
    invalid,
    metadata::{self, Barrier, Pack, Snapshot, bytes, text},
    object_source::{RangeStore, Source},
    process,
};
use git_rs_core::{CapacityBudget, Control, ReadLimits, SnapshotSession};
use openssl::sha::Sha256;
use std::sync::Arc;

pub(super) struct Input {
    pub generation: u64,
    pub epoch: u64,
    pub manifest: String,
}

fn manifest(packs: &[Pack]) -> String {
    let mut hash = Sha256::new();
    hash.update(b"git-rs/catalog/v1\0");
    hash.update(&(packs.len() as u64).to_be_bytes());
    for pack in packs {
        for field in [
            &pack.hash,
            &pack.pack_key,
            &pack.idx_key,
            &pack.pack_digest,
            &pack.idx_digest,
        ] {
            hash.update(&(field.len() as u64).to_be_bytes());
            hash.update(field.as_bytes());
        }
        hash.update(&pack.pack_size.to_be_bytes());
        hash.update(&pack.idx_size.to_be_bytes());
    }
    crate::protocol::hex(&hash.finish())
}

impl Input {
    fn snapshot(snapshot: &Snapshot) -> Result<Self> {
        if !snapshot
            .packs
            .windows(2)
            .all(|pair| pair[0].hash < pair[1].hash)
        {
            return Err(invalid("maintenance catalog is not uniquely ordered"));
        }
        Ok(Self {
            generation: snapshot.generation,
            epoch: snapshot.storage_epoch,
            manifest: manifest(&snapshot.packs),
        })
    }

    pub fn current(job: &Job) -> Result<Self> {
        job.check()?;
        let row = metadata::sql(&format!(
            "BEGIN; DO $job$ BEGIN PERFORM maintenance_assert_job({}); END $job$; SELECT generation,storage_epoch,encode(maintenance_catalog_manifest(id),'hex') FROM repositories WHERE id={}::uuid; COMMIT;",
            job.args(),
            text(&job.repo)
        ))?;
        let fields = row.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
        let [generation, epoch, manifest] = fields.as_slice() else {
            return Err(invalid("invalid maintenance catalog framing"));
        };
        if manifest.len() != 64 || crate::protocol::unhex(manifest.as_bytes())?.len() != 32 {
            return Err(invalid("invalid maintenance catalog digest"));
        }
        job.check()?;
        Ok(Self {
            generation: generation.parse()?,
            epoch: epoch.parse()?,
            manifest: (*manifest).into(),
        })
    }

    pub fn args(&self) -> Result<String> {
        Ok(format!(
            "{},{},{}",
            self.generation,
            self.epoch,
            bytes(&crate::protocol::unhex(self.manifest.as_bytes())?)
        ))
    }

    /// Always WAL-producing and exact. It is safe after ownership expiry because
    /// it acknowledges a prior result without executing that operation again.
    pub fn confirm(&self, job: &Job, output: &str) -> Result<Option<(u64, u64)>> {
        process::Operation::new(5).enter(|| {
            let row = metadata::sql(&format!(
                "BEGIN; SET LOCAL synchronous_commit=on; SELECT * FROM maintenance_confirm({}::uuid,{}::uuid,{},{},{},{}); COMMIT;",
                text(&job.token), text(&job.id), job.generation, job.authority.fence,
                self.args()?, bytes(&crate::protocol::unhex(output.as_bytes())?)))?;
            result(&row)
        })
    }
}

fn result(row: &str) -> Result<Option<(u64, u64)>> {
    if row.trim().is_empty() {
        return Ok(None);
    }
    let fields = row.trim_end_matches('\n').split('\t').collect::<Vec<_>>();
    let [generation, epoch] = fields.as_slice() else {
        return Err(invalid("invalid maintenance result framing"));
    };
    Ok(Some((generation.parse()?, epoch.parse()?)))
}

struct Build {
    source: Arc<Source>,
    store: Store,
    cache: Arc<Cache>,
    plan: UploadPlan,
    input: Input,
}

struct Built {
    pack: Pack,
    plan: UploadPlan,
    input: Input,
    // Drop the provider and this LAST hold before waiting for exclusive access.
    source: Option<Arc<Source>>,
}

impl CpuJob for Build {
    type Output = Built;
    fn run(self, control: &Control, budget: &CapacityBudget) -> Result<Built> {
        let identity = self.source.identity();
        let limits = ReadLimits {
            max_graph_objects: 1_000_000,
            max_graph_edges: 10_000_000,
            ..ReadLimits::default()
        };
        let provider = RangeStore::open(
            self.source.clone(),
            self.store,
            self.cache,
            budget.clone(),
            limits,
            control,
        )?;
        let snapshot =
            SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), limits)?;
        let all_objects = snapshot.catalog_objects()?;
        let pack = self.plan.write(&snapshot, &all_objects, control, budget)?;
        control.check()?;
        Ok(Built {
            pack,
            plan: self.plan,
            input: self.input,
            source: Some(self.source),
        })
    }
}

pub(super) fn run(
    job: Arc<Job>,
    cpu: &CpuJobs,
    store: &Store,
    cache: &Arc<Cache>,
    operation: Arc<process::Operation>,
) -> Result<Outcome> {
    job.check()?;
    store.require_unversioned()?;
    job.check()?;
    let expected: u64 = match std::str::from_utf8(&job.payload)
        .ok()
        .and_then(|value| value.parse().ok())
    {
        Some(value) => value,
        None => return Ok(Outcome::Dead("invalid_compaction_generation")),
    };
    let admitted = cpu
        .try_admit(operation.clone())?
        .ok_or_else(|| invalid("maintenance CPU admission busy"))?;
    let source = Source::maintenance(job.clone(), operation, admitted.budget())?;
    if source.snapshot.generation != expected {
        queue::refresh(&job, source.snapshot.generation)?;
        return Ok(Outcome::Complete);
    }
    let input = Input::snapshot(&source.snapshot)?;
    let plan = UploadPlan::begin_for_job(store.clone(), &job)?;
    let mut built = admitted
        .run(Build {
            source,
            store: store.clone(),
            cache: cache.clone(),
            plan,
            input,
        })?
        .ok_or_else(|| invalid("compaction stopped before completion"))?;
    built
        .source
        .as_ref()
        .ok_or_else(|| invalid("compaction source hold missing"))?
        .check()?;
    built.plan.uploaded()?;
    job.check()?;
    drop(built.value.source.take());
    // Actual CPU/output admission remains held by `built` through publication
    // and any ambiguous-COMMIT confirmation; only the reader hold is released.
    let barrier = Barrier::acquire(&job.repo, true)?;
    job.check()?;
    let current = Input::current(&job)?;
    if current.generation != built.input.generation
        || current.epoch != built.input.epoch
        || current.manifest != built.input.manifest
    {
        queue::refresh(&job, current.generation)?;
        return Ok(Outcome::Complete);
    }
    let pack = &built.pack;
    let output = manifest(std::slice::from_ref(pack));
    let query = format!(
        "BEGIN; SET LOCAL synchronous_commit=on; SELECT * FROM maintenance_swap({},{}::uuid,{},{},{},{},{},{},{},{},{}); COMMIT;",
        job.args(),
        text(&barrier.id),
        built.input.args()?,
        bytes(&crate::protocol::unhex(output.as_bytes())?),
        text(&pack.hash),
        text(&pack.pack_key),
        text(&pack.idx_key),
        text(&pack.pack_digest),
        text(&pack.idx_digest),
        pack.pack_size,
        pack.idx_size
    );
    let published = match metadata::sql(&query) {
        Ok(row) => result(&row)?,
        Err(_) => built.input.confirm(&job, &output)?,
    };
    if published != Some((built.input.generation + 1, built.input.epoch + 1)) {
        return Err(invalid(
            "compaction publication rejected or outcome unknown",
        ));
    }
    Ok(Outcome::Committed)
}
