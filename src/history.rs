//! Explicit administration over authorized, range-backed Rust snapshots.

use crate::{
    Result,
    blob::{Store, UploadPlan},
    cache::Cache,
    cpu_jobs::{Completed, CpuAdmission, CpuJob},
    engine::Engine,
    invalid,
    metadata::{self, Barrier, Pack, Snapshot, bytes, text},
    object_source::{RangeStore, Source},
    process,
    protocol::Oid,
};
use git_rs_core::{CapacityBudget, CapacityReservation, Control, ReadLimits, SnapshotSession};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Seek, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};

fn owner(snapshot: &Snapshot, principal: &str) -> Result<()> {
    let permission = metadata::sql(&format!(
        "SELECT 1 FROM permissions WHERE repo_id={}::uuid AND principal={} AND role='owner';",
        text(&snapshot.id),
        text(principal)
    ))?;
    if permission.trim() != "1" {
        return Err(invalid("owner permission required"));
    }
    Ok(())
}

struct PrivateBundle {
    file: File,
    temporary: PathBuf,
    destination: PathBuf,
    parent: PathBuf,
}
impl PrivateBundle {
    fn create(destination: &Path) -> Result<Self> {
        let parent = destination
            .parent()
            .ok_or_else(|| invalid("backup parent required"))?;
        if !destination.is_absolute() || fs::metadata(parent)?.permissions().mode() & 0o077 != 0 {
            return Err(invalid(
                "backup destination must be absolute in a private directory (0700)",
            ));
        }
        if fs::symlink_metadata(destination).is_ok() {
            return Err(invalid("backup destination already exists"));
        }
        let temporary = parent.join(format!(".bundle-{}", crate::scratch::identifier()?));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        Ok(Self {
            file,
            temporary,
            destination: destination.into(),
            parent: parent.into(),
        })
    }
    fn publish(&self) -> Result<()> {
        process::check_operation()?;
        self.file.sync_all()?;
        fs::hard_link(&self.temporary, &self.destination)?;
        File::open(&self.parent)?.sync_all()?;
        process::check_operation()
    }
}
impl Drop for PrivateBundle {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.temporary)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("backup temporary cleanup failed: {error}");
        }
    }
}

struct Backup {
    file: PrivateBundle,
    store: Store,
    cache: Arc<Cache>,
    source: Arc<Source>,
}
struct BackupReady {
    _file: PrivateBundle,
    source: Arc<Source>,
}
impl CpuJob for Backup {
    type Output = BackupReady;
    fn run(mut self, control: &Control, budget: &CapacityBudget) -> Result<Self::Output> {
        let snapshot = open(self.source.clone(), self.store, self.cache, control, budget)?;
        let manifest = snapshot.export_bundle(
            self.source
                .snapshot
                .refs
                .iter()
                .map(|(name, id)| (name.as_slice(), gix::ObjectId::from(id.0))),
            &self.source.snapshot.default_ref,
            &mut self.file.file,
        )?;
        self.file.file.flush()?;
        self.file.file.rewind()?;
        manifest.verify(&mut self.file.file)?;
        control.check()?;
        self.source.check()?;
        self.file.publish()?;
        Ok(BackupReady {
            _file: self.file,
            source: self.source,
        })
    }
}

fn operation() -> Result<Arc<process::Operation>> {
    process::Operation::current()
        .ok_or_else(|| invalid("administration requires a bounded operation"))
}

fn admit<'a>(engine: &'a Engine, cpu: &'a crate::cpu_jobs::CpuJobs) -> Result<CpuAdmission<'a>> {
    let _ = engine;
    cpu.try_admit(operation()?)?
        .ok_or_else(|| invalid("administration CPU admission busy"))
}

fn open(
    source: Arc<Source>,
    store: Store,
    cache: Arc<Cache>,
    control: &Control,
    budget: &CapacityBudget,
) -> Result<SnapshotSession<git_rs_core::pack::ranges::RangePackSet<RangeStore>>> {
    let identity = source.identity();
    let limits = ReadLimits {
        max_graph_objects: 1_000_000,
        max_graph_edges: 10_000_000,
        ..ReadLimits::default()
    };
    let provider = RangeStore::open(source, store, cache, budget.clone(), limits, control)?;
    SnapshotSession::open(provider, &identity, control.clone(), budget.clone(), limits)
}

struct ListHistory {
    source: Arc<Source>,
    store: Store,
    cache: Arc<Cache>,
    tip: gix::ObjectId,
    max: usize,
}

struct HistoryReady {
    history: git_rs_core::history::History,
    source: Arc<Source>,
}

impl CpuJob for ListHistory {
    type Output = HistoryReady;

    fn run(self, control: &Control, budget: &CapacityBudget) -> Result<Self::Output> {
        let snapshot = open(self.source.clone(), self.store, self.cache, control, budget)?;
        let history = snapshot.history(self.tip, self.max)?;
        control.check()?;
        Ok(HistoryReady {
            history,
            source: self.source,
        })
    }
}

/// An ordered, bounded result that keeps actual CPU admission, backing capacity
/// and the authorized source hold until the consumer has finished with it.
pub struct CommitHistory {
    completed: Completed<HistoryReady>,
}

impl CommitHistory {
    pub fn iter(&self) -> impl Iterator<Item = gix::ObjectId> + '_ {
        self.completed.history.iter()
    }

    pub fn len(&self) -> usize {
        self.completed.history.len()
    }
    pub fn is_empty(&self) -> bool {
        self.completed.history.is_empty()
    }
    pub fn check(&self) -> Result<()> {
        self.completed.source.check()
    }
}

/// List an authorized branch in maintained topological order. The default branch
/// is resolved from the same leased catalog as the physical commit inventory.
pub fn list(
    engine: &Engine,
    slug: &str,
    principal: &str,
    branch: Option<&[u8]>,
    max: usize,
) -> Result<CommitHistory> {
    if !(1..=1_000_000).contains(&max) {
        return Err(invalid("history count must be in 1..=1000000"));
    }
    let cpu = engine.blame.cpu();
    let admitted = admit(engine, &cpu)?;
    let source = Source::acquire(slug, principal, false, operation()?, admitted.budget())?;
    let branch = branch.unwrap_or(&source.snapshot.default_ref);
    if !branch.starts_with(b"refs/heads/") {
        return Err(invalid("history requires a full branch ref"));
    }
    let tip = source
        .snapshot
        .refs
        .get(branch)
        .ok_or_else(|| invalid("history branch is unavailable"))?;
    let tip = gix::ObjectId::from(tip.0);
    source.check()?;
    let completed = admitted
        .run(ListHistory {
            source,
            store: engine.store.clone(),
            cache: engine.cache.clone(),
            tip,
            max,
        })?
        .ok_or_else(|| invalid("history interrupted before completion"))?;
    completed.source.check()?;
    Ok(CommitHistory { completed })
}

pub fn backup(engine: &Engine, slug: &str, principal: &str, destination: &Path) -> Result<()> {
    let cpu = engine.blame.cpu();
    let admitted = admit(engine, &cpu)?;
    let source = Source::acquire_snapshot(slug, principal, operation()?, admitted.budget())?;
    source.check()?;
    let job = Backup {
        file: PrivateBundle::create(destination)?,
        source,
        store: engine.store.clone(),
        cache: engine.cache.clone(),
    };
    let completed = admitted
        .run(job)?
        .ok_or_else(|| invalid("backup interrupted before completion"))?;
    completed.source.check()
}

struct Truncate {
    file: PrivateBundle,
    store: Store,
    cache: Arc<Cache>,
    source: Arc<Source>,
    keep: usize,
    plan: UploadPlan,
}
struct TruncateReady {
    pack: Pack,
    refs: BTreeMap<Vec<u8>, Oid>,
    _ref_capacity: CapacityReservation,
    plan: UploadPlan,
    source: Arc<Source>,
}
impl CpuJob for Truncate {
    type Output = TruncateReady;
    fn run(mut self, control: &Control, budget: &CapacityBudget) -> Result<Self::Output> {
        let snapshot = open(self.source.clone(), self.store, self.cache, control, budget)?;
        let manifest = snapshot.export_bundle(
            self.source
                .snapshot
                .refs
                .iter()
                .map(|(name, id)| (name.as_slice(), gix::ObjectId::from(id.0))),
            &self.source.snapshot.default_ref,
            &mut self.file.file,
        )?;
        self.file.file.flush()?;
        self.file.file.rewind()?;
        manifest.verify(&mut self.file.file)?;
        control.check()?;
        self.source.check()?;
        self.file.publish()?;
        drop(manifest);
        let rewritten = snapshot.rewrite_history(
            self.source
                .snapshot
                .refs
                .iter()
                .filter(|(name, _)| name.starts_with(b"refs/heads/"))
                .map(|(name, id)| (name.as_slice(), gix::ObjectId::from(id.0))),
            self.keep,
            self.plan.operation.as_bytes(),
        )?;
        let selection =
            rewritten
                .snapshot()
                .select_objects(rewritten.refs().map(|(_, id)| id), None, &[])?;
        let pack = self
            .plan
            .write(rewritten.snapshot(), &selection, control, budget)?;
        // This is a second owned projection, not reuse of the core ref buffers.
        // Reserve every supported name plus conservative Vec/BTree bookkeeping
        // before allocating it; the source snapshot retains its own reservation.
        let _ref_capacity =
            budget.reservation(git_rs_core::MAX_REFS * (git_rs_core::MAX_REF_BYTES + 512))?;
        let mut refs = BTreeMap::new();
        for (name, id) in rewritten.refs() {
            control.check()?;
            refs.insert(
                name.to_vec(),
                Oid(id
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid("truncate requires SHA-1"))?),
            );
        }
        Ok(TruncateReady {
            pack,
            refs,
            _ref_capacity,
            plan: self.plan,
            source: self.source,
        })
    }
}

pub fn truncate(
    engine: &Engine,
    slug: &str,
    principal: &str,
    keep: usize,
    destination: &Path,
    drop_other_refs: bool,
) -> Result<()> {
    if !(1..=10_000).contains(&keep) {
        return Err(invalid("keep must be in 1..=10000"));
    }
    let cpu = engine.blame.cpu();
    let admitted = admit(engine, &cpu)?;
    let metadata_capacity = admitted.budget().reservation(128 * 1024 * 1024)?;
    let initial = metadata::snapshot(slug, principal, true)?;
    owner(&initial, principal)?;
    let id = initial.id.clone();
    drop(initial);
    engine.store.require_unversioned()?;
    let barrier = Barrier::acquire(&id, true)?;
    let snapshot = metadata::snapshot(slug, principal, true)?;
    owner(&snapshot, principal)?;
    if !drop_other_refs
        && snapshot
            .refs
            .keys()
            .any(|name| !name.starts_with(b"refs/heads/"))
    {
        return Err(invalid(
            "tags/notes/other refs require explicit --drop-other-refs",
        ));
    }
    let source = Source::held(
        snapshot,
        barrier,
        principal,
        true,
        operation()?,
        metadata_capacity,
    )?;
    source.check()?;
    let plan = UploadPlan::begin(
        engine.store.clone(),
        &source.snapshot.id,
        &crate::scratch::identifier()?,
    )?;
    let job = Truncate {
        file: PrivateBundle::create(destination)?,
        source,
        store: engine.store.clone(),
        cache: engine.cache.clone(),
        keep,
        plan,
    };
    let prepared = admitted
        .run(job)?
        .ok_or_else(|| invalid("truncate interrupted before completion"))?;
    prepared.source.check()?;
    prepared.plan.uploaded()?;
    prepared.source.check()?;
    publish_truncation(
        &prepared.source.snapshot,
        prepared.source.lease(),
        principal,
        &prepared.plan.operation,
        &prepared.refs,
        &prepared.pack,
    )?;
    drop(prepared);
    let _queued = crate::maintenance::queue::request_cleanup(slug, principal)?;
    Ok(())
}

fn publish_truncation(
    snapshot: &Snapshot,
    lease: &Barrier,
    principal: &str,
    operation: &str,
    refs: &BTreeMap<Vec<u8>, Oid>,
    pack: &Pack,
) -> Result<()> {
    let id = text(&snapshot.id);
    let manifest: Vec<u8> = refs
        .iter()
        .flat_map(|(name, oid)| {
            let mut line = oid.hex().into_bytes();
            line.push(b' ');
            line.extend_from_slice(name);
            line.push(b'\n');
            line
        })
        .collect();
    let mut query = format!(
        "BEGIN; SET LOCAL synchronous_commit=on; DO $body$ BEGIN PERFORM 1 FROM repositories WHERE id={id}::uuid AND generation={} AND storage_epoch={} FOR UPDATE; IF NOT FOUND THEN RAISE EXCEPTION 'repository changed during truncation'; END IF; IF NOT EXISTS(SELECT 1 FROM permissions WHERE repo_id={id}::uuid AND principal={} AND role='owner') THEN RAISE EXCEPTION 'owner permission required'; END IF; IF NOT EXISTS(SELECT 1 FROM repository_leases WHERE repo_id={id}::uuid AND id={}::uuid AND exclusive AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'truncate lease expired'; END IF; IF EXISTS(SELECT 1 FROM retired_objects WHERE repo_id={id}::uuid AND object_key IN ({},{})) THEN RAISE EXCEPTION 'retired output key cannot be reused'; END IF; END $body$;",
        snapshot.generation,
        snapshot.storage_epoch,
        text(principal),
        text(&lease.id),
        text(&pack.pack_key),
        text(&pack.idx_key)
    );
    query.push_str(&format!("INSERT INTO retired_objects(repo_id,object_key,retired_epoch,retirement_operation,reason) SELECT repo_id,pack_key,{},{}::uuid,'owner-truncation' FROM packs WHERE repo_id={id}::uuid UNION ALL SELECT repo_id,idx_key,{},{}::uuid,'owner-truncation' FROM packs WHERE repo_id={id}::uuid ON CONFLICT DO NOTHING; DELETE FROM packs WHERE repo_id={id}::uuid; DELETE FROM refs WHERE repo_id={id}::uuid; DELETE FROM ref_updates WHERE operation_id IN (SELECT id FROM operations WHERE repo_id={id}::uuid); DELETE FROM operations WHERE repo_id={id}::uuid; UPDATE repositories SET generation=generation+1,storage_epoch=storage_epoch+1 WHERE id={id}::uuid;", snapshot.storage_epoch + 1,text(operation), snapshot.storage_epoch + 1,text(operation)));
    for (name, oid) in refs {
        query.push_str(&format!(
            "INSERT INTO refs(repo_id,name,oid) VALUES({id}::uuid,{},{});",
            bytes(name),
            bytes(&oid.0)
        ));
    }
    query.push_str(&format!("INSERT INTO packs(repo_id,operation_id,native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size,generation) VALUES({id}::uuid,{}::uuid,{},{},{},{},{},{},{},{}); INSERT INTO operations(id,repo_id,principal,commands,generation) VALUES({}::uuid,{id}::uuid,{},{},{}); DELETE FROM staged_objects WHERE repo_id={id}::uuid AND operation_id={}::uuid; DO $lease$ BEGIN IF NOT EXISTS(SELECT 1 FROM repository_leases WHERE repo_id={id}::uuid AND id={}::uuid AND exclusive AND expires_at>clock_timestamp()) THEN RAISE EXCEPTION 'truncate lease expired'; END IF; END $lease$; COMMIT;",
        text(operation), text(&pack.hash), text(&pack.pack_key), text(&pack.idx_key), text(&pack.pack_digest), text(&pack.idx_digest), pack.pack_size, pack.idx_size, snapshot.generation + 1,
        text(operation), text(principal), bytes(&manifest), snapshot.generation + 1, text(operation), text(&lease.id)));
    if metadata::sql(&query).is_err() {
        let confirmed = process::Operation::new(5).enter(|| metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; UPDATE operations o SET confirmations=confirmations+1 WHERE o.id={}::uuid AND o.repo_id={id}::uuid AND o.principal={} AND o.commands={} AND o.generation={} AND EXISTS(SELECT 1 FROM packs p WHERE p.repo_id=o.repo_id AND p.operation_id=o.id AND p.native_hash={} AND p.pack_key={} AND p.idx_key={} AND p.pack_digest={} AND p.idx_digest={} AND p.pack_size={} AND p.idx_size={}) RETURNING o.generation; COMMIT;",
            text(operation), text(principal), bytes(&manifest), snapshot.generation + 1,
            text(&pack.hash), text(&pack.pack_key), text(&pack.idx_key), text(&pack.pack_digest), text(&pack.idx_digest), pack.pack_size, pack.idx_size)))?;
        if confirmed.trim().parse::<u64>().ok() != Some(snapshot.generation + 1) {
            return Err(invalid(
                "truncation rejected or outcome unknown; inspect metadata before retrying",
            ));
        }
    }
    Ok(())
}

pub fn cleanup(slug: &str, principal: &str) -> Result<crate::maintenance::queue::Status> {
    crate::maintenance::queue::request_cleanup(slug, principal)
}
