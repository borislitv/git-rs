use crate::protocol::{Oid, Update, hex, unhex};
use crate::{
    Result, auth, database, invalid,
    metrics::{self, Dependency, Metric},
    process, scratch,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct Pack {
    pub hash: String,
    pub pack_key: String,
    pub idx_key: String,
    pub pack_digest: String,
    pub idx_digest: String,
    pub pack_size: u64,
    pub idx_size: u64,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub id: String,
    pub generation: u64,
    pub storage_epoch: u64,
    pub default_ref: Vec<u8>,
    pub refs: BTreeMap<Vec<u8>, Oid>,
    pub packs: Vec<Pack>,
}

// No SQL value is interpolated as SQL syntax. Even non-UTF-8 ref names are
// encoded as bytea literals. tokio-postgres owns protocol and authentication;
// native-tls verifies the server chain and hostname.
pub fn bytes(value: &[u8]) -> String {
    format!("decode('{}','hex')", hex(value))
}
pub fn text(value: &str) -> String {
    format!("convert_from({},'UTF8')", bytes(value.as_bytes()))
}

pub fn sql(query: &str) -> Result<String> {
    metrics::measure(Metric::Dependency(Dependency::Sql), || {
        let mut output = String::new();
        database::rows(query, |row| {
            if output.len().saturating_add(row.len()).saturating_add(1) > 16 * 1024 * 1024 {
                return Err(invalid("SQL output limit exceeded"));
            }
            output.push_str(row);
            output.push('\n');
            Ok(())
        })?;
        Ok(output)
    })
}

// Keep one connection/transaction for snapshot consistency, but decode each row
// as it arrives. Hex-encoded legal refs alone can exceed sql()'s 16 MiB capture
// limit; pack catalogs also grow independently of the number of refs. The only
// retained allocation is the decoded Snapshot, not a second full wire image.
fn snapshot_rows(queries: &[&str], row: impl FnMut(&str) -> Result<()>) -> Result<()> {
    metrics::measure(Metric::Dependency(Dependency::Sql), || {
        database::session_rows(queries, row)
    })
}

/// A committed lease survives connection loss. Operations have at most 120 s;
/// the database grants 150 s, including subprocess termination margin. A crash
/// delays maintenance until expiry rather than releasing a live reader's hold.
pub struct Barrier {
    pub(crate) id: String,
    repo: String,
}

impl Barrier {
    pub fn acquire(id: &str, exclusive: bool) -> Result<Self> {
        let remaining = process::remaining()?;
        if remaining > Duration::from_secs(process::DEADLINE) {
            return Err(invalid("operation exceeds repository lease budget"));
        }
        let lease = scratch::identifier()?;
        let deadline = Instant::now() + remaining.min(Duration::from_secs(5));
        loop {
            let result = sql(&format!(
                "BEGIN; DO $body$ BEGIN PERFORM 1 FROM repositories WHERE id={}::uuid FOR UPDATE; IF NOT FOUND THEN RAISE EXCEPTION 'repository unavailable'; END IF; DELETE FROM repository_leases WHERE repo_id={}::uuid AND expires_at <= clock_timestamp(); END $body$; INSERT INTO repository_leases SELECT {}::uuid,{}::uuid,{},clock_timestamp()+interval '150 seconds' WHERE NOT EXISTS(SELECT 1 FROM repository_leases WHERE repo_id={}::uuid AND (exclusive OR {})) RETURNING 1; COMMIT;",
                text(id),
                text(id),
                text(&lease),
                text(id),
                exclusive,
                text(id),
                exclusive
            ))?;
            if result.trim() == "1" {
                return Ok(Self {
                    id: lease,
                    repo: id.into(),
                });
            }
            if Instant::now() >= deadline {
                return Err(invalid(
                    "repository busy; retry after active requests or crashed leases expire",
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn check(&self) -> Result<()> {
        if sql(&format!("SELECT 1 FROM repository_leases WHERE id={}::uuid AND repo_id={}::uuid AND expires_at > clock_timestamp();",text(&self.id),text(&self.repo)))?.trim() != "1" {
            return Err(invalid("repository lease expired"));
        }
        Ok(())
    }
}

impl Drop for Barrier {
    fn drop(&mut self) {
        // Releasing a hold must still work after request cancellation. This is a
        // separate, bounded metadata-only cleanup; failure leaves a safe expiry.
        let released = process::Operation::new(5).enter(|| {
            sql(&format!(
                "DELETE FROM repository_leases WHERE id={}::uuid AND repo_id={}::uuid;",
                text(&self.id),
                text(&self.repo)
            ))
        });
        if released.is_err() {
            eprintln!("repository lease release failed; expiry will recover it");
        }
    }
}

pub fn migrate() -> Result<()> {
    sql(&format!(
        "BEGIN; SELECT pg_advisory_xact_lock(hashtextextended('git-rs-schema',0)); {} COMMIT;",
        include_str!("../migrations/001.sql")
    ))?;
    Ok(())
}

pub fn create(slug: &str, owner: &str) -> Result<()> {
    auth::repository(slug)?;
    auth::principal(owner)?;
    let id = scratch::identifier()?;
    sql(&format!(
        "BEGIN; INSERT INTO repositories(id,slug) VALUES({}::uuid,{}); INSERT INTO permissions(repo_id,principal,role) VALUES({}::uuid,{},'owner'); COMMIT;",
        text(&id),
        text(slug),
        text(&id),
        text(owner)
    ))?;
    Ok(())
}

pub fn grant(slug: &str, actor: &str, subject: &str, role: &str, prefix: &[u8]) -> Result<()> {
    auth::repository(slug)?;
    auth::principal(actor)?;
    auth::principal(subject)?;
    if !["reader", "writer", "maintainer", "owner", "none"].contains(&role)
        || !prefix.starts_with(b"refs/")
        || !prefix.ends_with(b"/")
    {
        return Err(invalid("invalid grant"));
    }
    sql(&format!(
        "BEGIN; DO $body$ DECLARE r repositories%ROWTYPE; BEGIN SELECT * INTO STRICT r FROM repositories WHERE slug={} FOR UPDATE; IF NOT EXISTS(SELECT 1 FROM permissions WHERE repo_id=r.id AND principal={} AND role='owner') THEN RAISE EXCEPTION 'owner required'; END IF; DELETE FROM permissions WHERE repo_id=r.id AND principal={}; IF {} <> 'none' THEN INSERT INTO permissions VALUES(r.id,{},{},{}); END IF; UPDATE repositories SET generation=generation+1 WHERE id=r.id; END $body$; COMMIT;",
        text(slug),
        text(actor),
        text(subject),
        text(role),
        text(subject),
        text(role),
        bytes(prefix)
    ))?;
    Ok(())
}

pub fn snapshot(slug: &str, principal: &str, write: bool) -> Result<Snapshot> {
    auth::repository(slug)?;
    auth::principal(principal)?;
    read_snapshot(
        &[&snapshot_query(slug, principal, write, None)],
        None,
        &mut None,
    )?
    .ok_or_else(|| invalid("repository unavailable"))
}

fn snapshot_query(slug: &str, principal: &str, write: bool, lease: Option<&str>) -> String {
    let guard = lease.map(|id| format!(" AND EXISTS(SELECT 1 FROM repository_leases WHERE id={}::uuid AND repo_id=r.id AND NOT exclusive AND expires_at > clock_timestamp())", text(id))).unwrap_or_default();
    let repo = format!(
        "(SELECT id FROM repositories r WHERE slug={}{guard})",
        text(slug)
    );
    let role = if write { "AND p.role <> 'reader'" } else { "" };
    format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT 'repo',r.id,r.generation,encode(r.default_ref,'hex'),r.storage_epoch FROM repositories r JOIN permissions p ON r.id=p.repo_id WHERE r.slug={} AND p.principal={} {role}{guard}; SELECT 'ref',encode(name,'hex'),encode(oid,'hex') FROM refs WHERE repo_id={repo} ORDER BY name; SELECT 'pack',native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size FROM packs WHERE repo_id={repo} ORDER BY native_hash; COMMIT;",
        text(slug),
        text(principal)
    )
}

fn read_snapshot(
    queries: &[&str],
    lease_id: Option<&str>,
    lease: &mut Option<Barrier>,
) -> Result<Option<Snapshot>> {
    // Charge encoded row bytes plus conservative per-entry allocation overhead
    // (owned strings, Vec growth, BTree nodes). Reserve fixed header/root space.
    // This is a snapshot allocation budget, not a bound on the whole process RSS.
    const MAX_METADATA: usize = 128 * 1024 * 1024;
    let limit = match std::env::var("GIT_RS_METADATA_BYTES") {
        Ok(value) => value.parse::<usize>()?,
        Err(std::env::VarError::NotPresent) => MAX_METADATA,
        Err(error) => return Err(error.into()),
    };
    if !(4096..=MAX_METADATA).contains(&limit) {
        return Err(invalid(
            "metadata snapshot budget must be 4096..134217728 bytes",
        ));
    }
    let mut allocated = 4096usize;
    let mut snapshot = None;
    snapshot_rows(queries, |line| {
        if let Some(repo) = line.strip_prefix("lease\t") {
            let id = lease_id.ok_or_else(|| invalid("unexpected repository lease"))?;
            if lease.is_some() || snapshot.is_some() || repo.contains('\t') {
                return Err(invalid("invalid repository lease framing"));
            }
            *lease = Some(Barrier {
                id: id.into(),
                repo: repo.into(),
            });
            return Ok(());
        }
        if lease_id.is_some() && lease.is_none() {
            return Err(invalid("snapshot requires a repository lease"));
        }
        if line.starts_with("ref\t") || line.starts_with("pack\t") {
            allocated = allocated.saturating_add(line.len()).saturating_add(512);
            if allocated > limit {
                return Err(invalid("metadata snapshot budget exceeded"));
            }
        }
        let fields: Vec<_> = line.split('\t').collect();
        let Some(snapshot) = snapshot.as_mut() else {
            if let ["repo", id, generation, default_ref, storage_epoch] = fields.as_slice() {
                snapshot = Some(Snapshot {
                    id: (*id).into(),
                    generation: generation.parse()?,
                    default_ref: unhex(default_ref.as_bytes())?,
                    storage_epoch: storage_epoch.parse()?,
                    refs: BTreeMap::new(),
                    packs: Vec::new(),
                });
                return Ok(());
            }
            return Err(invalid("repository unavailable"));
        };
        match fields.as_slice() {
            ["ref", name, oid] => {
                snapshot
                    .refs
                    .insert(unhex(name.as_bytes())?, Oid::parse(oid.as_bytes())?);
            }
            [
                "pack",
                hash,
                pack_key,
                idx_key,
                pack_digest,
                idx_digest,
                pack_size,
                idx_size,
            ] => snapshot.packs.push(Pack {
                hash: (*hash).into(),
                pack_key: (*pack_key).into(),
                idx_key: (*idx_key).into(),
                pack_digest: (*pack_digest).into(),
                idx_digest: (*idx_digest).into(),
                pack_size: pack_size.parse()?,
                idx_size: idx_size.parse()?,
            }),
            _ => return Err(invalid("invalid metadata snapshot")),
        }
        Ok(())
    })?;
    Ok(snapshot)
}

/// Commit an authorized hold before reading one consistent catalog. Reuse one
/// native client connection without retaining the repository row lock during a
/// large snapshot. A streamed hold is cleaned up if the later read/COMMIT fails.
pub fn leased_snapshot(slug: &str, principal: &str, write: bool) -> Result<(Snapshot, Barrier)> {
    auth::repository(slug)?;
    auth::principal(principal)?;
    let remaining = process::remaining()?;
    if remaining > Duration::from_secs(process::DEADLINE) {
        return Err(invalid("operation exceeds repository lease budget"));
    }
    let deadline = Instant::now() + remaining.min(Duration::from_secs(5));
    let id = scratch::identifier()?;
    let role = if write { "AND p.role <> 'reader'" } else { "" };
    let query = format!(
        "BEGIN; DO $body$ DECLARE repo uuid; BEGIN SELECT r.id INTO STRICT repo FROM repositories r JOIN permissions p ON r.id=p.repo_id WHERE r.slug={} AND p.principal={} {role} FOR UPDATE OF r; DELETE FROM repository_leases WHERE repo_id=repo AND expires_at <= clock_timestamp(); END $body$; INSERT INTO repository_leases SELECT {}::uuid,r.id,false,clock_timestamp()+interval '150 seconds' FROM repositories r WHERE r.slug={} AND NOT EXISTS(SELECT 1 FROM repository_leases WHERE repo_id=r.id AND exclusive); SELECT 'lease',repo_id FROM repository_leases WHERE id={}::uuid; COMMIT;",
        text(slug),
        text(principal),
        text(&id),
        text(slug),
        text(&id)
    );
    // A separate protocol message confirms/flushes the lease before a later
    // catalog read can block. Both messages use the same checked-out connection.
    let catalog = snapshot_query(slug, principal, write, Some(&id));
    loop {
        let mut lease = None;
        let snapshot = read_snapshot(&[&query, &catalog], Some(&id), &mut lease)?;
        if let Some(lease) = lease {
            let snapshot = snapshot.ok_or_else(|| invalid("repository unavailable"))?;
            if snapshot.id != lease.repo {
                return Err(invalid("repository lease mismatch"));
            }
            return Ok((snapshot, lease));
        }
        if Instant::now() >= deadline {
            return Err(invalid(
                "repository busy; retry after active requests or crashed leases expire",
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[allow(clippy::too_many_arguments)]
pub fn publish(
    snapshot: &Snapshot,
    lease: &Barrier,
    operation: &str,
    principal: &str,
    commands: &[Update],
    fast_forward: &[bool],
    packs: &[Pack],
) -> Result<u64> {
    let manifest: Vec<u8> = commands
        .iter()
        .flat_map(|c| {
            let mut l = c.line();
            l.push(b'\n');
            l
        })
        .collect();
    let array = |values: Vec<String>, kind: &str| format!("ARRAY[{}]::{kind}[]", values.join(","));
    let call = format!(
        "SELECT publish({}::uuid,{}::uuid,{},{},{},{}::uuid,{},{},{},{});",
        text(&snapshot.id),
        text(operation),
        text(principal),
        bytes(&manifest),
        snapshot.storage_epoch,
        text(&lease.id),
        array(commands.iter().map(|c| bytes(&c.name)).collect(), "bytea"),
        array(commands.iter().map(|c| bytes(&c.old.0)).collect(), "bytea"),
        array(commands.iter().map(|c| bytes(&c.new.0)).collect(), "bytea"),
        array(
            fast_forward.iter().map(|b| b.to_string()).collect(),
            "boolean"
        )
    );
    let mut transaction = format!("BEGIN; SET LOCAL synchronous_commit=on; {call}");
    for pack in packs {
        transaction.push_str(&format!(
            "INSERT INTO packs VALUES({}::uuid,{}::uuid,{},{},{},{},{},{},{},(SELECT generation FROM repositories WHERE id={}::uuid)) ON CONFLICT(repo_id,native_hash) DO NOTHING;",
            text(&snapshot.id), text(operation), text(&pack.hash), text(&pack.pack_key), text(&pack.idx_key), text(&pack.pack_digest), text(&pack.idx_digest), pack.pack_size, pack.idx_size, text(&snapshot.id)));
    }
    transaction.push_str(&format!("DELETE FROM staged_objects s USING packs p WHERE s.repo_id=p.repo_id AND s.repo_id={}::uuid AND (s.object_key=p.pack_key OR s.object_key=p.idx_key); COMMIT;",text(&snapshot.id)));
    match sql(&transaction) {
        Ok(result) => Ok(result.trim().parse()?),
        Err(_) => {
            // A failed COMMIT transport has an unknown outcome. The same operation
            // and exact manifest can be confirmed by a fresh WAL-producing tx.
            let found = sql(&format!(
                "SELECT 1 FROM operations WHERE id={}::uuid AND repo_id={}::uuid AND principal={} AND commands={};",
                text(operation),
                text(&snapshot.id),
                text(principal),
                bytes(&manifest)
            ))?;
            if found.trim() != "1" {
                return Err(invalid("publication rejected or outcome unknown"));
            }
            let confirmed = sql(&format!(
                "BEGIN; SET LOCAL synchronous_commit=on; {call} COMMIT;"
            ))?;
            Ok(confirmed.trim().parse()?)
        }
    }
}
