use crate::protocol::{Oid, Update, hex, unhex};
use crate::{
    Result, auth, invalid,
    metrics::{self, Dependency, Metric},
    process, scratch,
};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
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
// encoded as bytea literals. psql/libpq owns protocol, TLS and authentication.
pub fn bytes(value: &[u8]) -> String {
    format!("decode('{}','hex')", hex(value))
}
pub fn text(value: &str) -> String {
    format!("convert_from({},'UTF8')", bytes(value.as_bytes()))
}

fn psql() -> Result<Command> {
    let local_dev = std::env::var("GIT_RS_DEV_HTTP").as_deref() == Ok("1")
        && matches!(
            std::env::var("PGHOST").as_deref(),
            Ok("127.0.0.1" | "localhost")
        );
    if std::env::var("PGSSLMODE").as_deref() != Ok("verify-full") && !local_dev {
        return Err(invalid(
            "PostgreSQL requires PGSSLMODE=verify-full outside loopback development",
        ));
    }
    let mut command = process::command("/usr/bin/psql");
    command.args(["-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-F", "\t"]);
    for (name, value) in std::env::vars_os() {
        if name.to_str().is_some_and(|s| s.starts_with("PG")) {
            command.env(name, value);
        }
    }
    command.env("PGCONNECT_TIMEOUT", "5").env(
        "PGOPTIONS",
        "-c statement_timeout=30000 -c lock_timeout=5000 -c search_path=git_rs,pg_catalog",
    );
    Ok(command)
}

pub fn sql(query: &str) -> Result<String> {
    metrics::measure(Metric::Dependency(Dependency::Sql), || {
        let output = process::capture(&mut psql()?, query.as_bytes(), 16 * 1024 * 1024)?;
        Ok(String::from_utf8(output)?)
    })
}

// Keep one connection/transaction for snapshot consistency, but decode each row
// as it arrives. Hex-encoded legal refs alone can exceed sql()'s 16 MiB capture
// limit; pack catalogs also grow independently of the number of refs. The only
// retained allocation is the decoded Snapshot, not a second full wire image.
fn snapshot_rows(query: &str, mut row: impl FnMut(&str) -> Result<()>) -> Result<()> {
    metrics::measure(Metric::Dependency(Dependency::Sql), || {
        snapshot_rows_inner(query, &mut row)
    })
}

fn snapshot_rows_inner(query: &str, mut row: impl FnMut(&str) -> Result<()>) -> Result<()> {
    process::check_operation()?;
    let mut child = process::Guard::spawn(
        psql()?
            // psql otherwise buffers an entire PGresult before writing stdout.
            .args(["-v", "FETCH_COUNT=256"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null()),
    )?;
    let mut stdin = child
        .0
        .stdin
        .take()
        .ok_or_else(|| invalid("missing SQL input"))?;
    let stdout = child
        .0
        .stdout
        .take()
        .ok_or_else(|| invalid("missing SQL output"))?;
    std::thread::scope(|scope| {
        let sender = scope.spawn(move || stdin.write_all(query.as_bytes()));
        let result = (|| -> Result<()> {
            let mut reader = BufReader::new(stdout);
            let mut line = Vec::new();
            loop {
                process::check_operation()?;
                line.clear();
                // Generated keys and hex-encoded 1024-byte names fit comfortably.
                // Bound an individual row even if database contents are corrupt.
                let count = reader.by_ref().take(8193).read_until(b'\n', &mut line)?;
                if count == 0 {
                    return Ok(());
                }
                if count > 8192 || line.last() != Some(&b'\n') {
                    return Err(invalid("invalid metadata row length or framing"));
                }
                row(std::str::from_utf8(&line[..line.len() - 1])?)?;
            }
        })();
        if result.is_err() {
            child.terminate();
        }
        let sent = sender.join().map_err(|_| invalid("SQL writer panicked"))?;
        result?;
        sent?;
        // Never expose a partial snapshot after a late SELECT/COMMIT failure.
        if !child.wait()?.success() {
            return Err(invalid("metadata snapshot transaction failed"));
        }
        Ok(())
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
        &snapshot_query(slug, principal, write, None),
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
    query: &str,
    lease_id: Option<&str>,
    lease: &mut Option<Barrier>,
) -> Result<Option<Snapshot>> {
    let mut snapshot = None;
    snapshot_rows(query, |line| {
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
        "BEGIN; DO $body$ DECLARE repo uuid; BEGIN SELECT r.id INTO STRICT repo FROM repositories r JOIN permissions p ON r.id=p.repo_id WHERE r.slug={} AND p.principal={} {role} FOR UPDATE OF r; DELETE FROM repository_leases WHERE repo_id=repo AND expires_at <= clock_timestamp(); END $body$; INSERT INTO repository_leases SELECT {}::uuid,r.id,false,clock_timestamp()+interval '150 seconds' FROM repositories r WHERE r.slug={} AND NOT EXISTS(SELECT 1 FROM repository_leases WHERE repo_id=r.id AND exclusive); SELECT 'lease',repo_id FROM repository_leases WHERE id={}::uuid; COMMIT; {}",
        text(slug),
        text(principal),
        text(&id),
        text(slug),
        text(&id),
        snapshot_query(slug, principal, write, Some(&id))
    );
    loop {
        let mut lease = None;
        let snapshot = read_snapshot(&query, Some(&id), &mut lease)?;
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
