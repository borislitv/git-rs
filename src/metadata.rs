use crate::protocol::{Oid, Update, hex, unhex};
use crate::{Result, auth, invalid, process, scratch};
use std::collections::BTreeMap;
use std::process::Command;
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
    let output = process::capture(&mut psql()?, query.as_bytes(), 16 * 1024 * 1024)?;
    Ok(String::from_utf8(output)?)
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
        "BEGIN; SELECT pg_advisory_xact_lock(hashtextextended('git-rs-schema',0)); DO $migration$ BEGIN IF to_regclass('git_rs.schema_version') IS NULL THEN EXECUTE $ddl${}$ddl$; ELSIF NOT (SELECT count(*)=1 AND min(version)=1 FROM git_rs.schema_version) THEN RAISE EXCEPTION 'unsupported metadata schema version'; END IF; END $migration$; COMMIT;",
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
    let repo = format!("(SELECT id FROM repositories WHERE slug={})", text(slug));
    let query = format!(
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT 'repo',r.id,r.generation,encode(r.default_ref,'hex'),r.storage_epoch FROM repositories r JOIN permissions p ON r.id=p.repo_id WHERE r.slug={} AND p.principal={} {}; SELECT 'ref',encode(name,'hex'),encode(oid,'hex') FROM refs WHERE repo_id={repo} ORDER BY name; SELECT 'pack',native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size FROM packs WHERE repo_id={repo} ORDER BY native_hash; COMMIT;",
        text(slug),
        text(principal),
        if write { "AND p.role <> 'reader'" } else { "" }
    );
    let output = sql(&query)?;
    let mut lines = output.lines();
    let header: Vec<_> = lines
        .next()
        .ok_or_else(|| invalid("repository unavailable"))?
        .split('\t')
        .collect();
    if header.len() != 5 || header[0] != "repo" {
        return Err(invalid("repository unavailable"));
    }
    let mut snapshot = Snapshot {
        id: header[1].into(),
        generation: header[2].parse()?,
        default_ref: unhex(header[3].as_bytes())?,
        storage_epoch: header[4].parse()?,
        refs: BTreeMap::new(),
        packs: Vec::new(),
    };
    for line in lines {
        let fields: Vec<_> = line.split('\t').collect();
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
    }
    Ok(snapshot)
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
