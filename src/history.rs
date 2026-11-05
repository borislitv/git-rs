use crate::{
    Result,
    engine::Engine,
    invalid,
    metadata::{self, Barrier, Pack, Snapshot, bytes, text},
    process,
    protocol::Oid,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

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

fn export(path: &Path, destination: &Path) -> Result<()> {
    // Never overwrite a backup. Build and verify privately, then link atomically
    // with create-if-absent semantics on the destination filesystem.
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("backup parent required"))?;
    if !destination.is_absolute() || fs::metadata(parent)?.permissions().mode() & 0o077 != 0 {
        return Err(invalid(
            "backup destination must be absolute in a private directory (0700)",
        ));
    }
    let temporary = parent.join(format!(".bundle-{}", crate::scratch::identifier()?));
    let result = (|| -> Result<()> {
        process::capture(
            process::git()
                .arg("-C")
                .arg(path)
                .args(["bundle", "create"])
                .arg(&temporary)
                .arg("--all"),
            b"",
            65536,
        )?;
        process::capture(
            process::git()
                .arg("-C")
                .arg(path)
                .args(["bundle", "verify"])
                .arg(&temporary),
            b"",
            65536,
        )?;
        fs::File::open(&temporary)?.sync_all()?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
        fs::hard_link(&temporary, destination)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = fs::remove_file(&temporary)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("backup temporary cleanup failed: {error}");
    }
    result
}

pub fn backup(engine: &Engine, slug: &str, principal: &str, destination: &Path) -> Result<()> {
    let initial = metadata::snapshot(slug, principal, false)?;
    let _barrier = Barrier::acquire(&initial.id, false)?;
    let snapshot = metadata::snapshot(slug, principal, false)?;
    let scratch = engine.materialize(&snapshot)?;
    export(&scratch.path, destination)
}

struct Commit {
    header: Vec<Vec<u8>>,
    parents: Vec<Oid>,
    message: Vec<u8>,
}

fn commit(path: &Path, oid: Oid) -> Result<Commit> {
    let raw = process::capture(
        process::git()
            .arg("-C")
            .arg(path)
            .args(["cat-file", "commit", &oid.hex()]),
        b"",
        1024 * 1024,
    )?;
    let split = raw
        .windows(2)
        .position(|w| w == b"\n\n")
        .ok_or_else(|| invalid("invalid commit headers"))?;
    let mut header = Vec::new();
    let mut parents = Vec::new();
    let mut skip_continuation = false;
    for line in raw[..split].split(|c| *c == b'\n') {
        if line.starts_with(b" ") {
            if !skip_continuation {
                header.push(line.to_vec());
            }
            continue;
        }
        skip_continuation = line.starts_with(b"gpgsig ")
            || line.starts_with(b"gpgsig-sha256 ")
            || line.starts_with(b"mergetag ");
        if skip_continuation {
            continue;
        }
        if let Some(parent) = line.strip_prefix(b"parent ") {
            parents.push(Oid::parse(parent)?);
        } else {
            header.push(line.to_vec());
        }
    }
    Ok(Commit {
        header,
        parents,
        message: raw[split + 2..].to_vec(),
    })
}

fn rewrite(path: &Path, tip: Oid, keep: usize) -> Result<Oid> {
    let list = process::capture(
        process::git().arg("-C").arg(path).args([
            "rev-list",
            "--topo-order",
            &format!("--max-count={keep}"),
            &tip.hex(),
            "--",
        ]),
        b"",
        (keep * (crate::protocol::OID_HEX + 1)) as u64,
    )?;
    let retained: BTreeSet<_> = list
        .split(|c| *c == b'\n')
        .filter(|l| !l.is_empty())
        .map(Oid::parse)
        .collect::<Result<_>>()?;
    let mut records = BTreeMap::new();
    let mut metadata_bytes = 0usize;
    for oid in &retained {
        let record = commit(path, *oid)?;
        metadata_bytes = metadata_bytes.saturating_add(
            record.message.len() + record.header.iter().map(Vec::len).sum::<usize>(),
        );
        if metadata_bytes > 64 * 1024 * 1024 {
            return Err(invalid("retained commit metadata exceeds 64 MiB budget"));
        }
        records.insert(*oid, record);
    }
    let mut rewritten = BTreeMap::new();
    let mut stack = vec![(tip, false)];
    while let Some((oid, ready)) = stack.pop() {
        if rewritten.contains_key(&oid) {
            continue;
        }
        let record = records
            .get(&oid)
            .ok_or_else(|| invalid("missing retained commit"))?;
        if !ready {
            stack.push((oid, true));
            for parent in &record.parents {
                if retained.contains(parent) && !rewritten.contains_key(parent) {
                    stack.push((*parent, false));
                }
            }
            continue;
        }
        let mut raw = Vec::new();
        for line in &record.header {
            raw.extend_from_slice(line);
            raw.push(b'\n');
            if line.starts_with(b"tree ") {
                for parent in &record.parents {
                    if let Some(new_parent) = rewritten.get(parent) {
                        writeln!(raw, "parent {}", Oid::hex(*new_parent))?;
                    }
                }
            }
        }
        raw.push(b'\n');
        raw.extend_from_slice(&record.message);
        let result = process::capture(
            process::git().arg("-C").arg(path).args([
                "hash-object",
                "-t",
                "commit",
                "-w",
                "--stdin",
            ]),
            &raw,
            4096,
        )?;
        rewritten.insert(
            oid,
            Oid::parse(result.strip_suffix(b"\n").unwrap_or(&result))?,
        );
    }
    rewritten
        .get(&tip)
        .copied()
        .ok_or_else(|| invalid("missing rewritten tip"))
}

pub fn truncate(
    engine: &Engine,
    slug: &str,
    principal: &str,
    keep: usize,
    destination: &Path,
    drop_other_refs: bool,
) -> Result<()> {
    if !(1..=10000).contains(&keep) {
        return Err(invalid("keep must be in 1..=10000"));
    }
    let initial = metadata::snapshot(slug, principal, true)?;
    owner(&initial, principal)?;
    engine.store.require_unversioned()?;
    let barrier = Barrier::acquire(&initial.id, true)?;
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
    let scratch = engine.materialize(&snapshot)?;
    export(&scratch.path, destination)?;
    let mut refs = BTreeMap::new();
    for (name, tip) in &snapshot.refs {
        if name.starts_with(b"refs/heads/") {
            refs.insert(name.clone(), rewrite(&scratch.path, *tip, keep)?);
        }
    }
    if refs.is_empty() {
        return Err(invalid("truncate requires a branch"));
    }
    let directory = scratch.path.join("rewrite");
    fs::create_dir(&directory)?;
    let tips: String = refs
        .values()
        .map(|oid| format!("{}\n", oid.hex()))
        .collect();
    let result = process::capture(
        process::git()
            .arg("-C")
            .arg(&scratch.path)
            .args([
                "pack-objects",
                "--revs",
                "--delta-base-offset",
                "--threads=1",
                "--window-memory=32m",
            ])
            .arg(directory.join("pack")),
        tips.as_bytes(),
        4096,
    )?;
    let hash = std::str::from_utf8(&result)?.trim();
    Oid::parse(hash.as_bytes())?;
    let operation = crate::scratch::identifier()?;
    let pack = engine
        .store
        .upload(&snapshot.id, &operation, &directory, hash)?;
    barrier.check()?;
    publish_truncation(&snapshot, principal, &operation, &refs, &pack)?;
    drop(barrier);
    // Reacquire after commit so interrupted cleanup can use the same path.
    cleanup(engine, slug, principal)?;
    Ok(())
}

fn publish_truncation(
    snapshot: &Snapshot,
    principal: &str,
    operation: &str,
    refs: &BTreeMap<Vec<u8>, Oid>,
    pack: &Pack,
) -> Result<()> {
    let id = text(&snapshot.id);
    let mut query = format!(
        "BEGIN; SET LOCAL synchronous_commit=on; DO $body$ BEGIN PERFORM 1 FROM repositories WHERE id={id}::uuid AND generation={} AND storage_epoch={} FOR UPDATE; IF NOT FOUND THEN RAISE EXCEPTION 'repository changed during truncation'; END IF; IF NOT EXISTS(SELECT 1 FROM permissions WHERE repo_id={id}::uuid AND principal={} AND role='owner') THEN RAISE EXCEPTION 'owner permission required'; END IF; END $body$;",
        snapshot.generation,
        snapshot.storage_epoch,
        text(principal)
    );
    query.push_str(&format!("INSERT INTO retired_objects SELECT repo_id,pack_key,{} FROM packs WHERE repo_id={id}::uuid UNION ALL SELECT repo_id,idx_key,{} FROM packs WHERE repo_id={id}::uuid; DELETE FROM packs WHERE repo_id={id}::uuid; DELETE FROM refs WHERE repo_id={id}::uuid; DELETE FROM ref_updates WHERE operation_id IN (SELECT id FROM operations WHERE repo_id={id}::uuid); DELETE FROM operations WHERE repo_id={id}::uuid; UPDATE repositories SET generation=generation+1,storage_epoch=storage_epoch+1 WHERE id={id}::uuid;", snapshot.storage_epoch+1, snapshot.storage_epoch+1));
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
    for (name, oid) in refs {
        query.push_str(&format!(
            "INSERT INTO refs VALUES({id}::uuid,{},{});",
            bytes(name),
            bytes(&oid.0)
        ));
    }
    query.push_str(&format!("INSERT INTO packs VALUES({id}::uuid,{}::uuid,{},{},{},{},{},{},{},{}); INSERT INTO operations(id,repo_id,principal,commands,generation) VALUES({}::uuid,{id}::uuid,{},{},{}); DELETE FROM staged_objects WHERE repo_id={id}::uuid AND operation_id={}::uuid; COMMIT;", text(operation), text(&pack.hash), text(&pack.pack_key), text(&pack.idx_key), text(&pack.pack_digest), text(&pack.idx_digest), pack.pack_size, pack.idx_size, snapshot.generation+1, text(operation), text(principal), bytes(&manifest), snapshot.generation+1,text(operation)));
    if metadata::sql(&query).is_err() {
        let confirmation = metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; UPDATE operations SET confirmations=confirmations+1 WHERE id={}::uuid AND repo_id={id}::uuid RETURNING generation; COMMIT;",
            text(operation)
        ))?;
        if confirmation.trim().parse::<u64>().ok() != Some(snapshot.generation + 1) {
            return Err(invalid(
                "truncation rejected or outcome unknown; inspect metadata before retrying",
            ));
        }
    }
    Ok(())
}

pub fn cleanup(engine: &Engine, slug: &str, principal: &str) -> Result<()> {
    let snapshot = metadata::snapshot(slug, principal, true)?;
    owner(&snapshot, principal)?;
    engine.store.require_unversioned()?;
    let barrier = Barrier::acquire(&snapshot.id, true)?;
    // Reauthorize after waiting and serialize this decision with grants. A new
    // epoch also fences a writer whose barrier session failed before cleanup.
    metadata::sql(&format!(
        "BEGIN; SET LOCAL synchronous_commit=on; DO $body$ BEGIN PERFORM 1 FROM repositories WHERE id={}::uuid FOR UPDATE; IF NOT EXISTS(SELECT 1 FROM permissions WHERE repo_id={}::uuid AND principal={} AND role='owner') THEN RAISE EXCEPTION 'owner permission required'; END IF; UPDATE repositories SET storage_epoch=storage_epoch+1,generation=generation+1 WHERE id={}::uuid; END $body$; COMMIT;",
        text(&snapshot.id),
        text(&snapshot.id),
        text(principal),
        text(&snapshot.id)
    ))?;
    metadata::sql(&format!(
        "INSERT INTO retired_objects SELECT repo_id,object_key,(SELECT storage_epoch FROM repositories WHERE id={}::uuid) FROM staged_objects s WHERE s.repo_id={}::uuid AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.pack_key=s.object_key OR p.idx_key=s.object_key) ON CONFLICT DO NOTHING;",
        text(&snapshot.id),
        text(&snapshot.id)
    ))?;
    let keys = metadata::sql(&format!(
        "SELECT object_key FROM retired_objects WHERE repo_id={}::uuid AND retired_epoch <= (SELECT storage_epoch FROM repositories WHERE id={}::uuid) AND NOT EXISTS(SELECT 1 FROM packs WHERE pack_key=object_key OR idx_key=object_key);",
        text(&snapshot.id),
        text(&snapshot.id)
    ))?;
    for key in keys.lines() {
        barrier.check()?;
        engine.store.delete_retired(key)?;
        // A planned PUT may finish remotely after a lost response. Keep its
        // tombstone so subsequent cleanup also removes a late-arriving object.
        metadata::sql(&format!(
            "BEGIN; DELETE FROM retired_objects WHERE repo_id={}::uuid AND object_key={}; DELETE FROM staged_objects WHERE object_key={} AND state='uploaded'; COMMIT;",
            text(&snapshot.id),
            text(key),
            text(key)
        ))?;
    }
    Ok(())
}
