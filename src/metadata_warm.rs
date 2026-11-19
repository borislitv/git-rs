//! Source credentials for this process may execute only the warm API. The DB
//! binds the login to a durable slot and copies its exact target into the hold.
use super::{Barrier, Snapshot, bytes, lease_seconds, read_snapshot, sql, text};
use crate::{Result, auth, invalid, metrics::Stage, process, repository, scratch};

pub(crate) fn slots() -> Result<Vec<u8>> {
    let mut slots = Vec::new();
    for row in sql("SELECT git_rs.warm_slots() ORDER BY 1;")?.lines() {
        let slot = row.parse()?;
        if !(1..=64).contains(&slot) || slots.last().is_some_and(|last| *last >= slot) {
            return Err(invalid("invalid warm slot listing"));
        }
        slots.push(slot);
    }
    Ok(slots)
}

pub(crate) struct HeldTarget {
    pub snapshot: Snapshot,
    pub lease: Barrier,
    pub principal: String,
    pub path: Vec<u8>,
}

pub(crate) fn acquire(slot: u8) -> Result<Option<HeldTarget>> {
    // Create the cleanup guard even before a possibly ambiguous implicit
    // commit. A failed release remains safe until the DB-clock expiry.
    let mut lease = Barrier {
        id: scratch::identifier()?,
        repo: String::new(),
        scoped_warm: true,
    };
    let seconds = lease_seconds(process::remaining()?)?;
    let id = format!("{}::uuid", text(&lease.id));
    let acquired = sql(&format!(
        "SELECT git_rs.warm_acquire({slot},{id},{seconds});"
    ))?;
    if acquired.trim().is_empty() {
        // NULL means the statement confirmed no hold was created.
        lease.id.clear();
        return Ok(None);
    }
    lease.repo = acquired.trim().to_owned();
    let identity = sql(&format!("SELECT * FROM git_rs.warm_identity({id});"))?;
    let Some((principal, path)) = identity.trim_end_matches('\n').split_once('\t') else {
        return Ok(None); // target or ACL changed after acquire
    };
    auth::principal(principal)?;
    let path = openssl::base64::decode_block(path)?;
    let query = format!("SELECT git_rs.warm_snapshot({id});");
    let Some(snapshot) = read_snapshot(&[(&query, Some(Stage::CatalogRead))], None, &mut None)?
    else {
        return Ok(None);
    };
    if snapshot.id != lease.repo || snapshot.refs.len() != 1 {
        return Err(invalid("warm repository lease mismatch"));
    }
    repository::ref_name(&snapshot.default_ref)?;
    Ok(Some(HeldTarget {
        snapshot,
        lease,
        principal: principal.to_owned(),
        path,
    }))
}

pub(super) fn release(id: &str) -> Result<String> {
    sql(&format!("SELECT git_rs.warm_release({}::uuid);", text(id)))
}

/// None selects the normal serving authority check, not a scoped fallback.
pub(crate) fn check(lease: &Barrier, snapshot: &Snapshot) -> Option<Result<()>> {
    lease.scoped_warm.then(|| {
        let tip = snapshot
            .refs
            .get(&snapshot.default_ref)
            .ok_or_else(|| invalid("missing scoped warm tip"))?;
        let found = sql(&format!(
            "SELECT git_rs.warm_check({}::uuid,{}::uuid,{},{},{});",
            text(&lease.id),
            text(&snapshot.id),
            snapshot.generation,
            snapshot.storage_epoch,
            bytes(&tip.0),
        ))?;
        if found.trim() != "t" {
            return Err(invalid("warm source authority changed"));
        }
        Ok(())
    })
}
