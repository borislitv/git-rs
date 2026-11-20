use super::{compaction::Input, queue::Job};
use crate::{
    Result,
    blob::Store,
    invalid,
    metadata::{self, Barrier, text},
};

/// Age only schedules this investigation. The exclusive source barrier and a
/// committed epoch advance fence serving publishers before any key is retired.
pub(super) fn inspect(job: &Job, store: &Store, batch: usize) -> Result<()> {
    job.check()?;
    store.require_unversioned()?;
    job.check()?;
    let barrier = Barrier::acquire(&job.repo, true)?;
    job.check()?;
    let input = Input::current(job)?;
    let published = metadata::sql(&format!(
        "BEGIN; SET LOCAL synchronous_commit=on; SELECT * FROM maintenance_inspect({},{}::uuid,{},{}); COMMIT;",
        job.args(),
        text(&barrier.id),
        input.args()?,
        batch
    ));
    // Confirm the exact journal even after a successful response: inspection may
    // or may not advance the epoch, depending on whether this batch retired keys.
    let _response = published;
    let confirmed = input.confirm(job, &input.manifest)?;
    if confirmed != Some((input.generation, input.epoch))
        && confirmed != Some((input.generation + 1, input.epoch + 1))
    {
        return Err(invalid("orphan inspection rejected or outcome unknown"));
    }
    job.check()
}

pub(super) fn delete(job: &Job, store: &Store, batch: usize) -> Result<()> {
    if !(1..=1000).contains(&batch) {
        return Err(invalid("retirement deletion batch is invalid"));
    }
    job.check()?;
    store.require_unversioned()?;
    let rows = metadata::sql(&format!(
        "BEGIN; DO $job$ BEGIN PERFORM maintenance_assert_job({}); END $job$; SELECT object_key FROM retired_objects t WHERE t.repo_id={}::uuid AND t.delete_after<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.pack_key=t.object_key OR p.idx_key=t.object_key) ORDER BY t.delete_after,t.object_key LIMIT {batch}; COMMIT;",
        job.args(),
        text(&job.repo)
    ))?;
    for key in rows.lines() {
        job.check()?;
        // This PG check is not a network fence. Delayed DELETE is safe because
        // durable tombstones prohibit every publisher from reusing the key.
        store.delete_retired(key)?;
        metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; SELECT (maintenance_assert_job({})).id; UPDATE retired_objects SET last_delete_at=clock_timestamp(),delete_after=clock_timestamp()+interval '60 seconds',delete_attempts=delete_attempts+1 WHERE repo_id={}::uuid AND object_key={}; COMMIT;",
            job.args(),
            text(&job.repo),
            text(key)
        ))?;
        // Never delete the tombstone or a planned intent after a 404/DELETE.
        // An arbitrarily late PUT must still be found and deleted again.
    }
    reconcile_uploads(job, store)?;
    Ok(())
}

fn reconcile_uploads(job: &Job, store: &Store) -> Result<()> {
    job.check()?;
    let cursor = metadata::sql(&format!(
        "BEGIN; DO $job$ BEGIN PERFORM maintenance_assert_job({}); END $job$; INSERT INTO maintenance_upload_scans(repo_id) VALUES({}::uuid) ON CONFLICT DO NOTHING; SELECT key_marker,upload_marker FROM maintenance_upload_scans WHERE repo_id={}::uuid; COMMIT;",
        job.args(),
        text(&job.repo),
        text(&job.repo)
    ))?;
    let fields = cursor
        .trim_end_matches('\n')
        .split('\t')
        .collect::<Vec<_>>();
    let [key, upload] = fields.as_slice() else {
        return Err(invalid("invalid multipart scan cursor"));
    };
    let operation = crate::process::Operation::current()
        .ok_or_else(|| invalid("multipart reconciliation requires an operation"))?;
    let control = git_rs_core::Control::new(operation.deadline());
    let page = store.pending_uploads(&job.repo, key, upload, &control)?;
    for pending in page.uploads {
        job.check()?;
        let retired = metadata::sql(&format!(
            "SELECT 1 FROM retired_objects t WHERE t.repo_id={}::uuid AND t.object_key={} AND NOT EXISTS(SELECT 1 FROM packs p WHERE p.pack_key=t.object_key OR p.idx_key=t.object_key);",
            text(&job.repo),
            text(&pending.key)
        ))?;
        if retired.trim() == "1" {
            job.check()?;
            store.abort_retired_upload(&pending.key, &pending.upload, &control)?;
        }
    }
    job.check()?;
    metadata::sql(&format!(
        "BEGIN; SET LOCAL synchronous_commit=on; DO $job$ BEGIN PERFORM maintenance_assert_job({}); END $job$; UPDATE maintenance_upload_scans SET key_marker={},upload_marker={} WHERE repo_id={}::uuid; COMMIT;",
        job.args(),
        text(&page.next_key),
        text(&page.next_upload),
        text(&job.repo)
    ))?;
    job.check()
}
