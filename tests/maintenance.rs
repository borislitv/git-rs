#![forbid(unsafe_code)]
//! Real PostgreSQL contracts for the public source-queue SQL boundary. These are
//! separate from Kubernetes candidacy qualification and never reset its fence.

use git_rs::{
    Result,
    metadata::{self, text},
    process::Operation,
};

fn rejected(query: &str) {
    let error = metadata::sql(query).expect_err("invalid authority or transition must be rejected");
    assert!(
        error.to_string().contains("SQLSTATE P0001"),
        "expected the function/trigger rejection, got {error}"
    );
}

struct Leader {
    owner: String,
    campaign: String,
    fence: u64,
    repo: String,
}

impl Leader {
    fn acquire() -> Result<Self> {
        metadata::migrate()?;
        let nonce = git_rs::scratch::identifier()?;
        let slug = format!("maintenance/contract-{nonce}");
        metadata::create(&slug, "alice")?;
        let repo = metadata::snapshot(&slug, "alice", false)?.id;
        let owner = format!("synthetic-pod/{nonce}");
        let campaign = git_rs::scratch::identifier()?;
        let row = metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_acquire({},{}::uuid,30); COMMIT;",
            text(&owner),
            text(&campaign)
        ))?;
        let fence = row.trim().parse()?;
        Ok(Self {
            owner,
            campaign,
            fence,
            repo,
        })
    }
    fn args(&self) -> String {
        format!(
            "{},{}::uuid,{}",
            text(&self.owner),
            text(&self.campaign),
            self.fence
        )
    }
    fn enqueue(&self, kind: &str, attempts: u32) -> Result<String> {
        let key = git_rs::scratch::identifier()?;
        Ok(metadata::sql(&format!(
            "SELECT maintenance_enqueue({}::uuid,{}, {}, ''::bytea, 100, {attempts});",
            text(&self.repo),
            text(kind),
            text(&key)
        ))?
        .trim()
        .to_owned())
    }
    fn claim(&self, token: &str) -> Result<String> {
        Ok(metadata::sql(&format!("BEGIN; SET LOCAL synchronous_commit=on; SELECT id,claim_generation,attempts FROM maintenance_claim({},{}::uuid,20); COMMIT;", self.args(), text(token)))?.trim_end_matches('\n').to_owned())
    }
    fn job_args(&self, job: &str, token: &str, generation: u64) -> String {
        format!(
            "{},{}::uuid,{}::uuid,{generation}",
            self.args(),
            text(job),
            text(token)
        )
    }
    fn finish(&self, job: &str, token: &str, generation: u64, outcome: &str) -> Result<()> {
        metadata::sql(&format!(
            "BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_finish({}, {},NULL,NULL,0); COMMIT;",
            self.job_args(job, token, generation),
            text(outcome)
        ))?;
        Ok(())
    }
}
impl Drop for Leader {
    fn drop(&mut self) {
        let _released = Operation::new(5)
            .enter(|| metadata::sql(&format!("SELECT maintenance_release({});", self.args())));
    }
}

#[test]
#[ignore = "requires an exclusively owned disposable PostgreSQL database; run serially"]
fn leadership_fence_is_monotonic_and_expired_campaigns_never_revive() -> Result<()> {
    Operation::new(120).enter(|| {
        let leader = Leader::acquire()?;
        let acquired = metadata::sql(&format!("SELECT maintenance_acquire({},{}::uuid,30);", text(&leader.owner), text(&leader.campaign)))?;
        assert_eq!(acquired.trim().parse::<u64>()?, leader.fence, "same live campaign confirms one acquisition");
        let other = git_rs::scratch::identifier()?;
        assert!(metadata::sql(&format!("SELECT maintenance_acquire('other-pod',{}::uuid,30);", text(&other)))?.trim().is_empty());
        rejected(&format!("SELECT maintenance_assert_leader({},{}::uuid,NULL);", text(&leader.owner), text(&leader.campaign)));
        rejected("UPDATE maintenance_leadership SET fence=fence-1 WHERE name='source';");
        rejected("DELETE FROM maintenance_leadership WHERE name='source';");
        metadata::sql("UPDATE maintenance_leadership SET lease_until=clock_timestamp()-interval '1 second' WHERE name='source';")?;
        rejected(&format!("SELECT maintenance_acquire({},{}::uuid,30);", text(&leader.owner), text(&leader.campaign)));
        rejected(&format!("SELECT maintenance_renew({},30);", leader.args()));
        let next = Leader::acquire()?;
        assert_eq!(next.fence, leader.fence + 1);
        rejected(&format!("SELECT maintenance_assert_leader({});", leader.args()));
        Ok(())
    })
}

#[test]
#[ignore = "requires an exclusively owned disposable PostgreSQL database; run serially"]
fn one_live_job_tuple_reclaims_with_a_fresh_token_and_rejects_old_work() -> Result<()> {
    Operation::new(120).enter(|| {
        let leader = Leader::acquire()?;
        let job = leader.enqueue("retire_gc", 2)?;
        let first = git_rs::scratch::identifier()?;
        assert_eq!(leader.claim(&first)?, format!("{job}\t1\t1"));
        assert!(leader.claim(&git_rs::scratch::identifier()?)?.is_empty(), "one leader cannot authorize overlapping live jobs");
        rejected(&format!("SELECT maintenance_assert_job({},{}::uuid,{}::uuid,NULL);", leader.args(), text(&job), text(&first)));
        metadata::sql(&format!("UPDATE maintenance_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id={}::uuid;", text(&job)))?;
        rejected(&format!("SELECT maintenance_heartbeat({},20);", leader.job_args(&job, &first, 1)));
        let second = git_rs::scratch::identifier()?;
        assert!(leader.claim(&second)?.is_empty(), "expired work enters durable backoff before another attempt");
        assert_eq!(metadata::sql(&format!("SELECT state,attempts FROM maintenance_jobs WHERE id={}::uuid;", text(&job)))?.trim(), "retry\t1");
        metadata::sql(&format!("UPDATE maintenance_jobs SET available_at=clock_timestamp() WHERE id={}::uuid;", text(&job)))?;
        assert_eq!(leader.claim(&second)?, format!("{job}\t2\t2"));
        rejected(&format!("SELECT maintenance_assert_job({});", leader.job_args(&job, &first, 1)));
        leader.finish(&job, &second, 2, "retry")?;
        assert_eq!(metadata::sql(&format!("SELECT state,attempts FROM maintenance_jobs WHERE id={}::uuid;", text(&job)))?.trim(), "dead\t2");
        assert!(leader.claim(&git_rs::scratch::identifier()?)?.is_empty(), "exhausted work does not resurrect");
        Ok(())
    })
}

#[test]
#[ignore = "requires an exclusively owned disposable PostgreSQL database; run serially"]
fn finish_confirmation_is_exact_immutable_and_survives_a_new_claim() -> Result<()> {
    Operation::new(120).enter(|| {
        let leader = Leader::acquire()?;
        let job = leader.enqueue("retire_gc", 3)?;
        let first = git_rs::scratch::identifier()?;
        assert_eq!(leader.claim(&first)?, format!("{job}\t1\t1"));
        leader.finish(&job, &first, 1, "retry")?;
        metadata::sql(&format!("UPDATE maintenance_jobs SET available_at=clock_timestamp() WHERE id={}::uuid;", text(&job)))?;
        let second = git_rs::scratch::identifier()?;
        assert_eq!(leader.claim(&second)?, format!("{job}\t2\t2"));
        let confirm = |token: &str, outcome: &str| metadata::sql(&format!("BEGIN; SET LOCAL synchronous_commit=on; SELECT maintenance_confirm_finish({}::uuid,{}::uuid,1,{}, {},NULL,NULL); COMMIT;", text(&job), text(token), leader.fence, text(outcome)));
        assert_eq!(confirm(&first, "retry")?.trim(), "retry");
        assert!(confirm(&first, "succeeded")?.trim().is_empty(), "an unrelated terminal result is not confirmation");
        assert!(confirm(&second, "retry")?.trim().is_empty(), "a new token cannot attest the old attempt");
        rejected(&format!("UPDATE maintenance_job_results SET outcome='succeeded' WHERE job_id={}::uuid;", text(&job)));
        rejected(&format!("DELETE FROM maintenance_job_results WHERE job_id={}::uuid;", text(&job)));
        assert_eq!(metadata::sql(&format!("SELECT state,claim_generation,attempts FROM maintenance_jobs WHERE id={}::uuid;", text(&job)))?.trim(), "running\t2\t2", "confirmation never replays the transition");
        leader.finish(&job, &second, 2, "succeeded")?;
        Ok(())
    })
}

#[test]
#[ignore = "requires an exclusively owned disposable PostgreSQL database; run serially"]
fn retired_keys_are_permanent_and_cannot_be_staged_or_attached_again() -> Result<()> {
    Operation::new(120).enter(|| {
        let leader = Leader::acquire()?;
        let operation = git_rs::scratch::identifier()?;
        let key = format!("repos/{}/operations/{operation}/old.pack", leader.repo);
        metadata::sql(&format!("INSERT INTO retired_objects(repo_id,object_key,retired_epoch,retirement_operation,reason) VALUES({}::uuid,{},0,{}::uuid,'contract');", text(&leader.repo), text(&key), text(&operation)))?;
        rejected(&format!("DELETE FROM retired_objects WHERE object_key={};", text(&key)));
        rejected(&format!("UPDATE retired_objects SET object_key=object_key||'-new' WHERE object_key={};", text(&key)));
        rejected(&format!("INSERT INTO staged_objects(repo_id,operation_id,object_key,state) VALUES({}::uuid,{}::uuid,{},'planned');", text(&leader.repo), text(&operation), text(&key)));
        rejected(&format!("INSERT INTO packs(repo_id,operation_id,native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size,generation) VALUES({}::uuid,{}::uuid,repeat('1',40),{},{},repeat('a',64),repeat('b',64),32,1096,0);", text(&leader.repo), text(&operation), text(&key), text(&(key.clone()+".idx"))));
        assert_eq!(metadata::sql(&format!("SELECT count(*) FROM retired_objects WHERE object_key={};", text(&key)))?.trim(), "1");
        assert_eq!(metadata::sql(&format!("SELECT count(*) FROM maintenance_jobs WHERE repo_id={}::uuid AND kind='retire_gc';", text(&leader.repo)))?.trim(), "1", "retirement and its cleanup enqueue are atomic");
        Ok(())
    })
}

#[test]
#[ignore = "requires an exclusively owned disposable PostgreSQL database; run serially"]
fn qualified_catalog_writes_ignore_the_callers_search_path() -> Result<()> {
    Operation::new(120).enter(|| {
        metadata::migrate()?;
        // Existing v2 databases must receive the corrected execution context
        // through the same refresh path, without rebuilding their source tables.
        metadata::sql("ALTER FUNCTION maintenance_protect_key() RESET search_path;
                       ALTER FUNCTION maintenance_retirement() RESET search_path;
                       ALTER FUNCTION maintenance_compact_enqueue() RESET search_path;")?;
        metadata::migrate()?;
        for (ordinal, path) in ["public, pg_catalog", "pg_temp, public, pg_catalog"]
            .into_iter()
            .enumerate()
        {
            let slug = format!("maintenance/search-path-{}", git_rs::scratch::identifier()?);
            metadata::create(&slug, "alice")?;
            let repo = text(&metadata::snapshot(&slug, "alice", false)?.id);
            let operation = text(&git_rs::scratch::identifier()?);
            let prefix = text(&format!("synthetic/search-path/{ordinal}/"));
            let shadows = if ordinal == 0 {
                ""
            } else {
                "CREATE TEMP TABLE repositories(id uuid,generation bigint) ON COMMIT DROP;
                 CREATE TEMP TABLE packs(repo_id uuid) ON COMMIT DROP;
                 CREATE TEMP TABLE retired_objects(object_key text) ON COMMIT DROP;
                 CREATE TEMP TABLE staged_objects(object_key text,retired_at timestamptz) ON COMMIT DROP;"
            };
            let state = metadata::sql(&format!(
                "BEGIN; SET LOCAL search_path={path}; {shadows}
                 INSERT INTO git_rs.staged_objects(repo_id,operation_id,object_key,state)
                     VALUES({repo}::uuid,{operation}::uuid,{prefix}||'retired.pack','planned');
                 INSERT INTO git_rs.packs(repo_id,operation_id,native_hash,pack_key,idx_key,
                     pack_digest,idx_digest,pack_size,idx_size,generation)
                     SELECT {repo}::uuid,{operation}::uuid,lpad(n::text,40,'0'),
                         {prefix}||n||'.pack',{prefix}||n||'.idx',repeat('a',64),repeat('b',64),32,1096,0
                     FROM generate_series(1,8) n;
                 INSERT INTO git_rs.retired_objects(repo_id,object_key,retired_epoch,retirement_operation,reason)
                     VALUES({repo}::uuid,{prefix}||'retired.pack',0,{operation}::uuid,'contract');
                 SELECT
                     (SELECT count(*) FROM git_rs.packs WHERE repo_id={repo}::uuid),
                     (SELECT count(*) FROM git_rs.staged_objects WHERE repo_id={repo}::uuid AND retired_at IS NOT NULL),
                     (SELECT count(*) FROM git_rs.maintenance_jobs WHERE repo_id={repo}::uuid AND kind='retire_gc'),
                     (SELECT count(*) FROM git_rs.maintenance_jobs WHERE repo_id={repo}::uuid AND kind='compact');
                 COMMIT;"
            ))?;
            assert_eq!(state.trim(), "8\t1\t1\t1", "catalog, retirement and enqueue must use source tables for {path}");
            for insert in [
                format!("INSERT INTO git_rs.staged_objects(repo_id,operation_id,object_key,state) VALUES({repo}::uuid,{operation}::uuid,{prefix}||'retired.pack','planned');"),
                format!("INSERT INTO git_rs.packs(repo_id,operation_id,native_hash,pack_key,idx_key,pack_digest,idx_digest,pack_size,idx_size,generation) VALUES({repo}::uuid,{operation}::uuid,repeat('f',40),{prefix}||'retired.pack',{prefix}||'new.idx',repeat('a',64),repeat('b',64),32,1096,0);")
            ] {
                rejected(&format!("BEGIN; SET LOCAL search_path={path}; {shadows} {insert} COMMIT;"));
            }
            assert_eq!(metadata::sql(&format!("SELECT count(*) FROM packs WHERE repo_id={repo}::uuid;"))?.trim(), "8", "rejected key reuse must not publish another pack");
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires an exclusively owned disposable PostgreSQL database; run serially"]
fn null_limits_cannot_disable_bounded_scans_or_change_a_live_tuple() -> Result<()> {
    Operation::new(120).enter(|| {
        let leader = Leader::acquire()?;
        let leadership = || metadata::sql("SELECT fence,owner_identity,campaign_id,lease_until,active_job_id FROM maintenance_leadership WHERE name='source';");
        let before = leadership()?;
        rejected(&format!("SELECT maintenance_acquire({},{}::uuid,NULL);", text(&leader.owner), text(&leader.campaign)));
        rejected(&format!("SELECT maintenance_renew({},NULL);", leader.args()));
        for limits in ["NULL,60,1000", "100,NULL,1000", "100,60,NULL"] {
            rejected(&format!("SELECT maintenance_scan({},{limits});", leader.args()));
        }
        rejected(&format!("SELECT maintenance_claim({},{}::uuid,NULL);", leader.args(), text(&git_rs::scratch::identifier()?)));
        assert_eq!(leadership()?, before, "invalid bounds cannot change leadership or claim work");

        let job = leader.enqueue("orphan_scan", 2)?;
        let token = git_rs::scratch::identifier()?;
        assert_eq!(leader.claim(&token)?, format!("{job}\t1\t1"));
        let tuple = leader.job_args(&job, &token, 1);
        let state = || metadata::sql(&format!("SELECT state,owner_token,claim_generation,attempts,lease_until FROM maintenance_jobs WHERE id={}::uuid;", text(&job)));
        let before = state()?;
        rejected(&format!("SELECT maintenance_heartbeat({tuple},NULL);"));
        rejected(&format!("SELECT maintenance_finish({tuple},NULL,NULL,NULL,0);"));
        rejected(&format!("SELECT maintenance_finish({tuple},'retry',NULL,NULL,NULL);"));
        // Reject NULL before LIMIT can become unbounded, including before the
        // otherwise valid job tries to inspect its repository or source barrier.
        rejected(&format!("SELECT * FROM maintenance_inspect({tuple},NULL,0,0,decode(repeat('00',32),'hex'),NULL);"));
        assert_eq!(state()?, before, "invalid bounds cannot renew or finish the live job");
        assert_eq!(metadata::sql(&format!("SELECT count(*) FROM maintenance_job_results WHERE job_id={}::uuid;", text(&job)))?.trim(), "0");
        leader.finish(&job, &token, 1, "succeeded")?;
        Ok(())
    })
}
