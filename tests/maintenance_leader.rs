#![forbid(unsafe_code)]
//! Source-schema and DB-clock contracts through public migration/SQL/queue APIs.
//! Each exact invocation requires its own disposable PostgreSQL database.

use git_rs::{
    Result,
    maintenance::queue,
    metadata::{self, text},
    process::Operation,
};
use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier, mpsc},
    thread,
    time::{Duration, Instant},
};

fn initialize_v1() -> Result<()> {
    metadata::sql(&format!(
        "BEGIN; {} COMMIT;",
        include_str!("../migrations/001.sql")
    ))?;
    Ok(())
}

#[test]
#[ignore = "requires a fresh exclusively owned PostgreSQL database"]
fn concurrent_v1_upgrade_and_v2_refresh_preserve_source_state() -> Result<()> {
    Operation::new(30).enter(|| {
        initialize_v1()?;
        metadata::create("migration/source", "alice")?;
        let before = metadata::snapshot("migration/source", "alice", true)?;
        let ready = Arc::new(Barrier::new(2));
        let mut upgrades = Vec::new();
        for _ in 0..2 {
            let ready = ready.clone();
            upgrades.push(thread::spawn(move || {
                Operation::new(10).enter(|| {
                    ready.wait();
                    metadata::migrate()
                })
            }));
        }
        for upgrade in upgrades {
            upgrade.join().expect("migration worker panicked")?;
        }
        metadata::migrate()?;
        assert_eq!(
            metadata::sql("SELECT version FROM schema_version;")?.trim(),
            "2"
        );
        assert_eq!(
            metadata::sql("SELECT count(*),min(fence) FROM maintenance_leadership;")?.trim(),
            "1\t0"
        );
        let campaign = git_rs::scratch::identifier()?;
        assert_eq!(
            metadata::sql(&format!(
                "SELECT maintenance_acquire('refresh-owner',{}::uuid,30);",
                text(&campaign)
            ))?
            .trim(),
            "1"
        );
        let leadership =
            metadata::sql("SELECT row_to_json(l)::text FROM maintenance_leadership l;")?;
        metadata::migrate()?;
        assert_eq!(
            metadata::sql("SELECT row_to_json(l)::text FROM maintenance_leadership l;")?,
            leadership,
            "refresh must not reset the durable fence or an active campaign"
        );
        let after = metadata::snapshot("migration/source", "alice", true)?;
        assert_eq!(
            (after.id, after.generation, after.storage_epoch, after.refs),
            (
                before.id,
                before.generation,
                before.storage_epoch,
                before.refs
            )
        );
        Ok(())
    })
}

#[test]
#[ignore = "requires a fresh exclusively owned PostgreSQL database"]
fn both_v1_upgrade_and_v2_refresh_replace_an_obsolete_publish_implementation() -> Result<()> {
    Operation::new(30).enter(|| {
        initialize_v1()?;
        metadata::create("migration/publish", "alice")?;
        let before = metadata::snapshot("migration/publish", "alice", true)?;
        let _lease = metadata::Barrier::acquire(&before.id, false)?;
        let lease_id = metadata::sql(&format!("SELECT id FROM repository_leases WHERE repo_id={}::uuid;", text(&before.id)))?;
        let operation = git_rs::scratch::identifier()?;
        let query = format!(
            "SELECT publish({}::uuid,{}::uuid,'alice','commands'::bytea,{},{}::uuid,ARRAY[convert_to('refs/heads/main','UTF8')],ARRAY[decode(repeat('00',20),'hex')],ARRAY[decode(repeat('01',20),'hex')],ARRAY[NULL::boolean]);",
            text(&before.id), text(&operation), before.storage_epoch, text(lease_id.trim())
        );
        for previous_version in ["1", "2"] {
            assert_eq!(metadata::sql("SELECT version FROM schema_version;")?.trim(), previous_version);
            metadata::sql("CREATE OR REPLACE FUNCTION publish(p_repo uuid,p_operation uuid,p_principal text,p_commands bytea,p_epoch bigint,p_lease uuid,p_names bytea[],p_old bytea[],p_new bytea[],p_fast_forward boolean[]) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN 777; END $$;")?;
            metadata::migrate()?;
            let error = metadata::sql(&query).expect_err("refresh must replace the obsolete permissive body");
            assert!(error.to_string().contains("SQLSTATE P0001") && error.to_string().contains("invalid command fields"), "{error}");
            assert_eq!(metadata::sql("SELECT (SELECT count(*) FROM operations),(SELECT count(*) FROM ref_updates),(SELECT count(*) FROM refs);")?.trim(), "0\t0\t0");
            let after = metadata::snapshot("migration/publish", "alice", true)?;
            assert_eq!((after.generation, after.storage_epoch, after.refs),
                (before.generation, before.storage_epoch, before.refs.clone()));
        }
        Ok(())
    })
}

#[test]
#[ignore = "requires a fresh exclusively owned PostgreSQL database"]
fn failed_upgrade_rolls_back_every_ddl_change_and_can_be_retried() -> Result<()> {
    Operation::new(30).enter(|| {
        initialize_v1()?;
        metadata::sql("CREATE TABLE git_rs.maintenance_jobs(marker text); INSERT INTO git_rs.maintenance_jobs VALUES('preserve');")?;
        let constraint = metadata::sql("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='git_rs.schema_version'::regclass;")?;
        let error = metadata::migrate().expect_err("an incompatible existing relation must reject migration");
        assert!(error.to_string().contains("SQLSTATE 42P07"), "{error}");
        assert_eq!(metadata::sql("SELECT version FROM schema_version;")?.trim(), "1");
        assert_eq!(metadata::sql("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='git_rs.schema_version'::regclass;")?, constraint);
        assert_eq!(metadata::sql("SELECT to_regclass('git_rs.maintenance_leadership') IS NULL;")?.trim(), "t");
        assert_eq!(metadata::sql("SELECT marker FROM git_rs.maintenance_jobs;")?.trim(), "preserve");
        metadata::sql("DROP TABLE git_rs.maintenance_jobs;")?;
        metadata::migrate()?;
        assert_eq!(metadata::sql("SELECT version FROM schema_version;")?.trim(), "2");
        Ok(())
    })
}

#[test]
#[ignore = "requires a fresh exclusively owned PostgreSQL database"]
fn unknown_schema_is_rejected_without_creating_source_or_queue_tables() -> Result<()> {
    Operation::new(30).enter(|| {
        metadata::sql("CREATE SCHEMA git_rs; CREATE TABLE git_rs.schema_version(version integer PRIMARY KEY); INSERT INTO git_rs.schema_version VALUES(99);")?;
        let error = metadata::migrate().expect_err("unknown schema must remain untouched");
        assert!(error.to_string().contains("SQLSTATE P0001"), "{error}");
        assert_eq!(metadata::sql("SELECT version FROM git_rs.schema_version;")?.trim(), "99");
        assert_eq!(metadata::sql("SELECT count(*) FROM pg_tables WHERE schemaname='git_rs';")?.trim(), "1");
        Ok(())
    })
}

#[test]
#[ignore = "requires a fresh exclusively owned PostgreSQL database"]
fn owner_enqueue_and_cancelled_wait_are_independent_of_worker_authority() -> Result<()> {
    Operation::new(30).enter(|| {
        metadata::migrate()?;
        metadata::create("queue/source", "alice")?;
        metadata::grant("queue/source", "alice", "bob", "reader", b"refs/")?;
        assert!(queue::request_cleanup("queue/source", "bob").is_err());
        assert_eq!(metadata::sql("SELECT count(*) FROM maintenance_jobs;")?.trim(), "0");
        let request = Operation::new(10);
        let queued = request.enter(|| queue::request_cleanup("queue/source", "alice"))?;
        assert_eq!(queued.state, "queued");
        request.cancel();
        assert!(request.enter(|| queue::wait(&queued.id, 300)).is_err());
        assert_eq!(queue::status(&queued.id)?.state, "queued");
        assert_eq!(queue::wait(&queued.id, 0)?.state, "queued");
        assert_eq!(metadata::sql("SELECT fence,owner_identity IS NULL,active_job_id IS NULL FROM maintenance_leadership;")?.trim(), "0\tt\tt");
        assert_eq!(metadata::sql("SELECT count(*) FROM retired_objects;")?.trim(), "0");
        Ok(())
    })
}

struct LockedRepository {
    child: Child,
    reader: Option<thread::JoinHandle<()>>,
}

impl LockedRepository {
    fn acquire(repo: &str) -> Result<Self> {
        let child = Command::new("/usr/bin/psql")
            .args(["-X", "-qAt", "-v", "ON_ERROR_STOP=1"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut lock = Self {
            child,
            reader: None,
        };
        let output = lock.child.stdout.take().expect("piped output");
        let (sent, ready) = mpsc::sync_channel(1);
        lock.reader = Some(thread::spawn(move || {
            let mut line = String::new();
            let result = BufReader::new(output).read_line(&mut line).map(|_| line);
            let _sent = sent.send(result);
        }));
        let input = lock.child.stdin.as_mut().expect("piped input");
        writeln!(
            input,
            "BEGIN; DO $$ BEGIN PERFORM 1 FROM git_rs.repositories WHERE id={}::uuid FOR UPDATE; END $$; SELECT 'locked';",
            text(repo)
        )?;
        input.flush()?;
        assert_eq!(
            ready.recv_timeout(Duration::from_secs(5))??.trim(),
            "locked"
        );
        Ok(lock)
    }
}

impl Drop for LockedRepository {
    fn drop(&mut self) {
        let _killed = self.child.kill();
        let _reaped = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _joined = reader.join();
        }
    }
}

#[test]
#[ignore = "requires a fresh exclusively owned PostgreSQL database and external psql"]
fn leadership_expiry_while_waiting_for_repository_lock_cannot_finish_a_live_job() -> Result<()> {
    Operation::new(30).enter(|| {
        metadata::migrate()?;
        metadata::create("fencing/source", "alice")?;
        let repo = metadata::snapshot("fencing/source", "alice", true)?.id;
        let campaign = git_rs::scratch::identifier()?;
        let token = git_rs::scratch::identifier()?;
        let fence = metadata::sql(&format!("SELECT maintenance_acquire('synthetic-owner',{}::uuid,30);", text(&campaign)))?;
        let job = metadata::sql(&format!("SELECT maintenance_enqueue({}::uuid,'retire_gc','lock-wait',''::bytea);", text(&repo)))?;
        let args = format!("'synthetic-owner',{}::uuid,{}", text(&campaign), fence.trim());
        metadata::sql(&format!("SELECT id FROM maintenance_claim({args},{}::uuid,20);", text(&token)))?;
        let lock = LockedRepository::acquire(&repo)?;
        metadata::sql("UPDATE maintenance_leadership SET lease_until=clock_timestamp()+interval '2 seconds';")?;
        let query = format!("BEGIN; SET LOCAL synchronous_commit=on; /* late-finish-contract */ SELECT maintenance_finish({args},{}::uuid,{}::uuid,1,'succeeded',NULL,NULL,0); COMMIT;", text(job.trim()), text(&token));
        let finish = thread::spawn(move || Operation::new(10).enter(|| metadata::sql(&query)));
        let waiting = (|| -> Result<()> {
            let until = Instant::now() + Duration::from_secs(4);
            while metadata::sql("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%late-finish-contract%';")?.trim() != "1" {
                assert!(Instant::now() < until, "finish did not reach the actual repository lock");
                thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(metadata::sql("SELECT lease_until>clock_timestamp() FROM maintenance_leadership;")?.trim(), "t");
            while metadata::sql("SELECT lease_until<=clock_timestamp() FROM maintenance_leadership;")?.trim() != "t" {
                assert!(Instant::now() < until, "DB-clock authority expiry was not observed");
                thread::sleep(Duration::from_millis(10));
            }
            Ok(())
        })();
        drop(lock);
        let result = finish.join().expect("finish worker panicked");
        waiting?;
        let error = result.expect_err("a formerly valid leader must recheck expiry after the lock wait");
        assert!(error.to_string().contains("SQLSTATE P0001"), "{error}");
        assert_eq!(metadata::sql("SELECT state FROM maintenance_jobs;")?.trim(), "running");
        assert_eq!(metadata::sql("SELECT count(*) FROM maintenance_job_results;")?.trim(), "0");
        Ok(())
    })
}
