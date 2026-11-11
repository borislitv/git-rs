#![forbid(unsafe_code)]
#[path = "support/blame_cache_support.rs"]
mod support;
use git_rs::{
    derived_cache::{
        BlameBuilds, BuildLease, BuildLimits, BuildLookup, CacheConfig, DerivedCache, Lookup,
    },
    process::Operation,
};
use std::{sync::mpsc, time::Duration};
use support::*;

fn claim(
    builds: &BlameBuilds,
    key: &git_rs::derived_cache::ArtifactKey,
    limits: BuildLimits,
) -> BuildLease {
    match builds.claim(key, authority(), limits).unwrap() {
        BuildLookup::Claimed(lease) => lease,
        BuildLookup::Pending => panic!("fresh build must be claimable"),
    }
}

#[test]
fn finite_build_profile_does_not_lift_opaque_limits() {
    assert_eq!(BuildLimits::default().hard_seconds, 3600);
    BuildLimits::default().validate().unwrap();
    for limits in [
        BuildLimits {
            hard_seconds: 3601,
            lease_seconds: 15,
        },
        BuildLimits {
            hard_seconds: 0,
            lease_seconds: 1,
        },
        BuildLimits {
            hard_seconds: 5,
            lease_seconds: 6,
        },
    ] {
        assert!(limits.validate().is_err());
    }
    let mut opaque = CacheConfig::new(id(), id());
    assert_eq!(opaque.max_payload_bytes, 8 * 1024 * 1024);
    opaque.job_seconds = 121;
    assert!(opaque.validate().is_err());
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn build_takeover_fences_old_owner_and_renewal_preserves_hard_deadline() {
    Operation::new(30).enter(|| {
        let (cache, namespace) = cache();
        let builds = BlameBuilds::new(&cache);
        let key = key(b"generation");
        let old = claim(&builds, &key, BuildLimits::default());
        assert!(matches!(builds.claim(&key, authority(), BuildLimits::default()).unwrap(), BuildLookup::Pending));
        let admin = Admin::new();
        let sql = format!("SELECT hard_until::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'");
        let hard = admin.scalar(&sql);
        let local = old.deadline();
        assert!(old.renew().unwrap());
        assert_eq!(admin.scalar(&sql), hard, "renewal must never reset the hard deadline");
        assert_eq!(old.deadline(), local);
        admin.sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'"));
        let fresh = claim(&builds, &key, BuildLimits::default());
        assert!(!old.renew().unwrap());
        assert!(old.check_authority().is_err());
        fresh.check_authority().unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn build_renewal_checks_database_time_after_its_row_lock_wait() {
    let (cache, namespace) = cache();
    let builds = BlameBuilds::new(&cache);
    let lease = Operation::new(15).enter(|| claim(&builds, &key(b"lock"), BuildLimits::default()));
    let locker = Admin::new();
    let observer = Admin::new();
    locker.sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()+interval '500 milliseconds' WHERE namespace='{namespace}'"));
    locker.sql(&format!("BEGIN; SELECT key_hash FROM git_rs_cache.blame_builds WHERE namespace='{namespace}' FOR UPDATE;"));
    let renew = std::thread::spawn(move || Operation::new(5).enter(|| lease.renew()));
    observer.until("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='git-rs-cache' AND wait_event_type='Lock' AND query LIKE '%renew_blame_build%')::text");
    observer.until(&format!("SELECT (lease_until<=clock_timestamp())::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"));
    locker.sql("ROLLBACK");
    assert!(
        !renew.join().unwrap().unwrap(),
        "expiry during a real lock wait must reject renewal"
    );
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn scoped_build_heartbeat_covers_owned_work_without_moving_hard_deadline() {
    let (cache, namespace) = cache();
    let builds = BlameBuilds::new(&cache);
    let lease = Operation::new(15).enter(|| {
        claim(
            &builds,
            &key(b"heartbeat"),
            BuildLimits {
                hard_seconds: 4,
                lease_seconds: 1,
            },
        )
    });
    let observer = Admin::new();
    let initial = observer.scalar(&format!(
        "SELECT lease_until::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"
    ));
    let hard = observer.scalar(&format!(
        "SELECT hard_until::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"
    ));
    let (finish, finished) = mpsc::channel();
    let work = std::thread::spawn(move || {
        lease.run_with_renewal(|context| {
            loop {
                context.check_local()?;
                match finished.recv_timeout(Duration::from_millis(10)) {
                    Ok(()) => return Ok(b"complete reusable mapping".to_vec()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(_) => return Err(git_rs::invalid("fixture gate closed")),
                }
            }
        })
    });
    observer.until(&format!("SELECT (clock_timestamp()>'{initial}'::timestamptz AND lease_until>clock_timestamp())::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"));
    assert_eq!(
        observer.scalar(&format!(
            "SELECT hard_until::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"
        )),
        hard
    );
    finish.send(()).unwrap();
    assert_eq!(work.join().unwrap().unwrap(), b"complete reusable mapping");
}

#[test]
#[ignore = "requires an owned PostgreSQL fixture; also run against a fresh v1 schema"]
fn migration_preserves_existing_opaque_bytes_and_is_idempotent() {
    Operation::new(30).enter(|| {
        let admin = Admin::new();
        let before = admin.scalar("SELECT version::text FROM git_rs_cache.schema_version WHERE singleton");
        if let Ok(expected) = std::env::var("GIT_RS_TEST_CACHE_SCHEMA_EXPECTED") { assert_eq!(before,expected); }
        println!("cache schema before upgrade: {before}");
        let config = CacheConfig::new(id(),id());
        admin.sql(&format!("SET ROLE git_rs_cache; SELECT git_rs_cache.register_namespace('{}','{}',{},{}); RESET ROLE;",config.namespace,config.owner,config.namespace_rows,config.namespace_bytes));
        let cache = DerivedCache::open(config).unwrap();
        let key = key(b"v1-opaque");
        let claim = match cache.get_or_claim(&key,authority()).unwrap() { Lookup::Claimed(claim)=>claim,_=>panic!("fresh fixture artifact") };
        assert!(cache.publish(&claim,authority().as_ref(),b"opaque bytes before upgrade").unwrap());
        cache.migrate().unwrap();
        cache.migrate().unwrap();
        assert_eq!(admin.scalar("SELECT version::text FROM git_rs_cache.schema_version WHERE singleton"),"2");
        cache.get_verified(&key,authority()).unwrap().unwrap().with_bytes(|bytes| {assert_eq!(bytes,b"opaque bytes before upgrade");Ok(())}).unwrap();
        claim_build_after_upgrade(&cache);
    });
}

fn claim_build_after_upgrade(cache: &DerivedCache) {
    let builds = BlameBuilds::new(cache);
    let lease = claim(&builds, &key(b"post-upgrade-build"), BuildLimits::default());
    lease.check_authority().unwrap();
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn cache_lease_loss_does_not_discard_a_source_authorized_complete_local_result() {
    let (cache, namespace) = cache();
    let builds = BlameBuilds::new(&cache);
    let lease = Operation::new(15)
        .enter(|| claim(&builds, &key(b"local-fallback"), BuildLimits::default()));
    let result = lease.run_with_renewal(|context| {
        Admin::new().sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace='{namespace}'"));
        assert!(context.check_authority().is_err(),"lost cache ownership must fence durable publication");
        Ok(b"complete local result remains usable".to_vec())
    }).unwrap();
    assert_eq!(result, b"complete local result remains usable");
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture"]
fn competing_claim_samples_database_time_after_the_build_row_lock_wait() {
    let (cache, namespace) = cache();
    let builds = BlameBuilds::new(&cache);
    let key = key(b"claim-lock-wait");
    let old = Operation::new(15).enter(|| claim(&builds, &key, BuildLimits::default()));
    let locker = Admin::new();
    let observer = Admin::new();
    locker.sql(&format!("UPDATE git_rs_cache.blame_builds SET lease_until=clock_timestamp()+interval '500 milliseconds' WHERE namespace='{namespace}'"));
    locker.sql(&format!("BEGIN; SELECT key_hash FROM git_rs_cache.blame_builds WHERE namespace='{namespace}' FOR UPDATE;"));
    let competitor = std::thread::spawn(move || {
        Operation::new(5).enter(|| builds.claim(&key, authority(), BuildLimits::default()))
    });
    observer.until("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='git-rs-cache' AND wait_event_type='Lock' AND query LIKE '%claim_blame_build%')::text");
    observer.until(&format!("SELECT (lease_until<=clock_timestamp())::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"));
    locker.sql("ROLLBACK");
    let answer = competitor.join().unwrap().unwrap();
    let fresh = match answer {
        BuildLookup::Claimed(lease) => lease,
        BuildLookup::Pending => panic!("claim must sample DB time after its actual row-lock wait"),
    };
    Operation::new(5).enter(|| {
        assert!(!old.renew().unwrap());
        fresh.check_authority().unwrap();
    });
    assert_eq!(
        observer.scalar(&format!(
            "SELECT generation::text FROM git_rs_cache.blame_builds WHERE namespace='{namespace}'"
        )),
        "2"
    );
}
