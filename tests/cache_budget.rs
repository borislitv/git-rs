#![forbid(unsafe_code)]

use git_rs::{
    Result,
    derived_cache::{
        Access, ArtifactKey, CacheConfig, CacheMaintenance, DerivedCache, KeyParts, Lookup,
    },
    process::Operation,
};
use std::{sync::Arc, time::Duration};

struct Source;
impl Access for Source {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        Ok(())
    }
}

fn identifier() -> String {
    let value = git_rs::scratch::identifier().unwrap();
    format!(
        "{}-{}-{}-{}-{}",
        &value[..8],
        &value[8..12],
        &value[12..16],
        &value[16..20],
        &value[20..]
    )
}

fn expire(namespace: &str, sql: &str) {
    let dsn = std::env::var("GIT_RS_TEST_CACHE_ADMIN_DSN").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let query = sql.replace("NAMESPACE", namespace);
        tokio::time::timeout(Duration::from_secs(5), client.batch_execute(&query))
            .await
            .unwrap()
            .unwrap();
        drop(client);
        driver.await.unwrap().unwrap();
    });
}

#[test]
#[ignore = "requires an owned PostgreSQL cache fixture; tests/derived_cache.py provisions it"]
fn full_artifact_row_quota_does_not_prevent_first_maintenance_claim() {
    Operation::new(30).enter(|| {
        let mut config = CacheConfig::new(identifier(), identifier());
        config.namespace_rows = 16;
        config.max_payload_bytes = 8192;
        config.inline_bytes = 8192;
        config.memory_bytes = 128 * 1024;
        config.namespace_bytes = 65536;
        config.grace_seconds = 1;
        let cache = DerivedCache::open(config.clone()).unwrap();
        cache.migrate().unwrap();
        let access = Arc::new(Source);
        for index in 0..6 {
            let input = [index];
            let key = ArtifactKey::new(KeyParts { repository: b"synthetic/quota", security: b"fixture",
                object_format: "opaque", kind: "test", inputs: &[&input], options: b"",
                representation: "raw-v1", producer: "fixture-v1" }).unwrap();
            let Lookup::Claimed(claim) = cache.get_or_claim(&key, access.clone()).unwrap() else {
                panic!("fresh key should obtain a claim");
            };
            if index < 5 { assert!(cache.publish(&claim, access.as_ref(), &[index; 32]).unwrap()); }
        }
        // Five jobs/payloads/directory rows plus the sixth job use all 16
        // artifact rows. Authority must remain available without raising quotas.
        let maintenance = CacheMaintenance::new(&cache);
        let lease = maintenance.claim().expect("maintenance must remain available at the artifact row quota")
            .expect("no previous maintenance owner exists");
        expire(&config.namespace, "UPDATE git_rs_cache.directory SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace='NAMESPACE'");
        assert_eq!(maintenance.expire_batch(&lease).unwrap(), 5);
        assert_eq!(maintenance.retire_payloads(&lease).unwrap(), 5);
        expire(&config.namespace, "UPDATE git_rs_cache.payloads SET delete_after=clock_timestamp()-interval '1 second' WHERE namespace='NAMESPACE'");
        assert_eq!(maintenance.delete_retired(&lease).unwrap(), 5);
    });
}
