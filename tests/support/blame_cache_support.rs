#![allow(dead_code)]
use git_rs::{
    Result,
    derived_cache::{Access, ArtifactKey, CacheConfig, DerivedCache, KeyParts},
    process::Operation,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub fn id() -> String {
    let id = git_rs::scratch::identifier().unwrap();
    format!(
        "{}-{}-{}-{}-{}",
        &id[..8],
        &id[8..12],
        &id[12..16],
        &id[16..20],
        &id[20..]
    )
}
pub fn key(name: &[u8]) -> ArtifactKey {
    ArtifactKey::new(KeyParts {
        repository: b"synthetic/blame-cache",
        security: b"fixture-readers-v1",
        object_format: "opaque",
        kind: "complete-body",
        inputs: &[name],
        options: b"native-root-v1",
        representation: "complete-v1",
        producer: "fixture-v1",
    })
    .unwrap()
}
pub struct Authority;
impl Access for Authority {
    fn check(&self, _: &ArtifactKey) -> Result<()> {
        Ok(())
    }
}
pub fn authority() -> Arc<dyn Access> {
    Arc::new(Authority)
}

pub struct Source {
    hold: git_rs::metadata::Barrier,
    expected: ArtifactKey,
    pub slug: String,
    pub repo: String,
}
impl Source {
    /// Acquires an actual independently budgeted source lease, not a request ID.
    pub fn acquire(expected: &ArtifactKey) -> Arc<Self> {
        Operation::new(180).enter(|| {
            let slug = format!("synthetic/blame-artifacts-{}", id());
            git_rs::metadata::create(&slug, "alice").unwrap();
            git_rs::metadata::grant(&slug, "alice", "bob", "reader", b"refs/").unwrap();
            let (snapshot, hold) = git_rs::metadata::leased_snapshot(&slug, "bob", false).unwrap();
            Arc::new(Self {
                hold,
                expected: expected.clone(),
                slug,
                repo: snapshot.id,
            })
        })
    }
}
impl Access for Source {
    fn check(&self, key: &ArtifactKey) -> Result<()> {
        if key != &self.expected {
            return Err(git_rs::invalid("artifact outside authorized source scope"));
        }
        self.hold.check()?;
        git_rs::metadata::snapshot(&self.slug, "bob", false)?;
        Ok(())
    }
}
pub fn cache() -> (DerivedCache, String) {
    let mut config = CacheConfig::new(id(), id());
    config.read_seconds = 5;
    config.grace_seconds = 1;
    let namespace = config.namespace.clone();
    let cache = DerivedCache::open(config).unwrap();
    Operation::new(15).enter(|| cache.migrate()).unwrap();
    (cache, namespace)
}
pub struct Admin {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    runtime: tokio::runtime::Runtime,
}
impl Admin {
    pub fn new() -> Self {
        let dsn =
            std::env::var("GIT_RS_TEST_CACHE_ADMIN_DSN").expect("owned loopback fixture admin DSN");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (client, driver) = runtime.block_on(async {
            let (client, connection) = tokio::time::timeout(
                Duration::from_secs(5),
                tokio_postgres::connect(&dsn, tokio_postgres::NoTls),
            )
            .await
            .unwrap()
            .unwrap();
            (client, tokio::spawn(connection))
        });
        Self {
            client,
            driver,
            runtime,
        }
    }
    pub fn sql(&self, sql: &str) {
        self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), self.client.batch_execute(sql))
                .await
                .unwrap()
                .unwrap();
        });
    }
    pub fn scalar(&self, sql: &str) -> String {
        self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), self.client.query_one(sql, &[]))
                .await
                .unwrap()
                .unwrap()
                .get(0)
        })
    }
    pub fn until(&self, sql: &str) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.scalar(sql) != "true" {
            assert!(
                Instant::now() < deadline,
                "observable SQL condition did not become true: {sql}"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Admin {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
