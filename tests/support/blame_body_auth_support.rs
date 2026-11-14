//! Public Engine regressions. The attacker uses only the cache role, never the
//! producer's independent HMAC key or source metadata credentials.
use super::{source_support, until};
use git_rs::{
    Result,
    blame::{Blame, Observer, Phase, Response, WorkStatistics},
    derived_cache::{CacheConfig, DerivedCache},
    process::Operation,
};
use git_rs_core::Control;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use zstd::zstd_safe;

#[derive(Default)]
struct Observed {
    hits: AtomicUsize,
    computes: AtomicUsize,
}
impl Observer for Observed {
    fn observe(&self, phase: Phase, _: &Control) -> Result<()> {
        if phase == Phase::CacheHit {
            self.hits.fetch_add(1, Ordering::AcqRel);
        }
        Ok(())
    }
    fn work_completed(&self, _: bool, _: WorkStatistics, _: &Control) {
        self.computes.fetch_add(1, Ordering::AcqRel);
    }
}
fn config() -> CacheConfig {
    let id = || {
        let id = git_rs::scratch::identifier().unwrap();
        format!(
            "{}-{}-{}-{}-{}",
            &id[..8],
            &id[8..12],
            &id[12..16],
            &id[16..20],
            &id[20..]
        )
    };
    let mut config = CacheConfig::new(id(), id());
    config.read_seconds = 60;
    Operation::new(15)
        .enter(|| DerivedCache::open(config.clone()).unwrap().migrate())
        .unwrap();
    config
}
fn service(config: &CacheConfig, secret: Option<&[u8; 32]>, observed: Arc<Observed>) -> Blame {
    let blame = Blame::new(1, 0).unwrap();
    let blame = match secret {
        Some(secret) => blame
            .with_authenticated_cache_key(config.clone(), secret)
            .unwrap(),
        None => blame.with_cache(DerivedCache::open(config.clone()).unwrap()),
    };
    blame.with_observer(observed)
}
fn replace_service(fixture: &mut source_support::Fixture, blame: Blame) {
    until(|| fixture.engine.blame.admitted() == 0);
    Arc::get_mut(&mut fixture.engine).unwrap().blame = blame;
}
fn read(fixture: &source_support::Fixture, principal: &str) -> Vec<u8> {
    let response = Operation::new(20)
        .enter(|| {
            fixture
                .engine
                .blame(&fixture.slug, principal, &fixture.request())
        })
        .unwrap();
    let Response::Complete(body) = response else {
        panic!("expected complete source result");
    };
    let mut output = Vec::new();
    Operation::new(20)
        .enter(|| body.write_to(&mut output))
        .unwrap();
    drop(body);
    until(|| fixture.engine.blame.admitted() == 0);
    output
}

struct StoredRoot {
    id: String,
    body: Vec<u8>,
}
struct CacheWriter {
    client: tokio_postgres::Client,
    driver: tokio::task::JoinHandle<std::result::Result<(), tokio_postgres::Error>>,
    runtime: tokio::runtime::Runtime,
}
impl CacheWriter {
    fn new() -> Self {
        let mut config: tokio_postgres::Config = if let Ok(dsn) = std::env::var("GIT_RS_CACHE_DSN")
        {
            dsn.parse().unwrap()
        } else {
            let mut config = tokio_postgres::Config::new();
            let host = std::env::var("GIT_RS_CACHE_PGHOST")
                .or_else(|_| std::env::var("PGHOST"))
                .unwrap_or_else(|_| "127.0.0.1".into());
            let database = std::env::var("GIT_RS_CACHE_PGDATABASE")
                .or_else(|_| std::env::var("PGDATABASE"))
                .unwrap_or_else(|_| "git_rs".into());
            // Cache user/password must NEVER fall back to source credentials.
            let user =
                std::env::var("GIT_RS_CACHE_PGUSER").unwrap_or_else(|_| "git_rs_cache".into());
            let password = std::env::var("GIT_RS_CACHE_PGPASSWORD")
                .unwrap_or_else(|_| "synthetic-cache-only".into());
            config
                .host(&host)
                .dbname(&database)
                .user(&user)
                .password(&password);
            let port = std::env::var("GIT_RS_CACHE_PGPORT")
                .or_else(|_| std::env::var("PGPORT"))
                .unwrap();
            config.port(port.parse().unwrap());
            config
        };
        assert!(
            matches!(config.get_hosts(), [tokio_postgres::config::Host::Tcp(host)] if host == "127.0.0.1" || host == "localhost"),
            "owned loopback cache fixture only"
        );
        config.connect_timeout(Duration::from_secs(3));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (client, driver) = runtime.block_on(async {
            let (client, connection) = tokio::time::timeout(
                Duration::from_secs(5),
                config.connect(tokio_postgres::NoTls),
            )
            .await
            .unwrap()
            .unwrap();
            (client, tokio::spawn(connection))
        });
        let writer = Self {
            client,
            driver,
            runtime,
        };
        let privileges = writer.query("SELECT r.rolsuper,coalesce((SELECT has_table_privilege(current_user,c.oid,'UPDATE') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='git_rs' AND c.relname='repositories'),false) FROM pg_roles r WHERE r.rolname=current_user", &[]);
        assert_eq!(privileges.len(), 1);
        assert!(
            !privileges[0].get::<_, bool>(0),
            "attacker must not be a superuser"
        );
        assert!(
            !privileges[0].get::<_, bool>(1),
            "attacker must not have source write credentials"
        );
        writer
    }
    fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Vec<tokio_postgres::Row> {
        self.runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), self.client.query(sql, params))
                .await
                .unwrap()
                .unwrap()
        })
    }
    fn root(&self, namespace: &str, except: Option<&str>) -> StoredRoot {
        let rows = self.query("SELECT p.id::text,p.body,p.encoding FROM git_rs_cache.payloads p JOIN git_rs_cache.manifest_roots m ON m.namespace=p.namespace AND m.payload=p.id JOIN git_rs_cache.directory d ON d.namespace=p.namespace AND d.payload=p.id WHERE p.namespace=$1::text::uuid AND m.kind=1 AND d.state='ready' AND ($2::text IS NULL OR p.id<>$2::text::uuid)", &[&namespace, &except]);
        assert_eq!(rows.len(), 1, "exact complete-body root, not a checkpoint");
        assert_eq!(
            rows[0].get::<_, String>(2),
            "raw-v1",
            "bounded small descriptor remains raw, independently of compressed child storage"
        );
        StoredRoot {
            id: rows[0].get(0),
            body: rows[0].get(1),
        }
    }
    fn replace_root(&self, namespace: &str, root: &StoredRoot, bytes: &[u8]) {
        let digest = openssl::sha::sha256(bytes);
        self.query("UPDATE git_rs_cache.payloads SET body=$3,digest=$4,size=octet_length($3::bytea),stored_size=NULL,stored_digest=NULL WHERE namespace=$1::text::uuid AND id=$2::text::uuid RETURNING id::text", &[&namespace, &root.id, &bytes, &&digest[..]]);
    }
    fn forge_inline(&self, namespace: &str, root: &StoredRoot) -> Vec<u8> {
        let rows = self.query("SELECT p.id::text,p.body,p.object_key,p.encoding,p.size,p.digest,p.stored_size,p.stored_digest FROM git_rs_cache.manifest_children e JOIN git_rs_cache.payloads p ON p.namespace=e.namespace AND p.id=e.child WHERE e.namespace=$1::text::uuid AND e.parent=$2::text::uuid ORDER BY e.ordinal", &[&namespace, &root.id]);
        assert_eq!(
            rows.len(),
            1,
            "real porcelain must occupy one physically inline compressed chunk"
        );
        assert!(rows[0].get::<_, Option<String>>(2).is_none());
        let child: String = rows[0].get(0);
        assert_eq!(
            rows[0].get::<_, String>(3),
            "zstd-v1",
            "the attack must reach real compressed storage"
        );
        let stored: Vec<u8> = rows[0].get(1);
        assert_eq!(rows[0].get::<_, i64>(6), stored.len() as i64);
        assert_eq!(rows[0].get::<_, Vec<u8>>(7), openssl::sha::sha256(&stored));
        let logical_size = usize::try_from(rows[0].get::<_, i64>(4)).unwrap();
        assert!(
            logical_size <= 4 * 1024 * 1024,
            "owned single-chunk fixture bound"
        );
        let mut body = vec![0; logical_size];
        let mut decoder = zstd_safe::DCtx::try_create().unwrap();
        assert_eq!(
            decoder
                .decompress(body.as_mut_slice(), stored.as_slice())
                .unwrap(),
            logical_size
        );
        assert!(body.len() >= 4096);
        assert_eq!(rows[0].get::<_, i64>(4), body.len() as i64);
        assert_eq!(rows[0].get::<_, Vec<u8>>(5), openssl::sha::sha256(&body));
        let offset = body
            .windows(b"Synthetic Author".len())
            .position(|part| part == b"Synthetic Author")
            .expect("real native author metadata");
        body[offset..offset + 16].copy_from_slice(b"Malicious Author");
        let digest = openssl::sha::sha256(&body);
        // Test-owned safe dependency API, not application codec/FFI. Emit the
        // complete checksum/content-size/window contract so trusted unsigned
        // acceptance proves this is not merely an invalid compressed fixture.
        let mut encoder = zstd_safe::CCtx::try_create().unwrap();
        for parameter in [
            zstd_safe::CParameter::CompressionLevel(3),
            zstd_safe::CParameter::WindowLog(20),
            zstd_safe::CParameter::NbWorkers(0),
            zstd_safe::CParameter::ChecksumFlag(true),
            zstd_safe::CParameter::ContentSizeFlag(true),
        ] {
            encoder.set_parameter(parameter).unwrap();
        }
        encoder
            .set_pledged_src_size(Some(body.len() as u64))
            .unwrap();
        let mut forged = vec![0; zstd_safe::compress_bound(body.len())];
        let written = encoder.compress2(forged.as_mut_slice(), &body).unwrap();
        forged.truncate(written);
        let stored_digest = openssl::sha::sha256(&forged);
        let mut manifest = root.body.clone();
        let offset = b"git-rs-complete-artifact\0\x01".len() + 5 + 4;
        manifest[offset..offset + 32].copy_from_slice(&digest);
        self.query("BEGIN", &[]);
        self.query("UPDATE git_rs_cache.payloads SET body=$3,digest=$4,size=$5,stored_size=$6,stored_digest=$7 WHERE namespace=$1::text::uuid AND id=$2::text::uuid RETURNING id::text", &[&namespace, &child, &forged, &&digest[..], &(body.len() as i64), &(forged.len() as i64), &&stored_digest[..]]);
        self.query("UPDATE git_rs_cache.manifest_children SET digest=$3,size=$4 WHERE namespace=$1::text::uuid AND parent=$2::text::uuid RETURNING ordinal", &[&namespace, &root.id, &&digest[..], &(body.len() as i64)]);
        self.replace_root(namespace, root, &manifest);
        self.query("COMMIT", &[]);
        body
    }
}
impl Drop for CacheWriter {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

#[test]
#[ignore = "requires owned source/cache PostgreSQL and source S3 fixture"]
fn authenticated_body_rejects_cache_role_forgery_after_all_ordinary_digests_are_repaired() {
    let config = config();
    let first = Arc::new(Observed::default());
    let mut fixture =
        source_support::Fixture::new(service(&config, Some(&[7; 32]), first.clone()), 256);
    assert_eq!(read(&fixture, "bob"), fixture.expected);
    assert_eq!(read(&fixture, "bob"), fixture.expected);
    assert_eq!(
        first.hits.load(Ordering::Acquire),
        1,
        "must exercise a real signed ready-body hit before attack"
    );
    let attacker = CacheWriter::new();
    let root = attacker.root(&config.namespace, None);
    let forged = attacker.forge_inline(&config.namespace, &root);
    assert_ne!(forged, fixture.expected);
    // Prove this is a semantic-authentication attack, not a broken checksum or
    // foreign source: the deliberately trusted-writer mode accepts these bytes.
    let trusted = Arc::new(Observed::default());
    replace_service(&mut fixture, service(&config, None, trusted.clone()));
    assert_eq!(read(&fixture, "bob"), forged);
    assert_eq!(trusted.hits.load(Ordering::Acquire), 1);
    let protected = Arc::new(Observed::default());
    replace_service(
        &mut fixture,
        service(&config, Some(&[7; 32]), protected.clone()),
    );
    assert_eq!(
        read(&fixture, "bob"),
        fixture.expected,
        "forged metadata must become a miss and fresh source computation, never a partial forged response"
    );
    assert_eq!(protected.hits.load(Ordering::Acquire), 0);
    assert_eq!(protected.computes.load(Ordering::Acquire), 1);
    assert_eq!(fixture.leases(), 0);
}

#[test]
#[ignore = "requires owned source/cache PostgreSQL and source S3 fixture"]
fn signing_mode_rejects_unsigned_legacy_and_rotated_body_keys() {
    for old_key in [None, Some([7; 32])] {
        let config = config();
        let mut fixture = source_support::Fixture::new(
            service(&config, old_key.as_ref(), Arc::new(Observed::default())),
            2,
        );
        assert_eq!(read(&fixture, "bob"), fixture.expected);
        let observed = Arc::new(Observed::default());
        replace_service(
            &mut fixture,
            service(&config, Some(&[8; 32]), observed.clone()),
        );
        assert_eq!(read(&fixture, "bob"), fixture.expected);
        assert_eq!(
            observed.hits.load(Ordering::Acquire),
            0,
            "unsigned legacy and another signing key must both miss"
        );
        assert_eq!(observed.computes.load(Ordering::Acquire), 1);
    }
}

#[test]
#[ignore = "requires owned source/cache PostgreSQL and source S3 fixture"]
fn authenticated_body_rejects_same_payload_replay_under_another_authorized_source_scope() {
    let config = config();
    let mut fixture = source_support::Fixture::new(
        service(&config, Some(&[7; 32]), Arc::new(Observed::default())),
        2,
    );
    assert_eq!(read(&fixture, "bob"), fixture.expected);
    let attacker = CacheWriter::new();
    let bob = attacker.root(&config.namespace, None);
    assert_eq!(read(&fixture, "alice"), fixture.expected);
    let alice = attacker.root(&config.namespace, Some(&bob.id));
    // Both authorized requests derive identical bytes; only their full keys
    // differ. Copy a valid tag as well as the manifest, and repair its root SHA.
    attacker.replace_root(&config.namespace, &alice, &bob.body);
    let observed = Arc::new(Observed::default());
    replace_service(
        &mut fixture,
        service(&config, Some(&[7; 32]), observed.clone()),
    );
    assert_eq!(read(&fixture, "alice"), fixture.expected);
    assert_eq!(
        observed.hits.load(Ordering::Acquire),
        0,
        "valid foreign-scope signature cannot authorize the same output bytes"
    );
    assert_eq!(observed.computes.load(Ordering::Acquire), 1);
}

#[test]
#[ignore = "requires owned source/cache PostgreSQL and source S3 fixture"]
fn authenticated_ready_body_still_requires_live_source_authority_before_output() {
    for fence in ["acl", "generation", "epoch"] {
        let config = config();
        let observed = Arc::new(Observed::default());
        let fixture =
            source_support::Fixture::new(service(&config, Some(&[7; 32]), observed.clone()), 2);
        assert_eq!(read(&fixture, "bob"), fixture.expected);
        let body = fixture.complete();
        assert_eq!(observed.hits.load(Ordering::Acquire), 1);
        match fence {
            "acl" => fixture.revoke(),
            "generation" => fixture.fence("generation=generation+1"),
            _ => fixture.fence("storage_epoch=storage_epoch+1"),
        }
        let mut bytes = Vec::new();
        assert!(
            Operation::new(5)
                .enter(|| body.write_to(&mut bytes))
                .is_err()
        );
        assert!(
            bytes.is_empty(),
            "valid signature never substitutes for source authority"
        );
        drop(body);
        until(|| fixture.engine.blame.admitted() == 0);
        assert_eq!(fixture.leases(), 0);
    }
}

#[test]
#[ignore = "requires owned source/cache PostgreSQL and source S3 fixture"]
fn signing_mode_rejects_stripped_and_incompatible_body_authentication_envelopes() {
    for stripped in [true, false] {
        let config = config();
        let mut fixture = source_support::Fixture::new(
            service(&config, Some(&[7; 32]), Arc::new(Observed::default())),
            2,
        );
        assert_eq!(read(&fixture, "bob"), fixture.expected);
        let attacker = CacheWriter::new();
        let root = attacker.root(&config.namespace, None);
        let manifest_length = b"git-rs-complete-artifact\0\x01".len() + 5 + 36;
        assert!(
            root.body.len() > manifest_length,
            "current signing producer must emit a body authentication envelope"
        );
        let mut bytes = root.body.clone();
        if stripped {
            bytes.truncate(manifest_length);
        } else {
            bytes[manifest_length] ^= 1;
        }
        attacker.replace_root(&config.namespace, &root, &bytes);
        let observed = Arc::new(Observed::default());
        replace_service(
            &mut fixture,
            service(&config, Some(&[7; 32]), observed.clone()),
        );
        assert_eq!(read(&fixture, "bob"), fixture.expected);
        assert_eq!(observed.hits.load(Ordering::Acquire), 0);
        assert_eq!(observed.computes.load(Ordering::Acquire), 1);
    }
}
