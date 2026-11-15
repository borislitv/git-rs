#![forbid(unsafe_code)]

use git_rs::{metadata, process};
use std::io::Write;
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires the isolated real DNS and TLS PostgreSQL fixture in docs/pg-dns.md"]
fn cache_pg_dns_cancel_recovery_without_blocking_backlog() {
    use git_rs::derived_cache::{CacheConfig, DerivedCache};
    use std::io::Read;
    use std::net::TcpStream;

    fn control(path: &str) -> bool {
        let Ok(mut stream) = TcpStream::connect("127.0.0.1:18054") else {
            return false;
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(4)))
            .unwrap();
        if write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )
        .is_err()
        {
            return false;
        }
        let mut response = String::new();
        stream.read_to_string(&mut response).is_ok() && response.starts_with("HTTP/1.0 200 ")
    }

    struct Release;
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = control("/release-all");
        }
    }

    assert_eq!(
        std::env::var("GIT_RS_PG_DNS_ISOLATED_FIXTURE").as_deref(),
        Ok("1")
    );
    assert_eq!(std::env::var("PGSSLMODE").as_deref(), Ok("verify-full"));
    assert!(std::env::var_os("GIT_RS_CACHE_S3_ENDPOINT").is_none());
    let raw = git_rs::scratch::identifier().unwrap();
    let namespace = format!(
        "{}-{}-{}-{}-{}",
        &raw[..8],
        &raw[8..12],
        &raw[12..16],
        &raw[16..20],
        &raw[20..]
    );
    let mut config = CacheConfig::new(namespace.clone(), namespace.clone());
    config.connections = 2;
    assert!(control("/recover"));
    let preflight = DerivedCache::open(config.clone()).unwrap();
    process::Operation::new(10)
        .enter(|| preflight.migrate())
        .expect("TLS SQL preflight must succeed before testing DNS starvation");
    drop(preflight);
    process::Operation::new(10).enter(|| {
        metadata::sql(&format!(
            "DELETE FROM git_rs_cache.maintenance_claims WHERE namespace={}::uuid; DELETE FROM git_rs_cache.namespaces WHERE id={}::uuid;",
            metadata::text(&namespace),
            metadata::text(&namespace),
        ))
        .unwrap();
        assert_eq!(
            metadata::sql(&format!(
                "SELECT count(*) FROM git_rs_cache.namespaces WHERE id={}::uuid;",
                metadata::text(&namespace),
            ))
            .unwrap()
            .trim(),
            "0",
            "preflight must not satisfy the recovery SQL witness"
        );
    });
    eprintln!("pg-dns: TLS SQL preflight passed");
    let cache = DerivedCache::open(config).unwrap();
    // Drop this before the cache/runtime on every failure, including assertion
    // unwind. The fixture's old libc requests must not delay test teardown.
    let _release = Release;
    assert!(control("/start-stall"));
    for round in 1..=2 {
        assert!(control("/next-round"));
        let operation = process::Operation::new(60);
        std::thread::scope(|scope| {
            let request = scope.spawn(|| operation.enter(|| cache.migrate()));
            let observed = control("/observed");
            let started = Instant::now();
            operation.cancel();
            let result = request.join().unwrap();
            assert!(
                observed,
                "round {round} must reach real A and AAAA resolution before cancellation"
            );
            assert!(
                result.is_err(),
                "cancelled DNS acquisition must not run migration"
            );
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "cancellation must return independently of system DNS"
            );
            eprintln!("pg-dns: cancelled round {round} after observed DNS");
        });
    }
    assert!(control("/recover"));
    let result = process::Operation::new(10).enter(|| cache.migrate());
    // Keep old DNS requests gated until AFTER this attempt. Releasing them first
    // would make the GAI mutant pass by freeing its two blocking workers.
    assert!(control("/release-all"));
    result.expect("fresh cache acquisition must not queue behind cancelled blocking DNS");
    process::Operation::new(10).enter(|| {
        assert_eq!(metadata::sql(&format!(
            "SELECT count(*) FROM git_rs_cache.namespaces WHERE id={}::uuid AND owner={}::uuid;",
            metadata::text(&namespace), metadata::text(&namespace),
        )).unwrap().trim(), "1", "recovery must execute real SQL, not merely resolve a hostname");
        metadata::sql(&format!(
            "DELETE FROM git_rs_cache.maintenance_claims WHERE namespace={}::uuid; DELETE FROM git_rs_cache.namespaces WHERE id={}::uuid;",
            metadata::text(&namespace),
            metadata::text(&namespace)
        ))
        .unwrap();
    });
}
