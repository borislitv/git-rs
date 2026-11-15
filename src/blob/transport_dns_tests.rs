//! Real system-configured DNS regression; run only via tests/support/s3_dns_fixture.py.
//! No fake Resolve, SQL fixture, source authorization or publication claim.

use super::{Store, global};
use crate::{Result, process::Operation};
use git_rs_core::Control;
use std::{
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

fn store(host: &str) -> Store {
    // Loopback-only fixture, deliberately without changing production endpoint
    // validation or introducing a runtime resolver selector.
    Store {
        endpoint: format!("http://{host}:18053"),
        bucket: "synthetic".into(),
        region: "us-east-1".into(),
        access: "synthetic".into(),
        secret: "synthetic".into(),
    }
}

fn read(host: &str, operation: &Arc<Operation>) -> (Result<()>, [u8; 4]) {
    let mut bytes = [b'?'; 4];
    let control = Control::new(operation.deadline());
    let result = operation.enter(|| store(host).source_range("probe", 4, 0, &mut bytes, &control));
    (result, bytes)
}

fn assert_read(host: &str) {
    let (result, bytes) = read(host, &Operation::new(10));
    result.unwrap_or_else(|error| panic!("real DNS/range read for {host}: {error}"));
    assert_eq!(bytes, *b"DATA", "immutable range bytes for {host}");
}

fn fixture_control(path: &str) {
    let transport = global().unwrap();
    transport.runtime.block_on(async {
        let response = transport
            .client
            .get(format!("http://127.0.0.1:18053/{path}"))
            .timeout(Duration::from_secs(4))
            .send()
            .await
            .expect("isolated DNS fixture control request");
        assert!(response.status().is_success(), "DNS fixture: {path}");
    });
}

fn assert_blocking_capacity() {
    // A real task on the production runtime, not a mock of its resolver. Four
    // outstanding libc lookups make this fail even after their callers returned.
    let transport = global().unwrap();
    let probe = transport.runtime.spawn_blocking(|| 42);
    let result = transport
        .runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(1), probe).await });
    assert_eq!(
        result
            .expect("cancelled DNS retained all four S3 blocking workers")
            .unwrap(),
        42,
    );
}

struct CancelOnDrop<'a>(&'a Operation);

impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[test]
#[ignore = "requires an isolated invalid or unreadable resolv.conf; see docs/s3-dns.md"]
fn s3_dns_system_config_failure_is_fail_closed() {
    assert!(matches!(
        std::env::var("GIT_RS_S3_DNS_ISOLATED_FIXTURE").as_deref(),
        Ok("invalid-config" | "unreadable-config"),
    ));
    // Exercise public Store construction, not the adapter's private constructor.
    // The fixture supplies otherwise valid synthetic HTTPS settings. This child
    // must not have initialized the process-global transport through another test.
    let started = Instant::now();
    let error = Store::from_env()
        .err()
        .expect("invalid/unreadable system DNS must prevent Store initialization");
    assert!(
        error
            .to_string()
            .contains("S3 system DNS configuration failed"),
        "configuration error must propagate through Store::from_env: {error}",
    );
    assert!(started.elapsed() < Duration::from_secs(3));
}

fn assert_hosts_only_read(mode: &str, host: &str) {
    assert_eq!(
        std::env::var("GIT_RS_S3_DNS_ISOLATED_FIXTURE").as_deref(),
        Ok(mode),
        "run the corresponding isolated hosts fixture mode",
    );
    fixture_control("health");
    fixture_control("hosts-unqueried");
    // Only IPv4 is present in /etc/hosts. Every DNS type for this name and
    // its search expansions is blackholed, never answered with NXDOMAIN.
    // A successful real range read must not depend on the missing AAAA family
    // or on trying a search-expanded name before the original hosts alias.
    assert_read(host);
    fixture_control("hosts-unqueried");
}

#[test]
#[ignore = "requires the isolated hosts-fqdn DNS blackhole fixture; see docs/s3-dns.md"]
fn s3_dns_ipv4_hosts_only_does_not_query_dns() {
    assert_hosts_only_read("hosts-fqdn", "hosts.dns-fixture.test");
}

#[test]
#[ignore = "requires the isolated hosts-short DNS blackhole fixture; see docs/s3-dns.md"]
fn s3_dns_short_hosts_alias_does_not_query_dns() {
    assert_hosts_only_read("hosts-short", "hostsalias");
}

#[test]
#[ignore = "requires isolated resolv.conf/hosts and the real DNS blackhole fixture; see docs/s3-dns.md"]
fn s3_dns_blackhole_cancel_recovery_without_blocking_backlog() {
    assert_eq!(
        std::env::var("GIT_RS_S3_DNS_ISOLATED_FIXTURE").as_deref(),
        Ok("1"),
        "run tests/support/s3_dns_fixture.py only in the documented isolated container",
    );
    fixture_control("health");

    // The short name exists only under resolv.conf's search. Hosts-only cases
    // run as separate tests so their RED does not mask this cancellation gate.
    assert_read("127.0.0.1");
    assert_read("searchprobe");
    fixture_control("observed?name=searchprobe.dns-fixture.test");

    let started = Instant::now();
    let (result, bytes) = read("timeout.stall.dns-fixture.test", &Operation::new(15));
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("S3 HTTP request failed"),
        "the unchanged five-second connect timeout must end the blackholed lookup",
    );
    assert!(
        started.elapsed() >= Duration::from_secs(4),
        "lookup failed before its timeout",
    );
    assert!(
        started.elapsed() < Duration::from_secs(9),
        "connect timeout was not bounded",
    );
    assert_eq!(bytes, [b'?'; 4], "failed DNS must not write response bytes");
    fixture_control("observed?name=timeout.stall.dns-fixture.test");
    assert_blocking_capacity();

    for ordinal in 0..12 {
        let host = format!("cancel-{ordinal}.stall.dns-fixture.test");
        let operation = Operation::new(15);
        thread::scope(|scope| {
            let (send, receive) = mpsc::sync_channel(1);
            let worker_host = host.clone();
            let worker_operation = operation.clone();
            let worker = scope.spawn(move || {
                send.send(read(&worker_host, &worker_operation)).unwrap();
            });
            // On assertion/setup failure, cancel before the scope joins too.
            let _cancel = CancelOnDrop(&operation);
            // Synchronize cancellation with an actual outbound DNS query, not a
            // sleep, fake resolver callback or an unpolled request future.
            fixture_control(&format!("observed?name={host}"));
            let cancelled = Instant::now();
            operation.cancel();
            let (result, bytes) = receive
                .recv_timeout(Duration::from_secs(3))
                .expect("operation cancellation did not return the DNS waiter");
            worker.join().unwrap();
            assert!(result.unwrap_err().to_string().contains("cancelled"));
            assert!(cancelled.elapsed() < Duration::from_secs(3));
            assert_eq!(
                bytes, [b'?'; 4],
                "cancelled DNS must not write response bytes"
            );
        });
        assert_blocking_capacity();
    }

    // Recover the *same* cancelled name. No successful answer existed to cache.
    // This catches poisoned/shared resolver state and unusable returned permits.
    fixture_control("recover");
    assert_read("cancel-11.stall.dns-fixture.test");
    assert_blocking_capacity();
}
