use super::{ManagedManager, Manager};
use crate::database::config::ConnectionConfig;
use postgres_native_tls::MakeTlsConnector;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio_postgres::{
    Config,
    config::{SslMode, TargetSessionAttrs},
};

#[test]
fn manager_rejects_unsupported_routes_and_cardinality_before_connecting() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let tls = MakeTlsConnector::new(native_tls::TlsConnector::new().unwrap());
    let mut hostaddr_only = Config::new();
    hostaddr_only.hostaddr("127.0.0.1".parse().unwrap());
    let mut unix = Config::new();
    unix.host("/synthetic/no-postgres-socket");
    let mut mismatched = Config::new();
    mismatched.host("no-network.invalid").host("other.invalid");
    mismatched.hostaddr("127.0.0.1".parse().unwrap());
    let mut bad_ports = Config::new();
    bad_ports.host("no-network.invalid").port(1).port(2);
    for (config, expected) in [
        (hostaddr_only, "PostgreSQL requires a TCP hostname"),
        (unix, "PostgreSQL requires a TCP hostname"),
        (
            mismatched,
            "invalid PostgreSQL host/address/port cardinality",
        ),
        (
            bad_ports,
            "invalid PostgreSQL host/address/port cardinality",
        ),
    ] {
        let manager = Manager::new(config, tls.clone());
        let error = runtime.block_on(manager.create()).err().unwrap();
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
#[ignore = "requires disposable TLS PostgreSQL configured with PG*; run serially"]
fn manager_preserves_startup_settings_sdk_statement_cache_and_detach() {
    let ConnectionConfig { mut config, tls } = ConnectionConfig::from_env(false).unwrap();
    assert_eq!(std::env::var("PGSSLMODE").unwrap(), "verify-full");
    config
        .application_name("pg-dns-settings-contract")
        .options("-c statement_timeout=12345 -c lock_timeout=2345 -c synchronous_commit=on")
        .keepalives(true)
        .keepalives_idle(Duration::from_secs(17))
        .keepalives_interval(Duration::from_secs(3))
        .keepalives_retries(2)
        .tcp_user_timeout(Duration::from_secs(19))
        .target_session_attrs(TargetSessionAttrs::ReadWrite);
    let manager = Manager::new(config, tls);
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut connection = manager.create().await.unwrap();
        let row = connection.query_one(
            "SELECT current_setting('application_name'), current_setting('statement_timeout'), current_setting('lock_timeout'), current_setting('synchronous_commit'), ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()",
            &[],
        ).await.unwrap();
        assert_eq!(row.get::<_, &str>(0), "pg-dns-settings-contract");
        assert_eq!(row.get::<_, &str>(1), "12345ms");
        assert_eq!(row.get::<_, &str>(2), "2345ms");
        assert_eq!(row.get::<_, &str>(3), "on");
        assert!(row.get::<_, bool>(4), "the settings contract must use TLS");
        let query = "SELECT 73::integer";
        let statement = connection.prepare_cached(query).await.unwrap();
        assert_eq!(connection.query_one(&statement, &[]).await.unwrap().get::<_, i32>(0), 73);
        assert_eq!(connection.statement_cache.size(), 1);
        connection.manager.statement_caches.clear();
        assert_eq!(connection.statement_cache.size(), 0, "SDK create must attach the cache");
        connection.prepare_cached(query).await.unwrap();
        manager.recycle(&mut connection, &deadpool::managed::Metrics::default()).await.unwrap();
        manager.detach(&mut connection);
        connection.manager.statement_caches.clear();
        assert_eq!(connection.statement_cache.size(), 1, "detach must remove the SDK registry entry");
    });
}

#[test]
#[ignore = "requires disposable TLS PG, PG* and GIT_RS_PG_DNS_ADDRESS; run serially"]
fn manager_host_fallback_preserves_tls_identity_and_cancel_endpoint() {
    let ConnectionConfig { config, tls } = ConnectionConfig::from_env(false).unwrap();
    assert_eq!(std::env::var("PGSSLMODE").as_deref(), Ok("verify-full"));
    assert_eq!(config.get_hosts().len(), 1);
    let tokio_postgres::config::Host::Tcp(hostname) = &config.get_hosts()[0] else {
        panic!("TLS fixture requires a TCP hostname")
    };
    let address = std::env::var("GIT_RS_PG_DNS_ADDRESS")
        .unwrap()
        .parse()
        .unwrap();
    let port = config.get_ports()[0];
    // This is a test input, not a second reconstruction algorithm: defaults are
    // intentional except credentials, TLS and the explicit two-host route.
    let credentials = || {
        let mut input = Config::new();
        input
            .user(config.get_user().unwrap())
            .dbname(config.get_dbname().unwrap());
        if let Some(password) = config.get_password() {
            input.password(password);
        }
        input
            .ssl_mode(SslMode::Require)
            .connect_timeout(Duration::from_secs(5));
        input
    };
    let mut fallback = credentials();
    fallback
        .host(hostname)
        .host(hostname)
        .hostaddr(address)
        .hostaddr(address)
        .port(0)
        .port(port);
    let manager = Manager::new(fallback, tls.clone());
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let connection = manager
            .create()
            .await
            .expect("second configured host/port must remain reachable");
        let observer = manager.create().await.unwrap();
        let pid: i32 = connection
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        let token = connection.cancel_token();
        let (query, ()) = tokio::join!(connection.simple_query("SELECT pg_sleep(20)"), async {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let row = observer
                        .query_one(
                            "SELECT wait_event = 'PgSleep' FROM pg_stat_activity WHERE pid=$1",
                            &[&pid],
                        )
                        .await
                        .unwrap();
                    if row.get::<_, Option<bool>>(0) == Some(true) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("query must really enter PostgreSQL before cancellation");
            tokio::time::timeout(Duration::from_secs(2), token.cancel_query(tls.clone()))
                .await
                .unwrap()
                .unwrap();
        });
        assert_eq!(query.unwrap_err().code().unwrap().code(), "57014");
        let mut wrong_identity = credentials();
        wrong_identity
            .host("wrong-identity.invalid")
            .hostaddr(address)
            .port(port);
        assert!(
            Manager::new(wrong_identity, tls).create().await.is_err(),
            "numeric dialing must not replace the original TLS hostname"
        );
        let healthy = manager
            .create()
            .await
            .expect("the same PG endpoint must remain healthy after TLS rejection");
        assert_eq!(
            healthy
                .query_one("SELECT 91::integer", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            91
        );
    });
}
