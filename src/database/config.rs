//! Connection policy shared by independent metadata and artifact-cache pools.
use crate::{Result, invalid};
use native_tls::{Certificate, Identity, Protocol, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::{env, fs, time::Duration};
use tokio_postgres::{
    Config,
    config::{SslMode, TargetSessionAttrs},
};

pub(crate) struct ConnectionConfig {
    pub config: Config,
    pub tls: MakeTlsConnector,
}

impl ConnectionConfig {
    pub fn from_env(cache: bool) -> Result<Self> {
        let setting = |name: &str| {
            if cache {
                env::var(format!("GIT_RS_CACHE_{name}")).ok().or_else(|| {
                    (!matches!(name, "PGUSER" | "PGPASSWORD"))
                        .then(|| env::var(name).ok())
                        .flatten()
                })
            } else {
                env::var(name).ok()
            }
        };
        let dsn = cache.then(|| env::var("GIT_RS_CACHE_DSN").ok()).flatten();
        let mut config = if let Some(dsn) = dsn.as_deref() {
            dsn.parse::<Config>()
                .map_err(|_| invalid("invalid cache PostgreSQL DSN"))?
        } else {
            let mut config = Config::new();
            config
                .host(&setting("PGHOST").ok_or_else(|| invalid("PGHOST is required"))?)
                .port(setting("PGPORT").unwrap_or_else(|| "5432".into()).parse()?)
                .user(
                    &setting("PGUSER")
                        .or_else(|| cache.then(|| "git_rs_cache".into()))
                        .ok_or_else(|| invalid("PGUSER is required"))?,
                )
                .dbname(&setting("PGDATABASE").ok_or_else(|| invalid("PGDATABASE is required"))?);
            if let Some(password) = setting("PGPASSWORD") {
                config.password(password);
            }
            config
        };
        let mode = setting("PGSSLMODE").unwrap_or_default();
        let plaintext = mode == "disable"
            && env::var("GIT_RS_DEV_HTTP").as_deref() == Ok("1")
            && config.get_hosts().len() == 1
            && config
                .get_hostaddrs()
                .iter()
                .all(|address| address.is_loopback())
            && matches!(&config.get_hosts()[0], tokio_postgres::config::Host::Tcp(host)
                if matches!(host.as_str(), "127.0.0.1" | "localhost"));
        if mode != "verify-full" && !plaintext {
            return Err(invalid(
                "PostgreSQL requires PGSSLMODE=verify-full outside loopback development",
            ));
        }
        if !plaintext
            && (config.get_hosts().is_empty()
                || config
                    .get_hosts()
                    .iter()
                    .any(|host| !matches!(host, tokio_postgres::config::Host::Tcp(_))))
        {
            return Err(invalid("verified PostgreSQL requires a TCP hostname"));
        }
        config.application_name(if cache { "git-rs-cache" } else { "git-rs" })
            .connect_timeout(Duration::from_secs(5))
            .options(if cache {
                "-c statement_timeout=5000 -c lock_timeout=1000 -c search_path=git_rs_cache,pg_catalog -c synchronous_commit=on"
            } else {
                "-c statement_timeout=30000 -c lock_timeout=5000 -c search_path=git_rs,pg_catalog"
            })
            .ssl_mode(if plaintext { SslMode::Disable } else { SslMode::Require });
        match setting("PGTARGETSESSIONATTRS").as_deref() {
            Some("read-write") => {
                config.target_session_attrs(TargetSessionAttrs::ReadWrite);
            }
            Some("any") | None => {}
            _ => return Err(invalid("unsupported PGTARGETSESSIONATTRS")),
        }
        let mut tls = TlsConnector::builder();
        tls.min_protocol_version(Some(Protocol::Tlsv12));
        if let Some(path) = setting("PGSSLROOTCERT") {
            tls.disable_built_in_roots(true);
            let roots = openssl::x509::X509::stack_from_pem(&fs::read(path)?)?;
            if roots.is_empty() {
                return Err(invalid("PostgreSQL CA bundle is empty"));
            }
            for root in roots {
                tls.add_root_certificate(Certificate::from_der(&root.to_der()?)?);
            }
        }
        match (setting("PGSSLCERT"), setting("PGSSLKEY")) {
            (Some(cert), Some(key)) => {
                tls.identity(Identity::from_pkcs8(&fs::read(cert)?, &fs::read(key)?)?);
            }
            (None, None) => {}
            _ => {
                return Err(invalid(
                    "PGSSLCERT and PGSSLKEY must be configured together",
                ));
            }
        }
        Ok(Self {
            config,
            tls: MakeTlsConnector::new(tls.build()?),
        })
    }
}
