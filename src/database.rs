//! A bounded process-local Tokio PostgreSQL pool. Failed or interrupted sessions
//! are destroyed, never recycled with an unfinished transaction or unread reply.
use crate::{
    Result, invalid,
    metrics::{self, Metric, Stage},
    process,
};
use deadpool_postgres::{Client, Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::StreamExt;
use native_tls::{Certificate, Identity, Protocol, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use std::{env, fs, pin::pin, sync::OnceLock, time::Duration};
use tokio::runtime::{Builder, Runtime};
use tokio_postgres::{
    Config, SimpleQueryMessage,
    config::{SslMode, TargetSessionAttrs},
};

const CONNECTIONS: usize = 8;
const OPTIONS: &str =
    "-c statement_timeout=30000 -c lock_timeout=5000 -c search_path=git_rs,pg_catalog";

struct Database {
    runtime: Runtime,
    pool: Pool,
    tls: MakeTlsConnector,
}

// ClientWrapper aborts its connection task when destroyed. Returning an object
// to deadpool is allowed only after complete response consumption and reset.
struct Checkout(Option<Client>, MakeTlsConnector);
impl Checkout {
    fn client(&self) -> &Client {
        self.0.as_ref().expect("owned PostgreSQL checkout")
    }
    fn recycle(mut self) {
        drop(self.0.take());
    }
}
impl Drop for Checkout {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            let cancel = client.cancel_token();
            let tls = self.1.clone();
            // Socket close alone need not interrupt a running server query.
            // This session is never recycled, even if cancellation cannot arrive.
            tokio::spawn(async move {
                let _ =
                    tokio::time::timeout(Duration::from_secs(1), cancel.cancel_query(tls)).await;
            });
            drop(Client::take(client));
        }
    }
}

impl Database {
    fn new() -> Result<Self> {
        let host = env::var("PGHOST").map_err(|_| invalid("PGHOST is required"))?;
        let mode = env::var("PGSSLMODE").unwrap_or_default();
        let plaintext = mode == "disable"
            && env::var("GIT_RS_DEV_HTTP").as_deref() == Ok("1")
            && matches!(host.as_str(), "127.0.0.1" | "localhost");
        if mode != "verify-full" && !plaintext {
            return Err(invalid(
                "PostgreSQL requires PGSSLMODE=verify-full outside loopback development",
            ));
        }
        let mut config = Config::new();
        config
            .host(&host)
            .port(
                env::var("PGPORT")
                    .unwrap_or_else(|_| "5432".into())
                    .parse()?,
            )
            .user(&env::var("PGUSER").map_err(|_| invalid("PGUSER is required"))?)
            .dbname(&env::var("PGDATABASE").map_err(|_| invalid("PGDATABASE is required"))?)
            .application_name("git-rs")
            .connect_timeout(Duration::from_secs(5))
            .options(OPTIONS)
            .ssl_mode(if plaintext {
                SslMode::Disable
            } else {
                SslMode::Require
            });
        if let Ok(password) = env::var("PGPASSWORD") {
            config.password(password);
        }
        match env::var("PGTARGETSESSIONATTRS").as_deref() {
            Ok("read-write") => {
                config.target_session_attrs(TargetSessionAttrs::ReadWrite);
            }
            Ok("any") | Err(_) => {}
            _ => return Err(invalid("unsupported PGTARGETSESSIONATTRS")),
        }
        let mut tls = TlsConnector::builder();
        tls.min_protocol_version(Some(Protocol::Tlsv12));
        if let Ok(path) = env::var("PGSSLROOTCERT") {
            tls.disable_built_in_roots(true);
            let roots = openssl::x509::X509::stack_from_pem(&fs::read(path)?)?;
            if roots.is_empty() {
                return Err(invalid("PostgreSQL CA bundle is empty"));
            }
            for root in roots {
                tls.add_root_certificate(Certificate::from_der(&root.to_der()?)?);
            }
        }
        match (env::var("PGSSLCERT"), env::var("PGSSLKEY")) {
            (Ok(cert), Ok(key)) => {
                tls.identity(Identity::from_pkcs8(&fs::read(cert)?, &fs::read(key)?)?);
            }
            (Err(_), Err(_)) => {}
            _ => {
                return Err(invalid(
                    "PGSSLCERT and PGSSLKEY must be configured together",
                ));
            }
        }
        // native-tls verifies the hostname and certificate chain by default.
        // Never enable its invalid-certificate/invalid-hostname bypasses.
        let tls = MakeTlsConnector::new(tls.build()?);
        let manager = Manager::from_config(
            config,
            tls.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(CONNECTIONS)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .create_timeout(Some(Duration::from_secs(5)))
            .build()?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        Ok(Self { runtime, pool, tls })
    }

    fn rows(
        &self,
        queries: &[(&str, Option<Stage>)],
        mut consume: impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        process::check_operation()?;
        let remaining = if process::Operation::current().is_some() {
            process::remaining()?
        } else {
            Duration::from_secs(process::DEADLINE)
        };
        self.runtime.block_on(async {
            let work = async {
                let acquire = metrics::Timer::start(Metric::Stage(Stage::SqlAcquire));
                let connection = Checkout(
                    Some(
                        self.pool
                            .get()
                            .await
                            .map_err(|_| invalid("PostgreSQL connection acquisition failed"))?,
                    ),
                    self.tls.clone(),
                );
                acquire.finish(true);
                for (query, stage) in queries {
                    let stage = stage.map(|stage| metrics::Timer::start(Metric::Stage(stage)));
                    let query_timer = metrics::Timer::start(Metric::Stage(Stage::SqlQuery));
                    let mut processed = 0;
                    let mut rows = pin!(
                        connection
                            .client()
                            .simple_query_raw(query)
                            .await
                            .map_err(sql_error)?
                    );
                    while let Some(message) = rows.next().await {
                        if let SimpleQueryMessage::Row(row) = message.map_err(sql_error)? {
                            process::check_operation()?;
                            let mut line = String::new();
                            for index in 0..row.len() {
                                let value = row.get(index).unwrap_or("");
                                if line.len().saturating_add(value.len()).saturating_add(1) > 8192 {
                                    return Err(invalid("metadata row length limit exceeded"));
                                }
                                if index != 0 {
                                    line.push('\t');
                                }
                                line.push_str(value);
                            }
                            consume(&line)?;
                            processed = (processed + 1) % 256;
                            if processed == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                    query_timer.finish(true);
                    if let Some(stage) = stage {
                        stage.finish(true);
                    }
                }
                // Also protect callers that accidentally leave an explicit tx or
                // session SET behind. RESET ALL restores our startup options.
                let reset = metrics::Timer::start(Metric::Stage(Stage::SqlReset));
                connection
                    .client()
                    .batch_execute("ROLLBACK; RESET ALL;")
                    .await
                    .map_err(sql_error)?;
                reset.finish(true);
                connection.recycle();
                Ok(())
            };
            let cancelled = async {
                loop {
                    if let Err(error) = process::check_operation() {
                        return error;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            tokio::select! {
                biased;
                error = cancelled => Err(error),
                result = tokio::time::timeout(remaining, work) => result
                    .map_err(|_| invalid("PostgreSQL operation deadline exceeded"))?,
            }
        })
    }
}

fn sql_error(error: tokio_postgres::Error) -> Box<dyn std::error::Error + Send + Sync> {
    // Database messages may contain identities, values or SQL. Publish SQLSTATE
    // only; transport/configuration failures never include connection secrets.
    match error.code() {
        Some(code) => invalid(&format!(
            "PostgreSQL operation failed (SQLSTATE {})",
            code.code()
        )),
        None => invalid("PostgreSQL transport failed"),
    }
}

pub(crate) fn rows(query: &str, consume: impl FnMut(&str) -> Result<()>) -> Result<()> {
    session_rows(&[(query, None)], consume)
}

pub(crate) fn session_rows(
    queries: &[(&str, Option<Stage>)],
    consume: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    static DATABASE: OnceLock<std::result::Result<Database, String>> = OnceLock::new();
    let database = DATABASE
        .get_or_init(|| {
            metrics::measure(Metric::Stage(Stage::SqlInit), Database::new)
                .map_err(|_| "PostgreSQL configuration invalid".into())
        })
        .as_ref()
        .map_err(|message| invalid(message))?;
    database.rows(queries, consume)
}
