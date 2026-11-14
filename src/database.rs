//! A bounded process-local Tokio PostgreSQL pool. Failed or interrupted sessions
//! are destroyed, never recycled with an unfinished transaction or unread reply.
use crate::{
    Result, invalid,
    metrics::{self, Metric, Stage},
    process,
};
use deadpool_postgres::{Client, Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::StreamExt;
use postgres_native_tls::MakeTlsConnector;
use std::{pin::pin, sync::OnceLock, time::Duration};
use tokio::runtime::{Builder, Runtime};
use tokio_postgres::SimpleQueryMessage;

pub(crate) mod config;

const CONNECTIONS: usize = 8;

struct Database {
    runtime: Runtime,
    pool: Pool,
    tls: MakeTlsConnector,
}

// ClientWrapper aborts its connection task when destroyed. Returning an object
// to deadpool is allowed only after complete response consumption and reset.
pub(crate) struct Checkout(pub Option<Client>, pub MakeTlsConnector);
impl Checkout {
    pub(crate) fn client(&self) -> &Client {
        self.0.as_ref().expect("owned PostgreSQL checkout")
    }
    pub(crate) fn recycle(mut self) {
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
        let config::ConnectionConfig { config, tls } = config::ConnectionConfig::from_env(false)?;
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

pub(crate) fn sql_error(error: tokio_postgres::Error) -> Box<dyn std::error::Error + Send + Sync> {
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
