use crate::{
    Result,
    database::{Checkout, config::ConnectionConfig, sql_error},
    invalid, process,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::StreamExt;
use postgres_native_tls::MakeTlsConnector;
use std::{future::Future, pin::pin, time::Duration};
use tokio::runtime::{Builder, Runtime};
use tokio_postgres::{
    Row,
    types::{ToSql, Type},
};

pub(crate) struct Postgres {
    runtime: Runtime,
    pool: Pool,
    tls: MakeTlsConnector,
}

impl Postgres {
    pub fn from_env(connections: usize) -> Result<Self> {
        let ConnectionConfig { config, tls } = ConnectionConfig::from_env(true)?;
        let manager = Manager::from_config(
            config,
            tls.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(connections)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .create_timeout(Some(Duration::from_secs(5)))
            .build()?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(2)
            .thread_name("git-rs-cache")
            .enable_all()
            .build()?;
        Ok(Self { runtime, pool, tls })
    }

    fn run<T>(&self, work: impl Future<Output = Result<T>>) -> Result<T> {
        process::check_operation()?;
        let remaining = if process::Operation::current().is_some() {
            process::remaining()?.min(Duration::from_secs(10))
        } else {
            Duration::from_secs(10)
        };
        self.runtime.block_on(async {
            let mut work = pin!(tokio::time::timeout(remaining, work));
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {
                    biased;
                    _ = poll.tick() => process::check_operation()?,
                    result = &mut work => return result.map_err(|_| invalid("cache PostgreSQL deadline exceeded"))?,
                }
            }
        })
    }

    async fn acquire(&self) -> Result<Checkout> {
        Ok(Checkout(
            Some(
                self.pool
                    .get()
                    .await
                    .map_err(|_| invalid("cache PostgreSQL pool unavailable"))?,
            ),
            self.tls.clone(),
        ))
    }

    /// Only static application SQL reaches this interface. SQL predicates also
    /// bound bytea lengths BEFORE PostgreSQL encodes a row; this checks the full
    /// returned collection. Interrupted streams destroy Checkout, never recycle it.
    pub fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        rows_limit: usize,
        bytes_limit: usize,
    ) -> Result<Vec<Row>> {
        self.run(async {
            let connection = self.acquire().await?;
            let mut rows = Vec::new();
            let mut bytes = 0usize;
            {
                let mut stream = pin!(
                    connection
                        .client()
                        .query_raw(sql, params.iter().copied())
                        .await
                        .map_err(sql_error)?
                );
                while let Some(row) = stream.next().await {
                    process::check_operation()?;
                    let row = row.map_err(sql_error)?;
                    for (index, column) in row.columns().iter().enumerate() {
                        bytes = bytes.saturating_add(match *column.type_() {
                            Type::BYTEA => row
                                .try_get::<_, Option<&[u8]>>(index)?
                                .map_or(0, |v| v.len()),
                            Type::TEXT | Type::VARCHAR => row
                                .try_get::<_, Option<&str>>(index)?
                                .map_or(0, |v| v.len()),
                            Type::INT8 | Type::INT4 | Type::BOOL => 8,
                            _ => return Err(invalid("unsupported cache binary result type")),
                        });
                    }
                    if bytes > bytes_limit || rows.len() == rows_limit {
                        return Err(invalid("cache binary result budget exceeded"));
                    }
                    rows.push(row);
                }
            }
            connection
                .client()
                .batch_execute("ROLLBACK; RESET ALL;")
                .await
                .map_err(sql_error)?;
            connection.recycle();
            Ok(rows)
        })
    }

    pub fn migrate(&self) -> Result<()> {
        self.run(async {
            let connection = self.acquire().await?;
            // Session ownership spans the independently transactional migration
            // files. An error/interruption destroys Checkout and releases this
            // lock; never recycle a session retaining migration authority.
            connection
                .client()
                .batch_execute("SELECT pg_advisory_lock(91079170002)")
                .await
                .map_err(sql_error)?;
            let exists: bool = connection
                .client()
                .query_one(
                    "SELECT to_regclass('git_rs_cache.schema_version') IS NOT NULL",
                    &[],
                )
                .await
                .map_err(sql_error)?
                .try_get(0)?;
            let mut version: i32 = if exists {
                connection
                    .client()
                    .query_one(
                        "SELECT version FROM git_rs_cache.schema_version WHERE singleton",
                        &[],
                    )
                    .await
                    .map_err(sql_error)?
                    .try_get(0)?
            } else {
                0
            };
            if version == 0 {
                connection
                    .client()
                    .batch_execute(include_str!("../../migrations/cache/001.sql"))
                    .await
                    .map_err(sql_error)?;
                version = 1;
            }
            if version == 1 {
                connection
                    .client()
                    .batch_execute(include_str!("../../migrations/cache/002.sql"))
                    .await
                    .map_err(sql_error)?;
                version = 2;
            }
            if version == 2 {
                connection
                    .client()
                    .batch_execute(include_str!("../../migrations/cache/003.sql"))
                    .await
                    .map_err(sql_error)?;
                version = 3;
            }
            if version != 3 {
                return Err(invalid("unsupported cache schema version"));
            }
            connection
                .client()
                .batch_execute("SELECT pg_advisory_unlock(91079170002); ROLLBACK; RESET ALL;")
                .await
                .map_err(sql_error)?;
            connection.recycle();
            Ok(())
        })
    }
}
