//! Async DNS before numeric SDK connection attempts, inside pool admission.

use crate::{Result, invalid, system_dns::SystemDns};
use deadpool::managed::{Manager as ManagedManager, Metrics, RecycleError, RecycleResult};
use deadpool_postgres::{ClientWrapper, Manager as SdkManager, ManagerConfig, RecyclingMethod};
use postgres_native_tls::MakeTlsConnector;
use rand::seq::SliceRandom;
use std::{net::IpAddr, ops::Deref};
use tokio::sync::OnceCell;
use tokio_postgres::{
    Config,
    config::{Host, LoadBalanceHosts},
};

pub(crate) type Pool = deadpool::managed::Pool<Manager>;
pub(crate) type Client = deadpool::managed::Object<Manager>;

pub(crate) struct Manager {
    config: Config,
    tls: MakeTlsConnector,
    dns: OnceCell<SystemDns>,
}

pub(crate) struct Connection {
    client: ClientWrapper,
    // Retain the SDK registry which attached this client's statement cache.
    manager: SdkManager,
}

impl Deref for Connection {
    type Target = ClientWrapper;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Manager {
    pub(crate) fn new(config: Config, tls: MakeTlsConnector) -> Self {
        Self {
            config,
            tls,
            dns: OnceCell::new(),
        }
    }

    async fn connect(&self, config: Config) -> Result<Connection> {
        let manager = SdkManager::from_config(
            config,
            self.tls.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let client = manager.create().await.map_err(super::sql_error)?;
        Ok(Connection { client, manager })
    }
}

impl ManagedManager for Manager {
    type Type = Connection;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    async fn create(&self) -> Result<Connection> {
        let hosts = self.config.get_hosts();
        let hostaddrs = self.config.get_hostaddrs();
        let ports = self.config.get_ports();
        // Keep the validated application contract: no Unix or hostaddr-only path.
        if hosts.is_empty() || hosts.iter().any(|host| !matches!(host, Host::Tcp(_))) {
            return Err(invalid("PostgreSQL requires a TCP hostname"));
        }
        if (!hostaddrs.is_empty() && hostaddrs.len() != hosts.len())
            || (ports.len() > 1 && ports.len() != hosts.len())
        {
            return Err(invalid("invalid PostgreSQL host/address/port cardinality"));
        }
        let random = self.config.get_load_balance_hosts() == LoadBalanceHosts::Random;
        let mut indices: Vec<_> = (0..hosts.len()).collect();
        if random {
            indices.shuffle(&mut rand::rng());
        }
        let mut last_error = invalid("PostgreSQL resolved no addresses");
        // Shuffle configured hosts first, then addresses within each host. Flattening
        // them would give hosts with more DNS answers greater selection weight.
        for index in indices {
            let Host::Tcp(hostname) = &hosts[index] else {
                unreachable!()
            };
            let mut addresses = if let Some(address) = hostaddrs.get(index) {
                vec![*address]
            } else if let Ok(address) = hostname.parse::<IpAddr>() {
                vec![address]
            } else {
                let resolved = async {
                    let dns = self
                        .dns
                        .get_or_try_init(|| async { SystemDns::new("PostgreSQL") })
                        .await?;
                    dns.lookup_ips(hostname).await
                }
                .await;
                match resolved {
                    Ok(addresses) => addresses,
                    Err(error) => {
                        last_error = error;
                        continue;
                    }
                }
            };
            if random {
                addresses.shuffle(&mut rand::rng());
            }
            if addresses.is_empty() {
                last_error = invalid("PostgreSQL resolved no addresses");
            }
            let port = ports
                .get(index)
                .or_else(|| ports.first())
                .copied()
                .unwrap_or(5432);
            for address in addresses {
                let mut config = connection_settings(&self.config);
                // hostaddr controls dialing; host remains the TLS identity. The SDK
                // also records this numeric endpoint for DNS-free CancelRequest.
                config.host(hostname).hostaddr(address).port(port);
                match self.connect(config).await {
                    Ok(connection) => return Ok(connection),
                    Err(error) => last_error = error,
                }
            }
        }
        Err(last_error)
    }

    async fn recycle(
        &self,
        connection: &mut Connection,
        metrics: &Metrics,
    ) -> RecycleResult<Self::Error> {
        connection
            .manager
            .recycle(&mut connection.client, metrics)
            .await
            .map_err(|error| match error {
                RecycleError::Message(message) => RecycleError::Message(message),
                RecycleError::Backend(error) => RecycleError::Backend(super::sql_error(error)),
            })
    }

    fn detach(&self, connection: &mut Connection) {
        connection.manager.detach(&mut connection.client);
    }
}

// Config's routing setters append and there are no clear methods. Copy every
// non-routing field of tokio-postgres 0.7.18 using its public API instead.
fn connection_settings(source: &Config) -> Config {
    let mut config = Config::new();
    if let Some(value) = source.get_user() {
        config.user(value);
    }
    if let Some(value) = source.get_password() {
        config.password(value);
    }
    if let Some(value) = source.get_dbname() {
        config.dbname(value);
    }
    if let Some(value) = source.get_options() {
        config.options(value);
    }
    if let Some(value) = source.get_application_name() {
        config.application_name(value);
    }
    config
        .ssl_mode(source.get_ssl_mode())
        .ssl_negotiation(source.get_ssl_negotiation())
        .keepalives(source.get_keepalives())
        .keepalives_idle(source.get_keepalives_idle())
        .target_session_attrs(source.get_target_session_attrs())
        .channel_binding(source.get_channel_binding())
        .load_balance_hosts(source.get_load_balance_hosts());
    if let Some(value) = source.get_connect_timeout() {
        config.connect_timeout(*value);
    }
    if let Some(value) = source.get_tcp_user_timeout() {
        config.tcp_user_timeout(*value);
    }
    if let Some(value) = source.get_keepalives_interval() {
        config.keepalives_interval(value);
    }
    if let Some(value) = source.get_keepalives_retries() {
        config.keepalives_retries(value);
    }
    config
}

#[cfg(test)]
#[path = "manager_tests.rs"]
mod tests;
