//! System-configured asynchronous resolution shared by the network adapters.

use crate::{Result, invalid};
use hickory_resolver::{
    Hosts, TokioResolver,
    config::{LookupIpStrategy, ResolveHosts},
    proto::{
        op::Query,
        rr::{Name as DnsName, RecordType},
    },
};
use std::{net::IpAddr, sync::Arc};

#[derive(Clone)]
pub(crate) struct SystemDns {
    resolver: TokioResolver,
    hosts: Arc<Hosts>,
}

impl SystemDns {
    /// Call inside the runtime which will own this resolver's network I/O.
    pub(crate) fn new(service: &str) -> Result<Self> {
        // Never substitute public nameservers for unreadable/invalid system config.
        let mut builder = TokioResolver::builder_tokio().map_err(|error| {
            invalid(&format!(
                "{service} system DNS configuration failed: {error}"
            ))
        })?;
        builder.options_mut().ip_strategy = LookupIpStrategy::Ipv6AndIpv4;
        builder.options_mut().use_hosts_file = ResolveHosts::Never;
        let hosts = Arc::new(Hosts::from_system().unwrap_or_default());
        let mut resolver = builder.build().map_err(|error| {
            invalid(&format!(
                "{service} DNS resolver initialization failed: {error}"
            ))
        })?;
        resolver.set_hosts(hosts.clone());
        Ok(Self { resolver, hosts })
    }

    pub(crate) async fn lookup_ips(&self, hostname: &str) -> Result<Vec<IpAddr>> {
        let mut addresses = Vec::new();
        if let Ok(original) = hostname.parse::<DnsName>() {
            // Check the original alias before search expansion, and both hosts
            // families before DNS. An IPv4 hosts hit must not wait for AAAA DNS.
            for record_type in [RecordType::AAAA, RecordType::A] {
                if let Some(lookup) = self
                    .hosts
                    .lookup_static_host(&Query::query(original.clone(), record_type))
                {
                    addresses.extend(
                        lookup
                            .answers()
                            .iter()
                            .filter_map(|record| record.data.ip_addr()),
                    );
                }
            }
        }
        if addresses.is_empty() {
            addresses.extend(self.resolver.lookup_ip(hostname).await?.iter());
        }
        Ok(addresses)
    }
}
