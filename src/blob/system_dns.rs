//! System-configured asynchronous DNS, without reqwest's public-DNS fallback.

use crate::{Result, invalid};
use hickory_resolver::{
    Hosts, TokioResolver,
    config::{LookupIpStrategy, ResolveHosts},
    proto::{
        op::Query,
        rr::{Name as DnsName, RecordType},
    },
};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::{net::SocketAddr, sync::Arc};

pub(super) struct SystemDns {
    resolver: TokioResolver,
    hosts: Arc<Hosts>,
}

impl SystemDns {
    pub(super) fn new() -> Result<Self> {
        // Load once during transport initialization, inside its Tokio context.
        // In particular, never replace an unreadable/invalid system config with
        // ResolverConfig::default() or a public nameserver list.
        let mut builder = TokioResolver::builder_tokio()
            .map_err(|error| invalid(&format!("S3 system DNS configuration failed: {error}")))?;
        builder.options_mut().ip_strategy = LookupIpStrategy::Ipv6AndIpv4;
        // Parse once and share the same snapshot with both lookup paths. Match
        // Hickory's optional-hosts-file policy; system DNS errors still fail above.
        builder.options_mut().use_hosts_file = ResolveHosts::Never;
        let hosts = Arc::new(Hosts::from_system().unwrap_or_default());
        let mut resolver = builder
            .build()
            .map_err(|error| invalid(&format!("S3 DNS resolver initialization failed: {error}")))?;
        resolver.set_hosts(hosts.clone());
        Ok(Self { resolver, hosts })
    }
}

impl Resolve for SystemDns {
    fn resolve(&self, name: Name) -> Resolving {
        let mut addresses = Vec::new();
        if let Ok(original) = name.as_str().parse::<DnsName>() {
            // Check the original alias before search expansion, and both hosts
            // families before async DNS. Otherwise an IPv4 hosts hit can wait
            // for a blackholed AAAA query. Match Ipv6AndIpv4's family order.
            for record_type in [RecordType::AAAA, RecordType::A] {
                if let Some(lookup) = self
                    .hosts
                    .lookup_static_host(&Query::query(original.clone(), record_type))
                {
                    addresses.extend(
                        lookup
                            .answers()
                            .iter()
                            .filter_map(|record| record.data.ip_addr())
                            .map(|ip| SocketAddr::new(ip, 0)),
                    );
                }
            }
        }
        if !addresses.is_empty() {
            let addresses: Addrs = Box::new(addresses.into_iter());
            return Box::pin(std::future::ready(Ok(addresses)));
        }
        let resolver = self.resolver.clone();
        Box::pin(async move {
            let lookup = resolver.lookup_ip(name.as_str()).await?;
            // Reqwest/Hyper supply the request's explicit or scheme-default port.
            let addresses: Addrs = Box::new(
                lookup
                    .iter()
                    .map(|ip| SocketAddr::new(ip, 0))
                    .collect::<Vec<_>>()
                    .into_iter(),
            );
            Ok(addresses)
        })
    }
}
