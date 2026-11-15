//! Reqwest adapter for system-configured asynchronous DNS.

use crate::Result;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::net::SocketAddr;

pub(super) struct SystemDns(crate::system_dns::SystemDns);

impl SystemDns {
    pub(super) fn new() -> Result<Self> {
        Ok(Self(crate::system_dns::SystemDns::new("S3")?))
    }
}

impl Resolve for SystemDns {
    fn resolve(&self, name: Name) -> Resolving {
        let resolver = self.0.clone();
        Box::pin(async move {
            let addresses = resolver.lookup_ips(name.as_str()).await?;
            // Reqwest/Hyper supply the request's explicit or scheme-default port.
            let addresses: Addrs = Box::new(addresses.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            Ok(addresses)
        })
    }
}
