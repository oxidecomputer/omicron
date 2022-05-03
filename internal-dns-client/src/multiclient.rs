// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::address::{
    Ipv6Subnet,
    ReservedRackSubnet,
    AZ_PREFIX,
    DNS_SERVER_PORT,
    DNS_PORT,
};
use slog::{info, Logger};
use std::net::{SocketAddr, SocketAddrV6};
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::TokioAsyncResolver;


/// A connection used to update multiple DNS servers.
pub struct Updater {
    clients: Vec<crate::Client>,
}

impl Updater {
    pub fn new(subnet: Ipv6Subnet<AZ_PREFIX>, log: Logger) -> Self {
        let clients = ReservedRackSubnet::new(subnet)
            .get_dns_subnets()
            .into_iter()
            .map(|dns_subnet| {
                let addr = dns_subnet.dns_address().ip();
                info!(log, "Adding DNS server: {}", addr);
                crate::Client::new(&format!("http://[{}]:{}", addr, DNS_SERVER_PORT), log.clone())
            })
            .collect::<Vec<_>>();

        Self {
            clients
        }
    }

    /// Sets a records on all DNS servers.
    ///
    /// Returns an error if setting the record fails on any server.
    pub async fn dns_records_set<'a>(
        &'a self,
        body: &'a Vec<crate::types::DnsKv>
    ) -> Result<(), crate::Error<crate::types::Error>> {

        // TODO: Could be sent concurrently.
        for client in &self.clients {
            client.dns_records_set(body).await?;
        }

        Ok(())
    }

    /// Deletes records in all DNS servers.
    ///
    /// Returns an error if deleting the record fails on any server.
    pub async fn dns_records_delete<'a>(
        &'a self,
        body: &'a Vec<crate::types::DnsRecordKey>
    ) -> Result<(), crate::Error<crate::types::Error>> {
        // TODO: Could be sent concurrently
        for client in &self.clients {
            client.dns_records_delete(body).await?;
        }
        Ok(())
    }
}

/// Creates a resolver using all internal DNS name servers.
pub fn create_resolver(subnet: Ipv6Subnet<AZ_PREFIX>)
    -> Result<TokioAsyncResolver, trust_dns_resolver::error::ResolveError>
{
    let mut rc = ResolverConfig::new();
    let dns_ips = ReservedRackSubnet::new(subnet)
        .get_dns_subnets()
        .into_iter()
        .map(|subnet| subnet.dns_address().ip())
        .collect::<Vec<_>>();

    for dns_ip in dns_ips {
        rc.add_name_server(NameServerConfig {
            socket_addr: SocketAddr::V6(SocketAddrV6::new(
                dns_ip,
                DNS_PORT,
                0,
                0,
            )),
            protocol: Protocol::Udp,
            tls_dns_name: None,
            trust_nx_responses: false,
            bind_addr: None,
        });
    }
    TokioAsyncResolver::tokio(rc, ResolverOpts::default())
}
