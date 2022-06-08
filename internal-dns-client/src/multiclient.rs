// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::types::{DnsKv, DnsRecord, DnsRecordKey, Srv};
use futures::stream::{self, StreamExt, TryStreamExt};
use omicron_common::address::{
    Ipv6Subnet, ReservedRackSubnet, AZ_PREFIX, DNS_PORT, DNS_SERVER_PORT,
};
use omicron_common::backoff::{
    internal_service_policy, retry_notify, BackoffError,
};
use slog::{info, warn, Logger};
use std::net::{SocketAddr, SocketAddrV6};
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::TokioAsyncResolver;

type DnsError = crate::Error<crate::types::Error>;

/// A connection used to update multiple DNS servers.
pub struct Updater {
    clients: Vec<crate::Client>,
}

impl Updater {
    /// Creates a new "Updater", capable of communicating with all
    /// DNS servers within the AZ.
    pub fn new(subnet: Ipv6Subnet<AZ_PREFIX>, log: Logger) -> Self {
        let clients = ReservedRackSubnet::new(subnet)
            .get_dns_subnets()
            .into_iter()
            .map(|dns_subnet| {
                let addr = dns_subnet.dns_address().ip();
                info!(log, "Adding DNS server: {}", addr);
                crate::Client::new(
                    &format!("http://[{}]:{}", addr, DNS_SERVER_PORT),
                    log.clone(),
                )
            })
            .collect::<Vec<_>>();

        Self { clients }
    }

    /// Utility function to insert:
    /// - A set of uniquely-named AAAA records, each corresponding to an address
    /// - An SRV record, pointing to each of the AAAA records.
    pub async fn insert_dns_records(
        &self,
        log: &Logger,
        aaaa: Vec<(crate::names::AAAA, SocketAddrV6)>,
        srv_key: crate::names::SRV,
    ) -> Result<(), DnsError> {
        let mut records = Vec::with_capacity(aaaa.len() + 1);

        // Add one DnsKv per AAAA, each with a single record.
        records.extend(aaaa.iter().map(|(name, addr)| DnsKv {
            key: DnsRecordKey { name: name.to_string() },
            records: vec![DnsRecord::Aaaa(*addr.ip())],
        }));

        // Add the DnsKv for the SRV, with a record for each AAAA.
        records.push(DnsKv {
            key: DnsRecordKey { name: srv_key.to_string() },
            records: aaaa
                .iter()
                .map(|(name, addr)| {
                    DnsRecord::Srv(Srv {
                        prio: 0,
                        weight: 0,
                        port: addr.port(),
                        target: name.to_string(),
                    })
                })
                .collect::<Vec<_>>(),
        });

        let set_record = || async {
            self.dns_records_set(&records)
                .await
                .map_err(BackoffError::transient)?;
            Ok::<(), BackoffError<DnsError>>(())
        };
        let log_failure = |error, _| {
            warn!(log, "Failed to set DNS records"; "error" => ?error);
        };

        retry_notify(internal_service_policy(), set_record, log_failure)
            .await?;
        Ok(())
    }

    /// Sets a records on all DNS servers.
    ///
    /// Returns an error if setting the record fails on any server.
    pub async fn dns_records_set<'a>(
        &'a self,
        body: &'a Vec<crate::types::DnsKv>,
    ) -> Result<(), DnsError> {
        stream::iter(&self.clients)
            .map(Ok::<_, DnsError>)
            .try_for_each_concurrent(
                None,
                |client| async move {
                    client.dns_records_set(body).await?;
                    Ok(())
                }
            ).await?;

        Ok(())
    }

    /// Deletes records in all DNS servers.
    ///
    /// Returns an error if deleting the record fails on any server.
    pub async fn dns_records_delete<'a>(
        &'a self,
        body: &'a Vec<crate::types::DnsRecordKey>,
    ) -> Result<(), DnsError> {
        stream::iter(&self.clients)
            .map(Ok::<_, DnsError>)
            .try_for_each_concurrent(
                None,
                |client| async move {
                    client.dns_records_delete(body).await?;
                    Ok(())
                }
            ).await?;

        Ok(())
    }
}

/// Creates a resolver using all internal DNS name servers.
pub fn create_resolver(
    subnet: Ipv6Subnet<AZ_PREFIX>,
) -> Result<TokioAsyncResolver, trust_dns_resolver::error::ResolveError> {
    let mut rc = ResolverConfig::new();
    let dns_ips = ReservedRackSubnet::new(subnet)
        .get_dns_subnets()
        .into_iter()
        .map(|subnet| subnet.dns_address().ip())
        .collect::<Vec<_>>();

    for dns_ip in dns_ips {
        rc.add_name_server(NameServerConfig {
            socket_addr: SocketAddr::V6(SocketAddrV6::new(
                dns_ip, DNS_PORT, 0, 0,
            )),
            protocol: Protocol::Udp,
            tls_dns_name: None,
            trust_nx_responses: false,
            bind_addr: None,
        });
    }
    TokioAsyncResolver::tokio(rc, ResolverOpts::default())
}
