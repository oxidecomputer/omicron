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
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::TokioAsyncResolver;

type DnsError = crate::Error<crate::types::Error>;

/// A connection used to update multiple DNS servers.
pub struct Updater {
    log: Logger,
    clients: Vec<crate::Client>,
}

pub trait Service {
    fn aaaa(&self) -> crate::names::AAAA;
    fn srv(&self) -> crate::names::SRV;
    fn address(&self) -> SocketAddrV6;
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

        Self { log, clients }
    }

    /// Inserts all service records into the DNS server.
    ///
    /// This method is most efficient when records are sorted by
    /// SRV key.
    pub async fn insert_dns_records(
        &self,
        records: &Vec<impl Service>,
    ) -> Result<(), DnsError> {
        let mut records = records.iter().peekable();

        while let Some(record) = records.next() {
            let srv = record.srv();
            info!(self.log, "Inserting DNS record: {:?}", srv);

            match &srv {
                &crate::names::SRV::Service(_) => {
                    let mut aaaa = vec![(record.aaaa(), record.address())];
                    while let Some(record) = records.peek() {
                        if record.srv() == srv {
                            let record = records.next().unwrap();
                            aaaa.push((record.aaaa(), record.address()));
                        } else {
                            break;
                        }
                    }

                    self.insert_dns_records_internal(aaaa, srv).await?;
                }
                &crate::names::SRV::Backend(_, _) => {
                    let aaaa = vec![(record.aaaa(), record.address())];
                    self.insert_dns_records_internal(aaaa, record.srv())
                        .await?;
                }
            };
        }
        Ok(())
    }

    /// Utility function to insert:
    /// - A set of uniquely-named AAAA records, each corresponding to an address
    /// - An SRV record, pointing to each of the AAAA records.
    async fn insert_dns_records_internal(
        &self,
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
            warn!(self.log, "Failed to set DNS records"; "error" => ?error);
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
            .try_for_each_concurrent(None, |client| async move {
                client.dns_records_set(body).await?;
                Ok(())
            })
            .await?;

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
            .try_for_each_concurrent(None, |client| async move {
                client.dns_records_delete(body).await?;
                Ok(())
            })
            .await?;

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

#[derive(Debug, Clone, thiserror::Error)]
pub enum ResolveError {
    #[error(transparent)]
    Resolve(#[from] trust_dns_resolver::error::ResolveError),

    #[error("Record not found for SRV key: {0}")]
    NotFound(crate::names::SRV),
}

/// A wrapper around a DNS resolver, providing a way to conveniently
/// look up IP addresses of services based on their SRV keys.
pub struct Resolver {
    inner: TokioAsyncResolver,
}

impl Resolver {
    /// Creates a DNS resolver, looking up DNS server addresses based on
    /// the provided subnet.
    pub fn new(subnet: Ipv6Subnet<AZ_PREFIX>) -> Result<Self, ResolveError> {
        Ok(Self { inner: create_resolver(subnet)? })
    }

    /// Convenience wrapper for [`Resolver::new`] which determines the subnet
    /// based on a provided IP address.
    pub fn new_from_ip(address: Ipv6Addr) -> Result<Self, ResolveError> {
        let subnet = Ipv6Subnet::<AZ_PREFIX>::new(address);

        Resolver::new(subnet)
    }

    /// Looks up a single [`Ipv6Addr`] based on the SRV name.
    /// Returns an error if the record does not exist.
    // TODO: There are lots of ways this API can expand: Caching,
    // actually respecting TTL, looking up ports, etc.
    //
    // For now, however, it serves as a very simple "get everyone using DNS"
    // API that can be improved upon later.
    pub async fn lookup_ipv6(
        &self,
        srv: crate::names::SRV,
    ) -> Result<Ipv6Addr, ResolveError> {
        let response = self.inner.ipv6_lookup(&srv.to_string()).await?;
        let address = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;
        Ok(*address)
    }

    pub async fn lookup_ip(
        &self,
        srv: crate::names::SRV,
    ) -> Result<IpAddr, ResolveError> {
        let response = self.inner.lookup_ip(&srv.to_string()).await?;
        let address = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;
        Ok(address)
    }
}
