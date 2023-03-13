// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DNS_ZONE;
use futures::stream::{self, StreamExt, TryStreamExt};
use omicron_common::address::{
    Ipv6Subnet, ReservedRackSubnet, AZ_PREFIX, DNS_PORT, DNS_SERVER_PORT,
};
use slog::{info, Logger};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use trust_dns_proto::rr::record_type::RecordType;
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::TokioAsyncResolver;

pub type DnsError = dns_service_client::Error<dns_service_client::types::Error>;

/// Describes how to find the DNS servers.
///
/// In production code, this is nearly always [`Ipv6Subnet<AZ_PREFIX>`],
/// but it allows a point of dependency-injection for tests to supply their
/// own address lookups.
pub trait DnsAddressLookup {
    fn dropshot_server_addrs(&self) -> Vec<SocketAddr>;

    fn dns_server_addrs(&self) -> Vec<SocketAddr>;
}

fn subnet_to_ips(
    subnet: Ipv6Subnet<AZ_PREFIX>,
) -> impl Iterator<Item = IpAddr> {
    ReservedRackSubnet::new(subnet)
        .get_dns_subnets()
        .into_iter()
        .map(|dns_subnet| IpAddr::V6(dns_subnet.dns_address().ip()))
}

impl DnsAddressLookup for Ipv6Subnet<AZ_PREFIX> {
    fn dropshot_server_addrs(&self) -> Vec<SocketAddr> {
        subnet_to_ips(*self)
            .map(|address| SocketAddr::new(address, DNS_SERVER_PORT))
            .collect()
    }

    fn dns_server_addrs(&self) -> Vec<SocketAddr> {
        subnet_to_ips(*self)
            .map(|address| SocketAddr::new(address, DNS_PORT))
            .collect()
    }
}

pub struct ServerAddresses {
    pub dropshot_server_addrs: Vec<SocketAddr>,
    pub dns_server_addrs: Vec<SocketAddr>,
}

impl DnsAddressLookup for ServerAddresses {
    fn dropshot_server_addrs(&self) -> Vec<SocketAddr> {
        self.dropshot_server_addrs.clone()
    }

    fn dns_server_addrs(&self) -> Vec<SocketAddr> {
        self.dns_server_addrs.clone()
    }
}

/// A connection used to update multiple DNS servers
///
/// Note that this is only suitable for use in writing an initial update to the
/// DNS servers.  After transfer-of-control to Nexus, Nexus uses a more
/// synchronized process for updating the DNS servers.  See RFD 367 for details.
pub struct Updater {
    log: Logger,
    clients: Vec<dns_service_client::Client>,
}

impl Updater {
    pub fn new(address_getter: &impl DnsAddressLookup, log: Logger) -> Self {
        let addrs = address_getter.dropshot_server_addrs();
        Self::new_from_addrs(addrs, log)
    }

    fn new_from_addrs(addrs: Vec<SocketAddr>, log: Logger) -> Self {
        let clients = addrs
            .into_iter()
            .map(|addr| {
                info!(log, "Adding DNS server: {}", addr);
                dns_service_client::Client::new(
                    &format!("http://{}", addr),
                    log.clone(),
                )
            })
            .collect::<Vec<_>>();

        Self { log, clients }
    }

    /// Attempts to write a DNS generation to the DNS servers
    pub async fn dns_initialize<'a>(
        &'a self,
        body: &'a dns_service_client::types::DnsConfig,
    ) -> Result<(), DnsError> {
        // XXX-dap log
        stream::iter(&self.clients)
            .map(Ok::<_, DnsError>)
            .try_for_each_concurrent(None, |client| async move {
                client.dns_config_put(body).await?;
                Ok(())
            })
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ResolveError {
    #[error(transparent)]
    Resolve(#[from] trust_dns_resolver::error::ResolveError),

    #[error("Record not found for SRV key: {0}")]
    NotFound(crate::SRV),

    #[error("Record not found for {0}")]
    NotFoundByString(String),
}

/// A wrapper around a DNS resolver, providing a way to conveniently
/// look up IP addresses of services based on their SRV keys.
#[derive(Clone)]
pub struct Resolver {
    inner: Box<TokioAsyncResolver>,
}

impl Resolver {
    pub fn new(
        address_getter: &impl DnsAddressLookup,
    ) -> Result<Self, ResolveError> {
        let dns_addrs = address_getter.dns_server_addrs();
        Self::new_from_addrs(dns_addrs)
    }

    fn new_from_addrs(
        dns_addrs: Vec<SocketAddr>,
    ) -> Result<Self, ResolveError> {
        let mut rc = ResolverConfig::new();
        for socket_addr in dns_addrs.into_iter() {
            rc.add_name_server(NameServerConfig {
                socket_addr,
                protocol: Protocol::Udp,
                tls_dns_name: None,
                trust_nx_responses: false,
                bind_addr: None,
            });
        }
        let inner =
            Box::new(TokioAsyncResolver::tokio(rc, ResolverOpts::default())?);

        Ok(Self { inner })
    }

    /// Convenience wrapper for [`Resolver::new`] which determines the subnet
    /// based on a provided IP address.
    pub fn new_from_ip(address: Ipv6Addr) -> Result<Self, ResolveError> {
        let subnet = Ipv6Subnet::<AZ_PREFIX>::new(address);

        Resolver::new(&subnet)
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
        srv: crate::SRV,
    ) -> Result<Ipv6Addr, ResolveError> {
        let name = format!("{}.{}", srv.to_string(), DNS_ZONE);
        let response = self.inner.ipv6_lookup(&name).await?;
        let address = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;
        Ok(*address)
    }

    /// Looks up a single [`SocketAddrV6`] based on the SRV name
    /// Returns an error if the record does not exist.
    pub async fn lookup_socket_v6(
        &self,
        srv: crate::SRV,
    ) -> Result<SocketAddrV6, ResolveError> {
        let name = format!("{}.{}", srv.to_string(), DNS_ZONE);
        let response = self.inner.lookup(&name, RecordType::SRV).await?;

        let rdata = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;

        Ok(match rdata {
            trust_dns_proto::rr::record_data::RData::SRV(srv) => {
                let name = srv.target();
                let response =
                    self.inner.ipv6_lookup(&name.to_string()).await?;

                let address = response.iter().next().ok_or_else(|| {
                    ResolveError::NotFoundByString(name.to_string())
                })?;

                SocketAddrV6::new(*address, srv.port(), 0, 0)
            }

            _ => {
                return Err(ResolveError::Resolve(
                    "SRV query did not return SRV RData!".into(),
                ));
            }
        })
    }

    pub async fn lookup_ip(
        &self,
        srv: crate::SRV,
    ) -> Result<IpAddr, ResolveError> {
        let response = self.inner.lookup_ip(&srv.to_string()).await?;
        let address = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;
        Ok(address)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::DnsConfigBuilder;
    use crate::{BackendName, ServiceName, SRV};
    use assert_matches::assert_matches;
    use omicron_test_utils::dev::test_setup_log;
    use slog::o;
    use std::str::FromStr;
    use tempfile::TempDir;
    use uuid::Uuid;

    struct DnsServer {
        // We hang onto the storage_path even though it's never used because
        // dropping it causes it to be cleaned up.
        #[allow(dead_code)]
        storage_path: Option<TempDir>,

        successful: bool,
        dns_server: dns_server::dns_server::ServerHandle,
        dropshot_server: dropshot::HttpServer<dns_server::http_server::Context>,
    }

    impl DnsServer {
        async fn create(log: &Logger) -> Self {
            let storage_path =
                TempDir::new().expect("Failed to create temporary directory");
            let config_store = dns_server::storage::Config {
                storage_path: storage_path
                    .path()
                    .to_string_lossy()
                    .into_owned()
                    .into(),
            };
            let store = dns_server::storage::Store::new(
                log.new(o!("component" => "DnsStore")),
                &config_store,
            )
            .unwrap();

            let (dns_server, dropshot_server) = dns_server::start_servers(
                log.clone(),
                store,
                &dns_server::dns_server::Config {
                    bind_address: "[::1]:0".to_string(),
                },
                &dropshot::ConfigDropshot {
                    bind_address: "[::1]:0".parse().unwrap(),
                    request_body_max_bytes: 8 * 1024,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            Self {
                storage_path: Some(storage_path),
                dns_server,
                dropshot_server,
                successful: false,
            }
        }

        fn dns_server_address(&self) -> SocketAddr {
            *self.dns_server.local_address()
        }

        fn dropshot_server_address(&self) -> SocketAddr {
            self.dropshot_server.local_addr()
        }

        fn cleanup_successful(mut self) {
            self.successful = true;
            drop(self)
        }
    }

    impl Drop for DnsServer {
        fn drop(&mut self) {
            if !self.successful {
                // If we didn't explicitly succeed, then we want to keep the
                // temporary directory around.
                let _ = self.storage_path.take().unwrap().into_path();
            }
        }
    }

    // A test-only way to infer DNS addresses.
    //
    // Rather than inferring DNS server addresses from the rack subnet,
    // they may be explicitly supplied. This results in easier-to-test code.
    #[derive(Default)]
    struct LocalAddressGetter {
        addrs: Vec<(SocketAddr, SocketAddr)>,
    }

    impl LocalAddressGetter {
        fn add_dns_server(
            &mut self,
            dns_address: SocketAddr,
            server_address: SocketAddr,
        ) {
            self.addrs.push((dns_address, server_address));
        }
    }

    impl DnsAddressLookup for LocalAddressGetter {
        fn dropshot_server_addrs(&self) -> Vec<SocketAddr> {
            self.addrs
                .iter()
                .map(|(_dns_address, dropshot_address)| *dropshot_address)
                .collect()
        }

        fn dns_server_addrs(&self) -> Vec<SocketAddr> {
            self.addrs
                .iter()
                .map(|(dns_address, _dropshot_address)| *dns_address)
                .collect()
        }
    }

    // The resolver cannot look up IPs before records have been inserted.
    #[tokio::test]
    async fn lookup_nonexistent_record_fails() {
        let logctx = test_setup_log("lookup_nonexistent_record_fails");
        let dns_server = DnsServer::create(&logctx.log).await;

        let mut address_getter = LocalAddressGetter::default();
        address_getter.add_dns_server(
            dns_server.dns_server_address(),
            dns_server.dropshot_server_address(),
        );

        let resolver = Resolver::new(&address_getter)
            .expect("Error creating localhost resolver");

        let err = resolver
            .lookup_ip(SRV::Service(ServiceName::Cockroach))
            .await
            .expect_err("Looking up non-existent service should fail");

        let dns_error = match err {
            ResolveError::Resolve(err) => err,
            _ => panic!("Unexpected error: {err}"),
        };
        assert!(
            matches!(
                dns_error.kind(),
                trust_dns_resolver::error::ResolveErrorKind::NoRecordsFound { .. },
            ),
            "Saw error: {dns_error}",
        );
        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }

    // Insert and retreive a single DNS record.
    #[tokio::test]
    async fn insert_and_lookup_one_record() {
        let logctx = test_setup_log("insert_and_lookup_one_record");
        let dns_server = DnsServer::create(&logctx.log).await;

        let mut address_getter = LocalAddressGetter::default();
        address_getter.add_dns_server(
            dns_server.dns_server_address(),
            dns_server.dropshot_server_address(),
        );

        let resolver = Resolver::new(&address_getter)
            .expect("Error creating localhost resolver");
        let updater = Updater::new(&address_getter, logctx.log.clone());

        let mut dns_config = DnsConfigBuilder::new();
        let ip = Ipv6Addr::from_str("ff::01").unwrap();
        let zone = dns_config.host_zone(Uuid::new_v4(), ip).unwrap();
        dns_config
            .service_backend(SRV::Service(ServiceName::Cockroach), &zone, 12345)
            .unwrap();
        let dns_config = dns_config.build();
        updater.dns_initialize(&dns_config).await.unwrap();

        let found_ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(found_ip, ip,);

        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }

    // Insert multiple DNS records of different types.
    #[tokio::test]
    async fn insert_and_lookup_multiple_records() {
        let logctx = test_setup_log("insert_and_lookup_multiple_records");
        let dns_server = DnsServer::create(&logctx.log).await;

        let mut address_getter = LocalAddressGetter::default();
        address_getter.add_dns_server(
            dns_server.dns_server_address(),
            dns_server.dropshot_server_address(),
        );

        let resolver = Resolver::new(&address_getter)
            .expect("Error creating localhost resolver");
        let updater = Updater::new(&address_getter, logctx.log.clone());

        let cockroach_addrs = [
            SocketAddrV6::new(
                Ipv6Addr::from_str("ff::01").unwrap(),
                1111,
                0,
                0,
            ),
            SocketAddrV6::new(
                Ipv6Addr::from_str("ff::02").unwrap(),
                2222,
                0,
                0,
            ),
            SocketAddrV6::new(
                Ipv6Addr::from_str("ff::03").unwrap(),
                3333,
                0,
                0,
            ),
        ];
        let clickhouse_addr = SocketAddrV6::new(
            Ipv6Addr::from_str("fe::01").unwrap(),
            4444,
            0,
            0,
        );
        let crucible_addr = SocketAddrV6::new(
            Ipv6Addr::from_str("fd::02").unwrap(),
            5555,
            0,
            0,
        );

        let srv_crdb = SRV::Service(ServiceName::Cockroach);
        let srv_clickhouse = SRV::Service(ServiceName::Clickhouse);
        let srv_backend = SRV::Backend(BackendName::Crucible, Uuid::new_v4());

        let mut dns_builder = DnsConfigBuilder::new();
        for db_ip in &cockroach_addrs {
            let zone =
                dns_builder.host_zone(Uuid::new_v4(), *db_ip.ip()).unwrap();
            dns_builder
                .service_backend(srv_crdb.clone(), &zone, db_ip.port())
                .unwrap();
        }

        let zone = dns_builder
            .host_zone(Uuid::new_v4(), *clickhouse_addr.ip())
            .unwrap();
        dns_builder
            .service_backend(
                srv_clickhouse.clone(),
                &zone,
                clickhouse_addr.port(),
            )
            .unwrap();

        let zone =
            dns_builder.host_zone(Uuid::new_v4(), *crucible_addr.ip()).unwrap();
        dns_builder
            .service_backend(srv_backend.clone(), &zone, crucible_addr.port())
            .unwrap();

        let mut dns_config = dns_builder.build();
        updater.dns_initialize(&dns_config).await.unwrap();

        // Look up Cockroach
        let ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert!(cockroach_addrs.iter().any(|addr| addr.ip() == &ip));

        // Look up Clickhouse
        let ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Clickhouse))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(&ip, clickhouse_addr.ip());

        // Look up Backend Service
        let ip = resolver
            .lookup_ipv6(srv_backend)
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(&ip, crucible_addr.ip());

        // If we deploy a new generation that removes all records, then we don't
        // find anything any more.
        dns_config.generation += 1;
        dns_config.zones[0].records = Vec::new();
        updater.dns_initialize(&dns_config).await.unwrap();

        // If we remove the records for all services, we won't find them any
        // more.  (e.g., there's no hidden caching going on)
        let error = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect_err("unexpectedly found records");
        assert_matches!(
            error,
            ResolveError::Resolve(error)
                if matches!(error.kind(),
                    trust_dns_resolver::error::ResolveErrorKind::NoRecordsFound { .. }
                )
        );

        // If we remove the zone altogether, we'll get a different resolution
        // error because the DNS server is no longer authoritative for this
        // zone.
        dns_config.generation += 1;
        dns_config.zones = Vec::new();
        updater.dns_initialize(&dns_config).await.unwrap();

        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn update_record() {
        let logctx = test_setup_log("update_record");
        let dns_server = DnsServer::create(&logctx.log).await;

        let mut address_getter = LocalAddressGetter::default();
        address_getter.add_dns_server(
            dns_server.dns_server_address(),
            dns_server.dropshot_server_address(),
        );

        let resolver = Resolver::new(&address_getter)
            .expect("Error creating localhost resolver");
        let updater = Updater::new(&address_getter, logctx.log.clone());

        // Insert a record, observe that it exists.
        let mut dns_builder = DnsConfigBuilder::new();
        let ip1 = Ipv6Addr::from_str("ff::01").unwrap();
        let zone = dns_builder.host_zone(Uuid::new_v4(), ip1).unwrap();
        let srv_crdb = SRV::Service(ServiceName::Cockroach);
        dns_builder.service_backend(srv_crdb.clone(), &zone, 12345).unwrap();
        let dns_config = dns_builder.build();
        updater.dns_initialize(&dns_config).await.unwrap();
        let found_ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(found_ip, ip1);

        // If we insert the same record with a new address, it should be
        // updated.
        let mut dns_builder = DnsConfigBuilder::new();
        let ip2 = Ipv6Addr::from_str("ee::02").unwrap();
        let zone = dns_builder.host_zone(Uuid::new_v4(), ip2).unwrap();
        let srv_crdb = SRV::Service(ServiceName::Cockroach);
        dns_builder.service_backend(srv_crdb.clone(), &zone, 54321).unwrap();
        let mut dns_config = dns_builder.build();
        dns_config.generation += 1;
        updater.dns_initialize(&dns_config).await.unwrap();
        let found_ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(found_ip, ip2);

        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }
}
