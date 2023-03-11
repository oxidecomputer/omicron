// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

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

pub type AAAARecord = (crate::AAAA, SocketAddrV6);

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
        let response = self.inner.ipv6_lookup(&srv.to_string()).await?;
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
        let response =
            self.inner.lookup(&srv.to_string(), RecordType::SRV).await?;

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
    use crate::{BackendName, ServiceName, AAAA, SRV};
    use omicron_test_utils::dev::test_setup_log;
    use std::str::FromStr;
    use std::sync::Arc;
    use tempfile::TempDir;
    use uuid::Uuid;

    struct DnsServer {
        _storage: TempDir,
        dns_server: dns_server::dns_server::Server,
        dropshot_server:
            dropshot::HttpServer<Arc<dns_server::dropshot_server::Context>>,
    }

    impl DnsServer {
        async fn create(log: &Logger) -> Self {
            let storage =
                TempDir::new().expect("Failed to create temporary directory");

            let db = Arc::new(sled::open(&storage.path()).unwrap());

            let dns_server = {
                let db = db.clone();
                let log = log.clone();
                let dns_config = dns_server::dns_server::Config {
                    bind_address: "[::1]:0".to_string(),
                    zone: internal_dns_names::DNS_ZONE.into(),
                };

                dns_server::dns_server::run(log, db, dns_config).await.unwrap()
            };

            let config = dns_server::Config {
                log: dropshot::ConfigLogging::StderrTerminal {
                    level: dropshot::ConfigLoggingLevel::Info,
                },
                dropshot: dropshot::ConfigDropshot {
                    bind_address: "[::1]:0".parse().unwrap(),
                    request_body_max_bytes: 1024,
                    ..Default::default()
                },
                data: dns_server::dns_data::Config {
                    nmax_messages: 16,
                    storage_path: storage.path().to_string_lossy().into(),
                },
            };

            let dropshot_server =
                dns_server::start_dropshot_server(config, log.clone(), db)
                    .await
                    .unwrap();

            Self { _storage: storage, dns_server, dropshot_server }
        }

        fn dns_server_address(&self) -> SocketAddr {
            self.dns_server.address
        }

        fn dropshot_server_address(&self) -> SocketAddr {
            self.dropshot_server.local_addr()
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

        let records = HashMap::from([(
            SRV::Service(ServiceName::Cockroach),
            vec![(
                AAAA::Zone(Uuid::new_v4()),
                SocketAddrV6::new(
                    Ipv6Addr::from_str("ff::01").unwrap(),
                    12345,
                    0,
                    0,
                ),
            )],
        )]);
        updater.insert_dns_records(&records).await.unwrap();

        let ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(
            &ip,
            records[&SRV::Service(ServiceName::Cockroach)][0].1.ip()
        );

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

        let records = HashMap::from([
            // Three Cockroach services
            (
                srv_crdb.clone(),
                vec![
                    (AAAA::Zone(Uuid::new_v4()), cockroach_addrs[0]),
                    (AAAA::Zone(Uuid::new_v4()), cockroach_addrs[1]),
                    (AAAA::Zone(Uuid::new_v4()), cockroach_addrs[2]),
                ],
            ),
            // One Clickhouse service
            (
                srv_clickhouse.clone(),
                vec![(AAAA::Zone(Uuid::new_v4()), clickhouse_addr)],
            ),
            // One Backend service
            (
                srv_backend.clone(),
                vec![(AAAA::Zone(Uuid::new_v4()), crucible_addr)],
            ),
        ]);
        updater.insert_dns_records(&records).await.unwrap();

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

        // If we remove the AAAA records for two of the CRDB services,
        // only one will remain.
        updater
            .dns_records_delete(&vec![
                DnsRecordKey { name: records[&srv_crdb][0].0.to_string() },
                DnsRecordKey { name: records[&srv_crdb][1].0.to_string() },
            ])
            .await
            .expect("Should have been able to delete record");
        let ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(&ip, cockroach_addrs[2].ip());

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
        let srv_crdb = SRV::Service(ServiceName::Cockroach);
        let mut records = HashMap::from([(
            srv_crdb.clone(),
            vec![(
                AAAA::Zone(Uuid::new_v4()),
                SocketAddrV6::new(
                    Ipv6Addr::from_str("ff::01").unwrap(),
                    12345,
                    0,
                    0,
                ),
            )],
        )]);
        updater.insert_dns_records(&records).await.unwrap();
        let ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(&ip, records[&srv_crdb][0].1.ip());

        // If we insert the same record with a new address, it should be
        // updated.
        records.get_mut(&srv_crdb).unwrap()[0].1 = SocketAddrV6::new(
            Ipv6Addr::from_str("ee::02").unwrap(),
            54321,
            0,
            0,
        );
        updater.insert_dns_records(&records).await.unwrap();
        let ip = resolver
            .lookup_ipv6(SRV::Service(ServiceName::Cockroach))
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(&ip, records[&srv_crdb][0].1.ip());

        logctx.cleanup_successful();
    }
}
