// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DNS_ZONE;
use omicron_common::address::{
    Ipv6Subnet, ReservedRackSubnet, AZ_PREFIX, DNS_PORT,
};
use slog::{debug, info};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use trust_dns_proto::rr::record_type::RecordType;
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::TokioAsyncResolver;

pub type DnsError = dns_service_client::Error<dns_service_client::types::Error>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ResolveError {
    #[error(transparent)]
    Resolve(#[from] trust_dns_resolver::error::ResolveError),

    #[error("Record not found for SRV key: {}", .0.dns_name())]
    NotFound(crate::ServiceName),

    #[error("Record not found for {0}")]
    NotFoundByString(String),
}

/// A wrapper around a DNS resolver, providing a way to conveniently
/// look up IP addresses of services based on their SRV keys.
#[derive(Clone)]
pub struct Resolver {
    log: slog::Logger,
    inner: Box<TokioAsyncResolver>,
}

impl Resolver {
    pub fn new_from_addrs(
        log: slog::Logger,
        dns_addrs: Vec<SocketAddr>,
    ) -> Result<Self, ResolveError> {
        info!(log, "new DNS resolver"; "addresses" => ?dns_addrs);

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

        Ok(Self { inner, log })
    }

    /// Convenience wrapper for [`Resolver::new_from_addrs`] that determines
    /// the subnet based on a provided IP address and then uses the DNS
    /// resolvers for that subnet.
    pub fn new_from_ip(
        log: slog::Logger,
        address: Ipv6Addr,
    ) -> Result<Self, ResolveError> {
        let subnet = Ipv6Subnet::<AZ_PREFIX>::new(address);
        Self::new_from_subnet(log, subnet)
    }

    // TODO-correctness This function and its callers make assumptions about how
    // many internal DNS servers there are on the subnet and where they are.  Is
    // that okay?  It would seem more flexible not to assume this.  Instead, we
    // could make a best effort to distribute the list of DNS servers as
    // configuration and then use new_from_addrs().  Further, we should use a
    // mechanism like node-cueball's "bootstrap resolvers", where the set of
    // nameservers is _itself_ provided by a DNS name.  We would periodically
    // re-resolve this.  That's how we'd learn about dynamic changes to the set
    // of DNS servers.
    pub fn servers_from_subnet(
        subnet: Ipv6Subnet<AZ_PREFIX>,
    ) -> Vec<SocketAddr> {
        ReservedRackSubnet::new(subnet)
            .get_dns_subnets()
            .into_iter()
            .map(|dns_subnet| {
                let ip_addr = IpAddr::V6(dns_subnet.dns_address().ip());
                SocketAddr::new(ip_addr, DNS_PORT)
            })
            .collect()
    }

    pub fn new_from_subnet(
        log: slog::Logger,
        subnet: Ipv6Subnet<AZ_PREFIX>,
    ) -> Result<Self, ResolveError> {
        let dns_ips = Self::servers_from_subnet(subnet);
        Resolver::new_from_addrs(log, dns_ips)
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
        srv: crate::ServiceName,
    ) -> Result<Ipv6Addr, ResolveError> {
        let name = format!("{}.{}", srv.dns_name(), DNS_ZONE);
        debug!(self.log, "lookup_ipv6 srv"; "dns_name" => &name);
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
        srv: crate::ServiceName,
    ) -> Result<SocketAddrV6, ResolveError> {
        let name = format!("{}.{}", srv.dns_name(), DNS_ZONE);
        debug!(self.log, "lookup_socket_v6 srv"; "dns_name" => &name);
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
        srv: crate::ServiceName,
    ) -> Result<IpAddr, ResolveError> {
        let name = format!("{}.{}", srv.dns_name(), DNS_ZONE);
        debug!(self.log, "lookup srv"; "dns_name" => &name);
        let response = self.inner.lookup_ip(&name).await?;
        let address = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;
        Ok(address)
    }
}

#[cfg(test)]
mod test {
    use super::ResolveError;
    use super::Resolver;
    use crate::{DnsConfigBuilder, ServiceName};
    use anyhow::Context;
    use assert_matches::assert_matches;
    use dns_service_client::types::DnsConfigParams;
    use dropshot::HandlerTaskMode;
    use omicron_test_utils::dev::test_setup_log;
    use slog::{o, Logger};
    use std::collections::HashMap;
    use std::net::Ipv6Addr;
    use std::net::SocketAddr;
    use std::net::SocketAddrV6;
    use std::str::FromStr;
    use tempfile::TempDir;
    use uuid::Uuid;

    struct DnsServer {
        // We hang onto the storage_path even though it's never used because
        // dropping it causes it to be cleaned up.
        #[allow(dead_code)]
        storage_path: Option<TempDir>,
        // Similarly, we hang onto the Dropshot server to keep it running.
        #[allow(dead_code)]
        dropshot_server: dropshot::HttpServer<dns_server::http_server::Context>,

        successful: bool,
        dns_server: dns_server::dns_server::ServerHandle,
        config_client: dns_service_client::Client,
        log: slog::Logger,
    }

    impl DnsServer {
        async fn create(log: &Logger) -> Self {
            let storage_path =
                TempDir::new().expect("Failed to create temporary directory");
            let config_store = dns_server::storage::Config {
                keep_old_generations: 3,
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
                    bind_address: "[::1]:0".parse().unwrap(),
                },
                &dropshot::ConfigDropshot {
                    bind_address: "[::1]:0".parse().unwrap(),
                    request_body_max_bytes: 8 * 1024,
                    default_handler_task_mode: HandlerTaskMode::Detached,
                },
            )
            .await
            .unwrap();

            let config_client = dns_service_client::Client::new(
                &format!("http://{}", dropshot_server.local_addr()),
                log.new(o!("component" => "dns_client")),
            );

            Self {
                storage_path: Some(storage_path),
                dns_server,
                dropshot_server,
                successful: false,
                config_client,
                log: log.clone(),
            }
        }

        fn dns_server_address(&self) -> SocketAddr {
            *self.dns_server.local_address()
        }

        fn cleanup_successful(mut self) {
            self.successful = true;
            drop(self)
        }

        fn resolver(&self) -> anyhow::Result<Resolver> {
            let log = self.log.new(o!("component" => "DnsResolver"));
            Resolver::new_from_addrs(log, vec![self.dns_server_address()])
                .context("creating resolver for test DNS server")
        }

        async fn update(
            &self,
            dns_config: &DnsConfigParams,
        ) -> anyhow::Result<()> {
            self.config_client
                .dns_config_put(&dns_config)
                .await
                .context("updating DNS")
                .map(|_| ())
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

    // The resolver cannot look up IPs before records have been inserted.
    #[tokio::test]
    async fn lookup_nonexistent_record_fails() {
        let logctx = test_setup_log("lookup_nonexistent_record_fails");
        let dns_server = DnsServer::create(&logctx.log).await;
        let resolver = dns_server.resolver().unwrap();

        let err = resolver
            .lookup_ip(ServiceName::Cockroach)
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

        let mut dns_config = DnsConfigBuilder::new();
        let ip = Ipv6Addr::from_str("ff::01").unwrap();
        let zone = dns_config.host_zone(Uuid::new_v4(), ip).unwrap();
        dns_config
            .service_backend_zone(ServiceName::Cockroach, &zone, 12345)
            .unwrap();
        let dns_config = dns_config.build();
        dns_server.update(&dns_config).await.unwrap();

        let resolver = dns_server.resolver().unwrap();
        let found_ip = resolver
            .lookup_ipv6(ServiceName::Cockroach)
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

        let srv_crdb = ServiceName::Cockroach;
        let srv_clickhouse = ServiceName::Clickhouse;
        let srv_backend = ServiceName::Crucible(Uuid::new_v4());

        let mut dns_builder = DnsConfigBuilder::new();
        for db_ip in &cockroach_addrs {
            let zone =
                dns_builder.host_zone(Uuid::new_v4(), *db_ip.ip()).unwrap();
            dns_builder
                .service_backend_zone(srv_crdb.clone(), &zone, db_ip.port())
                .unwrap();
        }

        let zone = dns_builder
            .host_zone(Uuid::new_v4(), *clickhouse_addr.ip())
            .unwrap();
        dns_builder
            .service_backend_zone(
                srv_clickhouse.clone(),
                &zone,
                clickhouse_addr.port(),
            )
            .unwrap();

        let zone =
            dns_builder.host_zone(Uuid::new_v4(), *crucible_addr.ip()).unwrap();
        dns_builder
            .service_backend_zone(
                srv_backend.clone(),
                &zone,
                crucible_addr.port(),
            )
            .unwrap();

        let mut dns_config = dns_builder.build();
        dns_server.update(&dns_config).await.unwrap();

        // Look up Cockroach
        let resolver = dns_server.resolver().unwrap();
        let ip = resolver
            .lookup_ipv6(ServiceName::Cockroach)
            .await
            .expect("Should have been able to look up IP address");
        assert!(cockroach_addrs.iter().any(|addr| addr.ip() == &ip));

        // Look up Clickhouse
        let ip = resolver
            .lookup_ipv6(ServiceName::Clickhouse)
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
        dns_config.zones[0].records = HashMap::new();
        dns_server.update(&dns_config).await.unwrap();

        // If we remove the records for all services, we won't find them any
        // more.  (e.g., there's no hidden caching going on)
        let error = resolver
            .lookup_ipv6(ServiceName::Cockroach)
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
        dns_server.update(&dns_config).await.unwrap();

        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn update_record() {
        let logctx = test_setup_log("update_record");
        let dns_server = DnsServer::create(&logctx.log).await;
        let resolver = dns_server.resolver().unwrap();

        // Insert a record, observe that it exists.
        let mut dns_builder = DnsConfigBuilder::new();
        let ip1 = Ipv6Addr::from_str("ff::01").unwrap();
        let zone = dns_builder.host_zone(Uuid::new_v4(), ip1).unwrap();
        let srv_crdb = ServiceName::Cockroach;
        dns_builder
            .service_backend_zone(srv_crdb.clone(), &zone, 12345)
            .unwrap();
        let dns_config = dns_builder.build();
        dns_server.update(&dns_config).await.unwrap();
        let found_ip = resolver
            .lookup_ipv6(ServiceName::Cockroach)
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(found_ip, ip1);

        // If we insert the same record with a new address, it should be
        // updated.
        let mut dns_builder = DnsConfigBuilder::new();
        let ip2 = Ipv6Addr::from_str("ee::02").unwrap();
        let zone = dns_builder.host_zone(Uuid::new_v4(), ip2).unwrap();
        let srv_crdb = ServiceName::Cockroach;
        dns_builder
            .service_backend_zone(srv_crdb.clone(), &zone, 54321)
            .unwrap();
        let mut dns_config = dns_builder.build();
        dns_config.generation += 1;
        dns_server.update(&dns_config).await.unwrap();
        let found_ip = resolver
            .lookup_ipv6(ServiceName::Cockroach)
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(found_ip, ip2);

        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }
}
