// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use hyper::client::connect::dns::Name;
use omicron_common::address::{
    Ipv6Subnet, ReservedRackSubnet, AZ_PREFIX, DNS_PORT,
};
use slog::{debug, error, info, trace};
use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};
use trust_dns_resolver::config::{
    LookupIpStrategy, NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::lookup::SrvLookup;
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
    resolver: TokioAsyncResolver,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// By implementing this trait, [Resolver] can be used as an argument to
// [reqwest::ClientBuilder::dns_resolver].
impl reqwest::dns::Resolve for Resolver {
    fn resolve(&self, name: Name) -> reqwest::dns::Resolving {
        let this = self.clone();
        Box::pin(async move {
            this.lookup_sockets_v6_raw(name.as_str())
                .await
                .map_err(|err| -> BoxError { Box::new(err) })
        })
    }
}

impl Resolver {
    /// Construct a new DNS resolver from specific DNS server addresses.
    pub fn new_from_addrs(
        log: slog::Logger,
        dns_addrs: &[SocketAddr],
    ) -> Result<Self, ResolveError> {
        info!(log, "new DNS resolver"; "addresses" => ?dns_addrs);

        let mut rc = ResolverConfig::new();
        let dns_server_count = dns_addrs.len();
        for &socket_addr in dns_addrs.into_iter() {
            rc.add_name_server(NameServerConfig {
                socket_addr,
                protocol: Protocol::Udp,
                tls_dns_name: None,
                trust_nx_responses: false,
                bind_addr: None,
            });
        }
        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = false;
        opts.num_concurrent_reqs = dns_server_count;
        // The underlay is IPv6 only, so this helps avoid needless lookups of
        // the IPv4 variant.
        opts.ip_strategy = LookupIpStrategy::Ipv6Only;
        opts.negative_max_ttl = Some(std::time::Duration::from_secs(15));
        let resolver = TokioAsyncResolver::tokio(rc, opts)?;

        Ok(Self { log, resolver })
    }

    /// Convenience wrapper for [`Resolver::new_from_subnet`] that determines
    /// the subnet based on a provided IP address and then uses the DNS
    /// resolvers for that subnet.
    pub fn new_from_ip(
        log: slog::Logger,
        address: Ipv6Addr,
    ) -> Result<Self, ResolveError> {
        let subnet = Ipv6Subnet::<AZ_PREFIX>::new(address);
        Self::new_from_subnet(log, subnet)
    }

    /// Return a resolver that uses the system configuration (usually
    /// /etc/resolv.conf) for the underlying nameservers.
    pub fn new_with_resolver(
        log: slog::Logger,
        resolver: TokioAsyncResolver,
    ) -> Self {
        Self { log, resolver }
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

    /// Create a DNS resolver using the implied DNS servers within this subnet.
    ///
    /// The addresses of the DNS servers are inferred within an Availability
    /// Zone's subnet: normally, each rack within an AZ (/48) gets a unique
    /// subnet (/56), but the FIRST /56 is reserved for internal DNS servers.
    ///
    /// For more details on this "reserved" rack subnet, refer to
    /// [omicron_common::address::ReservedRackSubnet].
    pub fn new_from_subnet(
        log: slog::Logger,
        subnet: Ipv6Subnet<AZ_PREFIX>,
    ) -> Result<Self, ResolveError> {
        let dns_ips = Self::servers_from_subnet(subnet);
        Resolver::new_from_addrs(log, &dns_ips)
    }

    /// Remove all entries from the resolver's cache.
    pub fn clear_cache(&self) {
        self.resolver.clear_cache();
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
        let name = srv.srv_name();
        debug!(self.log, "lookup_ipv6 srv"; "dns_name" => &name);
        let response = self.resolver.ipv6_lookup(&name).await?;
        let address = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;
        Ok(*address)
    }

    /// Returns the targets of the SRV records for a DNS name
    ///
    /// The returned values are generally other DNS names that themselves would
    /// need to be looked up to find A/AAAA records.
    pub async fn lookup_srv(
        &self,
        srv: crate::ServiceName,
    ) -> Result<Vec<(String, u16)>, ResolveError> {
        let name = srv.srv_name();
        trace!(self.log, "lookup_srv"; "dns_name" => &name);
        let response = self.resolver.srv_lookup(&name).await?;
        debug!(
            self.log,
            "lookup_srv";
            "dns_name" => &name,
            "response" => ?response
        );

        Ok(response
            .into_iter()
            .map(|srv| (srv.target().to_string(), srv.port()))
            .collect())
    }

    pub async fn lookup_all_ipv6(
        &self,
        srv: crate::ServiceName,
    ) -> Result<Vec<Ipv6Addr>, ResolveError> {
        let name = srv.srv_name();
        trace!(self.log, "lookup_all_ipv6 srv"; "dns_name" => &name);
        let response = self.resolver.srv_lookup(&name).await?;
        debug!(
            self.log,
            "lookup_all_ipv6 srv";
            "dns_name" => &name,
            "response" => ?response
        );
        let addrs = self
            .lookup_service_targets(response)
            .await
            .map(|addrv6| *addrv6.ip())
            .collect::<Vec<_>>();
        if !addrs.is_empty() {
            Ok(addrs)
        } else {
            Err(ResolveError::NotFound(srv))
        }
    }

    /// Looks up a single [`SocketAddrV6`] based on the SRV name
    /// Returns an error if the record does not exist.
    // TODO-robustness: any callers of this should probably be using
    // all the targets for a given SRV and not just the first one
    // we get, see [`Resolver::lookup_all_socket_v6`].
    pub async fn lookup_socket_v6(
        &self,
        service: crate::ServiceName,
    ) -> Result<SocketAddrV6, ResolveError> {
        let name = service.srv_name();
        trace!(self.log, "lookup_socket_v6 srv"; "dns_name" => &name);
        let response = self.resolver.srv_lookup(&name).await?;
        debug!(
            self.log,
            "lookup_socket_v6 srv";
            "dns_name" => &name,
            "response" => ?response
        );

        self.lookup_service_targets(response)
            .await
            .next()
            .ok_or_else(|| ResolveError::NotFound(service))
    }

    /// Returns the targets of the SRV records for a DNS name.
    ///
    /// Unlike [`Resolver::lookup_srv`], this will further lookup the returned
    /// targets and return a list of [`SocketAddrV6`].
    pub async fn lookup_all_socket_v6(
        &self,
        service: crate::ServiceName,
    ) -> Result<Vec<SocketAddrV6>, ResolveError> {
        let name = service.srv_name();
        trace!(self.log, "lookup_all_socket_v6 srv"; "dns_name" => &name);
        let response = self.resolver.srv_lookup(&name).await?;
        debug!(
            self.log,
            "lookup_all_socket_v6 srv";
            "dns_name" => &name,
            "response" => ?response
        );

        let results =
            self.lookup_service_targets(response).await.collect::<Vec<_>>();
        if !results.is_empty() {
            Ok(results)
        } else {
            Err(ResolveError::NotFound(service))
        }
    }

    // Returns an iterator of SocketAddrs for the specified SRV name.
    //
    // Acts on a raw string for compatibility with the reqwest::dns::Resolve
    // trait, rather than a strongly-typed service name.
    async fn lookup_sockets_v6_raw(
        &self,
        name: &str,
    ) -> Result<Box<dyn Iterator<Item = SocketAddr> + Send>, ResolveError> {
        debug!(self.log, "lookup_sockets_v6_raw srv"; "dns_name" => &name);
        let response = self.resolver.srv_lookup(name).await?;
        let mut results = self
            .lookup_service_targets(response)
            .await
            .map(|addrv6| SocketAddr::V6(addrv6))
            .peekable();
        if results.peek().is_some() {
            Ok(Box::new(results))
        } else {
            Err(ResolveError::NotFoundByString(name.to_string()))
        }
    }

    pub async fn lookup_ip(
        &self,
        srv: crate::ServiceName,
    ) -> Result<IpAddr, ResolveError> {
        let name = srv.srv_name();
        debug!(self.log, "lookup srv"; "dns_name" => &name);
        let response = self.resolver.lookup_ip(&name).await?;
        let address = response
            .iter()
            .next()
            .ok_or_else(|| ResolveError::NotFound(srv))?;
        Ok(address)
    }

    /// Returns an iterator of [`SocketAddrV6`]'s for the targets of the given
    /// SRV lookup response.
    // SRV records have a target, which is itself another DNS name that needs
    // to be looked up in order to get to the actual IP addresses. Many DNS
    // servers (including ours) return these IP addresses directly in the
    // response to the SRV query as Additional records. In theory
    // `SrvLookup::ip_iter()` would suffice but some issues:
    //   (1) it returns `IpAddr`'s rather than `SocketAddr`'s
    //   (2) it doesn't actually return all the addresses from the Additional
    //       section of the DNS server's response.
    //       See bluejekyll/trust-dns#1980
    //
    // (1) is not a huge deal as we can try to match up the targets ourselves
    // to grab the port for creating a `SocketAddr` but (2) means we need to do
    // the lookups explicitly.
    async fn lookup_service_targets(
        &self,
        service_lookup: SrvLookup,
    ) -> impl Iterator<Item = SocketAddrV6> + Send {
        let futures =
            std::iter::repeat((self.log.clone(), self.resolver.clone()))
                .zip(service_lookup.into_iter())
                .map(|((log, resolver), srv)| async move {
                    let target = srv.target();
                    let port = srv.port();
                    trace!(
                        log,
                        "lookup_service_targets: looking up SRV target";
                        "name" => ?target,
                    );
                    resolver
                        .ipv6_lookup(target.clone())
                        .await
                        .map(|ips| (ips, port))
                        .map_err(|err| (target.clone(), err))
                });
        // What do we do if some of these queries succeed while others fail?  We
        // may have some addresses, but the list might be incomplete.  That
        // might be okay for some use cases but not others.  For now, we do the
        // simple thing and return as many as we got and log any errors.
        // In the future, we'll want a more cueball-like resolver interface
        // that better deals with these cases.
        let log = self.log.clone();
        futures::future::join_all(futures)
            .await
            .into_iter()
            .flat_map(move |target| match target {
                Ok((ips, port)) => Some(
                    ips.into_iter()
                        .map(move |ip| SocketAddrV6::new(ip, port, 0, 0)),
                ),
                Err((target, err)) => {
                    error!(
                        log,
                        "lookup_service_targets: failed looking up target";
                        "name" => ?target,
                        "error" => ?err,
                    );
                    None
                }
            })
            .flatten()
    }
}

#[cfg(test)]
mod test {
    use super::ResolveError;
    use super::Resolver;
    use crate::DNS_ZONE;
    use crate::{DnsConfigBuilder, ServiceName};
    use anyhow::Context;
    use assert_matches::assert_matches;
    use dns_service_client::types::DnsConfigParams;
    use dropshot::{
        endpoint, ApiDescription, HandlerTaskMode, HttpError, HttpResponseOk,
        RequestContext,
    };
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
            self.dns_server.local_address()
        }

        fn cleanup_successful(mut self) {
            self.successful = true;
            drop(self)
        }

        fn resolver(&self) -> anyhow::Result<Resolver> {
            let log = self.log.new(o!("component" => "DnsResolver"));
            Resolver::new_from_addrs(log, &[self.dns_server_address()])
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
                .service_backend_zone(srv_crdb, &zone, db_ip.port())
                .unwrap();
        }

        let zone = dns_builder
            .host_zone(Uuid::new_v4(), *clickhouse_addr.ip())
            .unwrap();
        dns_builder
            .service_backend_zone(srv_clickhouse, &zone, clickhouse_addr.port())
            .unwrap();

        let zone =
            dns_builder.host_zone(Uuid::new_v4(), *crucible_addr.ip()).unwrap();
        dns_builder
            .service_backend_zone(srv_backend, &zone, crucible_addr.port())
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

        // Look up all the Cockroach addresses.
        let mut ips =
            resolver.lookup_all_ipv6(ServiceName::Cockroach).await.expect(
                "Should have been able to look up all CockroachDB addresses",
            );
        ips.sort();
        assert_eq!(
            ips,
            cockroach_addrs.iter().map(|s| *s.ip()).collect::<Vec<_>>()
        );

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
        dns_builder.service_backend_zone(srv_crdb, &zone, 12345).unwrap();
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
        dns_builder.service_backend_zone(srv_crdb, &zone, 54321).unwrap();
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

    // What follows is a "test endpoint" to validate that the integration of
    // the DNS server, resolver, and progenitor all work together correctly.

    #[endpoint {
        method = GET,
        path = "/test",
    }]
    async fn test_endpoint(
        rqctx: RequestContext<u32>,
    ) -> Result<HttpResponseOk<u32>, HttpError> {
        Ok(HttpResponseOk(*rqctx.context()))
    }

    fn api() -> ApiDescription<u32> {
        let mut api = ApiDescription::new();
        api.register(test_endpoint).unwrap();
        api
    }

    progenitor::generate_api!(
        spec = "tests/output/test-server.json",
        inner_type = slog::Logger,
        pre_hook = (|log: &slog::Logger, request: &reqwest::Request| {
            slog::debug!(log, "client request";
                "method" => %request.method(),
                "uri" => %request.url(),
                "body" => ?&request.body(),
            );
        }),
        post_hook = (|log: &slog::Logger, result: &Result<_, _>| {
            slog::debug!(log, "client response"; "result" => ?result);
        }),
    );

    // Verify that we have an up-to-date representation
    // of this server's API as JSON.
    //
    // We'll need this to be up-to-date to have a reliable
    // Progenitor client.
    fn expect_openapi_json_valid_for_test_server() {
        let api = api();
        let openapi = api.openapi("Test Server", "v0.1.0");
        let mut output = std::io::Cursor::new(Vec::new());
        openapi.write(&mut output).unwrap();
        expectorate::assert_contents(
            "tests/output/test-server.json",
            std::str::from_utf8(&output.into_inner()).unwrap(),
        );
    }

    fn start_test_server(
        log: slog::Logger,
        label: u32,
    ) -> dropshot::HttpServer<u32> {
        let config_dropshot = dropshot::ConfigDropshot {
            bind_address: "[::1]:0".parse().unwrap(),
            ..Default::default()
        };
        dropshot::HttpServerStarter::new(&config_dropshot, api(), label, &log)
            .unwrap()
            .start()
    }

    #[tokio::test]
    async fn resolver_can_be_used_with_progenitor_client() {
        let logctx =
            test_setup_log("resolver_can_be_used_with_progenitor_client");

        // Confirm that we can create a progenitor client for this server.
        expect_openapi_json_valid_for_test_server();

        // Next, create a DNS server, and a corresponding resolver.
        let dns_server = DnsServer::create(&logctx.log).await;
        let resolver = Resolver::new_from_addrs(
            logctx.log.clone(),
            &[dns_server.dns_server.local_address()],
        )
        .unwrap();

        // Start a test server, but don't register it with the DNS server (yet).
        let label = 1234;
        let server = start_test_server(logctx.log.clone(), label);
        let ip = match server.local_addr().ip() {
            std::net::IpAddr::V6(ip) => ip,
            _ => panic!("Expected IPv6"),
        };
        let port = server.local_addr().port();

        // Use the resolver -- referencing our DNS server -- in the construction
        // of a progenitor client.
        //
        // We'll use the SRV record for Nexus, even though it's just our
        // standalone test server.
        let dns_name = crate::ServiceName::Nexus.srv_name();
        let reqwest_client = reqwest::ClientBuilder::new()
            .dns_resolver(resolver.clone().into())
            .build()
            .expect("Failed to build client");

        // NOTE: We explicitly pass the port here, before DNS resolution,
        // because the DNS support in reqwest does not actually use the ports
        // returned by the resolver.
        let client = Client::new_with_client(
            &format!("http://{dns_name}:{port}"),
            reqwest_client,
            logctx.log.clone(),
        );

        // The DNS server is running, but has no records. Expect a failure.
        let err = client.test_endpoint().await.unwrap_err();
        assert!(
            err.to_string().contains("no record found"),
            "Unexpected Error (expected 'no record found'): {err}",
        );

        // Add a record for the new service.
        let mut dns_config = DnsConfigBuilder::new();
        let zone = dns_config.host_zone(Uuid::new_v4(), ip).unwrap();
        dns_config
            .service_backend_zone(ServiceName::Nexus, &zone, port)
            .unwrap();
        let dns_config = dns_config.build();
        dns_server.update(&dns_config).await.unwrap();

        // Confirm that we can access this record manually.
        let found_ip = resolver
            .lookup_ipv6(ServiceName::Nexus)
            .await
            .expect("Should have been able to look up IP address");
        assert_eq!(found_ip, ip);

        // Confirm that the progenitor client can access this record too.
        let value = client.test_endpoint().await.unwrap();
        assert_eq!(value.into_inner(), label);

        server.close().await.expect("Failed to stop test server");
        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn resolver_can_access_backup_dns_server() {
        let logctx = test_setup_log("resolver_can_access_backup_dns_server");

        // Confirm that we can create a progenitor client for this server.
        expect_openapi_json_valid_for_test_server();

        // Create DNS servers, and a corresponding resolver.
        let dns_server1 = DnsServer::create(&logctx.log).await;
        let dns_server2 = DnsServer::create(&logctx.log).await;
        let resolver = Resolver::new_from_addrs(
            logctx.log.clone(),
            &[
                dns_server1.dns_server.local_address(),
                dns_server2.dns_server.local_address(),
            ],
        )
        .unwrap();

        // Start a test server, but don't register it with the DNS server (yet).
        let label = 1234;
        let server = start_test_server(logctx.log.clone(), label);
        let ip = match server.local_addr().ip() {
            std::net::IpAddr::V6(ip) => ip,
            _ => panic!("Expected IPv6"),
        };
        let port = server.local_addr().port();

        // Use the resolver -- referencing our DNS server -- in the construction
        // of a progenitor client.
        //
        // We'll use the SRV record for Nexus, even though it's just our
        // standalone test server.
        let dns_name = crate::ServiceName::Nexus.srv_name();
        let reqwest_client = reqwest::ClientBuilder::new()
            .dns_resolver(resolver.clone().into())
            .build()
            .expect("Failed to build client");

        // NOTE: We explicitly pass the port here, before DNS resolution,
        // because the DNS support in reqwest does not actually use the ports
        // returned by the resolver.
        let client = Client::new_with_client(
            &format!("http://{dns_name}:{port}"),
            reqwest_client,
            logctx.log.clone(),
        );

        // The DNS server is running, but has no records. Expect a failure.
        let err = client.test_endpoint().await.unwrap_err();
        assert!(
            err.to_string().contains("no record found"),
            "Unexpected Error (expected 'no record found'): {err}",
        );

        // Add a record for the new service, but only to the second DNS server.
        // Since both servers are authoritative, we also shut down the first
        // server.
        let mut dns_config = DnsConfigBuilder::new();
        let zone = dns_config.host_zone(Uuid::new_v4(), ip).unwrap();
        dns_config
            .service_backend_zone(ServiceName::Nexus, &zone, port)
            .unwrap();
        let dns_config = dns_config.build();
        dns_server1.cleanup_successful();
        dns_server2.update(&dns_config).await.unwrap();

        // Confirm that the progenitor client can access this record,
        // even though the first DNS server won't respond anymore.
        let value = client.test_endpoint().await.unwrap();
        assert_eq!(value.into_inner(), label);

        server.close().await.expect("Failed to stop test server");
        dns_server2.cleanup_successful();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn resolver_doesnt_bail_on_missing_targets() {
        let logctx = test_setup_log("resolver_doesnt_bail_on_missing_targets");

        // Confirm that we can create a progenitor client for this server.
        expect_openapi_json_valid_for_test_server();

        // Next, create a DNS server, and a corresponding resolver.
        let dns_server = DnsServer::create(&logctx.log).await;
        let resolver = Resolver::new_from_addrs(
            logctx.log.clone(),
            &[dns_server.dns_server.local_address()],
        )
        .unwrap();

        // Create DNS config with a single service and multiple backends.
        let mut dns_config = DnsConfigBuilder::new();

        let id1 = Uuid::new_v4();
        let ip1 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x1);
        let addr1 = SocketAddrV6::new(ip1, 15001, 0, 0);
        let zone1 = dns_config.host_zone(id1, ip1).unwrap();
        dns_config
            .service_backend_zone(ServiceName::Cockroach, &zone1, addr1.port())
            .unwrap();

        let id2 = Uuid::new_v4();
        let ip2 = Ipv6Addr::new(0xfd, 0, 0, 0, 0, 0, 0, 0x2);
        let addr2 = SocketAddrV6::new(ip2, 15002, 0, 0);
        let zone2 = dns_config.host_zone(id2, ip2).unwrap();
        dns_config
            .service_backend_zone(ServiceName::Cockroach, &zone2, addr2.port())
            .unwrap();

        // Plumb records onto DNS server
        let mut dns_config = dns_config.build();
        dns_server.update(&dns_config).await.unwrap();

        // Using the resolver we should get back both addresses
        let mut addrs =
            resolver.lookup_all_socket_v6(ServiceName::Cockroach).await.expect(
                "Should have been able to look up all CockroachDB addresses",
            );
        addrs.sort();
        assert_eq!(addrs, [addr1, addr2]);

        // Now let's remove one of the AAAA records for a zone/target.
        // The lookup should still succeed and return the other address.
        dns_config.generation += 1;
        let root = dns_config
            .zones
            .iter_mut()
            .find(|zone| zone.zone_name == DNS_ZONE)
            .expect("root dns zone missing?");
        let zone1_records = root
            .records
            .remove(&zone1.dns_name())
            .expect("Cockroach Zone record missing?");
        // There should've been just the one address in this record
        assert_eq!(zone1_records, [ip1.into()]);

        // Both SRV records should stil exist
        let srv_records = root
            .records
            .get(&ServiceName::Cockroach.dns_name())
            .expect("Cockroach SRV records missing?");
        assert_eq!(srv_records.len(), 2);

        // Update DNS server
        dns_server.update(&dns_config).await.unwrap();

        // This time the resolver should only return one address
        let addrs = resolver
            .lookup_all_socket_v6(ServiceName::Cockroach)
            .await
            .expect("CockroachDB addresses lookup take 2");
        assert_eq!(addrs, [addr2]);

        // But both targets should still be there
        let mut targets =
            resolver.lookup_srv(ServiceName::Cockroach).await.unwrap();
        targets.sort();
        let mut expected_targets = [
            (format!("{}.{DNS_ZONE}.", zone1.dns_name()), addr1.port()),
            (format!("{}.{DNS_ZONE}.", zone2.dns_name()), addr2.port()),
        ];
        expected_targets.sort();
        assert_eq!(targets, expected_targets);

        // Finally, let's remove the last AAAA record as well
        dns_config.generation += 1;
        let root = dns_config
            .zones
            .iter_mut()
            .find(|zone| zone.zone_name == DNS_ZONE)
            .expect("root dns zone missing?");
        let zone2_records = root
            .records
            .remove(&zone2.dns_name())
            .expect("Cockroach Zone record missing?");
        // There should've been just the one address in this record
        assert_eq!(zone2_records, [ip2.into()]);

        // Update DNS server
        dns_server.update(&dns_config).await.unwrap();

        // This time the resolver should return an error
        let err = resolver
            .lookup_all_socket_v6(ServiceName::Cockroach)
            .await
            .expect_err("CockroachDB addresses lookup take 3 worked??");
        assert!(matches!(err, ResolveError::NotFound(ServiceName::Cockroach)));

        // But both targets should still be there
        let mut targets =
            resolver.lookup_srv(ServiceName::Cockroach).await.unwrap();
        targets.sort();
        assert_eq!(targets, expected_targets);

        dns_server.cleanup_successful();
        logctx.cleanup_successful();
    }
}
