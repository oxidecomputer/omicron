// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;
use std::net::SocketAddr;

use hickory_resolver::TokioResolver;
use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::ResolveHosts;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::proto::xfer::Protocol;
use omicron_common::address::DNS_PORT;
use reqwest::dns::Name;

/// Wrapper around hickory-resolver to provide name resolution
/// using a given set of DNS servers for use with reqwest.
pub struct Resolver(TokioResolver);

impl Resolver {
    pub fn new(dns_servers: &[IpAddr]) -> Resolver {
        assert!(!dns_servers.is_empty());
        let mut rc = ResolverConfig::new();
        for addr in dns_servers {
            rc.add_name_server(NameServerConfig::new(
                SocketAddr::new(*addr, DNS_PORT),
                Protocol::Udp,
            ));
        }
        let mut opts = ResolverOpts::default();
        // Enable edns for potentially larger records
        opts.edns0 = true;
        opts.use_hosts_file = ResolveHosts::Never;
        // Do as many requests in parallel as we have configured servers
        opts.num_concurrent_reqs = dns_servers.len();
        Resolver(
            TokioResolver::builder_with_config(
                rc,
                TokioConnectionProvider::default(),
            )
            .with_options(opts)
            .build(),
        )
    }
}

impl reqwest::dns::Resolve for Resolver {
    fn resolve(&self, name: Name) -> reqwest::dns::Resolving {
        let resolver = self.0.clone();
        Box::pin(async move {
            let ips = resolver.lookup_ip(name.as_str()).await?;
            let addrs = ips
                .into_iter()
                // hickory-resolver returns `IpAddr`s but reqwest wants
                // `SocketAddr`s (useful if you have a custom resolver that
                // returns a scoped IPv6 address). The port provided here
                // is ignored in favour of the scheme default (http/80,
                // https/443) or any custom port provided in the URL.
                .map(|ip| SocketAddr::new(ip, 0));
            Ok(Box::new(addrs) as Box<_>)
        })
    }
}
