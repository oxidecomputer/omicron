// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;
use std::net::SocketAddr;

use hickory_resolver::config::NameServerConfig;
use hickory_resolver::config::Protocol;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::TokioAsyncResolver;
use hyper::client::connect::dns::Name;
use omicron_common::address::DNS_PORT;

/// Wrapper around hickory-resolver to provide name resolution
/// using a given set of DNS servers for use with reqwest.
pub struct Resolver(TokioAsyncResolver);

impl Resolver {
    pub fn new(dns_servers: &[IpAddr]) -> Resolver {
        assert!(!dns_servers.is_empty());
        let mut rc = ResolverConfig::new();
        for addr in dns_servers {
            rc.add_name_server(NameServerConfig {
                socket_addr: SocketAddr::new(*addr, DNS_PORT),
                protocol: Protocol::Udp,
                tls_dns_name: None,
                trust_negative_responses: false,
                bind_addr: None,
            });
        }
        let mut opts = ResolverOpts::default();
        opts.edns0 = true;
        opts.use_hosts_file = false;
        // Do as many requests in parallel as we have configured servers
        opts.num_concurrent_reqs = dns_servers.len();
        Resolver(TokioAsyncResolver::tokio(rc, opts))
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
