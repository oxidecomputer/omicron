// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;
use std::net::SocketAddr;

use hyper::client::connect::dns::Name;
use omicron_common::address::DNS_PORT;
use trust_dns_resolver::config::NameServerConfig;
use trust_dns_resolver::config::Protocol;
use trust_dns_resolver::config::ResolverConfig;
use trust_dns_resolver::config::ResolverOpts;
use trust_dns_resolver::TokioAsyncResolver;

/// Wrapper around trust-dns-resolver to provide name resolution
/// using a given set of DNS servers for use with reqwest.
pub struct Resolver(TokioAsyncResolver);

impl Resolver {
    pub fn new(dns_servers: &[IpAddr]) -> Resolver {
        let mut rc = ResolverConfig::new();
        for addr in dns_servers {
            rc.add_name_server(NameServerConfig {
                socket_addr: SocketAddr::new(*addr, DNS_PORT),
                protocol: Protocol::Udp,
                tls_dns_name: None,
                trust_nx_responses: false,
                bind_addr: None,
            });
        }
        let mut opts = ResolverOpts::default();
        opts.use_hosts_file = false;
        opts.num_concurrent_reqs = dns_servers.len();
        Resolver(
            TokioAsyncResolver::tokio(rc, opts)
                .expect("creating resovler shouldn't fail"),
        )
    }
}

impl reqwest::dns::Resolve for Resolver {
    fn resolve(&self, name: Name) -> reqwest::dns::Resolving {
        let resolver = self.0.clone();
        Box::pin(async move {
            let ips = resolver.lookup_ip(name.as_str()).await?;
            Ok(Box::new(ips.into_iter().map(|ip| SocketAddr::new(ip, 0)))
                as Box<_>)
        })
    }
}
