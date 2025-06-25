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
            let mut ns_config = NameServerConfig::new(
                SocketAddr::new(*addr, DNS_PORT),
                Protocol::Udp,
            );
            // Explicltly set `trust_negative_responses` to false here to avoid
            // some churn.  It reasonably could be `true` here.
            //
            // Long ago, for internal DNS, we set this to false, disagreeing with
            // `NameServerConfig::new`'s defaults in that context.  We still set it false there,
            // with some reasoning why.  Other nameserver configurations are
            // either less picky, or only have one server, so the setting
            // doesn't matter, and we switched to `NameServerConfig::new` for
            // simplicity in many test configs.  Here, we could reasonably trust
            // that DNS servers queried by the rack will be reasonably behaved and more
            // human-operated than DNS servers in the rack, and set this to
            // true.  But then,
            // https://github.com/hickory-dns/hickory-dns/pull/3052 imminently
            // changes the default back to `false` for `NameServerConfig`
            // constructors.  So, sidestep the churn by setting this to false,
            // keeping it the same as it was until we decide it should be
            // otherwise.
            //
            // (The churn may also not materialize in hickory-dns 0.26.0, even
            // so. I've reached out and they may switch the `NameServerConfig`
            // builders to continue defaulting to trusting negative responses)
            ns_config.trust_negative_responses = false;
            rc.add_name_server(ns_config);
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
