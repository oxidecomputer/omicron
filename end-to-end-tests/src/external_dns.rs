//! End-to-end test for external DNS
//!
//! External DNS is not yet configured with any DNS names.  For now, this just
//! tests basic connectivity.

#![cfg(test)]

use anyhow::{anyhow, Context as _, Result};
use omicron_sled_agent::rack_setup::config::SetupServiceConfig;
use std::net::SocketAddr;
use std::path::Path;
use trust_dns_resolver::config::{
    NameServerConfig, Protocol, ResolverConfig, ResolverOpts,
};
use trust_dns_resolver::error::ResolveErrorKind;
use trust_dns_resolver::TokioAsyncResolver;

#[tokio::test]
async fn dns_smoke() -> Result<()> {
    let dns_addr = dns_addr();

    let mut resolver_config = ResolverConfig::new();
    resolver_config.add_name_server(NameServerConfig {
        socket_addr: dns_addr,
        protocol: Protocol::Udp,
        tls_dns_name: None,
        trust_nx_responses: false,
        bind_addr: None,
    });

    let resolver =
        TokioAsyncResolver::tokio(resolver_config, ResolverOpts::default())
            .context("failed to create resolver")?;
    match resolver.lookup_ip("oxide.test.").await {
        Ok(_) => {
            Err(anyhow!("unexpectedly resolved made-up external DNS name"))
        }
        Err(error) => match error.kind() {
            ResolveErrorKind::NoRecordsFound { .. } => Ok(()),
            _ => Err(anyhow!(
                "unexpected error querying external DNS: {:?}",
                error
            )),
        },
    }
}

fn dns_addr() -> SocketAddr {
    // If we can find config-rss.toml, grab the first address from the
    // configured services IP pool.  Otherwise, choose a lab default.
    // (This is the same mechanism used for finding Nexus addresses.)
    let rss_config_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../smf/sled-agent/non-gimlet/config-rss.toml");
    if rss_config_path.exists() {
        if let Ok(config) = SetupServiceConfig::from_file(rss_config_path) {
            if let Some(addr) = config
                .internal_services_ip_pool_ranges
                .iter()
                .flat_map(|range| range.iter())
                .next()
            {
                return (addr, 53).into();
            }
        }
    }

    ([192, 168, 1, 20], 53).into()
}
