// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resolves DNS names within the Oxide control plane
// XXX much of this is copied from dnswait

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::ValueEnum;
use internal_dns::resolver::Resolver;
use slog::{info, warn};
use std::net::SocketAddr;

#[derive(Debug, Parser)]
#[clap(name = "dnsping", about = "Periodically resolve DNS names")]
struct Opt {
    /// Nameserver(s) to query
    ///
    /// If unspecified, uses the system configuration (usually the nameservers
    /// configured in /etc/resolv.conf).
    #[clap(long, action)]
    nameserver_addresses: Vec<SocketAddr>,

    /// service name to be resolved (should be the target of a DNS name)
    #[arg(value_enum)]
    srv_name: ServiceName,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum ServiceName {
    Cockroach,
}

impl From<ServiceName> for internal_dns::ServiceName {
    fn from(value: ServiceName) -> Self {
        match value {
            ServiceName::Cockroach => internal_dns::ServiceName::Cockroach,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let log = dropshot::ConfigLogging::File {
        path: "/dev/stdout".into(),
        level: dropshot::ConfigLoggingLevel::Debug,
        if_exists: dropshot::ConfigLoggingIfExists::Append,
    }
    .to_logger("dnsping")
    .context("creating log")?;

    let resolver = if opt.nameserver_addresses.is_empty() {
        info!(&log, "using system configuration");
        let async_resolver =
            trust_dns_resolver::AsyncResolver::tokio_from_system_conf()
                .context("initializing resolver from system configuration")?;
        Resolver::new_with_resolver(log.clone(), async_resolver)
    } else {
        let addrs = opt.nameserver_addresses;
        info!(&log, "using explicit nameservers"; "nameservers" => ?addrs);
        Resolver::new_from_addrs(log.clone(), addrs)
            .context("creating resolver with explicit nameserver addresses")?
    };

    let srv_name = internal_dns::ServiceName::from(opt.srv_name);

    loop {
        info!(&log, "start resolution"; "srv_name" => ?srv_name);
        match resolver.lookup_srv(srv_name.clone()).await {
            Ok(result) => info!(&log, "found"; "results" => ?result),
            Err(error) => warn!(&log, "failed"; "error" => ?error),
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
