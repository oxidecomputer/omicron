// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Resolves DNS names within the Oxide control plane

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::ValueEnum;
use internal_dns_resolver::ResolveError;
use internal_dns_resolver::Resolver;
use slog::{info, warn};
use std::net::SocketAddr;

#[derive(Debug, Parser)]
#[clap(name = "dnswait", about = "Resolves DNS names in the control plane")]
struct Opt {
    /// Nameserver(s) to query
    ///
    /// If unspecified, uses the system configuration (usually the nameservers
    /// configured in /etc/resolv.conf).
    #[clap(long, action)]
    nameserver_addresses: Vec<SocketAddr>,

    /// Service name to be resolved (should be the target of a DNS name)
    #[arg(value_enum)]
    srv_name: ServiceName,

    /// Output service host names only, omitting the port
    #[clap(long, short = 'H', action)]
    hostname_only: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum ServiceName {
    Cockroach,
    ClickhouseKeeper,
    ClickhouseServer,
}

impl From<ServiceName> for internal_dns_types::names::ServiceName {
    fn from(value: ServiceName) -> Self {
        match value {
            ServiceName::Cockroach => {
                internal_dns_types::names::ServiceName::Cockroach
            }
            ServiceName::ClickhouseServer => {
                internal_dns_types::names::ServiceName::ClickhouseServer
            }
            ServiceName::ClickhouseKeeper => {
                internal_dns_types::names::ServiceName::ClickhouseKeeper
            }
        }
    }
}

fn main() -> Result<()> {
    oxide_tokio_rt::run(async {
        let opt = Opt::parse();
        let log = dropshot::ConfigLogging::File {
            path: "/dev/stderr".into(),
            level: dropshot::ConfigLoggingLevel::Info,
            if_exists: dropshot::ConfigLoggingIfExists::Append,
        }
        .to_logger("dnswait")
        .context("creating log")?;

        let resolver = if opt.nameserver_addresses.is_empty() {
            info!(&log, "using system configuration");
            Resolver::new_from_system_conf(log.clone())
                .context("initializing resolver from system configuration")?
        } else {
            let addrs = opt.nameserver_addresses;
            info!(&log, "using explicit nameservers"; "nameservers" => ?addrs);
            Resolver::new_from_addrs(log.clone(), &addrs).context(
                "creating resolver with explicit nameserver addresses",
            )?
        };

        let result = omicron_common::backoff::retry_notify(
            omicron_common::backoff::retry_policy_internal_service(),
            || async {
                let dns_name =
                    internal_dns_types::names::ServiceName::from(opt.srv_name);
                resolver.lookup_srv(dns_name).await.map_err(|error| match error
                {
                    ResolveError::Resolve(_)
                    | ResolveError::NotFound(_)
                    | ResolveError::NotFoundByString(_) => {
                        omicron_common::backoff::BackoffError::transient(error)
                    }
                })
            },
            |error, delay| {
                warn!(
                    &log,
                    "DNS query failed; will try again";
                    "error" => format!("{:#}", error),
                    "delay" => ?delay,
                );
            },
        )
        .await
        .context("unexpectedly gave up")?;

        for (target, port) in result {
            if opt.hostname_only {
                println!("{}", target)
            } else {
                println!("{}:{}", target, port)
            }
        }

        Ok(())
    })
}
