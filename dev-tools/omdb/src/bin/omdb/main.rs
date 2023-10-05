// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI for debugging Omicron internal state
//!
//! GROUND RULES:
//!
//! 1. There aren't a lot of ground rules here.  At least for now, this is a
//!    place to put any kind of runtime tooling for Omicron that seems useful.
//!    You can query the database directly (see notes in db.rs), use internal
//!    APIs, etc.  To the degree that we can stick to stable interfaces, great.
//!    But at this stage we'd rather have tools that work on latest than not
//!    have them because we couldn't prioritize keeping them stable.
//!
//! 2. Debuggers should never lie!  Documentation and command names should be
//!    precise about what they're reporting.  In a working system, these things
//!    might all be the same:
//!
//!        - the list of instances with zones and propolis processes running on
//!          a sled
//!        - the list of instances that sled agent knows about
//!        - the list of instances that Nexus or the database reports should be
//!          running on a sled
//!
//!    But in a broken system, these things might be all different.  People use
//!    debuggers to understand broken systems.  The debugger should say which of
//!    these it's reporting, rather than "the list of instances on a sled".
//!
//! 3. Where possible, when the tool encounters something unexpected, it should
//!    print what it can (including the error message and bad data) and then
//!    continue.  It generally shouldn't stop on the first error.  (We often
//!    find strange things when debugging but we need our tools to tell us as
//!    much as they can!)

use anyhow::Context;
use clap::Parser;
use clap::Subcommand;
use omicron_common::address::Ipv6Subnet;
use std::net::SocketAddr;
use std::net::SocketAddrV6;

mod db;
mod nexus;
mod oximeter;
mod sled_agent;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Omdb::parse();

    let log = dropshot::ConfigLogging::StderrTerminal {
        level: args.log_level.clone(),
    }
    .to_logger("omdb")
    .context("failed to create logger")?;

    match &args.command {
        OmdbCommands::Db(db) => db.run_cmd(&args, &log).await,
        OmdbCommands::Nexus(nexus) => nexus.run_cmd(&args, &log).await,
        OmdbCommands::Oximeter(oximeter) => oximeter.run_cmd(&log).await,
        OmdbCommands::SledAgent(sled) => sled.run_cmd(&args, &log).await,
    }
}

/// Omicron debugger (unstable)
///
/// This tool provides commands for directly querying Omicron components about
/// their internal state using internal APIs.  This is a prototype.  The
/// commands and output are unstable and may change.
#[derive(Debug, Parser)]
struct Omdb {
    /// log level filter
    #[arg(
        env,
        long,
        value_parser = parse_dropshot_log_level,
        default_value = "warn",
    )]
    log_level: dropshot::ConfigLoggingLevel,

    #[arg(env = "OMDB_DNS_SERVER", long)]
    dns_server: Option<SocketAddr>,

    #[command(subcommand)]
    command: OmdbCommands,
}

impl Omdb {
    async fn dns_lookup_all(
        &self,
        log: slog::Logger,
        service_name: internal_dns::ServiceName,
    ) -> Result<Vec<SocketAddrV6>, anyhow::Error> {
        let resolver = self.dns_resolver(log).await?;
        resolver
            .lookup_all_socket_v6(service_name)
            .await
            .with_context(|| format!("looking up {:?} in DNS", service_name))
    }

    async fn dns_resolver(
        &self,
        log: slog::Logger,
    ) -> Result<internal_dns::resolver::Resolver, anyhow::Error> {
        match &self.dns_server {
            Some(dns_server) => {
                internal_dns::resolver::Resolver::new_from_addrs(
                    log,
                    &[*dns_server],
                )
                .with_context(|| {
                    format!(
                        "creating DNS resolver for DNS server {:?}",
                        dns_server
                    )
                })
            }
            None => {
                // In principle, we should look at /etc/resolv.conf to find the
                // DNS servers.  In practice, this usually isn't populated
                // today.  See oxidecomputer/omicron#2122.
                //
                // However, the address selected below should work for most
                // existing Omicron deployments today.  That's because while the
                // base subnet is in principle configurable in config-rss.toml,
                // it's very uncommon to change it from the default value used
                // here.
                //
                // Yet another option would be to find a local IP address that
                // looks like it's probably on the underlay network and use that
                // to find the subnet to use.  But again, this is unlikely to be
                // wrong and it's easy to override.
                let subnet =
                    Ipv6Subnet::new("fd00:1122:3344:0100::".parse().unwrap());
                eprintln!("note: using DNS server for subnet {}", subnet.net());
                eprintln!(
                    "note: (if this is not right, use --dns-server \
                    to specify an alternate DNS server)",
                );
                internal_dns::resolver::Resolver::new_from_subnet(log, subnet)
                    .with_context(|| {
                        format!(
                            "creating DNS resolver for subnet {}",
                            subnet.net()
                        )
                    })
            }
        }
    }
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum OmdbCommands {
    /// Query the control plane database (CockroachDB)
    Db(db::DbArgs),
    /// Debug a specific Nexus instance
    Nexus(nexus::NexusArgs),
    /// Query oximeter collector state
    Oximeter(oximeter::OximeterArgs),
    /// Debug a specific Sled
    SledAgent(sled_agent::SledAgentArgs),
}

fn parse_dropshot_log_level(
    s: &str,
) -> Result<dropshot::ConfigLoggingLevel, anyhow::Error> {
    serde_json::from_str(&format!("{:?}", s)).context("parsing log level")
}
