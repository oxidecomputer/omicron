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
use anyhow::anyhow;
use anyhow::ensure;
use clap::Args;
use clap::ColorChoice;
use clap::Parser;
use clap::Subcommand;
use futures::StreamExt;
use internal_dns_types::names::ServiceName;
use omicron_common::address::Ipv6Subnet;
use std::net::SocketAddr;
use std::net::SocketAddrV6;
use tokio::net::TcpSocket;

mod crucible_agent;
mod db;
mod helpers;
mod mgs;
mod nexus;
mod oximeter;
mod oxql;
mod reconfigurator;
mod sled_agent;
mod support_bundle;

fn main() -> Result<(), anyhow::Error> {
    oxide_tokio_rt::run(async {
        let args = Omdb::parse();

        let log = dropshot::ConfigLogging::StderrTerminal {
            level: args.log_level.clone(),
        }
        .to_logger("omdb")
        .context("failed to create logger")?;

        match &args.command {
            OmdbCommands::Db(db) => db.run_cmd(&args, &log).await,
            OmdbCommands::Mgs(mgs) => mgs.run_cmd(&args, &log).await,
            OmdbCommands::Nexus(nexus) => nexus.run_cmd(&args, &log).await,
            OmdbCommands::Oximeter(oximeter) => {
                oximeter.run_cmd(&args, &log).await
            }
            OmdbCommands::Oxql(oxql) => oxql.run_cmd(&args, &log).await,
            OmdbCommands::Reconfigurator(reconfig) => {
                reconfig.run_cmd(&args, &log).await
            }
            OmdbCommands::SledAgent(sled) => sled.run_cmd(&args, &log).await,
            OmdbCommands::CrucibleAgent(crucible) => {
                crucible.run_cmd(&args).await
            }
        }
    })
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
        global = true,
    )]
    log_level: dropshot::ConfigLoggingLevel,

    #[arg(
        long,
        env = "OMDB_DNS_SERVER",
        global = true,
        help_heading = helpers::CONNECTION_OPTIONS_HEADING,
    )]
    dns_server: Option<SocketAddr>,

    /// Allow potentially-destructive subcommands.
    #[arg(
        short = 'w',
        long = "destructive",
        global = true,
        help_heading = helpers::SAFETY_OPTIONS_HEADING,
    )]
    allow_destructive: bool,

    #[command(flatten)]
    output: OutputOpts,

    #[command(subcommand)]
    command: OmdbCommands,
}

#[derive(Debug, Args)]
struct OutputOpts {
    /// Color output
    #[arg(long, global = true, value_enum, default_value_t)]
    color: ColorChoice,
}

mod check_allow_destructive {
    /// Zero-size type that potentially-destructive functions can accept to
    /// ensure `Omdb::check_allow_destructive` has been called.
    // This is tucked away inside a module to prevent it from being constructed
    // by anything other than `Omdb::check_allow_destructive`.
    #[must_use]
    pub(crate) struct DestructiveOperationToken(());

    impl super::Omdb {
        pub(crate) fn check_allow_destructive(
            &self,
        ) -> anyhow::Result<DestructiveOperationToken> {
            anyhow::ensure!(
                self.allow_destructive,
                "This command is potentially destructive. \
                 Pass the `-w` / `--destructive` flag to allow it."
            );
            Ok(DestructiveOperationToken(()))
        }
    }
}

impl Omdb {
    /// Return the socket addresses of all instances of a service in DNS
    async fn dns_lookup_all(
        &self,
        log: slog::Logger,
        service_name: ServiceName,
    ) -> Result<Vec<SocketAddrV6>, anyhow::Error> {
        let resolver = self.dns_resolver(log).await?;
        resolver
            .lookup_all_socket_v6(service_name)
            .await
            .with_context(|| format!("looking up {:?} in DNS", service_name))
    }

    /// Return the socket address of one instance of a service that we can at
    /// least successfully connect to
    async fn dns_lookup_one(
        &self,
        log: slog::Logger,
        service_name: ServiceName,
    ) -> Result<SocketAddrV6, anyhow::Error> {
        let addrs = self.dns_lookup_all(log, service_name).await?;
        ensure!(
            !addrs.is_empty(),
            "expected at least one address from successful DNS lookup for {:?}",
            service_name
        );

        // The caller is going to pick one of these addresses to connect to.
        // Let's try to pick one that's at least not obviously broken by
        // attempting to connect to whatever we found and returning any that we
        // successfully connected to.  It'd be nice if we could return the
        // socket directly, but our callers are creating reqwest clients that
        // cannot easily consume a socket directly.
        //
        // This approach scales poorly and there are many failure modes that
        // this does not cover.  But in the absence of better connection
        // management, and with the risks in `omdb` being pretty low, and the
        // value of it working pretty high, here we are.  This approach should
        // not be replicated elsewhere.
        async fn try_connect(
            sockaddr_v6: SocketAddrV6,
        ) -> Result<(), anyhow::Error> {
            let _ = TcpSocket::new_v6()
                .context("creating socket")?
                .connect(SocketAddr::from(sockaddr_v6))
                .await
                .with_context(|| format!("connect \"{}\"", sockaddr_v6))?;
            Ok(())
        }

        let mut socket_stream = futures::stream::iter(addrs)
            .map(async move |sockaddr_v6| {
                (sockaddr_v6, try_connect(sockaddr_v6).await)
            })
            .buffer_unordered(3);

        while let Some((sockaddr, connect_result)) = socket_stream.next().await
        {
            match connect_result {
                Ok(()) => return Ok(sockaddr),
                Err(error) => {
                    eprintln!(
                        "warning: failed to connect to {:?} at {}: {:#}",
                        service_name, sockaddr, error
                    );
                }
            }
        }

        Err(anyhow!("failed to connect to any instances of {:?}", service_name))
    }

    async fn dns_resolver(
        &self,
        log: slog::Logger,
    ) -> Result<internal_dns_resolver::Resolver, anyhow::Error> {
        match &self.dns_server {
            Some(dns_server) => {
                internal_dns_resolver::Resolver::new_from_addrs(
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
                internal_dns_resolver::Resolver::new_from_subnet(log, subnet)
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
    /// Debug a specific crucible-agent
    CrucibleAgent(crucible_agent::CrucibleAgentArgs),
    /// Query the control plane database (CockroachDB)
    Db(db::DbArgs),
    /// Debug a specific Management Gateway Service instance
    Mgs(mgs::MgsArgs),
    /// Debug a specific Nexus instance
    Nexus(nexus::NexusArgs),
    /// Query oximeter collector state
    Oximeter(oximeter::OximeterArgs),
    /// Enter the Oximeter Query Language shell for interactive querying.
    Oxql(oxql::OxqlArgs),
    /// Interact with the Reconfigurator system
    Reconfigurator(reconfigurator::ReconfiguratorArgs),
    /// Debug a specific Sled
    SledAgent(sled_agent::SledAgentArgs),
}

fn parse_dropshot_log_level(
    s: &str,
) -> Result<dropshot::ConfigLoggingLevel, anyhow::Error> {
    serde_json::from_str(&format!("{:?}", s)).context("parsing log level")
}
