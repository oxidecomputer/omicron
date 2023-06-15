// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Small CLI to view and inspect zone bundles from the sled agent.

use anyhow::anyhow;
use anyhow::Context;
use camino::Utf8PathBuf;
use clap::Parser;
use clap::Subcommand;
use futures::stream::StreamExt;
use omicron_common::address::SLED_AGENT_PORT;
use sled_agent_client::Client;
use slog::Drain;
use slog::Level;
use slog::LevelFilter;
use slog::Logger;
use slog_term::FullFormat;
use slog_term::TermDecorator;
use std::net::Ipv6Addr;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

fn parse_log_level(s: &str) -> anyhow::Result<Level> {
    s.parse().map_err(|_| anyhow!("Invalid log level"))
}

/// Operate on sled agent zone bundles.
///
/// Zoneb bundles are the collected state of a service zone. This includes
/// information about the processes running in the zone, and the system on which
/// they're running.
#[derive(Clone, Debug, Parser)]
struct Cli {
    /// The IPv6 address for the sled agent to operate on.
    #[arg(long, default_value_t = Ipv6Addr::LOCALHOST)]
    host: Ipv6Addr,
    /// The port on which to connect to the sled agent.
    #[arg(long, default_value_t = SLED_AGENT_PORT)]
    port: u16,
    /// The log level for the command.
    #[arg(long, value_parser = parse_log_level, default_value_t = Level::Warning)]
    log_level: Level,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Debug, Subcommand)]
enum Cmd {
    /// List the zones available for collecting bundles from.
    ListZones,
    /// List existing bundles for a zone.
    #[clap(visible_alias = "ls")]
    List {
        /// The name of the zone to list bundles for.
        zone_name: String,
    },
    /// Request the sled agent create a new zone bundle.
    Create {
        /// The name of the zone to list bundles for.
        zone_name: String,
    },
    /// Get a zone bundle from the sled agent.
    Get {
        /// The name of the zone to fetch the bundle for.
        zone_name: String,
        /// The ID of the bundle to fetch.
        #[arg(long, group = "id")]
        bundle_id: Option<Uuid>,
        /// Create a new bundle, and then fetch it.
        #[arg(long, group = "id")]
        create: bool,
        /// The output file.
        ///
        /// If not specified, the output file is named by the bundle ID itself.
        #[arg(long)]
        output: Option<Utf8PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let addr = format!("http://[{}]:{}", args.host, args.port);
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, args.log_level).fuse();
    let log = Logger::root(drain, slog::o!("unit" => "zb"));
    let client = Client::new(&addr, log);
    match args.cmd {
        Cmd::ListZones => {
            let zones = client
                .zones_list()
                .await
                .context("failed to list zones")?
                .into_inner();
            for zone in zones {
                println!("{zone}");
            }
        }
        Cmd::List { zone_name } => {
            let bundles = client
                .zone_bundle_list(&zone_name)
                .await
                .context("failed to list zone bundles")?
                .into_inner();
            for bundle in bundles {
                println!("{}/{}", bundle.id.zone_name, bundle.id.bundle_id);
            }
        }
        Cmd::Create { zone_name } => {
            let bundle = client
                .zone_bundle_create(&zone_name)
                .await
                .context("failed to create zone bundle")?
                .into_inner();
            println!(
                "Created zone bundle: {}/{}",
                bundle.id.zone_name, bundle.id.bundle_id
            );
        }
        Cmd::Get { zone_name, bundle_id, create, output } => {
            let bundle_id = if create {
                let bundle = client
                    .zone_bundle_create(&zone_name)
                    .await
                    .context("failed to create zone bundle")?
                    .into_inner();
                println!(
                    "Created zone bundle: {}/{}",
                    bundle.id.zone_name, bundle.id.bundle_id
                );
                bundle.id.bundle_id
            } else {
                bundle_id.expect("clap should have ensured this was Some(_)")
            };
            let output = output.unwrap_or_else(|| {
                Utf8PathBuf::from(format!("{}.tar.gz", bundle_id))
            });
            let bundle = client
                .zone_bundle_get(&zone_name, &bundle_id)
                .await
                .context("failed to get zone bundle")?
                .into_inner();
            let mut f = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&output)
                .await
                .context("failed to open output file")?;
            let mut stream = bundle.into_inner();
            while let Some(maybe_bytes) = stream.next().await {
                let bytes =
                    maybe_bytes.context("failed to fetch all bundle data")?;
                f.write_all(&bytes)
                    .await
                    .context("failed to write bundle data")?;
            }
        }
    }
    Ok(())
}
