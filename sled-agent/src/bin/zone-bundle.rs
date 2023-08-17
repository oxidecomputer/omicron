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
/// Zone bundles are the collected state of a service zone. This includes
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

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum ListFields {
    ZoneName,
    BundleId,
    TimeCreated,
    Cause,
    Version,
}

impl std::fmt::Display for ListFields {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use ListFields::*;
        match self {
            ZoneName => write!(f, "zone-name"),
            BundleId => write!(f, "bundle-id"),
            TimeCreated => write!(f, "time-created"),
            Cause => write!(f, "cause"),
            Version => write!(f, "version"),
        }
    }
}

impl ListFields {
    fn all() -> Vec<Self> {
        use ListFields::*;
        vec![ZoneName, BundleId, TimeCreated]
    }
}

#[derive(Clone, Debug, Subcommand)]
enum Cmd {
    /// List the zones available for collecting bundles from.
    ListZones,
    /// List existing bundles for a zone or all zones.
    #[clap(visible_alias = "ls")]
    List {
        /// A filter for the zones whose bundles should be listed.
        ///
        /// If provided, this is used to filter the existing zones. Any zone
        /// with a name containing the provided substring will be used, and its
        /// zone bundles listed.
        filter: Option<String>,
        /// Generate parseable output.
        #[arg(long, short, default_value_t = false)]
        parseable: bool,
        /// Fields to print.
        #[arg(long, short = 'o', default_values_t = ListFields::all(), value_delimiter = ',')]
        fields: Vec<ListFields>,
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
        #[arg(long, group = "id", required = true)]
        bundle_id: Option<Uuid>,
        /// Create a new bundle, and then fetch it.
        #[arg(long, group = "id", required = true)]
        create: bool,
        /// The output file.
        ///
        /// If not specified, the output file is named by the bundle ID itself.
        #[arg(long)]
        output: Option<Utf8PathBuf>,
    },
    /// Delete a zone bundle.
    #[clap(visible_aliases = ["del", "rm"])]
    Delete {
        /// The name of the zone to delete a bundle for.
        zone_name: String,
        /// The ID of the bundle to delete.
        bundle_id: Uuid,
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
    let log = Logger::root(drain, slog::o!("unit" => "zone-bundle"));
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
        Cmd::List { filter, parseable, fields } => {
            let bundles = client
                .zone_bundle_list_all()
                .await
                .context("failed to list zone bundles")?
                .into_inner()
                .into_iter()
                .filter(|bundle| {
                    if let Some(filter) = &filter {
                        bundle.id.zone_name.contains(filter)
                    } else {
                        true
                    }
                })
                .collect::<Vec<_>>();
            if bundles.is_empty() {
                return Ok(());
            }
            if parseable {
                for bundle in bundles {
                    let line = fields
                        .iter()
                        .map(|field| match field {
                            ListFields::ZoneName => bundle.id.zone_name.clone(),
                            ListFields::BundleId => {
                                bundle.id.bundle_id.to_string()
                            }
                            ListFields::TimeCreated => {
                                bundle.time_created.to_rfc3339()
                            }
                            ListFields::Cause => format!("{:?}", bundle.cause),
                            ListFields::Version => bundle.version.to_string(),
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    println!("{line}");
                }
            } else {
                const ZONE_NAME_WIDTH: usize = 64;
                const BUNDLE_ID_WIDTH: usize = 36;
                const TIMESTAMP_WIDTH: usize = 34;
                const CAUSE_WIDTH: usize = 20;
                const VERSION_WIDTH: usize = 7;
                for field in fields.iter() {
                    match field {
                        ListFields::ZoneName => {
                            print!("{:ZONE_NAME_WIDTH$} ", "Zone")
                        }
                        ListFields::BundleId => {
                            print!("{:BUNDLE_ID_WIDTH$} ", "Bundle ID")
                        }
                        ListFields::TimeCreated => {
                            print!("{:TIMESTAMP_WIDTH$} ", "Created")
                        }
                        ListFields::Cause => {
                            print!("{:CAUSE_WIDTH$} ", "Cause")
                        }
                        ListFields::Version => {
                            print!("{:VERSION_WIDTH$} ", "Version")
                        }
                    }
                }
                println!();
                for bundle in bundles {
                    for field in fields.iter() {
                        match field {
                            ListFields::ZoneName => print!(
                                "{:ZONE_NAME_WIDTH$} ",
                                bundle.id.zone_name
                            ),
                            ListFields::BundleId => print!(
                                "{:BUNDLE_ID_WIDTH$} ",
                                bundle.id.bundle_id
                            ),
                            ListFields::TimeCreated => print!(
                                "{:TIMESTAMP_WIDTH$} ",
                                bundle.time_created
                            ),
                            ListFields::Cause => {
                                print!("{:CAUSE_WIDTH$?} ", bundle.cause,)
                            }
                            ListFields::Version => {
                                print!("{:VERSION_WIDTH$} ", bundle.version,)
                            }
                        }
                    }
                    println!();
                }
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
        Cmd::Delete { zone_name, bundle_id } => {
            client
                .zone_bundle_delete(&zone_name, &bundle_id)
                .await
                .context("failed to delete zone bundle")?;
        }
    }
    Ok(())
}
