// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Small CLI to view and inspect zone bundles from the sled agent.

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use camino::Utf8PathBuf;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use futures::stream::StreamExt;
use omicron_common::address::SLED_AGENT_PORT;
use sled_agent_client::Client;
use sled_agent_client::types::CleanupContextUpdate;
use sled_agent_client::types::Duration;
use sled_agent_client::types::PriorityDimension;
use sled_agent_client::types::PriorityOrder;
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
    ///
    /// This attempts to find an address that looks like the sled-agent's, e.g.,
    /// one on the addrobject `underlay0/sled6`. If one is not found, this will
    /// use localhost, i.e., `::1`.
    #[arg(long)]
    host: Option<Ipv6Addr>,
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

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum UtilizationFields {
    Directory,
    BytesUsed,
    BytesAvailable,
    DatasetQuota,
    PctAvailable,
    PctQuota,
}

impl std::fmt::Display for UtilizationFields {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use UtilizationFields::*;
        match self {
            Directory => write!(f, "directory"),
            BytesUsed => write!(f, "bytes-used"),
            BytesAvailable => write!(f, "bytes-available"),
            DatasetQuota => write!(f, "dataset-quota"),
            PctAvailable => write!(f, "pct-available"),
            PctQuota => write!(f, "pct-quota"),
        }
    }
}

impl UtilizationFields {
    fn all() -> Vec<Self> {
        use UtilizationFields::*;
        vec![
            Directory,
            BytesUsed,
            BytesAvailable,
            DatasetQuota,
            PctAvailable,
            PctQuota,
        ]
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
    /// Get a zone bundle from the sled agent.
    Get {
        /// The name of the zone to fetch the bundle for.
        zone_name: String,
        /// The ID of the bundle to fetch.
        #[arg(long, required = true)]
        bundle_id: Uuid,
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
    /// Fetch the zone bundle cleanup context.
    ///
    /// This returns the data used to manage automatic cleanup of zone bundles,
    /// including the period; the directories searched; and the strategy used to
    /// preserve bundles.
    #[clap(visible_alias = "context")]
    CleanupContext,
    /// Set parameters of the zone bundle cleanup context.
    #[clap(visible_alias = "set-context")]
    SetCleanupContext(SetCleanupContextArgs),
    /// Return the utilization of the datasets allocated for zone bundles.
    Utilization {
        /// Generate parseable output.
        #[arg(long, short, default_value_t = false)]
        parseable: bool,
        /// Fields to print.
        #[arg(long, short = 'o', default_values_t = UtilizationFields::all(), value_delimiter = ',')]
        fields: Vec<UtilizationFields>,
    },
    /// Trigger an explicit request to cleanup low-priority zone bundles.
    Cleanup,
}

// Number of expected sort dimensions. Must match
// `sled_agent::zone_bundle::PriorityOrder::EXPECTED_SIZE`.
const EXPECTED_DIMENSIONS: usize = 2;

#[derive(Args, Clone, Debug)]
#[group(required = true, multiple = true)]
struct SetCleanupContextArgs {
    /// The new period on which to run automatic cleanups, in seconds.
    #[arg(long)]
    period: Option<u64>,
    /// The new order used to determine priority when cleaning up bundles.
    #[arg(long, value_delimiter = ',')]
    priority: Option<Vec<PriorityDimension>>,
    /// The limit on the underlying dataset quota allowed for zone bundles.
    ///
    /// This should be expressed as percentage of the dataset quota.
    #[arg(long, value_parser = clap::value_parser!(u8).range(0..=100))]
    storage_limit: Option<u8>,
}

// Fetch an address on `underlay0/sled6` if it exists, or use localhost.
async fn fetch_underlay_address() -> anyhow::Result<Ipv6Addr> {
    #[cfg(not(target_os = "illumos"))]
    return Ok(Ipv6Addr::LOCALHOST);
    #[cfg(target_os = "illumos")]
    {
        use illumos_utils::ipadm::Ipadm;
        use std::net::IpAddr;
        const EXPECTED_ADDR_OBJ: &str = "underlay0/sled6";
        match Ipadm::addrobj_addr(EXPECTED_ADDR_OBJ).await {
            // If we failed because there was no such interface, then fall back
            // to localhost.
            Ok(None) => Ok(Ipv6Addr::LOCALHOST),
            Ok(Some(addr)) => match addr.addr() {
                IpAddr::V6(ipv6) => Ok(ipv6),
                IpAddr::V4(ipv4) => bail!(
                    "Unexpectedly got IPv4 address for {}: {}",
                    EXPECTED_ADDR_OBJ,
                    ipv4
                ),
            },
            Err(e) => bail!(
                "failed to get address for addrobj {EXPECTED_ADDR_OBJ}: {e}",
            ),
        }
    }
}

fn main() -> anyhow::Result<()> {
    oxide_tokio_rt::run(main_impl())
}

async fn main_impl() -> anyhow::Result<()> {
    let args = Cli::parse();
    let host = match args.host {
        Some(host) => host,
        None => fetch_underlay_address()
            .await
            .context("failed to fetch underlay address")?,
    };
    let addr = format!("http://[{}]:{}", host, args.port);
    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, args.log_level).fuse();
    let log = Logger::root(drain, slog::o!("unit" => "zone-bundle"));

    // Create a client.
    //
    // We'll build one manually first, because the default uses quite a low
    // timeout, and some operations around creating or transferring bundles can
    // take a bit.
    let client = reqwest::ClientBuilder::new()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("failed to build client")?;
    let client = Client::new_with_client(&addr, client, log);
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
                .zone_bundle_list_all(filter.as_deref())
                .await
                .context("failed to list zone bundles")?
                .into_inner();
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
                            ListFields::Cause => {
                                format!("{:?}", bundle.cause)
                            }
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
        Cmd::Get { zone_name, bundle_id, output } => {
            let output = output.unwrap_or_else(|| {
                Utf8PathBuf::from(format!("{}.tar.gz", bundle_id))
            });
            let bundle = client
                .zone_bundle_get(&zone_name, &bundle_id)
                .await
                .context("failed to get zone bundle")?
                .into_inner();
            let mut f = tokio::fs::File::create(&output)
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
        Cmd::CleanupContext => {
            let context = client
                .zone_bundle_cleanup_context()
                .await
                .context("failed to fetch cleanup context")?;
            println!("Period: {}s", context.period.0.secs);
            println!("Priority: {:?}", context.priority.0);
            println!("Storage limit: {}%", context.storage_limit.0);
        }
        Cmd::SetCleanupContext(args) => {
            let priority = match args.priority {
                None => None,
                Some(pri) => {
                    let Ok(arr): Result<
                        [PriorityDimension; EXPECTED_DIMENSIONS],
                        _,
                    > = pri.try_into() else {
                        bail!(
                            "must provide {EXPECTED_DIMENSIONS} priority dimensions"
                        );
                    };
                    Some(PriorityOrder::from(arr))
                }
            };
            let ctx = CleanupContextUpdate {
                period: args.period.map(|secs| Duration { nanos: 0, secs }),
                priority,
                storage_limit: args.storage_limit,
            };
            client
                .zone_bundle_cleanup_context_update(&ctx)
                .await
                .context("failed to update zone bundle cleanup context")?;
        }
        Cmd::Utilization { parseable, fields } => {
            let utilization_by_dir = client
                .zone_bundle_utilization()
                .await
                .context("failed to get zone bundle utilization")?;
            const BYTES_USED_SIZE: usize = 16;
            const BYTES_AVAIL_SIZE: usize = 16;
            const QUOTA_SIZE: usize = 16;
            const PCT_OF_AVAIL_SIZE: usize = 10;
            const PCT_OF_QUOTA_SIZE: usize = 10;
            if !utilization_by_dir.is_empty() {
                use UtilizationFields::*;
                if parseable {
                    for (dir, utilization) in utilization_by_dir.iter() {
                        for (i, field) in fields.iter().enumerate() {
                            match field {
                                Directory => print!("{}", dir),
                                BytesUsed => {
                                    print!("{}", utilization.bytes_used)
                                }
                                BytesAvailable => {
                                    print!("{}", utilization.bytes_available)
                                }
                                DatasetQuota => {
                                    print!("{}", utilization.dataset_quota)
                                }
                                PctAvailable => print!(
                                    "{}",
                                    as_pct(
                                        utilization.bytes_used,
                                        utilization.bytes_available
                                    )
                                ),
                                PctQuota => print!(
                                    "{}",
                                    as_pct(
                                        utilization.bytes_used,
                                        utilization.dataset_quota
                                    )
                                ),
                            }
                            if i < fields.len() - 1 {
                                print!(",");
                            }
                        }
                        println!();
                    }
                } else {
                    let dir_col_size = utilization_by_dir
                        .keys()
                        .map(|d| d.len())
                        .max()
                        .unwrap();
                    for field in fields.iter() {
                        match field {
                            Directory => {
                                print!("{:dir_col_size$}", "Directory")
                            }
                            BytesUsed => {
                                print!("{:BYTES_USED_SIZE$}", "Bytes used")
                            }
                            BytesAvailable => print!(
                                "{:BYTES_AVAIL_SIZE$}",
                                "Bytes available"
                            ),
                            DatasetQuota => {
                                print!("{:QUOTA_SIZE$}", "Dataset quota")
                            }
                            PctAvailable => {
                                print!("{:PCT_OF_AVAIL_SIZE$}", "% of limit")
                            }
                            PctQuota => {
                                print!("{:PCT_OF_QUOTA_SIZE$}", "% of quota")
                            }
                        }
                        print!(" ");
                    }
                    println!();
                    for (dir, utilization) in utilization_by_dir.iter() {
                        for field in fields.iter() {
                            match field {
                                Directory => {
                                    print!("{:dir_col_size$}", dir)
                                }
                                BytesUsed => print!(
                                    "{:BYTES_USED_SIZE$}",
                                    as_human_bytes(utilization.bytes_used)
                                ),
                                BytesAvailable => print!(
                                    "{:BYTES_AVAIL_SIZE$}",
                                    as_human_bytes(utilization.bytes_available)
                                ),
                                DatasetQuota => print!(
                                    "{:QUOTA_SIZE$}",
                                    as_human_bytes(utilization.dataset_quota)
                                ),
                                PctAvailable => print!(
                                    "{:PCT_OF_AVAIL_SIZE$}",
                                    as_pct_str(
                                        utilization.bytes_used,
                                        utilization.bytes_available
                                    )
                                ),
                                PctQuota => print!(
                                    "{:PCT_OF_QUOTA_SIZE$}",
                                    as_pct_str(
                                        utilization.bytes_used,
                                        utilization.dataset_quota
                                    )
                                ),
                            }
                            print!(" ");
                        }
                        println!();
                    }
                }
            }
        }
        Cmd::Cleanup => {
            let cleaned = client
                .zone_bundle_cleanup()
                .await
                .context("failed to trigger zone bundle cleanup")?;
            const COUNT_SIZE: usize = 5;
            const BYTES_SIZE: usize = 16;
            if !cleaned.is_empty() {
                let dir_col_size =
                    cleaned.keys().map(|d| d.len()).max().unwrap();
                println!(
                    "{:dir_col_size$} {:COUNT_SIZE$} {:BYTES_SIZE$}",
                    "Directory", "Count", "Bytes",
                );
                for (dir, counts) in cleaned.iter() {
                    println!(
                        "{:dir_col_size$} {:<COUNT_SIZE$} {:<BYTES_SIZE$}",
                        dir, counts.bundles, counts.bytes,
                    );
                }
            }
        }
    }
    Ok(())
}

// Compute used / avail as a percentage.
fn as_pct(used: u64, avail: u64) -> u64 {
    (used * 100) / avail
}

// Format used / avail as a percentage string.
fn as_pct_str(used: u64, avail: u64) -> String {
    format!("{}%", as_pct(used, avail))
}

// Format the provided `size` in bytes as a human-friendly byte estimate.
fn as_human_bytes(size: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;
    let size = size as f64;
    if size >= TIB {
        format!("{:0.2} TiB", size / TIB)
    } else if size >= GIB {
        format!("{:0.2} GiB", size / GIB)
    } else if size >= MIB {
        format!("{:0.2} MiB", size / MIB)
    } else if size >= KIB {
        format!("{:0.2} KiB", size / KIB)
    } else {
        format!("{} B", size)
    }
}
