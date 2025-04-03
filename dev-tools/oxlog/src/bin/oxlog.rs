// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tool for discovering oxide related logfiles on sleds

use chrono::{DateTime, Utc};
use clap::{ArgAction, Args, Parser, Subcommand};
use oxlog::{DateRange, Filter, LogFile, Zones};
use std::collections::BTreeSet;

#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// List all zones found on the filesystem
    Zones,

    /// List logs for a given service
    Logs {
        /// The name of the zone
        zone: String,

        /// The name of the service to list logs for
        service: Option<String>,

        /// Print available metadata
        #[arg(short, long)]
        metadata: bool,

        #[command(flatten)]
        filter: FilterArgs,

        /// Show log files with an `mtime` before this timestamp. May be absolute or relative,
        /// e.g. '2025-04-01T01:01:01', '-1 hour', 'yesterday'
        #[arg(short = 'B', long, value_parser = parse_timestamp)]
        before: Option<DateTime<Utc>>,

        /// Show log files with an `mtime` after this timestamp. May be absolute or relative,
        /// e.g. '2025-04-01T01:01:01', '-1 hour', 'yesterday'
        #[arg(short = 'A', long, value_parser = parse_timestamp)]
        after: Option<DateTime<Utc>>,
    },

    /// List the names of all services in a zone, from the perspective of oxlog.
    /// Use these names with `oxlog logs` to filter output to logs from a
    /// specific service.
    Services {
        /// The name of the zone
        zone: String,
    },
}

#[derive(Args, Debug)]
#[group(required = true, multiple = true)]
struct FilterArgs {
    /// Print only the current log file
    #[arg(short, long)]
    current: bool,

    /// Print only the archived log files
    #[arg(short, long)]
    archived: bool,

    /// Print only the extra log files
    #[arg(short, long)]
    extra: bool,

    /// Show log files even if they are empty
    #[arg(short, long, action=ArgAction::SetTrue)]
    show_empty: bool,
}

fn parse_timestamp(date_str: &str) -> Result<DateTime<Utc>, String> {
    let timestamp = parse_datetime::parse_datetime(date_str)
        .map_err(|e| format!("could not parse timestamp: {e}"))?;

    // No-op, the local TZ on the rack is always UTC.
    Ok(timestamp.with_timezone(&Utc))
}

fn main() -> Result<(), anyhow::Error> {
    sigpipe::reset();

    let cli = Cli::parse();

    match cli.command {
        Commands::Zones => {
            for zone in Zones::load()?.zones.keys() {
                println!("{zone}");
            }
            Ok(())
        }
        Commands::Logs { zone, service, metadata, filter, before, after } => {
            let zones = Zones::load()?;
            let date_range = match (before, after) {
                (None, None) => None,
                _ => Some(DateRange::new(
                    before.unwrap_or(DateTime::<Utc>::MAX_UTC),
                    after.unwrap_or(DateTime::<Utc>::MIN_UTC),
                )),
            };

            let filter = Filter {
                current: filter.current,
                archived: filter.archived,
                extra: filter.extra,
                show_empty: filter.show_empty,
                date_range,
            };
            let print_metadata = |f: &LogFile| {
                println!(
                    "{}\t{}\t{}",
                    f.path,
                    f.size.map_or_else(|| "-".to_string(), |s| s.to_string()),
                    f.modified
                        .map_or_else(|| "-".to_string(), |s| s.to_rfc3339())
                );
            };

            let logs = zones.zone_logs(&zone, filter);
            for (svc_name, svc_logs) in logs {
                if let Some(service) = &service {
                    if svc_name != service.as_str() {
                        continue;
                    }
                }
                if filter.current {
                    if let Some(current) = &svc_logs.current {
                        if metadata {
                            print_metadata(current);
                        } else {
                            println!("{}", current.path);
                        }
                    }
                }
                if filter.archived {
                    for f in &svc_logs.archived {
                        if metadata {
                            print_metadata(f);
                        } else {
                            println!("{}", f.path);
                        }
                    }
                }
                if filter.extra {
                    for f in &svc_logs.extra {
                        if metadata {
                            print_metadata(f);
                        } else {
                            println!("{}", f.path);
                        }
                    }
                }
            }
            Ok(())
        }
        Commands::Services { zone } => {
            let zones = Zones::load()?;

            // We want all logs that exist, anywhere, so we can find their
            // service names.
            let filter = Filter {
                current: true,
                archived: true,
                extra: true,
                show_empty: true,
                date_range: None,
            };

            // Collect a unique set of services, based on the logs in the
            // specified zone
            let services: BTreeSet<String> =
                zones.zone_logs(&zone, filter).into_keys().collect();

            for svc in services {
                println!("{}", svc);
            }

            Ok(())
        }
    }
}
