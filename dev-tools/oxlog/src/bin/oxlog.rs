// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tool for discovering oxide related logfiles on sleds

use clap::{ArgAction, Args, Parser, Subcommand};
use oxlog::{Filter, LogFile, Zones};

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
        // The name of the zone
        zone: String,

        /// The name of the service to list logs for
        service: Option<String>,

        /// Print available metadata
        #[arg(short, long)]
        metadata: bool,

        #[command(flatten)]
        filter: FilterArgs,
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
        Commands::Logs { zone, service, metadata, filter } => {
            let zones = Zones::load()?;
            let filter = Filter {
                current: filter.current,
                archived: filter.archived,
                extra: filter.extra,
                show_empty: filter.show_empty,
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
            for (svc_name, mut svc_logs) in logs {
                if let Some(service) = &service {
                    if svc_name != service.as_str() {
                        continue;
                    }
                }
                svc_logs.archived.sort();
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
    }
}
