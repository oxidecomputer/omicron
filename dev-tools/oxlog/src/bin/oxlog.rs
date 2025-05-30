// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tool for discovering oxide related logfiles on sleds

use clap::{ArgAction, Args, Parser, Subcommand};
use glob::Pattern;
use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use jiff::{Span, Timestamp};
use oxlog::{DateRange, Filter, LogFile, Zones};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::str::FromStr;

/// The number of threads to given to the Rayon thread pool.
/// The default thread-per-physical core is excessive on a Gimlet.
const MAX_THREADS: usize = 12;

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
        /// The glob pattern to match against zone names
        zone_glob: GlobPattern,

        /// The name of the service to list logs for
        service: Option<String>,

        /// Print available metadata
        #[arg(short, long)]
        metadata: bool,

        #[command(flatten)]
        filter: FilterArgs,

        /// Show log files with an `mtime` before this timestamp. May be absolute or relative,
        /// e.g. '2025-04-01T01:01:01', '-1 hour', '3 days ago'
        #[arg(short = 'B', long, value_parser = parse_timestamp_now)]
        before: Option<Timestamp>,

        /// Show log files with an `mtime` after this timestamp. May be absolute or relative,
        /// e.g. '2025-04-01T01:01:01', '-1 hour', '3 days ago'
        #[arg(short = 'A', long, value_parser = parse_timestamp_now)]
        after: Option<Timestamp>,
    },

    /// List the names of all services in matching zones, from the perspective of oxlog.
    /// Use these names with `oxlog logs` to filter output to logs from a
    /// specific service.
    Services {
        /// The glob pattern to match against zone names
        zone_glob: GlobPattern,
    },
}

#[derive(Clone, Debug)]
struct GlobPattern(Pattern);

impl FromStr for GlobPattern {
    type Err = glob::PatternError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Pattern::new(s).map(GlobPattern)
    }
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

fn parse_timestamp_now(date_str: &str) -> Result<Timestamp, anyhow::Error> {
    parse_timestamp(Timestamp::now(), date_str)
}

fn parse_timestamp(
    relative_to: Timestamp,
    date_str: &str,
) -> Result<Timestamp, anyhow::Error> {
    // Parse as both a TimeStamp and a DateTime to provide maximum flexibility to users.
    // Timestamp must have a timezone, while DateTime must not have a "Z" TZ.
    let timestamp = date_str.parse::<Timestamp>();
    let datetime = date_str.parse::<DateTime>();
    let span = date_str.parse::<Span>();

    match (timestamp, datetime, span) {
        (Ok(ts), _, _) => Ok(ts),
        (_, Ok(dt), _) => Ok(dt.to_zoned(TimeZone::UTC)?.timestamp()),
        (_, _, Ok(s)) => {
            // Convert to Zoned for addition, Timestamp cannot be offset by a full day or more.
            let zoned = relative_to.to_zoned(TimeZone::UTC);
            Ok(zoned.saturating_add(s).timestamp())
        }
        (Err(e), Err(_), Err(_)) => {
            Err(anyhow::anyhow!("could not parse timestamp: {e}"))
        }
    }
}

fn main() -> Result<(), anyhow::Error> {
    sigpipe::reset();

    let num_threads = std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(MAX_THREADS)
        .min(MAX_THREADS);
    rayon::ThreadPoolBuilder::new().num_threads(num_threads).build_global()?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Zones => {
            for zone in Zones::load()?.zones.keys() {
                println!("{zone}");
            }
            Ok(())
        }
        Commands::Logs {
            zone_glob,
            service,
            metadata,
            filter,
            before,
            after,
        } => {
            let zones = Zones::load()?;
            let date_range = match (before, after) {
                (None, None) => None,
                _ => Some(DateRange::new(
                    before.unwrap_or(Timestamp::MAX),
                    after.unwrap_or(Timestamp::MIN),
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
                        .map_or_else(|| "-".to_string(), |s| s.to_string())
                );
            };

            let zones = zones.matching_zone_logs(&zone_glob.0, filter);
            for logs in zones {
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
            }
            Ok(())
        }
        Commands::Services { zone_glob } => {
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
            let services: BTreeSet<String> = zones
                .matching_zone_logs(&zone_glob.0, filter)
                .into_iter()
                .flat_map(|l| l.into_keys())
                .collect();

            for svc in services {
                println!("{}", svc);
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_timestamp;

    use jiff::civil::date;
    use jiff::{Timestamp, ToSpan};

    #[test]
    fn test_parse_timestamp() {
        let now: Timestamp = "2025-01-04 08:01:02Z".parse().unwrap();

        let ts = parse_timestamp(now, "2025-04-07T10:01:00Z").unwrap();
        let expected =
            date(2025, 4, 7).at(10, 1, 0, 0).in_tz("UTC").unwrap().timestamp();
        assert_eq!(expected, ts);

        let dt = parse_timestamp(now, "2025-04-07T10:01:00").unwrap();
        let expected =
            date(2025, 4, 7).at(10, 1, 0, 0).in_tz("UTC").unwrap().timestamp();
        assert_eq!(expected, dt);

        let dt_no_t = parse_timestamp(now, "2025-04-07 19:01:00").unwrap();
        let expected =
            date(2025, 4, 7).at(19, 1, 0, 0).in_tz("UTC").unwrap().timestamp();
        assert_eq!(expected, dt_no_t);

        let dt_short = parse_timestamp(now, "2025-04-07T10").unwrap();
        let expected =
            date(2025, 4, 7).at(10, 0, 0, 0).in_tz("UTC").unwrap().timestamp();
        assert_eq!(expected, dt_short);

        let relative = parse_timestamp(now, "-6 hours").unwrap();
        let expected = now.saturating_sub(6.hours()).unwrap();
        assert_eq!(expected, relative);

        let ago = parse_timestamp(now, "10 days ago").unwrap();
        let expected =
            now.in_tz("UTC").unwrap().saturating_sub(10.days()).timestamp();
        assert_eq!(expected, ago);
    }
}
