// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI tool to apply offline updates to ClickHouse schema.

// Copyright 2023 Oxide Computer Company

use anyhow::Context;
use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use clap::Subcommand;
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use oximeter_db::Client;
use oximeter_db::OXIMETER_VERSION;
use slog::Drain;
use slog::Level;
use slog::LevelFilter;
use slog::Logger;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::net::SocketAddrV6;

const DEFAULT_HOST: SocketAddr = SocketAddr::V6(SocketAddrV6::new(
    Ipv6Addr::LOCALHOST,
    CLICKHOUSE_TCP_PORT,
    0,
    0,
));

fn parse_log_level(s: &str) -> anyhow::Result<Level> {
    s.parse().map_err(|_| anyhow!("Invalid log level"))
}

/// Tool to apply offline updates to ClickHouse schema.
#[derive(Clone, Debug, Parser)]
struct Args {
    /// IP address and port at which to access ClickHouse via the native TCP
    /// protocol.
    #[arg(long, default_value_t = DEFAULT_HOST, env = "CLICKHOUSE_HOST")]
    host: SocketAddr,

    /// Directory from which to read schema files for each version.
    #[arg(
        short = 's',
        long,
        default_value_t = Utf8PathBuf::from("/opt/oxide/oximeter/schema")
    )]
    schema_directory: Utf8PathBuf,

    /// The log level while running the command.
    #[arg(
        short,
        long,
        value_parser = parse_log_level,
        default_value_t = Level::Warning
    )]
    log_level: Level,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Debug, Subcommand)]
enum Cmd {
    /// List all schema in the directory available for an upgrade
    #[clap(visible_alias = "ls")]
    List,
    /// Apply an upgrade to a specific version
    #[clap(visible_aliases = ["up", "apply"])]
    Upgrade {
        /// The version to which to upgrade.
        #[arg(default_value_t = OXIMETER_VERSION)]
        version: u64,
    },
}

fn build_logger(level: Level) -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, level).fuse();
    Logger::root(drain, slog::o!("unit" => "clickhouse_schema_updater"))
}

fn main() -> anyhow::Result<()> {
    oxide_tokio_rt::run(async {
        let args = Args::parse();
        let log = build_logger(args.log_level);
        let client = Client::new(args.host, &log);
        let is_replicated = client.is_oximeter_cluster().await?;
        match args.cmd {
            Cmd::List => {
                let latest = client
                    .read_latest_version()
                    .await
                    .context("Failed to read latest version")?;
                let available_versions =
                    Client::read_available_schema_versions(
                        &log,
                        is_replicated,
                        &args.schema_directory,
                    )
                    .await?;
                println!("Latest version: {latest}");
                println!("Available versions:");
                for ver in available_versions {
                    print!(" {ver}");
                    if ver == latest {
                        print!(" (reported by database)");
                    }
                    if ver == OXIMETER_VERSION {
                        print!(" (expected by oximeter)");
                    }
                    println!();
                }
            }
            Cmd::Upgrade { version } => {
                client
                    .ensure_schema(
                        is_replicated,
                        version,
                        args.schema_directory,
                    )
                    .await
                    .context("Failed to upgrade schema")?;
                println!(
                    "Upgrade to oximeter database version {version} complete"
                );
            }
        }
        Ok(())
    })
}
