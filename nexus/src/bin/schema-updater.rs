// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Upgrades CRDB schema

use anyhow::{anyhow, bail};
use camino::Utf8PathBuf;
use clap::Parser;
use clap::Subcommand;
use nexus_db_model::schema::SCHEMA_VERSION;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::SemverVersion;
use omicron_common::nexus_config::SchemaConfig;
use omicron_common::postgres_config::PostgresConfigWithUrl;
use omicron_nexus::db;
use slog::Drain;
use slog::Level;
use slog::LevelFilter;
use slog::Logger;
use slog_term::FullFormat;
use slog_term::TermDecorator;
use std::collections::BTreeSet;
use std::sync::Arc;

fn parse_log_level(s: &str) -> anyhow::Result<Level> {
    s.parse().map_err(|_| anyhow!("Invalid log level"))
}

/// Utility to upgrade database schema
#[derive(Clone, Debug, Parser)]
struct Cli {
    /// URL to access CockroachDB
    // TODO: Set a default that makes this a little easier to use?
    #[arg(long = "crdb")]
    url: PostgresConfigWithUrl,

    /// Path to schema changes
    #[arg(short = 's', long, default_value_t = Utf8PathBuf::from("/var/nexus/schema/crdb"))]
    schema_directory: Utf8PathBuf,

    /// The log level for the command.
    #[arg(long, value_parser = parse_log_level, default_value_t = Level::Warning)]
    log_level: Level,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Debug, Subcommand)]
enum Cmd {
    /// List versions that we can use for upgrade
    #[clap(visible_alias = "ls")]
    List,
    /// Performs an upgrade to a specific version
    #[clap(visible_alias = "up")]
    Upgrade {
        #[arg(default_value_t = SCHEMA_VERSION)]
        version: SemverVersion,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let decorator = TermDecorator::new().build();
    let drain = FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = LevelFilter::new(drain, args.log_level).fuse();
    let log = Logger::root(drain, slog::o!("unit" => "schema_updater"));

    let crdb_cfg = db::Config { url: args.url };
    let pool = Arc::new(db::Pool::new(&log, &crdb_cfg));
    let schema_config =
        SchemaConfig { schema_dir: args.schema_directory.into() };

    // We use the unchecked constructor of the datastore because we
    // don't want to block on someone else applying an upgrade.
    let datastore = DataStore::new_unchecked(pool).map_err(|e| anyhow!(e))?;

    match args.cmd {
        Cmd::List => {
            let current_version = datastore
                .database_schema_version()
                .await
                .map(|v| v.to_string())
                .unwrap_or_else(|_| "Unknown".to_string());

            println!("Current Version: {current_version}");

            let mut dir =
                tokio::fs::read_dir(&schema_config.schema_dir).await.map_err(
                    |e| anyhow!("Failed to read from schema directory: {e}"),
                )?;

            let mut all_versions = BTreeSet::new();
            while let Some(entry) = dir
                .next_entry()
                .await
                .map_err(|e| anyhow!("Failed to read schema dir: {e}"))?
            {
                if entry.file_type().await.map_err(|e| anyhow!(e))?.is_dir() {
                    let name = entry
                        .file_name()
                        .into_string()
                        .map_err(|_| anyhow!("Non-unicode schema dir"))?;
                    if let Ok(observed_version) = name.parse::<SemverVersion>()
                    {
                        all_versions.insert(observed_version);
                    } else {
                        bail!("Failed to parse {name} as a semver version");
                    }
                }
            }

            println!("Known Versions:");
            for version in &all_versions {
                let mut extra = String::new();
                if version.to_string() == current_version {
                    extra.push_str(" (reported by database)");
                };
                if version == &SCHEMA_VERSION {
                    extra.push_str(" (expected by Nexus)");
                };

                println!("  {version}{extra}")
            }
        }
        Cmd::Upgrade { version } => {
            println!("Upgrading to {version}");
            datastore
                .ensure_schema(&log, version.clone(), Some(&schema_config))
                .await
                .map_err(|e| anyhow!(e))?;
            println!("Upgrade to {version} complete");
        }
    }
    Ok(())
}
