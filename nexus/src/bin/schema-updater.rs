// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Upgrades CRDB schema

use anyhow::anyhow;
use camino::Utf8PathBuf;
use clap::Parser;
use clap::Subcommand;
use nexus_config::PostgresConfigWithUrl;
use nexus_config::SchemaConfig;
use nexus_db_model::AllSchemaVersions;
use nexus_db_model::SCHEMA_VERSION;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::SemverVersion;
use slog::Drain;
use slog::Level;
use slog::LevelFilter;
use slog::Logger;
use slog_term::FullFormat;
use slog_term::TermDecorator;
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
    let pool = Arc::new(db::Pool::new_single_host(&log, &crdb_cfg));
    let schema_config = SchemaConfig { schema_dir: args.schema_directory };
    let all_versions = AllSchemaVersions::load(&schema_config.schema_dir)?;

    // We use the unchecked constructor of the datastore because we
    // don't want to block on someone else applying an upgrade.
    let datastore =
        DataStore::new_unchecked(log.clone(), pool).map_err(|e| anyhow!(e))?;

    match args.cmd {
        Cmd::List => {
            let (current_version, target_version) = datastore
                .database_schema_version()
                .await
                .map(|(v, t)| (v.to_string(), t.map(|t| t.to_string())))
                .unwrap_or_else(|_| ("Unknown".to_string(), None));

            println!("Current Version in database: {current_version}");
            println!("Target Version in database: {target_version:?}");
            println!("Known Versions:");
            for version in all_versions.iter_versions() {
                let mut extra = String::new();
                if version.semver().to_string() == current_version {
                    extra.push_str(" (reported by database)");
                };
                if version.is_current_software_version() {
                    extra.push_str(" (expected by Nexus)");
                };

                println!("  {}{extra}", version.semver())
            }
        }
        Cmd::Upgrade { version } => {
            println!("Upgrading to {version}");
            datastore
                .ensure_schema(&log, version.clone(), Some(&all_versions))
                .await
                .map_err(|e| anyhow!(e))?;
            println!("Upgrade to {version} complete");
        }
    }
    Ok(())
}
