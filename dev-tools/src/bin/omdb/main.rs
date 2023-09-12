// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI for debugging Omicron internal state

use anyhow::Context;
use clap::Parser;
use clap::Subcommand;

mod db;
mod nexus;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Omdb::parse();

    let log = dropshot::ConfigLogging::StderrTerminal {
        level: dropshot::ConfigLoggingLevel::Warn,
    }
    .to_logger("omdb")
    .context("failed to create logger")?;

    match args.command {
        OmdbCommands::Nexus(nexus) => nexus.run_cmd(&log).await,
        OmdbCommands::Db(db) => db.run_cmd(&log).await,
    }
}

/// Omicron debugger
#[derive(Debug, Parser)]
struct Omdb {
    #[command(subcommand)]
    command: OmdbCommands,
}

#[derive(Debug, Subcommand)]
enum OmdbCommands {
    /// Query the control plane database (CockroachDB)
    Db(db::DbArgs),
    /// Debug a specific Nexus instance
    Nexus(nexus::NexusArgs),
}
