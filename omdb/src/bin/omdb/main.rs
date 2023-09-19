// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! CLI for debugging Omicron internal state
//!
//! GROUND RULES:
//!
//! 1. There aren't a lot of ground rules here.  At least for now, this is a
//!    place to put any kind of runtime tooling for Omicron that seems useful.
//!    You can query the database directly (see notes in db.rs), use internal
//!    APIs, etc.  To the degree that we can stick to stable interfaces, great.
//!    But at this stage we'd rather have tools that work on latest than not
//!    have them because we couldn't prioritize keeping them stable.
//!
//! 2. Where possible, when the tool encounters something unexpected, it should
//!    print what it can (including the error message and bad data) and then
//!    continue.  It generally shouldn't stop on the first error.  (We often
//!    find strange things when debugging but we need our tools to tell us as
//!    much as they can!)

use anyhow::Context;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::Parser;
use clap::Subcommand;

mod db;
mod nexus;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut cmd = Omdb::command();
    // This is pretty cheesy, but appears the easiest way to get the automated
    // tests to use a consistent terminal width.
    if let Ok(width_override) = std::env::var("OMDB_TEST_WIDTH") {
        if width_override.to_lowercase() == "1" {
            cmd = cmd.max_term_width(80)
        }
    }
    let args = Omdb::from_arg_matches(&cmd.get_matches()).context("omdb")?;

    let log = dropshot::ConfigLogging::StderrTerminal { level: args.log_level }
        .to_logger("omdb")
        .context("failed to create logger")?;

    match args.command {
        OmdbCommands::Nexus(nexus) => nexus.run_cmd(&log).await,
        OmdbCommands::Db(db) => db.run_cmd(&log).await,
    }
}

/// Omicron debugger (unstable)
///
/// This tool provides commands for directly querying Omicron components about
/// their internal state using internal APIs.  This is a prototype.  The
/// commands and output are unstable and may change.
#[derive(Debug, Parser)]
struct Omdb {
    /// log level filter
    #[arg(
        env,
        long,
        value_parser = parse_dropshot_log_level,
        default_value = "warn",
    )]
    log_level: dropshot::ConfigLoggingLevel,

    #[command(subcommand)]
    command: OmdbCommands,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum OmdbCommands {
    /// Query the control plane database (CockroachDB)
    Db(db::DbArgs),
    /// Debug a specific Nexus instance
    Nexus(nexus::NexusArgs),
}

fn parse_dropshot_log_level(
    s: &str,
) -> Result<dropshot::ConfigLoggingLevel, anyhow::Error> {
    serde_json::from_str(&format!("{:?}", s)).context("parsing log level")
}
