// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update the database

use clap::Args;
use clap::Subcommand;

#[derive(Debug, Args)]
pub struct DbArgs {
    #[command(subcommand)]
    command: DbCommands,
}

#[derive(Debug, Subcommand)]
enum DbCommands {
    /// print information about control plane services
    Services {
        /// URL of the database SQL interface
        db_url: String,
    },
}

impl DbArgs {
    pub async fn run_cmd(
        &self,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        match &self.command {
            DbCommands::Services { db_url } => {
                cmd_db_services(log, &db_url).await
            }
        }
    }
}

async fn cmd_db_services(
    log: &slog::Logger,
    db_url: &str,
) -> Result<(), anyhow::Error> {
    todo!();
}
