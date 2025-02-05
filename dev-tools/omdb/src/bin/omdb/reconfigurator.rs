// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that interact with Reconfigurator

use crate::db::DbUrlOptions;
use crate::Omdb;
use anyhow::Context as _;
use camino::Utf8PathBuf;
use clap::Args;
use clap::Subcommand;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use slog::Logger;

/// Arguments to the "omdb reconfigurator" subcommand
#[derive(Debug, Args)]
pub struct ReconfiguratorArgs {
    /// URL of the database SQL interface
    #[clap(flatten)]
    db_url_opts: DbUrlOptions,

    #[command(subcommand)]
    command: ReconfiguratorCommands,
}

/// Subcommands for the "omdb reconfigurator" subcommand
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum ReconfiguratorCommands {
    /// Save the current Reconfigurator state to a file
    Export(ExportArgs),
}

#[derive(Debug, Args, Clone)]
struct ExportArgs {
    /// where to save the output
    output_file: Utf8PathBuf,
}

impl ReconfiguratorArgs {
    /// Run a `omdb reconfigurator` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        match &self.command {
            ReconfiguratorCommands::Export(export_args) => {
                self.db_url_opts
                    .with_datastore(omdb, log, |opctx, datastore| async move {
                        cmd_reconfigurator_export(
                            &opctx,
                            &datastore,
                            export_args,
                        )
                        .await
                    })
                    .await?;
            }
        }
        Ok(())
    }
}

/// Packages up database state that's used as input to the Reconfigurator
/// planner into a file so that it can be loaded into `reconfigurator-cli`
async fn cmd_reconfigurator_export(
    opctx: &OpContext,
    datastore: &DataStore,
    export_args: &ExportArgs,
) -> anyhow::Result<()> {
    // See Nexus::blueprint_planning_context().
    eprint!("assembling reconfigurator state ... ");
    let state = nexus_reconfigurator_preparation::reconfigurator_state_load(
        opctx, datastore,
    )
    .await?;
    eprintln!("done");

    let output_path = &export_args.output_file;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&output_path)
        .with_context(|| format!("open {:?}", output_path))?;
    serde_json::to_writer_pretty(&file, &state)
        .with_context(|| format!("write {:?}", output_path))?;
    eprintln!("wrote {}", output_path);
    Ok(())
}
