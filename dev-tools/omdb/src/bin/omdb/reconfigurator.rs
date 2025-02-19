// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that interact with Reconfigurator

use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::DbUrlOptions;
use crate::Omdb;
use anyhow::Context as _;
use camino::Utf8PathBuf;
use clap::Args;
use clap::Subcommand;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::GenericUuid;
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
    /// Save the current Reconfigurator state to a file and remove historical
    /// artifacts from the live system (e.g., non-target blueprints)
    Archive(ExportArgs),
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
        self.db_url_opts
            .with_datastore(omdb, log, |opctx, datastore| async move {
                match &self.command {
                    ReconfiguratorCommands::Export(export_args) => {
                        let _state = cmd_reconfigurator_export(
                            &opctx,
                            &datastore,
                            export_args,
                        )
                        .await?;
                        Ok(())
                    }
                    ReconfiguratorCommands::Archive(archive_args) => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_reconfigurator_archive(
                            &opctx,
                            &datastore,
                            archive_args,
                            token,
                        )
                        .await
                    }
                }
            })
            .await
    }
}

/// Packages up database state that's used as input to the Reconfigurator
/// planner into a file so that it can be loaded into `reconfigurator-cli`
async fn cmd_reconfigurator_export(
    opctx: &OpContext,
    datastore: &DataStore,
    export_args: &ExportArgs,
) -> anyhow::Result<UnstableReconfiguratorState> {
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
    Ok(state)
}

async fn cmd_reconfigurator_archive(
    opctx: &OpContext,
    datastore: &DataStore,
    archive_args: &ExportArgs,
    _destruction_token: DestructiveOperationToken,
) -> anyhow::Result<()> {
    // First, ensure we can successfully export all our state.
    let saved_state =
        cmd_reconfigurator_export(opctx, datastore, archive_args).await?;

    // Now, delete every blueprint in `saved_state` except the current target.
    // There are two TOCTOU issues here, both of which we can live with:
    //
    // 1. New blueprints might have been created since we loaded the state to
    //    save; these won't be deleted. This is fine, and is the same as if new
    //    blueprints were created after we finished this archive operation.
    // 2. The target blueprint might have changed since we loaded the state, and
    //    might now point to a different blueprint that is in `saved_state`.
    //    We'll try to delete it below, but that will fail since the current
    //    target blueprint can't be deleted. The operator will need to run the
    //    archive process again and save _both_ state files, since we might have
    //    successfully archived some blueprints before hitting this error. We
    //    attempt to notice this and log a message for the operator in this
    //    case.
    let target_blueprint_id = saved_state
        .target_blueprint
        .context(
            "system has no current target blueprint: \
             cannot remove non-target blueprints",
        )?
        .target_id;

    let mut ndeleted = 0;

    eprintln!("removing non-target blueprints ...");
    for blueprint in &saved_state.blueprints {
        if blueprint.id == target_blueprint_id {
            continue;
        }

        let authz_blueprint = authz::Blueprint::new(
            authz::FLEET,
            blueprint.id.into_untyped_uuid(),
            LookupType::ById(blueprint.id.into_untyped_uuid()),
        );
        let err = match datastore
            .blueprint_delete(opctx, &authz_blueprint)
            .await
        {
            Ok(()) => {
                eprintln!("  successfully deleted blueprint {}", blueprint.id);
                ndeleted += 1;
                continue;
            }
            Err(err @ Error::Conflict { .. }) => {
                if ndeleted > 0 {
                    eprintln!(
                        "  failed to delete blueprint {}; has this blueprint \
                           become the target? If so, rerun this command but \
                           keep both archive output files ({ndeleted} \
                           blueprints were deleted prior to this failure)",
                        blueprint.id
                    );
                }
                err
            }
            Err(err) => err,
        };
        return Err(err).with_context(|| {
            format!("failed to delete blueprint {}", blueprint.id)
        });
    }

    if ndeleted == 0 {
        eprintln!("done (no non-target blueprints existed)");
    } else {
        let plural = if ndeleted == 1 { "" } else { "s" };
        eprintln!("done ({ndeleted} blueprint{plural} deleted)",);
    }

    Ok(())
}
