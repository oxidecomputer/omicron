// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that interact with Reconfigurator

use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::DbUrlOptions;
use crate::Omdb;
use anyhow::Context as _;
use async_bb8_diesel::AsyncRunQueryDsl;
use camino::Utf8PathBuf;
use clap::Args;
use clap::Subcommand;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use nexus_db_model::BpTarget;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use slog::Logger;
use std::collections::BTreeMap;

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
    /// Show recent history of blueprints
    History,
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
                    ReconfiguratorCommands::History => {
                        cmd_reconfigurator_history(&opctx, &datastore).await
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

/// Show recent history of blueprints
async fn cmd_reconfigurator_history(
    opctx: &OpContext,
    datastore: &DataStore,
    // XXX-dap accept limit option?  paginate?
) -> anyhow::Result<()> {
    const LIMIT: i64 = 128;

    // Select recent targets.
    let mut targets: Vec<_> = {
        use nexus_db_queries::db::schema::bp_target::dsl;
        let conn = datastore
            .pool_connection_for_tests()
            .await
            .context("obtaining connection")?;
        dsl::bp_target
            .select(BpTarget::as_select())
            .order_by(dsl::version.desc())
            .limit(LIMIT)
            .get_results_async(&*conn)
            .await
            .context("listing targets")?
    };

    // Select everything from the blueprint table.
    // This shouldn't be very large.
    let mut all_blueprints: BTreeMap<BlueprintUuid, BlueprintMetadata> =
        BTreeMap::new();
    let mut paginator = Paginator::new(SQL_BATCH_SIZE);
    while let Some(p) = paginator.next() {
        let records_batch = datastore
            .blueprints_list(opctx, &p.current_pagparams())
            .await
            .context("batch of blueprints")?;
        paginator = p.found_batch(&records_batch, &|b| *b.id.as_untyped_uuid());
        all_blueprints.extend(
            records_batch
                .into_iter()
                .map(|b| (b.id, BlueprintMetadata::from(b))),
        );
    }

    // Sort the target list in increasing order.
    // (This should be the same as reversing it.)
    targets.sort_by_key(|b| b.version);

    // Now, print the history.
    if targets.len() == usize::try_from(LIMIT).unwrap() {
        println!("... (earlier history omitted)");
    }

    println!("{:>5} {:24} {:36}", "VERSN", "TIME", "BLUEPRINT");
    let mut prev: Option<BlueprintUuid> = None;
    for t in targets {
        let target_id = BlueprintUuid::from(t.blueprint_id);

        print!(
            "{:>5} {} {} {:>8}",
            t.version,
            humantime::format_rfc3339_millis(t.time_made_target.into()),
            target_id,
            if t.enabled { "enabled" } else { "disabled" },
        );

        let same_blueprint = matches!(prev,
            Some(prev_id) if prev_id == target_id
        );
        if same_blueprint {
            // The only change here could be to the enable/disable bit.
            // There's nothing else to say.
            println!();
        } else {
            // The blueprint id changed.
            let comment = match all_blueprints.get(&target_id) {
                Some(b) => &b.comment,
                None => "blueprint details no longer available",
            };
            println!(": {}", comment);
        }

        prev = Some(target_id);
    }

    Ok(())
}
