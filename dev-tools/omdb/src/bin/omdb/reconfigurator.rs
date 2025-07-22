// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that interact with Reconfigurator

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::DbUrlOptions;
use anyhow::Context as _;
use async_bb8_diesel::AsyncRunQueryDsl;
use camino::Utf8PathBuf;
use clap::Args;
use clap::Subcommand;
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use nexus_db_model::BpTarget;
use nexus_db_model::SqlU32;
use nexus_db_queries::authz;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::pagination::Paginator;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::ReconfiguratorChickenSwitches;
use nexus_types::deployment::UnstableReconfiguratorState;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use slog::Logger;
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use tabled::Tabled;

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
    History(HistoryArgs),
    /// Show the recent history of chicken switch settings
    ChickenSwitchesHistory(ChickenSwitchesHistoryArgs),
}

#[derive(Debug, Args, Clone)]
struct ExportArgs {
    /// where to save the output
    output_file: Utf8PathBuf,
}

#[derive(Debug, Args, Clone)]
struct ChickenSwitchesHistoryArgs {
    /// how far back in the history to show (number of targets)
    #[clap(long, default_value_t = 128)]
    limit: u32,
}

#[derive(Debug, Args, Clone)]
struct HistoryArgs {
    /// how far back in the history to show (number of targets)
    #[clap(long, default_value_t = 128)]
    limit: u32,

    /// also attempt to diff blueprints
    #[clap(long, default_value_t = false)]
    diff: bool,
}

impl ReconfiguratorArgs {
    /// Run a `omdb reconfigurator` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &Logger,
    ) -> anyhow::Result<()> {
        self.db_url_opts
            .with_datastore(
                omdb,
                log,
                async move |opctx, datastore| match &self.command {
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
                    ReconfiguratorCommands::History(history_args) => {
                        cmd_reconfigurator_history(
                            &opctx,
                            &datastore,
                            history_args,
                        )
                        .await
                    }
                    ReconfiguratorCommands::ChickenSwitchesHistory(args) => {
                        cmd_reconfigurator_chicken_switches_history(
                            &opctx, &datastore, args,
                        )
                        .await
                    }
                },
            )
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
    history_args: &HistoryArgs,
) -> anyhow::Result<()> {
    // Select recent targets.
    let limit = history_args.limit;
    let mut targets: Vec<_> = {
        use nexus_db_schema::schema::bp_target::dsl;
        let conn = datastore
            .pool_connection_for_tests()
            .await
            .context("obtaining connection")?;
        dsl::bp_target
            .select(BpTarget::as_select())
            .order_by(dsl::version.desc())
            .limit(i64::from(limit))
            .get_results_async(&*conn)
            .await
            .context("listing targets")?
    };

    // Select everything from the blueprint table.
    // This shouldn't be very large.
    let mut all_blueprints: BTreeMap<BlueprintUuid, BlueprintMetadata> =
        BTreeMap::new();
    let mut paginator =
        Paginator::new(SQL_BATCH_SIZE, dropshot::PaginationOrder::Ascending);
    while let Some(p) = paginator.next() {
        let records_batch = datastore
            .blueprints_list(opctx, &p.current_pagparams())
            .await
            .context("batch of blueprints")?;
        paginator = p.found_batch(&records_batch, &|b| *b.id.as_untyped_uuid());
        all_blueprints.extend(records_batch.into_iter().map(|b| (b.id, b)));
    }

    // Sort the target list in increasing order.
    // (This should be the same as reversing it.)
    targets.sort_by_key(|b| b.version);

    // Now, print the history.
    println!("{:>5} {:24} {:36}", "VERSN", "TIME", "BLUEPRINT");
    if targets.len() == usize::try_from(limit).unwrap() {
        println!("... (earlier history omitted)");
    }

    // prev_blueprint_id is `None` only during the first iteration.
    let mut prev_blueprint_id: Option<BlueprintUuid> = None;

    // prev_blueprint is `None` if any of these is true:
    // - if we're not printing diffs
    // - if this is the first iteration of the loop
    // - if the previous blueprint was missing from the database or we didn't
    //   load it because _it_ was the first iteration of the loop or _its_
    //   parent was missing from the database
    let mut prev_blueprint: Option<Blueprint> = None;

    for t in targets {
        let target_id = BlueprintUuid::from(t.blueprint_id);

        print!(
            "{:>5} {} {} {:>8}",
            t.version,
            humantime::format_rfc3339_millis(t.time_made_target.into()),
            target_id,
            if t.enabled { "enabled" } else { "disabled" },
        );

        if prev_blueprint_id == Some(target_id) {
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

            match (
                // are we printing diffs?
                history_args.diff,
                // was the previous blueprint (if any) in the database?
                prev_blueprint_id
                    .and_then(|prev_id| all_blueprints.get(&prev_id)),
                // is this blueprint in the database?
                all_blueprints.get(&target_id),
            ) {
                (true, Some(previous), Some(_)) => {
                    // In this case, we are printing diffs and both the previous
                    // and current blueprints are in the database.
                    //
                    // We might already have loaded the full previous blueprint,
                    // if we took this branch on the last iteration.  But we
                    // might not have, if that blueprint's parent was absent
                    // from the database.
                    let previous_blueprint = match prev_blueprint {
                        Some(p) => p,
                        None => {
                            blueprint_load(opctx, datastore, previous.id)
                                .await?
                        }
                    };
                    assert_eq!(previous_blueprint.id, previous.id);
                    let current_blueprint =
                        blueprint_load(opctx, datastore, target_id).await?;
                    {
                        let diff = current_blueprint
                            .diff_since_blueprint(&previous_blueprint);
                        println!("{}", diff.display());
                    }
                    prev_blueprint = Some(current_blueprint);
                }
                _ => {
                    prev_blueprint = None;
                }
            };
        }

        prev_blueprint_id = Some(target_id);
    }

    Ok(())
}
/// Show recent history of chicken switches
async fn cmd_reconfigurator_chicken_switches_history(
    opctx: &OpContext,
    datastore: &DataStore,
    history_args: &ChickenSwitchesHistoryArgs,
) -> anyhow::Result<()> {
    let mut history = vec![];
    let limit = history_args.limit;
    let batch_size = NonZeroU32::min(limit.try_into().unwrap(), SQL_BATCH_SIZE);
    let mut paginator = Paginator::<SqlU32>::new(
        batch_size,
        dropshot::PaginationOrder::Descending,
    );
    while let Some(p) = paginator.next() {
        if history.len() >= limit as usize {
            break;
        }
        let batch = datastore
            .reconfigurator_chicken_switches_list(opctx, &p.current_pagparams())
            .await
            .context("batch of chicken switches")?;
        paginator = p.found_batch(&batch, &|b| SqlU32::new(b.version));
        history.extend(batch.into_iter());
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SwitchesRow {
        version: String,
        planner_enabled: String,
        time_modified: String,
    }

    let rows: Vec<_> = history
        .into_iter()
        .map(|s| {
            let ReconfiguratorChickenSwitches {
                version,
                planner_enabled,
                time_modified,
            } = s;
            SwitchesRow {
                version: version.to_string(),
                planner_enabled: planner_enabled.to_string(),
                time_modified: time_modified.to_string(),
            }
        })
        .collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn blueprint_load(
    opctx: &OpContext,
    datastore: &DataStore,
    id: BlueprintUuid,
) -> anyhow::Result<Blueprint> {
    let id = *id.as_untyped_uuid();
    let authz_blueprint =
        authz::Blueprint::new(authz::FLEET, id, LookupType::ById(id));
    datastore
        .blueprint_read(opctx, &authz_blueprint)
        .await
        .with_context(|| format!("read blueprint {}", id))
}
