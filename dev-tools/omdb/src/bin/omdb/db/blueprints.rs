// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db blueprint` subcommands

use std::collections::BTreeSet;
use std::num::NonZeroU32;

use crate::db::DbFetchOptions;
use crate::db::check_limit;
use crate::db::first_page;
use crate::nexus::BlueprintIdOrCurrentTarget;
use anyhow::Context;
use anyhow::bail;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::PlanningReport;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::GenericUuid;
use slog_error_chain::InlineErrorChain;
use tabled::Tabled;
use uuid::Uuid;

#[derive(Debug, Args, Clone)]
pub(super) struct BlueprintsArgs {
    #[command(subcommand)]
    command: BlueprintCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum BlueprintCommands {
    /// Show the planner report for a blueprint
    PlannerReport(PlannerReportArgs),
}

#[derive(Debug, Args, Clone)]
struct PlannerReportArgs {
    #[command(subcommand)]
    command: PlannerReportCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum PlannerReportCommands {
    /// List available planner reports
    List,
    /// Show the planner report for a blueprint
    Show(ShowPlannerReportArgs),
}

#[derive(Debug, Args, Clone)]
struct ShowPlannerReportArgs {
    /// The blueprint that produced the desired planner report
    blueprint_id: BlueprintIdOrCurrentTarget,
}

pub(super) async fn cmd_db_blueprints(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &BlueprintsArgs,
) -> anyhow::Result<()> {
    match &args.command {
        BlueprintCommands::PlannerReport(PlannerReportArgs {
            command: PlannerReportCommands::List,
        }) => {
            cmd_db_blueprint_planner_report_list(
                opctx,
                datastore,
                fetch_opts.fetch_limit,
            )
            .await
        }
        BlueprintCommands::PlannerReport(PlannerReportArgs {
            command: PlannerReportCommands::Show(args),
        }) => {
            cmd_db_blueprint_planner_report_show(opctx, datastore, args).await
        }
    }
}

async fn cmd_db_blueprint_planner_report_list(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_limit: NonZeroU32,
) -> anyhow::Result<()> {
    use nexus_db_schema::schema::debug_log_blueprint_planning::dsl;

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BlueprintRow {
        id: BlueprintUuid,
        time_created: DateTime<Utc>,
        has_report: &'static str,
        comment: String,
    }

    let conn = datastore
        .pool_connection_for_tests()
        .await
        .context("failed to get db connection")?;

    // Fetch a page of blueprints.
    let blueprints = datastore
        .blueprints_list(opctx, &first_page(fetch_limit))
        .await
        .context("listing blueprints")?;
    check_limit(&blueprints, fetch_limit, || "loading blueprints");
    let bp_ids =
        blueprints.iter().map(|bp| to_db_typed_uuid(bp.id)).collect::<Vec<_>>();

    // Determine whether each blueprint has a report.
    let bps_with_report: Vec<Uuid> = dsl::debug_log_blueprint_planning
        .filter(dsl::blueprint_id.eq_any(bp_ids))
        .select(dsl::blueprint_id)
        .load_async(&*conn)
        .await
        .context("loading blueprint report list")?;
    let bps_with_report = bps_with_report
        .into_iter()
        .map(|id| BlueprintUuid::from_untyped_uuid(id))
        .collect::<BTreeSet<_>>();

    let rows = blueprints
        .into_iter()
        .map(|bp| {
            let has_report =
                if bps_with_report.contains(&bp.id) { "yes" } else { "no" };
            BlueprintRow {
                id: bp.id,
                time_created: bp.time_created,
                has_report,
                comment: bp.comment,
            }
        })
        .collect::<Vec<_>>();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{table}");

    Ok(())
}

async fn cmd_db_blueprint_planner_report_show(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &ShowPlannerReportArgs,
) -> anyhow::Result<()> {
    use nexus_db_schema::schema::debug_log_blueprint_planning::dsl;

    let blueprint_id = args
        .blueprint_id
        .resolve_to_id_via_db(opctx, datastore)
        .await
        .context("failed to resolve blueprint argument to id")?;

    let conn = datastore
        .pool_connection_for_tests()
        .await
        .context("failed to get database connection")?;
    let maybe_debug_log: Option<serde_json::Value> =
        dsl::debug_log_blueprint_planning
            .filter(dsl::blueprint_id.eq(to_db_typed_uuid(blueprint_id)))
            .select(dsl::debug_blob)
            .first_async(&*conn)
            .await
            .optional()
            .with_context(|| {
                format!("failed to load debug log for blueprint {blueprint_id}")
            })?;
    let Some(debug_log) = maybe_debug_log else {
        bail!("no debug log found for blueprint {blueprint_id}");
    };

    // If we fail to parse the JSON blob, we can always dump it directly.
    let dump_raw_blob = |reason: &str| {
        println!(
            "failed to parse debug log for blueprint {blueprint_id}: {reason}"
        );
        println!("dumping raw debug log:");
        println!("{debug_log:#}");
    };

    // See `DebugLogBlueprintPlanning`; there is no explicit type for parsing
    // `debug_log`, because we don't want to expose such a thing in Nexus
    // proper. We'll peel out the git commit of the Nexus that produced this
    // report, compare it against our own, then try to parse the report.
    let Some(log_git_commit) =
        debug_log.get("git-commit").and_then(|v| v.as_str())
    else {
        dump_raw_blob("missing `git-commit` key");
        return Ok(());
    };

    let our_git_commit = env!("VERGEN_GIT_SHA");
    if our_git_commit != log_git_commit {
        eprintln!(
            "WARNING: planner report debug log was produced by a Nexus \
             on git commit {log_git_commit}, but omdb was built from \
             {our_git_commit}. We will attempt to parse it anyway."
        );
    }

    let Some(report_raw) = debug_log.get("report") else {
        dump_raw_blob("missing `report` key");
        return Ok(());
    };

    let report =
        match serde_json::from_value::<PlanningReport>(report_raw.clone()) {
            Ok(report) => report,
            Err(err) => {
                let err = InlineErrorChain::new(&err);
                dump_raw_blob(&format!("failed to parse report: {err}"));
                return Ok(());
            }
        };

    println!("planner report for blueprint {blueprint_id}:");
    println!("{report}");

    let operator_notes = report.operator_notes().into_notes();
    if !operator_notes.is_empty() {
        println!("\nnotes for customer operator:");
        for note in operator_notes {
            println!("  * {note}");
        }
    }

    Ok(())
}
