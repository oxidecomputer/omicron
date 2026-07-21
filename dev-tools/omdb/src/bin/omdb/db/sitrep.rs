// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db sitrep` subcommands

use crate::Omdb;
use crate::db::DbFetchOptions;
use crate::db::check_limit;
use crate::helpers::const_max_len;
use crate::helpers::datetime_opt_rfc3339_concise;
use crate::helpers::should_colorize;
use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use clap::Args;
use clap::Subcommand;
use diesel::prelude::*;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::model;
use nexus_types::fm;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::PaginationOrder;
use omicron_git_version::GitVersion;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;
use std::fmt;
use tabled::Tabled;
use uuid::Uuid;

use nexus_db_schema::schema::fm_sitrep_history::dsl as history_dsl;
use nexus_db_schema::schema::inv_collection::dsl as inv_collection_dsl;

#[derive(Debug, Args, Clone)]
pub(super) struct SitrepArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand, Clone)]
enum Commands {
    /// List the current situation report history.
    History(SitrepHistoryArgs),

    /// Show the current situation report.
    ///
    /// This is an alias for `omdb db sitrep info current`.
    Current(ShowOptions),

    /// Show details on a situation report.
    #[clap(alias = "show")]
    Info {
        /// The sitrep to show, identified by UUID, version number, or
        /// "current".
        ///
        /// A value of "current" selects the current sitrep. An integer
        /// (optionally prefixed with "v", e.g. "3" or "v3") selects the
        /// sitrep with that version number in the sitrep history. Any other
        /// value is parsed as a sitrep UUID.
        #[clap(value_name = "UUID|VERSION|current")]
        sitrep: SitrepSelector,

        #[clap(flatten)]
        opts: ShowOptions,
    },

    /// Show the analysis report for the requested sitrep, if one exists.
    AnalysisReport(AnalysisReportArgs),

    /// Run the slippy sitrep linter over the requested sitrep.
    ///
    /// Slippy checks a sitrep for states that should be impossible, or that
    /// would be problematic for the fault management subsystem to consume.
    #[clap(alias = "lint")]
    Slippy {
        /// The sitrep to lint, identified by UUID, version number, or
        /// "current".
        ///
        /// A value of "current" selects the current sitrep. An integer
        /// (optionally prefixed with "v", e.g. "3" or "v3") selects the
        /// sitrep with that version number in the sitrep history. Any other
        /// value is parsed as a sitrep UUID.
        #[clap(value_name = "UUID|VERSION|current")]
        sitrep: SitrepSelector,
    },
}

#[derive(Debug, Args, Clone)]
pub(super) struct SitrepHistoryArgs {
    /// If present, start at this sitrep version.
    ///
    /// If this is not set, the list will start with the current sitrep. This
    /// option is useful when the number of sitreps exceeds the database fetch
    /// limit.
    #[arg(long, short, alias = "starting_at")]
    from: Option<u32>,
}

#[derive(Debug, Args, Clone)]
struct AnalysisReportArgs {
    /// The sitrep whose analysis report to show, identified by UUID, version
    /// number, or "current".
    ///
    /// A value of "current" selects the current sitrep. An integer
    /// (optionally prefixed with "v", e.g. "3" or "v3") selects the sitrep
    /// with that version number in the sitrep history. Any other value is
    /// parsed as a sitrep UUID.
    #[clap(value_name = "UUID|VERSION|current")]
    sitrep: SitrepSelector,

    #[clap(flatten)]
    opts: ShowOptions,
}

#[derive(Debug, Args, Clone)]
struct ShowOptions {
    #[clap(long, short)]
    json: bool,
}

/// Selects a sitrep by UUID, by version number, or the current one.
#[derive(Debug, Clone, Copy)]
enum SitrepSelector {
    Current,
    Id(SitrepUuid),
    Version(u32),
}

impl std::str::FromStr for SitrepSelector {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("current") {
            return Ok(Self::Current);
        }
        // Accept an optional leading 'v', since `omdb db sitrep history`
        // displays versions as "v1", "v2", etc.
        let (s, is_definitely_version) = match s.strip_prefix(['v', 'V']) {
            Some(rest) => (rest, true),
            None => (s, false),
        };
        // A value which parses as a `u32` will not also parse as a valid UUID,
        // since a `u32` is at most 10 decimal digits, while a UUID requires 32
        // hex digits. Even if a valid UUID value omits dashes, `u32::from_str`
        // will not accept it, as there are insufficient digits.
        match s.parse::<u32>() {
            Ok(version) => return Ok(Self::Version(version)),
            // If the value doesn't parse as a u32 but had the version prefix,
            // bail out now.
            Err(e) if is_definitely_version => {
                return Err(anyhow::Error::from(e).context(
                    "a sitrep version (prefixed with 'v' or 'V') must be a \
                     valid 32-bit unsigned integer",
                ));
            }
            // Otherwise, fall back to parsing as a UUID.
            Err(_) => {}
        }
        match s.parse() {
            Ok(id) => Ok(Self::Id(id)),
            Err(_) => Err(anyhow::anyhow!(
                "expected a sitrep UUID, a version number, or \"current\""
            )),
        }
    }
}

impl SitrepSelector {
    fn display_for_error<'a>(
        &'a self,
        id: &'a SitrepUuid,
    ) -> impl fmt::Display + 'a {
        struct Displayer<'a> {
            sel: &'a SitrepSelector,
            id: &'a SitrepUuid,
        }
        impl fmt::Display for Displayer<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let Self { sel, id } = self;
                match sel {
                    SitrepSelector::Current => {
                        write!(f, "the current fault-management sitrep ({id})")
                    }
                    SitrepSelector::Version(v) => {
                        write!(f, "fault-management sitrep v{v} ({id})")
                    }
                    SitrepSelector::Id(id) => {
                        write!(f, "fault-management sitrep {id}")
                    }
                }
            }
        }
        Displayer { sel: &self, id }
    }

    /// Determine the sitrep UUID, and optional version number, for this
    /// selector.
    ///
    /// The version number is optional because selecting a sitrep by its UUID
    /// may refer to a sitrep which has not been committed to the history table.
    /// When selecting by UUID, we attempt to resolve the version record from
    /// the `fm_sitrep_history` table if one exists, but return `None` if the
    /// sitrep is not part of the history.
    async fn resolve(
        &self,
        datastore: &DataStore,
        opctx: &OpContext,
    ) -> anyhow::Result<(Option<fm::SitrepVersion>, SitrepUuid)> {
        match self {
            SitrepSelector::Current => {
                let version = datastore
                    .fm_current_sitrep_version(&opctx)
                    .await
                    .context("failed to look up the current sitrep version")?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "no current sitrep version exists at this time!"
                        )
                    })?;

                let id = version.id;
                Ok((Some(version), id))
            }
            SitrepSelector::Version(v) => {
                let version = history_dsl::fm_sitrep_history
                    .filter(history_dsl::version.eq(model::SqlU32::new(*v)))
                    .select(model::SitrepVersion::as_select())
                    .first_async(&*datastore.pool_connection_for_tests().await?)
                    .await
                    .optional()
                    .with_context(|| {
                        format!("failed to look up sitrep version v{v}")
                    })?
                    .map(fm::SitrepVersion::from)
                    .ok_or_else(|| anyhow::anyhow!("no sitrep v{v} exists"))?;
                let id = version.id;
                Ok((Some(version), id))
            }
            SitrepSelector::Id(id) => {
                // If we are looking up a sitrep by UUID, it may or may not
                // exist in the history. Let's see if it does!
                let maybe_version = history_dsl::fm_sitrep_history
                    .filter(history_dsl::sitrep_id.eq(id.into_untyped_uuid()))
                    .select(model::SitrepVersion::as_select())
                    .first_async(&*datastore.pool_connection_for_tests().await?)
                    .await
                    .optional()
                    .with_context(|| {
                        format!("failed to look up sitrep version for ID {id}")
                    })?
                    .map(Into::into);
                Ok((maybe_version, *id))
            }
        }
    }
}

pub(super) async fn cmd_db_sitrep(
    omdb: &Omdb,
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &SitrepArgs,
) -> anyhow::Result<()> {
    let colored =
        should_colorize(omdb.output.color, supports_color::Stream::Stdout);
    match args.command {
        Commands::History(ref args) => {
            cmd_db_sitrep_history(opctx, datastore, fetch_opts, args).await
        }
        Commands::Info { sitrep, opts: ref args } => {
            cmd_db_sitrep_show(
                opctx, datastore, fetch_opts, args, sitrep, colored,
            )
            .await
        }
        Commands::Current(ref args) => {
            cmd_db_sitrep_show(
                opctx,
                datastore,
                fetch_opts,
                args,
                SitrepSelector::Current,
                colored,
            )
            .await
        }
        Commands::AnalysisReport(ref args) => {
            cmd_db_sitrep_analysis_report(
                opctx, datastore, fetch_opts, args, colored,
            )
            .await
        }
        Commands::Slippy { sitrep } => {
            cmd_db_sitrep_slippy(opctx, datastore, sitrep).await
        }
    }
}

pub(super) async fn cmd_db_sitrep_history(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &SitrepHistoryArgs,
) -> anyhow::Result<()> {
    let ctx = || {
        if let Some(from) = args.from {
            format!(
                "listing fault management sitrep history (starting at {from})"
            )
        } else {
            "listing fault management sitrep history".to_string()
        }
    };

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct SitrepRow {
        v: u32,
        id: Uuid,
        #[tabled(display_with = "datetime_opt_rfc3339_concise")]
        created_at: Option<DateTime<Utc>>,
        comment: String,
    }

    let marker = args.from.map(model::SqlU32::new);
    let pagparams = DataPageParams {
        marker: marker.as_ref(),
        direction: PaginationOrder::Descending,
        limit: fetch_opts.fetch_limit,
    };
    let versions = datastore
        .fm_sitrep_version_list(&opctx, &pagparams)
        .await
        .with_context(ctx)?;

    check_limit(&versions, fetch_opts.fetch_limit, ctx);

    let mut rows = Vec::with_capacity(versions.len());
    for v in versions {
        let (comment, time_created) =
            match datastore.fm_sitrep_metadata_read(&opctx, v.id).await {
                Ok(s) => (s.comment, Some(s.time_created)),
                Err(e) => {
                    // If the sitrep has an entry in the history table, we
                    // expect that it will not yet have been archived and
                    // deleted, so this is an error rather a case of it just
                    // no longer existing.
                    eprintln!(
                        "failed to fetch metadata for sitrep {} (v{}): {e}",
                        v.id, v.version
                    );
                    ("<ERROR>".to_string(), None)
                }
            };
        rows.push(SitrepRow {
            v: v.version,
            id: v.id.into_untyped_uuid(),
            created_at: time_created,
            comment,
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{table}");

    Ok(())
}

async fn cmd_db_sitrep_show(
    opctx: &OpContext,
    datastore: &DataStore,
    _fetch_opts: &DbFetchOptions,
    opts: &ShowOptions,
    sitrep_selector: SitrepSelector,
    colored: bool,
) -> anyhow::Result<()> {
    let current_version = datastore
        .fm_current_sitrep_version(&opctx)
        .await
        .context("failed to look up the current sitrep version")?;

    let conn = datastore.pool_connection_for_tests().await?;
    let (maybe_version, id) =
        sitrep_selector.resolve(&datastore, &opctx).await?;
    let err_ctx = sitrep_selector.display_for_error(&id);
    let sitrep = datastore
        .fm_sitrep_read(&opctx, id)
        .await
        .with_context(|| format!("failed to look up {err_ctx}"))?;

    if opts.json {
        serde_json::to_writer_pretty(std::io::stdout(), &sitrep)
            .with_context(|| format!("failed to serialize {err_ctx}"))?;
        return Ok(());
    }

    let fm::Sitrep { metadata, cases, ereports_by_id } = sitrep;
    let fm::SitrepMetadata {
        id,
        creator_id,
        time_created,
        parent_sitrep_id,
        inv_collection_id,
        comment,
        next_inv_min_time_started,
        alert_generation,
        support_bundle_generation,
    } = metadata;

    const ID: &'static str = "ID";
    const PARENT_SITREP_ID: &'static str = "parent sitrep ID";
    const CREATED_BY: &'static str = "created by";
    const CREATED_AT: &'static str = "created at";
    const COMMENT: &'static str = "comment";
    const STATUS: &'static str = "status";
    const VERSION: &'static str = "  version";
    const MADE_CURRENT_AT: &'static str = "  made current at";
    const INV_COLLECTION_ID: &'static str = "inventory collection ID";
    const INV_STARTED_AT: &'static str = "  started at";
    const INV_FINISHED_AT: &'static str = "  finished at";
    const NEXT_INV_MIN_START: &'static str =
        "  next inventory minimum start time";
    const ALERT_GEN: &'static str = "alert generation";
    const SUPPORT_BUNDLE_GEN: &'static str = "support bundle generation";
    const TOTAL_EREPORTS: &'static str = "ereports in this sitrep";

    const WIDTH: usize = const_max_len(&[
        ID,
        PARENT_SITREP_ID,
        CREATED_AT,
        CREATED_BY,
        COMMENT,
        STATUS,
        VERSION,
        MADE_CURRENT_AT,
        INV_COLLECTION_ID,
        INV_STARTED_AT,
        INV_FINISHED_AT,
        NEXT_INV_MIN_START,
        ALERT_GEN,
        SUPPORT_BUNDLE_GEN,
        TOTAL_EREPORTS,
    ]);

    println!("\n{:=<80}", "== FAULT MANAGEMENT SITUATION REPORT ");
    println!("    {ID:>WIDTH$}: {id:?}");
    println!("    {PARENT_SITREP_ID:>WIDTH$}: {parent_sitrep_id:?}");
    println!("    {CREATED_BY:>WIDTH$}: {creator_id}");
    println!("    {CREATED_AT:>WIDTH$}: {time_created}");
    if comment.is_empty() {
        println!("    {COMMENT:>WIDTH$}: N/A\n");
    } else {
        println!("    {COMMENT:>WIDTH$}:");
        println!("{}\n", textwrap::indent(&comment, "      "));
    }

    match maybe_version {
        None => println!(
            "    {STATUS:>WIDTH$}: not committed to the sitrep history"
        ),
        Some(fm::SitrepVersion { version, time_made_current, .. }) => {
            if matches!(current_version, Some(ref v) if v.id == id) {
                println!("    {STATUS:>WIDTH$}: this is the current sitrep!",);
            } else {
                println!("    {STATUS:>WIDTH$}: in the sitrep history");
            }
            println!("    {VERSION:>WIDTH$}: v{version}");
            println!("    {MADE_CURRENT_AT:>WIDTH$}: {time_made_current}");
            match current_version {
                Some(v) if v.id == id => {}
                Some(fm::SitrepVersion { version, id, .. }) => {
                    println!(
                        "(i)   note: the current sitrep is {id:?} \
                        (at v{version})",
                    );
                }
                None => {
                    eprintln!(
                        "/!\\ WEIRD: this sitrep is in the sitrep history, \
                         but there is no current sitrep. this should not \
                         happen!"
                    );
                }
            };
        }
    }

    println!("\n{:=<80}", "== DIAGNOSIS INPUTS ");
    println!("    {INV_COLLECTION_ID:>WIDTH$}: {inv_collection_id:?}");
    let inv_collection = inv_collection_dsl::inv_collection
        .filter(
            inv_collection_dsl::id.eq(inv_collection_id.into_untyped_uuid()),
        )
        .select(model::InvCollection::as_select())
        .first_async(&*conn)
        .await
        .optional();
    match inv_collection {
        Err(err) => {
            eprintln!(
                "/!\\ failed to fetch inventory collection details: {err}"
            );
        }
        Ok(Some(model::InvCollection { time_started, time_done, .. })) => {
            println!("    {INV_STARTED_AT:>WIDTH$}: {time_started}");
            println!("    {INV_FINISHED_AT:>WIDTH$}: {time_done}");
        }
        Ok(None) => {
            println!(
                "      note: this collection no longer exists (perhaps it has \
                 been pruned?)"
            )
        }
    }
    println!("    {NEXT_INV_MIN_START:>WIDTH$}: {next_inv_min_time_started}");
    println!("    ");
    println!("    rendezvous resource generation numbers:");
    println!("    {ALERT_GEN:>WIDTH$}: {alert_generation}");
    println!("    {SUPPORT_BUNDLE_GEN:>WIDTH$}: {support_bundle_generation}");
    println!("    ");
    println!("    {TOTAL_EREPORTS}: {}", ereports_by_id.len());
    // TODO(eliza): perhaps display a table summarizing those ereports? possibly
    // behind a verbose flag?

    if !cases.is_empty() {
        println!("\n{:=<80}\n", "== CASES ");
        for case in cases {
            println!("{}", case.display_indented(4, Some(id)).colored(colored));
        }
    }

    Ok(())
}

async fn cmd_db_sitrep_slippy(
    opctx: &OpContext,
    datastore: &DataStore,
    selector: SitrepSelector,
) -> anyhow::Result<()> {
    let (_, id) = selector.resolve(&datastore, &opctx).await?;
    let err_ctx = selector.display_for_error(&id);
    let sitrep = datastore
        .fm_sitrep_read(opctx, id)
        .await
        .with_context(|| format!("failed to read {err_ctx}"))?;

    // Lint against the parent sitrep too when it still exists; it may have
    // been GC'd, in which case only the sitrep-internal checks run.
    let parent = match sitrep.parent_id() {
        Some(parent_id) => {
            match datastore.fm_sitrep_read(opctx, parent_id).await {
                Ok(parent) => Some(parent),
                Err(e) => {
                    eprintln!(
                        "note: parent sitrep {parent_id} could not be read \
                     (perhaps it has been garbage collected?), so only \
                     sitrep-internal checks will run: {e}"
                    );
                    None
                }
            }
        }
        None => None,
    };

    let report = nexus_fm_slippy::Slippy::new(&sitrep, parent.as_ref())
        .into_report(nexus_fm_slippy::SlippyReportSortKey::Severity);
    println!("{}", report.display());

    Ok(())
}

async fn cmd_db_sitrep_analysis_report(
    opctx: &OpContext,
    datastore: &DataStore,
    _fetch_opts: &DbFetchOptions,
    args: &AnalysisReportArgs,
    colored: bool,
) -> anyhow::Result<()> {
    let &AnalysisReportArgs { sitrep, ref opts } = args;
    let (_, id) = sitrep.resolve(&datastore, &opctx).await?;
    let err_ctx = sitrep.display_for_error(&id);
    let report =
        load_analysis_report(datastore, id).await.with_context(|| {
            format!("failed to load analysis report for {err_ctx}",)
        })?;
    print_analysis_report(&report, opts.json, colored)?;

    Ok(())
}

fn print_analysis_report(
    report: &model::fm::SitrepAnalysisReport,
    json: bool,
    colored: bool,
) -> anyhow::Result<()> {
    use nexus_types::fm::analysis_reports::{AnalysisReport, InputReport};

    let model::fm::SitrepAnalysisReport {
        sitrep_id: _,
        git_commit,
        input_report,
        analysis_report,
    } = report;

    let our_git_commit = GitVersion::current();
    let git_commit = git_commit.parse::<GitVersion>().expect("infallible");
    if our_git_commit != git_commit {
        eprintln!(
            "note: this sitrep analysis report was produced by a Nexus \
             on git commit {git_commit}. this omdb was built from \
             {our_git_commit}."
        );
        if our_git_commit.is_dirty() || git_commit.is_dirty() {
            eprintln!(
                "note: dirty repositories (those with uncommitted changes) \
                 will never be considered equal, even if the SHA is the same."
            );
        }
    }

    if json {
        let value = serde_json::json!({
            "git_commit": &git_commit,
            "input_report": input_report,
            "analysis_report": analysis_report,
        });
        serde_json::to_writer_pretty(std::io::stdout(), &value)
            .context("failed to serialize analysis report as JSON")?;
        return Ok(());
    }

    println!("\n{:=<80}", "== ANALYSIS INPUT REPORT ");
    match serde_json::from_value::<InputReport>(input_report.clone()) {
        Ok(report) => {
            println!("{}", report.display_multiline(0).colored(colored))
        }
        Err(e) => {
            eprintln!(
                "WARNING: failed to parse input report; falling back to \
                less structured output: {e}"
            );
            let displayer = nexus_types::fm::display::Json::new(&input_report)
                .colored(colored);
            println!("{displayer}");
        }
    }

    println!("\n{:=<80}", "== ANALYSIS REPORT ");
    match serde_json::from_value::<AnalysisReport>(analysis_report.clone()) {
        Ok(report) => {
            println!("{}", report.display_multiline(0).colored(colored))
        }
        Err(e) => {
            eprintln!(
                "WARNING: failed to parse analysis report; falling back to \
                less structured output: {e}"
            );
            let displayer =
                nexus_types::fm::display::Json::new(&analysis_report)
                    .colored(colored);
            println!("{displayer}");
        }
    }

    Ok(())
}

async fn load_analysis_report(
    datastore: &DataStore,
    sitrep: SitrepUuid,
) -> anyhow::Result<model::fm::SitrepAnalysisReport> {
    use nexus_db_schema::schema::fm_sitrep_analysis_report::dsl;

    Ok(dsl::fm_sitrep_analysis_report
        .filter(dsl::sitrep_id.eq(sitrep.into_untyped_uuid()))
        .select(model::fm::SitrepAnalysisReport::as_select())
        .first_async(&*datastore.pool_connection_for_tests().await?)
        .await?)
}
