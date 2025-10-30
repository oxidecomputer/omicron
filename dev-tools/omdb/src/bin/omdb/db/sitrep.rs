// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db sitrep` subcommands

use crate::db::DbFetchOptions;
use crate::db::check_limit;
use crate::helpers::const_max_len;
use crate::helpers::datetime_rfc3339_concise;
use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use clap::Args;
use clap::Subcommand;
use diesel::prelude::*;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::model;
use nexus_db_queries::db::pagination::paginated;
use nexus_types::fm;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::PaginationOrder;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SitrepUuid;
use tabled::Tabled;
use uuid::Uuid;

use nexus_db_schema::schema::fm_sitrep::dsl as sitrep_dsl;
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
    Current(ShowArgs),

    /// Show details on a situation report.
    #[clap(alias = "show")]
    Info {
        /// The UUID of the sitrep to show, or "current" to show the current
        /// sitrep.
        sitrep: SitrepIdOrCurrent,

        #[clap(flatten)]
        args: ShowArgs,
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
struct ShowArgs {}

#[derive(Debug, Clone, Copy)]
enum SitrepIdOrCurrent {
    Current,
    Id(SitrepUuid),
}

impl std::str::FromStr for SitrepIdOrCurrent {
    type Err = omicron_uuid_kinds::ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("current") {
            Ok(Self::Current)
        } else {
            let id = s.parse()?;
            Ok(Self::Id(id))
        }
    }
}

pub(super) async fn cmd_db_sitrep(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &SitrepArgs,
) -> anyhow::Result<()> {
    match args.command {
        Commands::History(ref args) => {
            cmd_db_sitrep_history(datastore, fetch_opts, args).await
        }
        Commands::Info { sitrep, ref args } => {
            cmd_db_sitrep_show(opctx, datastore, fetch_opts, args, sitrep).await
        }
        Commands::Current(ref args) => {
            cmd_db_sitrep_show(
                opctx,
                datastore,
                fetch_opts,
                args,
                SitrepIdOrCurrent::Current,
            )
            .await
        }
    }
}

pub(super) async fn cmd_db_sitrep_history(
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
        #[tabled(display_with = "datetime_rfc3339_concise")]
        created_at: DateTime<Utc>,
        comment: String,
    }

    let conn = datastore.pool_connection_for_tests().await?;
    let marker = args.from.map(model::SqlU32::new);
    let pagparams = DataPageParams {
        marker: marker.as_ref(),
        direction: PaginationOrder::Descending,
        limit: fetch_opts.fetch_limit,
    };
    let sitreps: Vec<(model::SitrepVersion, model::SitrepMetadata)> =
        paginated(
            history_dsl::fm_sitrep_history,
            history_dsl::version,
            &pagparams,
        )
        .inner_join(
            sitrep_dsl::fm_sitrep.on(history_dsl::sitrep_id.eq(sitrep_dsl::id)),
        )
        .select((
            model::SitrepVersion::as_select(),
            model::SitrepMetadata::as_select(),
        ))
        .load_async(&*conn)
        .await
        .with_context(ctx)?;

    check_limit(&sitreps, fetch_opts.fetch_limit, ctx);

    let rows = sitreps.into_iter().map(|(version, metadata)| {
        let model::SitrepMetadata {
            id,
            time_created,
            comment,
            creator_id: _,
            parent_sitrep_id: _,
            inv_collection_id: _,
        } = metadata;
        SitrepRow {
            v: version.version.into(),
            id: id.into_untyped_uuid(),
            created_at: time_created,
            comment,
        }
    });

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
    _args: &ShowArgs,
    sitrep: SitrepIdOrCurrent,
) -> anyhow::Result<()> {
    let ctx = || match sitrep {
        SitrepIdOrCurrent::Current => {
            "looking up the current fault management sitrep".to_string()
        }
        SitrepIdOrCurrent::Id(id) => {
            format!("looking up fault management sitrep {id:?}")
        }
    };
    let conn = datastore.pool_connection_for_tests().await?;

    let (maybe_version, sitrep) = match sitrep {
        SitrepIdOrCurrent::Id(id) => {
            let sitrep =
                datastore.fm_sitrep_read(opctx, id).await.with_context(ctx)?;
            let version = history_dsl::fm_sitrep_history
                .filter(history_dsl::sitrep_id.eq(id.into_untyped_uuid()))
                .select(model::SitrepVersion::as_select())
                .first_async(&*conn)
                .await
                .optional()
                .with_context(ctx)?
                .map(Into::into);
            (version, sitrep)
        }
        SitrepIdOrCurrent::Current => {
            let Some((version, sitrep)) =
                datastore.fm_sitrep_read_current(opctx).await?
            else {
                anyhow::bail!("no current sitrep exists at this time");
            };
            (Some(version), sitrep)
        }
    };

    let fm::Sitrep { metadata } = sitrep;
    let fm::SitrepMetadata {
        id,
        creator_id,
        time_created,
        parent_sitrep_id,
        inv_collection_id,
        comment,
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
            let current_version =
                datastore.fm_current_sitrep_version(&opctx).await;
            if matches!(current_version, Ok(Some(ref v)) if v.id == id) {
                println!("    {STATUS:>WIDTH$}: this is the current sitrep!",);
            } else {
                println!("    {STATUS:>WIDTH$}: in the sitrep history");
            }
            println!("    {VERSION:>WIDTH$}: v{version}");
            println!("    {MADE_CURRENT_AT:>WIDTH$}: {time_made_current}");
            match current_version {
                Ok(Some(v)) if v.id == id => {}
                Ok(Some(fm::SitrepVersion { version, id, .. })) => {
                    println!(
                        "(i)   note: the current sitrep is {id:?} \
                        (at v{version})",
                    );
                }
                Ok(None) => {
                    eprintln!(
                        "/!\\ WEIRD: this sitrep is in the sitrep history, \
                         but there is no current sitrep. this should not \
                         happen!"
                    );
                }
                Err(err) => {
                    eprintln!(
                        "/!\\ failed to determine the current sitrep \
                         version: {err}"
                    );
                }
            };
        }
    }

    println!("\n{:-<80}", "== DIAGNOSIS INPUTS ");
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

    Ok(())
}
