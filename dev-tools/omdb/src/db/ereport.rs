// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db ereport` subcommands

use super::DbFetchOptions;
use super::check_limit;
use crate::helpers::const_max_len;
use crate::helpers::datetime_opt_rfc3339_concise;
use crate::helpers::datetime_rfc3339_concise;
use crate::helpers::display_option_blank;
use crate::helpers::display_option_invalid;

use anyhow::Context;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use diesel::dsl::{count_distinct, min};
use diesel::prelude::*;
use nexus_db_lookup::DbConnection;
use nexus_db_model::ereport as model;
use nexus_db_model::ereport::DbEna;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use nexus_db_schema::schema::ereport::dsl;
use nexus_types::fm::ereport::Ena;
use nexus_types::fm::ereport::Reporter;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::GenericUuid;
use tabled::Tabled;
use uuid::Uuid;

#[derive(Debug, Args, Clone)]
pub(super) struct EreportArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand, Clone)]
enum Commands {
    /// List ereports
    #[clap(alias = "ls")]
    List(ListArgs),

    /// Show an ereport
    #[clap(alias = "show")]
    Info(InfoArgs),

    /// List ereport reporters
    Reporters(ReportersArgs),
}

#[derive(Debug, Args, Clone)]
struct InfoArgs {
    /// The reporter restart UUID of the ereport to show
    restart_id: EreporterRestartUuid,
    /// The ENA of the ereport within the reporter restart
    ena: Ena,
}

#[derive(Debug, Args, Clone)]
struct ListArgs {
    /// Include only ereports from systems with the provided serial numbers.
    #[clap(long = "serial", short)]
    serials: Vec<String>,

    /// Include only ereports from the provided reporter restart IDs.
    #[clap(long = "id", short)]
    ids: Vec<Uuid>,

    /// Include only ereports with the provided class strings.
    #[clap(long = "class", short)]
    classes: Vec<String>,

    /// Include only ereports collected before this timestamp
    #[clap(long, short)]
    before: Option<DateTime<Utc>>,

    /// Include only ereports collected after this timestamp
    #[clap(long, short)]
    after: Option<DateTime<Utc>>,
}

#[derive(Debug, Args, Clone)]
struct ReportersArgs {
    #[clap(long = "type", short = 't')]
    slot_type: Option<nexus_types::inventory::SpType>,

    #[clap(long = "slot", short = 's', requires = "slot_type")]
    slot: Option<u16>,

    serial: Option<String>,
}

pub(super) async fn cmd_db_ereport(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &EreportArgs,
) -> anyhow::Result<()> {
    match args.command {
        Commands::List(ref args) => {
            cmd_db_ereport_list(datastore, fetch_opts, args).await
        }
        Commands::Info(ref args) => {
            cmd_db_ereport_info(datastore, fetch_opts, args).await
        }

        Commands::Reporters(ref args) => {
            cmd_db_ereporters(datastore, args).await
        }
    }
}

async fn cmd_db_ereport_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &ListArgs,
) -> anyhow::Result<()> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct EreportRow<'report> {
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_collected: DateTime<Utc>,
        restart_id: Uuid,
        ena: Ena,
        #[tabled(display_with = "display_option_blank")]
        class: Option<&'report str>,
        #[tabled(display_with = "display_option_invalid")]
        source: Option<Reporter>,
        #[tabled(display_with = "display_option_blank", rename = "S/N")]
        serial: Option<&'report str>,
        #[tabled(display_with = "display_option_blank", rename = "P/N")]
        part_number: Option<&'report str>,
    }

    impl<'report> From<&'report db::model::Ereport> for EreportRow<'report> {
        fn from(ereport: &'report db::model::Ereport) -> Self {
            let source = match ereport.reporter() {
                Ok(reporter) => Some(reporter),
                Err(e) => {
                    eprintln!(
                        "error: ereport {} has an invalid reporter. {e}.",
                        ereport.id()
                    );
                    None
                }
            };
            let &db::model::Ereport {
                restart_id,
                ena: DbEna(ena),
                time_collected,
                ref class,
                ref serial_number,
                ref part_number,
                ..
            } = ereport;
            EreportRow {
                time_collected,
                restart_id: restart_id.into_untyped_uuid(),
                ena,
                class: class.as_deref(),
                source,
                serial: serial_number.as_deref(),
                part_number: part_number.as_deref(),
            }
        }
    }

    if let (Some(before), Some(after)) = (args.before, args.after) {
        anyhow::ensure!(
            after < before,
            "if both `--after` and `--before` are included, after must be
             earlier than before"
        );
    }

    let conn = datastore.pool_connection_for_tests().await?;

    let ctx = || "loading ereports";
    let mut query = dsl::ereport
        .select(db::model::Ereport::as_select())
        .limit(fetch_opts.fetch_limit.get().into())
        .order_by((dsl::time_collected, dsl::restart_id, dsl::ena))
        .into_boxed();

    if !args.serials.is_empty() {
        query = query.filter(dsl::serial_number.eq_any(args.serials.clone()));
    }

    if !args.classes.is_empty() {
        query = query.filter(dsl::class.eq_any(args.classes.clone()));
    }

    if !args.ids.is_empty() {
        query = query.filter(dsl::restart_id.eq_any(args.ids.clone()));
    }
    if let Some(before) = args.before {
        query = query.filter(dsl::time_collected.lt(before));
    }

    if let Some(after) = args.after {
        query = query.filter(dsl::time_collected.gt(after));
    }

    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let ereports = query.load_async(&*conn).await.with_context(ctx)?;
    check_limit(&ereports, fetch_opts.fetch_limit, ctx);

    let mut rows = ereports.iter().map(EreportRow::from).collect::<Vec<_>>();

    // Sort everything by time collected so that the host-OS and SP ereports are
    // interspersed by time collected, reporter, and ENA. Use
    // `std::cmp::Reverse` so that more recent ereports are displayed first.
    rows.sort_by_key(|row| {
        (
            std::cmp::Reverse(row.time_collected),
            row.restart_id,
            std::cmp::Reverse(row.ena),
        )
    });

    let mut table = tabled::Table::new(rows);
    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));

    println!("{table}");

    Ok(())
}

async fn cmd_db_ereport_info(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &InfoArgs,
) -> anyhow::Result<()> {
    let &InfoArgs { restart_id, ena } = args;
    let ereport_id = ereport_types::EreportId { restart_id, ena };
    let conn = datastore.pool_connection_for_tests().await?;
    let ereport = ereport_fetch(&conn, fetch_opts, ereport_id).await?;

    const ENA: &str = "ENA";
    const TIME_COLLECTED: &str = "collected at";
    const TIME_DELETED: &str = "deleted at";
    const COLLECTOR_ID: &str = "collected by";
    const CLASS: &str = "class";
    const REPORTER: &str = "reported by";
    const RESTART_ID: &str = "restart ID";
    const PART_NUMBER: &str = "  part number";
    const SERIAL_NUMBER: &str = "  serial number";
    const WIDTH: usize = const_max_len(&[
        CLASS,
        TIME_COLLECTED,
        TIME_DELETED,
        COLLECTOR_ID,
        REPORTER,
        PART_NUMBER,
        SERIAL_NUMBER,
    ]);
    let db::model::Ereport {
        ena: DbEna(ena),
        restart_id,
        time_deleted,
        time_collected,
        collector_id,
        ref part_number,
        ref serial_number,
        ref class,
        ref report,
        reporter,
    } = ereport;
    println!("\n{:=<80}", "== EREPORT METADATA ");
    println!("    {ENA:>WIDTH$}: {ena}");
    match class {
        Some(class) => println!("    {CLASS:>WIDTH$}: {class}"),
        None => println!("/!\\ {CLASS:>WIDTH$}: <unknown>"),
    }
    if let Some(time_deleted) = time_deleted {
        println!("(i) {TIME_DELETED:>WIDTH$}: {time_deleted}");
    }
    println!("    {TIME_COLLECTED:>WIDTH$}: {time_collected}");
    println!("    {COLLECTOR_ID:>WIDTH$}: {collector_id}");
    match Reporter::try_from(reporter) {
        Err(err) => eprintln!("{err}"),
        Ok(Reporter::Sp { sp_type, slot }) => {
            println!(
                "    {REPORTER:>WIDTH$}: {sp_type:?} {slot} (service processor)"
            )
        }
        Ok(Reporter::HostOs { sled }) => {
            println!("    {REPORTER:>WIDTH$}: sled {sled:?} (host OS)");
        }
    }
    println!("    {RESTART_ID:>WIDTH$}: {restart_id}");
    println!(
        "    {PART_NUMBER:>WIDTH$}: {}",
        part_number.as_deref().unwrap_or("<unknown>")
    );
    println!(
        "    {SERIAL_NUMBER:>WIDTH$}: {}",
        serial_number.as_deref().unwrap_or("<unknown>")
    );

    println!("\n{:=<80}", "== EREPORT ");
    serde_json::to_writer_pretty(std::io::stdout(), &report)
        .with_context(|| format!("failed to serialize ereport: {report:?}"))?;
    println!();

    Ok(())
}

async fn ereport_fetch(
    conn: &async_bb8_diesel::Connection<DbConnection>,
    fetch_opts: &DbFetchOptions,
    id: ereport_types::EreportId,
) -> anyhow::Result<db::model::Ereport> {
    let restart_id = id.restart_id.into_untyped_uuid();
    let ena = DbEna::from(id.ena);

    let query = dsl::ereport
        .filter(dsl::restart_id.eq(restart_id))
        .filter(dsl::ena.eq(ena))
        .select(db::model::Ereport::as_select());
    let result = if !fetch_opts.include_deleted {
        query.filter(dsl::time_deleted.is_null()).first_async(conn).await
    } else {
        query.first_async(conn).await
    };
    result
        .optional()
        .with_context(|| format!("failed to query for ereport matching {id}"))?
        .ok_or_else(|| anyhow::anyhow!("no ereport {id} found"))
}

async fn cmd_db_ereporters(
    datastore: &DataStore,
    args: &ReportersArgs,
) -> anyhow::Result<()> {
    let &ReportersArgs { slot, slot_type, ref serial } = args;
    let slot_type = slot_type.map(nexus_db_model::SpType::from);

    let conn = datastore.pool_connection_for_tests().await?;
    let reporters = (*conn).transaction_async({
        let serial = serial.clone();
        async move |conn| {
            // Selecting all reporters may require a full table scan, depending
            // on filters.
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            let mut query = dsl::ereport
                .group_by((
                    dsl::restart_id,
                    dsl::reporter,
                    dsl::sled_id,
                    dsl::sp_slot,
                    dsl::sp_type,
                    dsl::serial_number,
                    dsl::part_number
                ))
                .select((
                    dsl::restart_id,
                    model::Reporter::as_select(),
                    dsl::serial_number,
                    dsl::part_number,
                    min(dsl::time_collected),
                    count_distinct(dsl::ena),
                ))
                .into_boxed();

            if let Some(slot) = slot {
                if slot_type.is_some() {
                    query = query
                        .filter(dsl::sp_slot.eq(db::model::SqlU16::new(slot)));
                } else {
                    anyhow::bail!(
                        "cannot filter reporters by slot without a value for `--type`"
                    )
                }
            }

            if let Some(slot_type) = slot_type {
                query = query
                    .filter(dsl::sp_type.eq(slot_type));
            }

            if let Some(serial) = serial {
                query = query.filter(dsl::serial_number.eq(serial.clone()));
            }

            query
                .load_async::<(Uuid, model::Reporter, Option<String>, Option<String>, Option<DateTime<Utc>>, i64)>(
                    &conn,
                )
                .await.context("listing reporter entries")
        }
    }).await?;

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ReporterRow {
        #[tabled(display_with = "datetime_opt_rfc3339_concise")]
        first_seen: Option<DateTime<Utc>>,
        id: Uuid,
        #[tabled(display_with = "display_option_invalid")]
        identity: Option<Reporter>,
        #[tabled(display_with = "display_option_blank", rename = "S/N")]
        serial: Option<String>,
        #[tabled(display_with = "display_option_blank", rename = "P/N")]
        part_number: Option<String>,
        ereports: i64,
    }

    let mut rows = reporters
        .into_iter()
        .map(|(id, reporter, serial, part_number, first_seen, ereports)| {
            let identity = match reporter.try_into() {
                Ok(reporter) => Some(reporter),
                Err(err) => {
                    eprintln!(
                        "error: encounted an invalid reporter entry for \
                         reporter ID {id}: {err}",
                    );
                    None
                }
            };
            ReporterRow {
                first_seen,
                id,
                identity,
                serial,
                part_number,
                ereports,
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.first_seen);

    let mut table = tabled::Table::new(rows);
    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));

    println!("{table}");

    Ok(())
}
