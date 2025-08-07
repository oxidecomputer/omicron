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
use ereport_types::Ena;
use ereport_types::EreporterRestartUuid;
use nexus_db_lookup::DbConnection;
use nexus_db_model::SpType;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use nexus_db_schema::schema::host_ereport::dsl as host_dsl;
use nexus_db_schema::schema::sp_ereport::dsl as sp_dsl;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
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
        class: Option<String>,
        source: db::model::Reporter,
        #[tabled(display_with = "display_option_blank", rename = "S/N")]
        serial: Option<&'report str>,
        #[tabled(display_with = "display_option_blank", rename = "P/N")]
        part_number: Option<&'report str>,
    }

    impl<'report> From<&'report db::model::SpEreport> for EreportRow<'report> {
        fn from(ereport: &'report db::model::SpEreport) -> Self {
            let &db::model::SpEreport {
                time_collected,
                restart_id,
                ena,
                ref class,
                sp_type,
                sp_slot,
                ref serial_number,
                ref part_number,
                ..
            } = ereport;
            EreportRow {
                time_collected,
                restart_id: restart_id.into_untyped_uuid(),
                ena: ena.into(),
                class: class.clone(),
                source: db::model::Reporter::Sp {
                    sp_type: sp_type.into(),
                    slot: sp_slot.0,
                },
                serial: serial_number.as_deref(),
                part_number: part_number.as_deref(),
            }
        }
    }

    impl<'report> From<&'report db::model::HostEreport> for EreportRow<'report> {
        fn from(ereport: &'report db::model::HostEreport) -> Self {
            let &db::model::HostEreport {
                time_collected,
                restart_id,
                ena,
                ref class,
                sled_id,
                ref sled_serial,
                ..
            } = ereport;
            EreportRow {
                time_collected,
                restart_id: restart_id.into_untyped_uuid(),
                ena: ena.into(),
                class: class.clone(),
                source: db::model::Reporter::HostOs { sled: sled_id.into() },
                serial: Some(&sled_serial),
                part_number: None, // TODO(eliza): go get this from inventory?
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

    let ctx = || "loading SP ereports";
    let mut query = sp_dsl::sp_ereport
        .select(db::model::SpEreport::as_select())
        .limit(fetch_opts.fetch_limit.get().into())
        .order_by((sp_dsl::time_collected, sp_dsl::restart_id, sp_dsl::ena))
        .into_boxed();

    if !args.serials.is_empty() {
        query =
            query.filter(sp_dsl::serial_number.eq_any(args.serials.clone()));
    }

    if !args.classes.is_empty() {
        query = query.filter(sp_dsl::class.eq_any(args.classes.clone()));
    }

    if !args.ids.is_empty() {
        query = query.filter(sp_dsl::restart_id.eq_any(args.ids.clone()));
    }
    if let Some(before) = args.before {
        query = query.filter(sp_dsl::time_collected.lt(before));
    }

    if let Some(after) = args.after {
        query = query.filter(sp_dsl::time_collected.gt(after));
    }

    if !fetch_opts.include_deleted {
        query = query.filter(sp_dsl::time_deleted.is_null());
    }

    let sp_ereports = query.load_async(&*conn).await.with_context(ctx)?;
    check_limit(&sp_ereports, fetch_opts.fetch_limit, ctx);

    let ctx = || "loading host OS ereports";
    let mut query = host_dsl::host_ereport
        .select(db::model::HostEreport::as_select())
        .limit(fetch_opts.fetch_limit.get().into())
        .order_by((
            host_dsl::time_collected,
            host_dsl::restart_id,
            host_dsl::ena,
        ))
        .into_boxed();

    if !args.serials.is_empty() {
        query =
            query.filter(host_dsl::sled_serial.eq_any(args.serials.clone()));
    }

    if !args.classes.is_empty() {
        query = query.filter(host_dsl::class.eq_any(args.classes.clone()));
    }

    if !args.ids.is_empty() {
        query = query.filter(host_dsl::restart_id.eq_any(args.ids.clone()));
    }

    if let Some(before) = args.before {
        query = query.filter(host_dsl::time_collected.lt(before));
    }

    if let Some(after) = args.after {
        query = query.filter(host_dsl::time_collected.gt(after));
    }

    if !fetch_opts.include_deleted {
        query = query.filter(host_dsl::time_deleted.is_null());
    }

    let host_ereports = query.load_async(&*conn).await.with_context(ctx)?;
    check_limit(&host_ereports, fetch_opts.fetch_limit, ctx);

    let mut rows = sp_ereports
        .iter()
        .map(EreportRow::from)
        .chain(host_ereports.iter().map(EreportRow::from))
        .collect::<Vec<_>>();

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
    let db::model::Ereport { id, metadata, reporter, report } =
        ereport_fetch(&conn, fetch_opts, ereport_id).await?;

    let db::model::EreportMetadata {
        time_collected,
        time_deleted,
        collector_id,
        part_number,
        serial_number,
        class,
    } = metadata;
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
    println!("\n{:=<80}", "== EREPORT METADATA ");
    println!("    {ENA:>WIDTH$}: {}", id.ena);
    match class {
        Some(class) => println!("    {CLASS:>WIDTH$}: {class}"),
        None => println!("/!\\ {CLASS:>WIDTH$}: <unknown>"),
    }
    if let Some(time_deleted) = time_deleted {
        println!("(i) {TIME_DELETED:>WIDTH$}: {time_deleted}");
    }
    println!("    {TIME_COLLECTED:>WIDTH$}: {time_collected}");
    println!("    {COLLECTOR_ID:>WIDTH$}: {collector_id}");
    match reporter {
        db::model::Reporter::Sp { sp_type, slot } => {
            println!(
                "    {REPORTER:>WIDTH$}: {sp_type:?} {slot} (service processor)"
            )
        }
        db::model::Reporter::HostOs { sled } => {
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

    Ok(())
}

async fn ereport_fetch(
    conn: &async_bb8_diesel::Connection<DbConnection>,
    fetch_opts: &DbFetchOptions,
    id: ereport_types::EreportId,
) -> anyhow::Result<db::model::Ereport> {
    let restart_id = id.restart_id.into_untyped_uuid();
    let ena = db::model::DbEna::from(id.ena);

    let sp_query = sp_dsl::sp_ereport
        .filter(sp_dsl::restart_id.eq(restart_id))
        .filter(sp_dsl::ena.eq(ena))
        .select(db::model::SpEreport::as_select());
    let sp_result = if !fetch_opts.include_deleted {
        sp_query.filter(sp_dsl::time_deleted.is_null()).first_async(conn).await
    } else {
        sp_query.first_async(conn).await
    };
    if let Some(report) = sp_result.optional().with_context(|| {
        format!("failed to query for SP ereport matching {id}")
    })? {
        return Ok(report.into());
    }

    let host_query = host_dsl::host_ereport
        .filter(host_dsl::restart_id.eq(restart_id))
        .filter(host_dsl::ena.eq(ena))
        .select(db::model::HostEreport::as_select());
    let host_result = if !fetch_opts.include_deleted {
        host_query
            .filter(host_dsl::time_deleted.is_null())
            .first_async(conn)
            .await
    } else {
        host_query.first_async(conn).await
    };
    if let Some(report) = host_result.optional().with_context(|| {
        format!("failed to query for host OS ereport matching {id}")
    })? {
        return Ok(report.into());
    }

    Err(anyhow::anyhow!("no ereport {id} found"))
}

async fn cmd_db_ereporters(
    datastore: &DataStore,
    args: &ReportersArgs,
) -> anyhow::Result<()> {
    let &ReportersArgs { slot, slot_type, ref serial } = args;
    let slot_type = slot_type.map(nexus_db_model::SpType::from);

    let conn = datastore.pool_connection_for_tests().await?;
    let sp_ereporters = (*conn).transaction_async({
        let serial = serial.clone();
        async move |conn| {
            // Selecting all reporters may require a full table scan, depending
            // on filters.
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            let mut query = sp_dsl::sp_ereport
                .group_by((
                    sp_dsl::restart_id,
                    sp_dsl::sp_slot,
                    sp_dsl::sp_type,
                    sp_dsl::serial_number,
                    sp_dsl::part_number
                ))
                .select((
                    sp_dsl::restart_id,
                    sp_dsl::sp_slot,
                    sp_dsl::sp_type,
                    sp_dsl::serial_number,
                    sp_dsl::part_number,
                    min(sp_dsl::time_collected),
                    count_distinct(sp_dsl::ena),
                ))
                .into_boxed();

            if let Some(slot) = slot {
                if slot_type.is_some() {
                    query = query
                        .filter(sp_dsl::sp_slot.eq(db::model::SqlU16::new(slot)));
                } else {
                    anyhow::bail!(
                        "cannot filter reporters by slot without a value for `--type`"
                    )
                }
            }

            if let Some(slot_type) = slot_type {
                query = query
                    .filter(sp_dsl::sp_type.eq(slot_type));
            }

            if let Some(serial) = serial {
                query = query.filter(sp_dsl::serial_number.eq(serial.clone()));
            }

            query
                .load_async::<(Uuid, db::model::SqlU16, SpType, Option<String>, Option<String>, Option<DateTime<Utc>>, i64)>(
                    &conn,
                )
                .await.context("listing SP reporter entries")
        }
    }).await?;

    let host_ereporters = if slot_type != Some(SpType::Sled) || slot.is_some() {
        // If we have a SP type filter or a SP slot number, don't include host
        // OS reporters (for now).
        // TODO: if the SP type is "sled", can we get the sled UUID for that
        // slot from inventory?
        eprintln!(
            "selecting reporters with type {slot_type:?} and slot {slot:?} will \
             skip host OS ereports",
        );
        Vec::new()
    } else {
        (*conn).transaction_async({
            let serial = serial.clone();
            async move |conn| {
                // Selecting all reporters may require a full table scan, depending
                // on filters.
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
                let mut query = host_dsl::host_ereport
                    .group_by((
                        host_dsl::restart_id,
                        host_dsl::sled_id,
                        host_dsl::sled_serial,
                    ))
                    .select((
                        host_dsl::restart_id,
                        host_dsl::sled_id,
                        host_dsl::sled_serial,
                        min(host_dsl::time_collected),
                        count_distinct(host_dsl::ena),
                    ))
                    .into_boxed();

                if let Some(serial) = serial {
                    query = query.filter(host_dsl::sled_serial.eq(serial.clone()));
                }

                query
                    .load_async::<(Uuid, Uuid, String,  Option<DateTime<Utc>>, i64)>(
                        &conn,
                    )
                    .await.context("listing host OS reporter entries")
            }
        }).await?
    };

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ReporterRow {
        #[tabled(display_with = "datetime_opt_rfc3339_concise")]
        first_seen: Option<DateTime<Utc>>,
        id: Uuid,
        identity: db::model::Reporter,
        #[tabled(display_with = "display_option_blank", rename = "S/N")]
        serial: Option<String>,
        #[tabled(display_with = "display_option_blank", rename = "P/N")]
        part_number: Option<String>,
        ereports: i64,
    }

    let rows = {
        let sp_rows = sp_ereporters.into_iter().map(
            |(
                restart_id,
                slot,
                sp_type,
                serial,
                part_number,
                first_seen,
                ereports,
            )| {
                ReporterRow {
                    first_seen,
                    identity: db::model::Reporter::Sp {
                        slot: slot.0,
                        sp_type: sp_type.into(),
                    },
                    serial,
                    part_number,
                    id: restart_id,
                    ereports,
                }
            },
        );
        let host_rows = host_ereporters.into_iter().map(
            |(restart_id, sled_id, serial, first_seen, ereports)| ReporterRow {
                first_seen,
                identity: db::model::Reporter::HostOs {
                    sled: SledUuid::from_untyped_uuid(sled_id),
                },
                serial: Some(serial),
                part_number: None,
                id: restart_id,
                ereports,
            },
        );
        let mut rows = sp_rows.chain(host_rows).collect::<Vec<_>>();
        rows.sort_by_key(|row| row.first_seen);
        rows
    };

    let mut table = tabled::Table::new(rows);

    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));

    println!("{table}");

    Ok(())
}
