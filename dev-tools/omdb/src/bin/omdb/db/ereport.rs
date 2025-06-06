// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db ereport` subcommands

use super::DbFetchOptions;
use super::check_limit;
use crate::helpers::const_max_len;
use crate::helpers::datetime_rfc3339_concise;
use crate::helpers::display_option_blank;

use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::DateTime;
use chrono::Utc;
use clap::Args;
use clap::Subcommand;
use diesel::prelude::*;
use ereport_types::Ena;
use ereport_types::EreporterRestartUuid;
use nexus_db_model::SpType;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_schema::schema::sp_ereport::dsl as sp_dsl;
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
    restart_id: EreporterRestartUuid,
    ena: Ena,
}

#[derive(Debug, Args, Clone)]
struct ListArgs {
    /// Include only ereports from the system with the provided serial number.
    #[clap(long)]
    serial: Option<String>,

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
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &EreportArgs,
) -> anyhow::Result<()> {
    match args.command {
        Commands::List(ref args) => {
            cmd_db_ereport_list(opctx, datastore, fetch_opts, args).await
        }
        Commands::Info(ref args) => {
            cmd_db_ereport_info(opctx, datastore, args).await
        }

        Commands::Reporters(ref args) => {
            cmd_db_ereporters(datastore, args).await
        }
    }
}

async fn cmd_db_ereport_list(
    _: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &ListArgs,
) -> anyhow::Result<()> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct EreportRow {
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_collected: DateTime<Utc>,
        restart_id: Uuid,
        ena: Ena,
        src: SrcType,
        #[tabled(rename = "#")]
        slot: u16,
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct WithSerial<'a, T: Tabled> {
        #[tabled(inline)]
        inner: T,
        #[tabled(display_with = "display_option_blank", rename = "S/N")]
        serial: Option<&'a str>,
        #[tabled(display_with = "display_option_blank", rename = "P/N")]
        part_number: Option<&'a str>,
    }

    impl From<&'_ db::model::SpEreport> for EreportRow {
        fn from(ereport: &db::model::SpEreport) -> Self {
            let &db::model::SpEreport {
                time_collected,
                restart_id,
                ena,
                sp_type,
                sp_slot,
                ..
            } = ereport;
            EreportRow {
                time_collected,
                restart_id: restart_id.into_untyped_uuid(),
                ena: ena.into(),
                src: sp_type.into(),
                slot: sp_slot.0.into(),
            }
        }
    }

    impl<'report, T> From<&'report db::model::SpEreport> for WithSerial<'report, T>
    where
        T: Tabled,
        T: From<&'report db::model::SpEreport>,
    {
        fn from(ereport: &'report db::model::SpEreport) -> Self {
            let inner = T::from(ereport);
            WithSerial {
                inner,
                serial: ereport.serial_number.as_deref(),
                part_number: ereport.part_number.as_deref(),
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

    if let Some(ref serial) = args.serial {
        query = query.filter(sp_dsl::serial_number.eq(serial.clone()));
    }

    if let Some(before) = args.before {
        query = query.filter(sp_dsl::time_collected.lt(before));
    }

    if let Some(after) = args.after {
        query = query.filter(sp_dsl::time_collected.gt(after));
    }

    let sp_ereports = query.load_async(&*conn).await.with_context(ctx)?;
    check_limit(&sp_ereports, fetch_opts.fetch_limit, ctx);

    // let host_ereports: Vec<HostEreport> = Vec::new(); // TODO

    let mut table = if args.serial.is_some() {
        tabled::Table::new(sp_ereports.iter().map(EreportRow::from))
    } else {
        tabled::Table::new(
            sp_ereports.iter().map(WithSerial::<'_, EreportRow>::from),
        )
    };

    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));

    println!("{table}");

    Ok(())
}

#[derive(strum::Display)]
enum SrcType {
    Switch,
    Power,
    #[strum(to_string = "Sled (SP)")]
    SledSp,
    // #[strum(to_string = "Sled (OS)")]
    // SledOS,
}

impl From<SpType> for SrcType {
    fn from(sp_type: SpType) -> Self {
        match sp_type {
            SpType::Power => SrcType::Power,
            SpType::Sled => SrcType::SledSp,
            SpType::Switch => SrcType::Switch,
        }
    }
}

async fn cmd_db_ereport_info(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &InfoArgs,
) -> anyhow::Result<()> {
    let &InfoArgs { restart_id, ena } = args;
    let ereport_id = ereport_types::EreportId { restart_id, ena };
    let db::model::Ereport { id, metadata, reporter, report } =
        datastore.ereport_fetch(opctx, ereport_id).await?;

    let db::model::EreportMetadata {
        time_collected,
        collector_id,
        part_number,
        serial_number,
    } = metadata;
    const ENA: &str = "ENA";
    const TIME_COLLECTED: &str = "collected at";
    const COLLECTOR_ID: &str = "collected by";
    const REPORTER: &str = "reported by";
    const RESTART_ID: &str = "restart ID";
    const PART_NUMBER: &str = "  part number";
    const SERIAL_NUMBER: &str = "  serial number";
    const WIDTH: usize = const_max_len(&[
        TIME_COLLECTED,
        COLLECTOR_ID,
        REPORTER,
        PART_NUMBER,
        SERIAL_NUMBER,
    ]);
    println!("\n{:=<80}", "== EREPORT METADATA ");
    println!("    {ENA:>WIDTH$}: {}", id.ena);
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

async fn cmd_db_ereporters(
    datastore: &DataStore,
    args: &ReportersArgs,
) -> anyhow::Result<()> {
    let &ReportersArgs { slot, slot_type, ref serial } = args;

    let conn = datastore.pool_connection_for_tests().await?;
    let mut sp_query = sp_dsl::sp_ereport
        .select((
            sp_dsl::restart_id,
            sp_dsl::sp_slot,
            sp_dsl::sp_type,
            sp_dsl::serial_number,
            sp_dsl::part_number,
        ))
        .order_by((
            sp_dsl::sp_type,
            sp_dsl::sp_slot,
            sp_dsl::serial_number,
            sp_dsl::restart_id,
        ))
        .distinct()
        .into_boxed();

    if let Some(slot) = slot {
        if slot_type.is_some() {
            sp_query = sp_query
                .filter(sp_dsl::sp_slot.eq(db::model::SqlU16::new(slot)));
        } else {
            anyhow::bail!(
                "cannot filter reporters by slot without a value for `--type`"
            )
        }
    }

    if let Some(slot_type) = slot_type {
        sp_query = sp_query
            .filter(sp_dsl::sp_type.eq(SpType::from(slot_type.clone())));
    }

    if let Some(serial) = serial {
        sp_query = sp_query.filter(sp_dsl::serial_number.eq(serial.clone()));
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ReporterRow {
        #[tabled(rename = "TYPE")]
        ty: SrcType,
        #[tabled(rename = "#")]
        slot: u16,
        #[tabled(display_with = "display_option_blank", rename = "S/N")]
        serial: Option<String>,
        #[tabled(display_with = "display_option_blank", rename = "P/N")]
        part_number: Option<String>,
        id: Uuid,
    }

    let sp_ereports = sp_query
        .load_async::<(Uuid, db::model::SqlU16, SpType, Option<String>, Option<String>)>(
            &*conn,
        )
        .await
        .context("listing SP reporter entries")?;

    let mut table = tabled::Table::new(sp_ereports.into_iter().map(
        |(restart_id, slot, sp_type, serial, part_number)| ReporterRow {
            ty: sp_type.into(),
            slot: slot.into(),
            serial,
            part_number,
            id: restart_id,
        },
    ));

    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));

    println!("{table}");

    Ok(())
}
