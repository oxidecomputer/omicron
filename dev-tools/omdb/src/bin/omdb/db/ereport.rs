// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db ereport` subcommands

use super::DbFetchOptions;
use crate::helpers::const_max_len;

use anyhow::Context;
use clap::Args;
use clap::Subcommand;
use ereport_types::Ena;
use ereport_types::EreporterRestartUuid;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;

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
}

#[derive(Debug, Args, Clone)]
struct InfoArgs {
    restart_id: EreporterRestartUuid,
    ena: Ena,
}

#[derive(Debug, Args, Clone)]
struct ListArgs {}

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
    }
}

async fn cmd_db_ereport_list(
    _opctx: &OpContext,
    _datastore: &DataStore,
    _fetch_opts: &DbFetchOptions,
    _args: &ListArgs,
) -> anyhow::Result<()> {
    Err(anyhow::anyhow!("not yet implemented"))
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
