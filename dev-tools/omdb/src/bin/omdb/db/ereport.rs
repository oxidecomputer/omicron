// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! `omdb db ereport` subcommands

use super::DbFetchOptions;
use super::check_limit;
use crate::Omdb;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use crate::helpers::const_max_len;
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
use diesel::AggregateExpressionMethods;
use diesel::dsl::{count, max};
use diesel::prelude::*;
use internal_dns_types::names::ServiceName;
use nexus_db_lookup::DbConnection;
use nexus_db_model::EreporterRestart;
use nexus_db_model::ereport as model;
use nexus_db_model::ereport::DbEna;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use nexus_db_schema::schema::ereport::dsl;
use nexus_db_schema::schema::ereporter_restart::dsl as restart_dsl;
use nexus_types::fm::ereport::Ena;
use nexus_types::fm::ereport::Reporter;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::RackUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::HashMap;
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

    /// Summarize ereports by class, marking which classes a diagnosis engine
    /// in Nexus consumes (fetched from Nexus's lockstep API; falls back to
    /// `?` if Nexus is unreachable).
    Classes(ClassesArgs),
}

#[derive(Debug, Args, Clone)]
struct ClassesArgs {
    /// URL of the Nexus lockstep API. If not provided, looks up an instance
    /// in internal DNS.
    #[clap(
        long,
        env = "OMDB_NEXUS_URL",
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    nexus_internal_url: Option<String>,
}

#[derive(Debug, Args, Clone)]
struct InfoArgs {
    /// The reporter restart UUID of the ereport to show
    restart_id: EreporterRestartUuid,
    /// The ENA of the ereport within the reporter restart
    ena: Ena,

    /// If set, output the ereport as JSON, rather than using the default
    /// human-readable format.
    ///
    /// Note that this will output a JSON object containing the raw ereport data
    /// along with additional metadata from the database record for this
    /// ereport. To emit *only* the original JSON received from the reporter,
    /// add the `--raw` flag in addition to `--json`.
    #[clap(long, short)]
    json: bool,

    /// If outputting the ereport as JSON, output *only* the raw JSON received
    /// from the reporter in its original form, omitting additional metadata
    /// from the database.
    #[clap(long, short, requires("json"))]
    raw: bool,
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

    /// Filter ereports based on whether or not their database records have
    /// been marked as "seen" (included in a committed sitrep).
    ///
    /// Note that ereports which have not been marked in the database *may*
    /// still have been included in a sitrep. Marking of ereport database
    /// records occurs in the background, and may not be up to date with the
    /// latest sitrep. If an ereport has been marked as seen, it has
    /// *definitely* been included in a committed sitrep, but if an ereport
    /// has not been marked, it *may or may not* have been included in a
    /// committed sitrep.
    #[clap(long = "seen", short = 'm', value_enum, default_value_t)]
    seen: Seen,
}

#[derive(Debug, Args, Clone)]
struct ReportersArgs {
    #[clap(long = "type", short = 't')]
    slot_type: Option<nexus_types::inventory::SpType>,

    #[clap(long = "slot", short = 's', requires = "slot_type")]
    slot: Option<u16>,

    #[clap(long = "serial")]
    serial: Option<String>,
}

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum,
)]
enum Seen {
    /// Include all ereports, regardless of whether or not they have been
    /// marked.
    #[default]
    All,
    /// Include only ereports whose database records have been marked as seen.
    Marked,
    /// Include only ereports whose database records have NOT been marked as
    /// seen.
    Unmarked,
}

pub(super) async fn cmd_db_ereport(
    omdb: &Omdb,
    log: &slog::Logger,
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

        Commands::Classes(ref args) => {
            cmd_db_ereport_classes(omdb, log, datastore, args).await
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

        /// The underlying ereport record. This is not displayed when
        /// formatting, but is retained so that the `EreportRow` can be
        /// converted into an `EreportRowWithMark` if the markedness is needed.
        #[tabled(skip)]
        ereport: &'report db::model::Ereport,
    }

    /// Adds a `MARK` column to `EreportRow` to indicate whether or not the
    /// ereport has been marked as seen in the database.
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct EreportRowWithMark<'report> {
        mark: &'static str,
        #[tabled(inline)]
        ereport: EreportRow<'report>,
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
                ereport,
            }
        }
    }

    impl<'report> From<EreportRow<'report>> for EreportRowWithMark<'report> {
        fn from(row: EreportRow<'report>) -> Self {
            // The column is named "MARK" and its values are either "seen" or
            // "", because I wanted to make it as obvious as possible that an
            // unmarked ereport may still have been seen in a sitrep. The more
            // obvious option of a "SEEN" column with values "yes" and "no" is
            // potentially misleading, as "no" would suggest the ereport has not
            // been seen, when actually it has not been *marked*. So, the column
            // is named "MARK". But, I wanted to include the word "seen", so
            // marked-as-seen ereports have the value "seen".
            let mark =
                if row.ereport.marked_seen_in.is_some() { "seen" } else { "" };
            Self { mark, ereport: row }
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

    match args.seen {
        Seen::Marked => query = query.filter(dsl::marked_seen_in.is_not_null()),
        Seen::Unmarked => query = query.filter(dsl::marked_seen_in.is_null()),
        Seen::All => {}
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

    let mut table = if args.seen == Seen::All {
        // If both marked and unmarked ereports were included, add the "mark"
        // column.
        tabled::Table::new(rows.into_iter().map(EreportRowWithMark::from))
    } else {
        tabled::Table::new(rows)
    };
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
    let &InfoArgs { restart_id, ena, json, raw } = args;
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
    const RACK_ID: &str = "rack ID";
    const SLED_ID: &str = "  sled ID";
    const PART_NUMBER: &str = "  part number";
    const SERIAL_NUMBER: &str = "  serial number";
    const MARKED_SEEN_IN: &str = "marked seen in sitrep";
    const WIDTH: usize = const_max_len(&[
        ENA,
        CLASS,
        TIME_COLLECTED,
        TIME_DELETED,
        COLLECTOR_ID,
        RESTART_ID,
        REPORTER,
        RACK_ID,
        SLED_ID,
        PART_NUMBER,
        SERIAL_NUMBER,
        MARKED_SEEN_IN,
    ]);

    if json && raw {
        let ereport = ereport.report;
        serde_json::to_writer_pretty(std::io::stdout(), &ereport)
            .with_context(|| {
                format!("failed to serialize raw ereport: {ereport:?}")
            })?;
    } else if json {
        let raw_report = ereport.report.clone();
        match nexus_types::fm::Ereport::try_from(ereport) {
            Ok(ereport) => {
                serde_json::to_writer_pretty(std::io::stdout(), &ereport)
                    .with_context(|| {
                        format!(
                            "failed to serialize ereport with metadata: {ereport:?}"
                        )
                    })?;
            }
            Err(e) => {
                eprintln!(
                    "WARNING: failed to interpret ereport metadata, falling \
                     back to raw JSON: {e}"
                );
                serde_json::to_writer_pretty(std::io::stdout(), &raw_report)
                    .with_context(|| {
                        format!(
                            "failed to serialize raw ereport: {raw_report:?}"
                        )
                    })?;
            }
        }
    } else {
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
            marked_seen_in,
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
                    "    {REPORTER:>WIDTH$}: {sp_type:?} {slot} \
                     (service processor)"
                )
            }
            Ok(Reporter::HostOs { sled, slot }) => {
                if let Some(slot) = slot {
                    println!("    {REPORTER:>WIDTH$}: sled {slot} (host OS)");
                } else {
                    println!(
                        "    {REPORTER:>WIDTH$}: <unknown sled slot> (host OS)"
                    );
                }
                println!("    {SLED_ID:>WIDTH$}: {sled:?}")
            }
        }
        println!("    {RESTART_ID:>WIDTH$}: {restart_id}");

        // Grab the reporter entry so that we can print the rack ID.
        let reporter = restart_dsl::ereporter_restart
            .filter(restart_dsl::id.eq(restart_id.into_untyped_uuid()))
            .select(db::model::EreporterRestart::as_select())
            .first_async(&*conn)
            .await;
        match reporter {
            Ok(reporter) => {
                println!("    {RACK_ID:>WIDTH$}: {}", reporter.rack_id());
            }
            Err(err) => {
                eprintln!(
                    "error: failed to fetch ereporter_restart entry for \
                     {restart_id}: {err}"
                );
            }
        }

        println!(
            "    {PART_NUMBER:>WIDTH$}: {}",
            part_number.as_deref().unwrap_or("<unknown>")
        );
        println!(
            "    {SERIAL_NUMBER:>WIDTH$}: {}",
            serial_number.as_deref().unwrap_or("<unknown>")
        );
        println!("    {MARKED_SEEN_IN:>WIDTH$}: {marked_seen_in:?}",);

        println!("\n{:=<80}", "== EREPORT ");
        println!("{}", erebor::Displayer::new(&report));
    }
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
    let &ReportersArgs { slot_type, slot, serial: ref want_serial } = args;
    let slot_type = slot_type.map(nexus_db_model::SpType::from);

    let conn = datastore.pool_connection_for_tests().await?;
    let (restarts, ereport_info) = (*conn)
        .transaction_async(async move |conn| {
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

            // The canonical list of reporter restarts comes from the
            // `ereporter_restart` table.
            let mut query = restart_dsl::ereporter_restart
                .select(EreporterRestart::as_select())
                .into_boxed();
            if let Some(slot_type) = slot_type {
                query = query.filter(restart_dsl::slot_type.eq(slot_type));
            }
            if let Some(slot) = slot {
                query = query
                    .filter(restart_dsl::slot.eq(db::model::SqlU16::new(slot)));
            }
            let restarts = query
                .load_async::<EreporterRestart>(&conn)
                .await
                .context("listing ereporter restarts")?;

            // The `ereporter_restart` table doesn't record the reporter's sled
            // ID, VPD identity, or ereport count, so gather those from the
            // `ereport` table, grouped by restart ID. The sled ID is part of
            // the `GROUP BY` (rather than aggregated) since it is constant for
            // a given restart ID; the VPD identity may start out NULL and be
            // filled in later, so we take its `MAX` (ignoring NULLs) as we do
            // for the slot number. This may require a full table scan.
            let ereport_info = dsl::ereport
                .group_by((dsl::restart_id, dsl::sled_id))
                .select((
                    dsl::restart_id,
                    dsl::sled_id,
                    max(dsl::serial_number),
                    max(dsl::part_number),
                    count(dsl::ena).aggregate_distinct(),
                ))
                .load_async::<(
                    Uuid,
                    Option<Uuid>,
                    Option<String>,
                    Option<String>,
                    i64,
                )>(&conn)
                .await
                .context("querying ereport metadata by restart ID")?;

            Ok::<_, anyhow::Error>((restarts, ereport_info))
        })
        .await?;

    // Index the per-restart `ereport` metadata by restart ID.
    let mut ereport_info = ereport_info
        .into_iter()
        .map(|(id, sled_id, serial, part_number, ereports)| {
            (id, (sled_id, serial, part_number, ereports))
        })
        .collect::<HashMap<_, _>>();

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ReporterRow {
        #[tabled(display_with = "datetime_rfc3339_concise")]
        first_seen: DateTime<Utc>,
        id: Uuid,
        #[tabled(display_with = "display_option_invalid")]
        identity: Option<Reporter>,
        #[tabled(display_with = "display_option_blank", rename = "S/N")]
        serial: Option<String>,
        #[tabled(display_with = "display_option_blank", rename = "P/N")]
        part_number: Option<String>,
        rack_id: RackUuid,
        ereports: i64,
    }

    let mut rows = restarts
        .into_iter()
        .filter_map(|restart| {
            let id = restart.id.into_untyped_uuid();
            let (sled_id, serial, part_number, ereports) =
                ereport_info.remove(&id).unwrap_or((None, None, None, 0));
            // If we are filtering by serial number, skip any that don't match.
            if want_serial.is_some() && serial.as_ref() != want_serial.as_ref()
            {
                return None;
            }
            // The reporter identity combines the restart's location (from
            // `ereporter_restart`) with the sled ID (from `ereport`).
            let identity = match (model::Reporter {
                reporter: restart.reporter,
                sled_id: sled_id
                    .map(|sled| SledUuid::from_untyped_uuid(sled).into()),
                slot_type: restart.slot_type,
                slot: restart.slot,
            })
            .try_into()
            {
                Ok(reporter) => Some(reporter),
                Err(err) => {
                    eprintln!(
                        "error: encountered an invalid reporter entry for \
                         reporter ID {id}: {err}",
                    );
                    None
                }
            };
            Some(ReporterRow {
                first_seen: restart.time_first_seen,
                id,
                identity,
                serial,
                part_number,
                ereports,
                rack_id: *restart.rack_id(),
            })
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

async fn cmd_db_ereport_classes(
    omdb: &Omdb,
    log: &slog::Logger,
    datastore: &DataStore,
    args: &ClassesArgs,
) -> anyhow::Result<()> {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    // Try to fetch the known list from Nexus. If anything fails, fall back
    // to "?" for every row — DB totals are still useful even without Nexus.
    let known_from_nexus =
        fetch_known_classes_from_nexus(omdb, log, args).await;
    let known: BTreeSet<String> = match &known_from_nexus {
        Ok(list) => list.iter().cloned().collect(),
        Err(err) => {
            eprintln!(
                "warning: could not fetch known ereport classes from Nexus: \
                 {err:#}"
            );
            BTreeSet::new()
        }
    };
    let nexus_reachable = known_from_nexus.is_ok();

    let conn = datastore.pool_connection_for_tests().await?;

    // Both queries are backed by partial indexes (`lookup_ereports_by_class`
    // and `lookup_unmarked_ereports_by_class`) and do not full-table-scan;
    // see explain tests in nexus-db-queries.
    let totals: Vec<(Option<String>, i64)> =
        DataStore::ereport_class_totals_query()
            .load_async(&*conn)
            .await
            .context("loading per-class totals")?;
    let unmarkeds: Vec<(Option<String>, i64)> =
        DataStore::ereport_unmarked_class_totals_query()
            .load_async(&*conn)
            .await
            .context("loading per-class unmarked counts")?;

    // Merge by class. Key: Option<String> so NULL gets its own bucket.
    #[derive(Default)]
    struct ClassCounts {
        total: i64,
        unmarked: i64,
    }
    let mut by_class: BTreeMap<Option<String>, ClassCounts> = BTreeMap::new();
    for (class, total) in totals {
        by_class.entry(class).or_default().total = total;
    }
    for (class, unmarked) in unmarkeds {
        by_class.entry(class).or_default().unmarked = unmarked;
    }

    // Whether the deployed Nexus has a diagnosis engine that consumes a
    // given ereport class.
    #[derive(PartialEq, Eq)]
    enum KnownToNexus {
        /// Class has rows in the DB AND is in the list returned by Nexus.
        Yes,
        /// Class has rows in the DB but is NOT in the list returned by Nexus.
        No,
        /// Class is NULL — strict-match policy means the loader never
        /// surfaces these to FM analysis.
        NullClass,
        /// Could not reach Nexus — known/unknown is undetermined.
        Unknown,
    }
    impl std::fmt::Display for KnownToNexus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(match self {
                Self::Yes => "yes",
                Self::No => "no",
                Self::NullClass => "-",
                Self::Unknown => "?",
            })
        }
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ClassRow<'a> {
        known: KnownToNexus,
        total: i64,
        unmarked: i64,
        /// Variable-length, so it goes last: wrapping on a narrow terminal
        /// won't disrupt the fixed-width numeric columns.
        class: &'a str,
    }

    let mut rows: Vec<ClassRow<'_>> = by_class
        .iter()
        .map(|(class, ClassCounts { total, unmarked })| {
            let (known_marker, class_str): (KnownToNexus, &str) = match class {
                None => (KnownToNexus::NullClass, "(NULL)"),
                Some(c) if !nexus_reachable => {
                    (KnownToNexus::Unknown, c.as_str())
                }
                Some(c) => {
                    let k = if known.contains(c.as_str()) {
                        KnownToNexus::Yes
                    } else {
                        KnownToNexus::No
                    };
                    (k, c.as_str())
                }
            };
            ClassRow {
                known: known_marker,
                total: *total,
                unmarked: *unmarked,
                class: class_str,
            }
        })
        .collect();

    // Sort: unknown-but-present first (highest unmarked), then known, then
    // undetermined, then NULL.
    rows.sort_by(|a, b| {
        let priority = |row: &ClassRow<'_>| match row.known {
            KnownToNexus::No => 0,
            KnownToNexus::Yes => 1,
            KnownToNexus::Unknown => 2,
            KnownToNexus::NullClass => 3,
        };
        priority(a)
            .cmp(&priority(b))
            .then_with(|| b.unmarked.cmp(&a.unmarked))
            .then_with(|| a.class.cmp(b.class))
    });

    if nexus_reachable {
        println!(
            "note: KNOWN reflects which classes the currently-deployed Nexus \
             knows how\nto consume.\n"
        );
    } else {
        println!(
            "note: could not reach Nexus to determine known ereport classes.\n"
        );
    }

    let mut table = tabled::Table::new(&rows);
    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));
    println!("{table}");

    // Footer: classes Nexus knows about but with no rows in the database.
    if nexus_reachable {
        let seen_known: BTreeSet<&str> = rows
            .iter()
            .filter(|r| r.known == KnownToNexus::Yes)
            .map(|r| r.class)
            .collect();
        let absent: Vec<&String> =
            known.iter().filter(|c| !seen_known.contains(c.as_str())).collect();
        if !absent.is_empty() {
            println!(
                "\nClasses Nexus knows about but with no rows in the database:"
            );
            for c in absent {
                println!("  {c}");
            }
        }
    }

    Ok(())
}

async fn fetch_known_classes_from_nexus(
    omdb: &Omdb,
    log: &slog::Logger,
    args: &ClassesArgs,
) -> anyhow::Result<Vec<String>> {
    let nexus_url = match &args.nexus_internal_url {
        Some(url) => url.clone(),
        None => {
            let addr = omdb
                .dns_lookup_one(log.clone(), ServiceName::NexusLockstep)
                .await
                .context("resolving Nexus lockstep service via internal DNS")?;
            format!("http://{addr}")
        }
    };
    let client = nexus_lockstep_client::Client::new(&nexus_url, log.clone());
    let resp = client
        .fm_known_ereport_classes_list()
        .await
        .context("calling Nexus fm_known_ereport_classes_list")?;
    Ok(resp.into_inner())
}
