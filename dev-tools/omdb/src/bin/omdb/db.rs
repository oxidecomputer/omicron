// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! omdb commands that query or update the database
//!
//! GROUND RULES: There aren't many ground rules (see top-level docs).  But
//! where possible, stick to operations provided by `DataStore` rather than
//! querying the database directly.  The DataStore operations generally provide
//! a safer level of abstraction.  But there are cases where we want to do
//! things that really don't need to be in the DataStore -- i.e., where `omdb`
//! would be the only consumer -- and in that case it's okay to query the
//! database directly.

// NOTE: emanates from Tabled macros
#![allow(clippy::useless_vec)]

use crate::helpers::CONNECTION_OPTIONS_HEADING;
use crate::helpers::DATABASE_OPTIONS_HEADING;
use crate::Omdb;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use camino::Utf8PathBuf;
use chrono::SecondsFormat;
use clap::ArgAction;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use diesel::expression::SelectableHelper;
use diesel::query_dsl::QueryDsl;
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::OptionalExtension;
use diesel::TextExpressionMethods;
use gateway_client::types::SpType;
use ipnetwork::IpNetwork;
use nexus_config::PostgresConfigWithUrl;
use nexus_db_model::Dataset;
use nexus_db_model::Disk;
use nexus_db_model::DnsGroup;
use nexus_db_model::DnsName;
use nexus_db_model::DnsVersion;
use nexus_db_model::DnsZone;
use nexus_db_model::ExternalIp;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::Instance;
use nexus_db_model::InvCollection;
use nexus_db_model::IpAttachState;
use nexus_db_model::IpKind;
use nexus_db_model::NetworkInterface;
use nexus_db_model::NetworkInterfaceKind;
use nexus_db_model::Probe;
use nexus_db_model::Project;
use nexus_db_model::Region;
use nexus_db_model::RegionSnapshot;
use nexus_db_model::Sled;
use nexus_db_model::Snapshot;
use nexus_db_model::SnapshotState;
use nexus_db_model::SwCaboose;
use nexus_db_model::SwRotPage;
use nexus_db_model::Vmm;
use nexus_db_model::Volume;
use nexus_db_model::VpcSubnet;
use nexus_db_model::Zpool;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::read_only_resources_associated_with_volume;
use nexus_db_queries::db::datastore::CrucibleTargets;
use nexus_db_queries::db::datastore::DataStoreConnection;
use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::ServiceKind;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params::DnsRecord;
use nexus_types::internal_api::params::Srv;
use nexus_types::inventory::CabooseWhich;
use nexus_types::inventory::Collection;
use nexus_types::inventory::RotPageWhich;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::MacAddr;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use sled_agent_client::types::VolumeConstructionRequest;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::num::NonZeroU32;
use std::sync::Arc;
use strum::IntoEnumIterator;
use tabled::Tabled;
use uuid::Uuid;

const NO_ACTIVE_PROPOLIS_MSG: &str = "<no active Propolis>";
const NOT_ON_SLED_MSG: &str = "<not on any sled>";

struct MaybePropolisId(Option<Uuid>);
struct MaybeSledId(Option<Uuid>);

impl From<&InstanceAndActiveVmm> for MaybePropolisId {
    fn from(value: &InstanceAndActiveVmm) -> Self {
        Self(value.instance().runtime().propolis_id)
    }
}

impl Display for MaybePropolisId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(id) = self.0 {
            write!(f, "{}", id)
        } else {
            write!(f, "{}", NO_ACTIVE_PROPOLIS_MSG)
        }
    }
}

impl From<&InstanceAndActiveVmm> for MaybeSledId {
    fn from(value: &InstanceAndActiveVmm) -> Self {
        Self(value.sled_id())
    }
}

impl Display for MaybeSledId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(id) = self.0 {
            write!(f, "{}", id)
        } else {
            write!(f, "{}", NOT_ON_SLED_MSG)
        }
    }
}

#[derive(Debug, Args)]
pub struct DbArgs {
    #[clap(flatten)]
    db_url_opts: DbUrlOptions,

    #[clap(flatten)]
    fetch_opts: DbFetchOptions,

    #[command(subcommand)]
    command: DbCommands,
}

#[derive(Debug, Args)]
pub struct DbUrlOptions {
    /// URL of the database SQL interface
    #[clap(
        long,
        env = "OMDB_DB_URL",
        global = true,
        help_heading = CONNECTION_OPTIONS_HEADING,
    )]
    db_url: Option<PostgresConfigWithUrl>,
}

impl DbUrlOptions {
    async fn resolve_pg_url(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> anyhow::Result<PostgresConfigWithUrl> {
        match &self.db_url {
            Some(cli_or_env_url) => Ok(cli_or_env_url.clone()),
            None => {
                eprintln!(
                    "note: database URL not specified.  Will search DNS."
                );
                eprintln!("note: (override with --db-url or OMDB_DB_URL)");
                let addrs = omdb
                    .dns_lookup_all(
                        log.clone(),
                        internal_dns::ServiceName::Cockroach,
                    )
                    .await?;

                format!(
                    "postgresql://root@{}/omicron?sslmode=disable",
                    addrs
                        .into_iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
                .parse()
                .context("failed to parse constructed postgres URL")
            }
        }
    }

    pub async fn connect(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> anyhow::Result<Arc<DataStore>> {
        let db_url = self.resolve_pg_url(omdb, log).await?;
        eprintln!("note: using database URL {}", &db_url);

        let db_config = db::Config { url: db_url.clone() };
        let pool = Arc::new(db::Pool::new(&log.clone(), &db_config));

        // Being a dev tool, we want to try this operation even if the schema
        // doesn't match what we expect.  So we use `DataStore::new_unchecked()`
        // here.  We will then check the schema version explicitly and warn the
        // user if it doesn't match.
        let datastore = Arc::new(
            DataStore::new_unchecked(log.clone(), pool)
                .map_err(|e| anyhow!(e).context("creating datastore"))?,
        );
        check_schema_version(&datastore).await;
        Ok(datastore)
    }
}

#[derive(Debug, Args)]
pub struct DbFetchOptions {
    /// limit to apply to queries that fetch rows
    #[clap(
        long = "fetch-limit",
        default_value_t = NonZeroU32::new(500).unwrap(),
        env = "OMDB_FETCH_LIMIT",
        global = true,
        help_heading = DATABASE_OPTIONS_HEADING,
    )]
    fetch_limit: NonZeroU32,

    /// whether to include soft-deleted records when enumerating objects that
    /// can be soft-deleted
    #[clap(
        long,
        default_value_t = false,
        global = true,
        help_heading = DATABASE_OPTIONS_HEADING,
    )]
    include_deleted: bool,
}

/// Subcommands that query or update the database
#[derive(Debug, Subcommand)]
enum DbCommands {
    /// Print information about the rack
    Rack(RackArgs),
    /// Print information about disks
    Disks(DiskArgs),
    /// Print information about internal and external DNS
    Dns(DnsArgs),
    /// Print information about collected hardware/software inventory
    Inventory(InventoryArgs),
    /// Save the current Reconfigurator inputs to a file
    ReconfiguratorSave(ReconfiguratorSaveArgs),
    /// Print information about sleds
    Sleds(SledsArgs),
    /// Print information about customer instances
    Instances(InstancesOptions),
    /// Print information about the network
    Network(NetworkArgs),
    /// Print information about snapshots
    Snapshots(SnapshotArgs),
    /// Validate the contents of the database
    Validate(ValidateArgs),
}

#[derive(Debug, Args)]
struct RackArgs {
    #[command(subcommand)]
    command: RackCommands,
}

#[derive(Debug, Subcommand)]
enum RackCommands {
    /// Summarize current racks
    List,
}

#[derive(Debug, Args)]
struct DiskArgs {
    #[command(subcommand)]
    command: DiskCommands,
}

#[derive(Debug, Subcommand)]
enum DiskCommands {
    /// Get info for a specific disk
    Info(DiskInfoArgs),
    /// Summarize current disks
    List,
    /// Determine what crucible resources are on the given physical disk.
    Physical(DiskPhysicalArgs),
}

#[derive(Debug, Args)]
struct DiskInfoArgs {
    /// The UUID of the volume
    uuid: Uuid,
}

#[derive(Debug, Args)]
struct DiskPhysicalArgs {
    /// The UUID of the physical disk
    uuid: Uuid,
}

#[derive(Debug, Args)]
struct DnsArgs {
    #[command(subcommand)]
    command: DnsCommands,
}

#[derive(Debug, Subcommand)]
enum DnsCommands {
    /// Summarize current version of all DNS zones
    Show,
    /// Show what changed in a given DNS version
    Diff(DnsVersionArgs),
    /// Show the full contents of a given DNS zone and version
    Names(DnsVersionArgs),
}

#[derive(Debug, Args)]
struct DnsVersionArgs {
    /// name of a DNS group
    #[arg(value_enum)]
    group: CliDnsGroup,
    /// version of the group's data
    version: u32,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliDnsGroup {
    Internal,
    External,
}

impl CliDnsGroup {
    fn dns_group(&self) -> DnsGroup {
        match self {
            CliDnsGroup::Internal => DnsGroup::Internal,
            CliDnsGroup::External => DnsGroup::External,
        }
    }
}

#[derive(Debug, Args)]
struct InstancesOptions {
    /// Only show the running instances
    #[arg(short, long, action=ArgAction::SetTrue)]
    running: bool,
}

#[derive(Debug, Args)]
struct InventoryArgs {
    #[command(subcommand)]
    command: InventoryCommands,
}

#[derive(Debug, Subcommand)]
enum InventoryCommands {
    /// list all baseboards ever found
    BaseboardIds,
    /// list all cabooses ever found
    Cabooses,
    /// list and show details from particular collections
    Collections(CollectionsArgs),
    /// list all root of trust pages ever found
    RotPages,
}

#[derive(Debug, Args)]
struct CollectionsArgs {
    #[command(subcommand)]
    command: CollectionsCommands,
}

#[derive(Debug, Subcommand)]
enum CollectionsCommands {
    /// list collections
    List,
    /// show what was found in a particular collection
    Show(CollectionsShowArgs),
}

#[derive(Debug, Args)]
struct CollectionsShowArgs {
    /// id of the collection
    id: CollectionUuid,
    /// show long strings in their entirety
    #[clap(long)]
    show_long_strings: bool,
}

#[derive(Debug, Args)]
struct ReconfiguratorSaveArgs {
    /// where to save the output
    output_file: Utf8PathBuf,
}

#[derive(Debug, Args)]
struct SledsArgs {
    /// Show sleds that match the given filter
    #[clap(short = 'F', long, value_enum)]
    filter: Option<SledFilter>,
}

#[derive(Debug, Args)]
struct NetworkArgs {
    #[command(subcommand)]
    command: NetworkCommands,

    /// Print out raw data structures from the data store.
    #[clap(long, global = true)]
    verbose: bool,
}

#[derive(Debug, Subcommand)]
enum NetworkCommands {
    /// List external IPs
    ListEips,
    /// List virtual network interfaces
    ListVnics,
}

#[derive(Debug, Args)]
struct SnapshotArgs {
    #[command(subcommand)]
    command: SnapshotCommands,
}

#[derive(Debug, Subcommand)]
enum SnapshotCommands {
    /// Get info for a specific snapshot
    Info(SnapshotInfoArgs),
    /// Summarize current snapshots
    List,
}

#[derive(Debug, Args)]
struct SnapshotInfoArgs {
    /// The UUID of the snapshot
    uuid: Uuid,
}

#[derive(Debug, Args)]
struct ValidateArgs {
    #[command(subcommand)]
    command: ValidateCommands,
}

#[derive(Debug, Subcommand)]
enum ValidateCommands {
    /// Validate each `volume_references` column in the region snapshots table
    ValidateVolumeReferences,

    /// Find either region snapshots Nexus knows about that the corresponding
    /// Crucible agent says were deleted, or region snapshots that Nexus doesn't
    /// know about.
    ValidateRegionSnapshots,
}

impl DbArgs {
    /// Run a `omdb db` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let datastore = self.db_url_opts.connect(omdb, log).await?;
        let opctx = OpContext::for_tests(log.clone(), datastore.clone());
        match &self.command {
            DbCommands::Rack(RackArgs { command: RackCommands::List }) => {
                cmd_db_rack_list(&opctx, &datastore, &self.fetch_opts).await
            }
            DbCommands::Disks(DiskArgs {
                command: DiskCommands::Info(uuid),
            }) => cmd_db_disk_info(&opctx, &datastore, uuid).await,
            DbCommands::Disks(DiskArgs { command: DiskCommands::List }) => {
                cmd_db_disk_list(&datastore, &self.fetch_opts).await
            }
            DbCommands::Disks(DiskArgs {
                command: DiskCommands::Physical(uuid),
            }) => {
                cmd_db_disk_physical(&opctx, &datastore, &self.fetch_opts, uuid)
                    .await
            }
            DbCommands::Dns(DnsArgs { command: DnsCommands::Show }) => {
                cmd_db_dns_show(&opctx, &datastore, &self.fetch_opts).await
            }
            DbCommands::Dns(DnsArgs { command: DnsCommands::Diff(args) }) => {
                cmd_db_dns_diff(&opctx, &datastore, &self.fetch_opts, args)
                    .await
            }
            DbCommands::Dns(DnsArgs { command: DnsCommands::Names(args) }) => {
                cmd_db_dns_names(&opctx, &datastore, &self.fetch_opts, args)
                    .await
            }
            DbCommands::Inventory(inventory_args) => {
                cmd_db_inventory(
                    &opctx,
                    &datastore,
                    &self.fetch_opts,
                    inventory_args,
                )
                .await
            }
            DbCommands::ReconfiguratorSave(reconfig_save_args) => {
                cmd_db_reconfigurator_save(
                    &opctx,
                    &datastore,
                    reconfig_save_args,
                )
                .await
            }
            DbCommands::Sleds(args) => {
                cmd_db_sleds(&opctx, &datastore, &self.fetch_opts, args).await
            }
            DbCommands::Instances(instances_options) => {
                cmd_db_instances(
                    &opctx,
                    &datastore,
                    &self.fetch_opts,
                    instances_options.running,
                )
                .await
            }
            DbCommands::Network(NetworkArgs {
                command: NetworkCommands::ListEips,
                verbose,
            }) => {
                cmd_db_eips(&opctx, &datastore, &self.fetch_opts, *verbose)
                    .await
            }
            DbCommands::Network(NetworkArgs {
                command: NetworkCommands::ListVnics,
                verbose,
            }) => {
                cmd_db_network_list_vnics(
                    &datastore,
                    &self.fetch_opts,
                    *verbose,
                )
                .await
            }
            DbCommands::Snapshots(SnapshotArgs {
                command: SnapshotCommands::Info(uuid),
            }) => cmd_db_snapshot_info(&opctx, &datastore, uuid).await,
            DbCommands::Snapshots(SnapshotArgs {
                command: SnapshotCommands::List,
            }) => cmd_db_snapshot_list(&datastore, &self.fetch_opts).await,
            DbCommands::Validate(ValidateArgs {
                command: ValidateCommands::ValidateVolumeReferences,
            }) => cmd_db_validate_volume_references(&datastore).await,
            DbCommands::Validate(ValidateArgs {
                command: ValidateCommands::ValidateRegionSnapshots,
            }) => cmd_db_validate_region_snapshots(&datastore).await,
        }
    }
}

/// Check the version of the schema in the database and report whether it
/// appears to be compatible with this tool.
///
/// This is just advisory.  We will not abort if the version appears
/// incompatible because in practice it may well not matter and it's very
/// valuable for this tool to work if it possibly can.
async fn check_schema_version(datastore: &DataStore) {
    let expected_version = nexus_db_model::SCHEMA_VERSION;
    let version_check = datastore.database_schema_version().await;

    match version_check {
        Ok((found_version, found_target)) => {
            if let Some(target) = found_target {
                eprintln!(
                    "note: database schema target exists (mid-upgrade?) ({})",
                    target
                );
            }

            if found_version == expected_version {
                eprintln!(
                    "note: database schema version matches expected ({})",
                    expected_version
                );
                return;
            }

            eprintln!(
                "WARN: found schema version {}, expected {}",
                found_version, expected_version
            );
        }
        Err(error) => {
            eprintln!("WARN: failed to query schema version: {:#}", error);
        }
    };

    eprintln!(
        "{}",
        textwrap::fill(
            "It's possible the database is running a version that's different \
            from what this tool understands.  This may result in errors or \
            incorrect output.",
            80
        )
    );
}

/// Check the result of a query to see if it hit the given limit.  If so, warn
/// the user that our output may be incomplete and that they might try a larger
/// one.  (We don't want to bail out, though.  Incomplete data is better than no
/// data.)
fn check_limit<I, F, D>(items: &[I], limit: NonZeroU32, context: F)
where
    F: FnOnce() -> D,
    D: Display,
{
    if items.len() == usize::try_from(limit.get()).unwrap() {
        limit_error(limit, context);
    }
}

fn limit_error<F, D>(limit: NonZeroU32, context: F)
where
    F: FnOnce() -> D,
    D: Display,
{
    eprintln!(
        "WARN: {}: found {} items (the limit).  There may be more items \
            that were ignored.  Consider overriding with --fetch-limit.",
        context(),
        limit,
    );
}

/// Returns pagination parameters to fetch the first page of results for a
/// paginated endpoint
fn first_page<'a, T>(limit: NonZeroU32) -> DataPageParams<'a, T> {
    DataPageParams {
        marker: None,
        direction: dropshot::PaginationOrder::Ascending,
        limit,
    }
}

/// Helper function to look up an instance with the given ID.
async fn lookup_instance(
    datastore: &DataStore,
    instance_id: Uuid,
) -> anyhow::Result<Option<Instance>> {
    use db::schema::instance::dsl;

    let conn = datastore.pool_connection_for_tests().await?;
    dsl::instance
        .filter(dsl::id.eq(instance_id))
        .limit(1)
        .select(Instance::as_select())
        .get_result_async(&*conn)
        .await
        .optional()
        .with_context(|| format!("loading instance {instance_id}"))
}

#[derive(Clone, Debug)]
struct ServiceInfo {
    service_kind: ServiceKind,
    disposition: BlueprintZoneDisposition,
}

/// Helper function to look up the service with the given ID.
///
/// Requires the caller to first have fetched the current target blueprint.
async fn lookup_service_info(
    service_id: Uuid,
    blueprint: &Blueprint,
) -> anyhow::Result<Option<ServiceInfo>> {
    let Some(zone_config) = blueprint
        .all_omicron_zones(BlueprintZoneFilter::All)
        .find_map(|(_sled_id, zone_config)| {
            if zone_config.id.into_untyped_uuid() == service_id {
                Some(zone_config)
            } else {
                None
            }
        })
    else {
        return Ok(None);
    };

    let service_kind = match &zone_config.zone_type {
        BlueprintZoneType::BoundaryNtp(_)
        | BlueprintZoneType::InternalNtp(_) => ServiceKind::Ntp,
        BlueprintZoneType::Clickhouse(_) => ServiceKind::Clickhouse,
        BlueprintZoneType::ClickhouseKeeper(_) => ServiceKind::ClickhouseKeeper,
        BlueprintZoneType::CockroachDb(_) => ServiceKind::Cockroach,
        BlueprintZoneType::Crucible(_) => ServiceKind::Crucible,
        BlueprintZoneType::CruciblePantry(_) => ServiceKind::CruciblePantry,
        BlueprintZoneType::ExternalDns(_) => ServiceKind::ExternalDns,
        BlueprintZoneType::InternalDns(_) => ServiceKind::InternalDns,
        BlueprintZoneType::Nexus(_) => ServiceKind::Nexus,
        BlueprintZoneType::Oximeter(_) => ServiceKind::Oximeter,
    };

    Ok(Some(ServiceInfo { service_kind, disposition: zone_config.disposition }))
}

/// Helper function to looks up a probe with the given ID.
async fn lookup_probe(
    datastore: &DataStore,
    probe_id: Uuid,
) -> anyhow::Result<Option<Probe>> {
    use db::schema::probe::dsl;

    let conn = datastore.pool_connection_for_tests().await?;
    dsl::probe
        .filter(dsl::id.eq(probe_id))
        .limit(1)
        .select(Probe::as_select())
        .get_result_async(&*conn)
        .await
        .optional()
        .with_context(|| format!("loading probe {probe_id}"))
}

/// Helper function to looks up a project with the given ID.
async fn lookup_project(
    datastore: &DataStore,
    project_id: Uuid,
) -> anyhow::Result<Option<Project>> {
    use db::schema::project::dsl;

    let conn = datastore.pool_connection_for_tests().await?;
    dsl::project
        .filter(dsl::id.eq(project_id))
        .limit(1)
        .select(Project::as_select())
        .get_result_async(&*conn)
        .await
        .optional()
        .with_context(|| format!("loading project {project_id}"))
}

// Disks

/// Run `omdb db disk list`.
async fn cmd_db_disk_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DiskRow {
        name: String,
        id: String,
        size: String,
        state: String,
        attached_to: String,
    }

    let ctx = || "listing disks".to_string();

    use db::schema::disk::dsl;
    let mut query = dsl::disk.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let disks = query
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(Disk::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading disks")?;

    check_limit(&disks, fetch_opts.fetch_limit, ctx);

    let rows = disks.into_iter().map(|disk| DiskRow {
        name: disk.name().to_string(),
        id: disk.id().to_string(),
        size: disk.size.to_string(),
        state: disk.runtime().disk_state,
        attached_to: match disk.runtime().attach_instance_id {
            Some(uuid) => uuid.to_string(),
            None => "-".to_string(),
        },
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Run `omdb db rack info`.
async fn cmd_db_rack_list(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RackRow {
        id: String,
        initialized: bool,
        tuf_base_url: String,
        rack_subnet: String,
    }

    let ctx = || "listing racks".to_string();

    let limit = fetch_opts.fetch_limit;
    let rack_list = datastore
        .rack_list(opctx, &first_page(limit))
        .await
        .context("listing racks")?;
    check_limit(&rack_list, limit, ctx);

    let rows = rack_list.into_iter().map(|rack| RackRow {
        id: rack.id().to_string(),
        initialized: rack.initialized,
        tuf_base_url: rack.tuf_base_url.unwrap_or_else(|| "-".to_string()),
        rack_subnet: rack
            .rack_subnet
            .map(|subnet| subnet.to_string())
            .unwrap_or_else(|| "-".to_string()),
    });

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Run `omdb db disk info <UUID>`.
async fn cmd_db_disk_info(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &DiskInfoArgs,
) -> Result<(), anyhow::Error> {
    // The row describing the instance
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct UpstairsRow {
        host_serial: String,
        disk_name: String,
        instance_name: String,
        propolis_zone: String,
        volume_id: String,
        disk_state: String,
    }

    // The rows describing the downstairs regions for this disk/volume
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DownstairsRow {
        host_serial: String,
        region: String,
        zone: String,
        physical_disk: String,
    }

    use db::schema::disk::dsl as disk_dsl;

    let conn = datastore.pool_connection_for_tests().await?;

    let disk = disk_dsl::disk
        .filter(disk_dsl::id.eq(args.uuid))
        .limit(1)
        .select(Disk::as_select())
        .load_async(&*conn)
        .await
        .context("loading requested disk")?;

    let Some(disk) = disk.into_iter().next() else {
        bail!("no disk: {} found", args.uuid);
    };

    // For information about where this disk is attached.
    let mut rows = Vec::new();

    // If the disk is attached to an instance, show information
    // about that instance.
    let usr = if let Some(instance_uuid) = disk.runtime().attach_instance_id {
        // Get the instance this disk is attached to
        use db::schema::instance::dsl as instance_dsl;
        use db::schema::vmm::dsl as vmm_dsl;
        let instances: Vec<InstanceAndActiveVmm> = instance_dsl::instance
            .filter(instance_dsl::id.eq(instance_uuid))
            .left_join(
                vmm_dsl::vmm.on(vmm_dsl::id
                    .nullable()
                    .eq(instance_dsl::active_propolis_id)
                    .and(vmm_dsl::time_deleted.is_null())),
            )
            .limit(1)
            .select((Instance::as_select(), Option::<Vmm>::as_select()))
            .load_async(&*conn)
            .await
            .context("loading requested instance")?
            .into_iter()
            .map(|i: (Instance, Option<Vmm>)| i.into())
            .collect();

        let Some(instance) = instances.into_iter().next() else {
            bail!("no instance: {} found", instance_uuid);
        };

        let instance_name = instance.instance().name().to_string();
        let disk_name = disk.name().to_string();
        if instance.vmm().is_some() {
            let propolis_id =
                instance.instance().runtime().propolis_id.unwrap();
            let my_sled_id = instance.sled_id().unwrap();

            let (_, my_sled) = LookupPath::new(opctx, datastore)
                .sled_id(my_sled_id)
                .fetch()
                .await
                .context("failed to look up sled")?;

            UpstairsRow {
                host_serial: my_sled.serial_number().to_string(),
                disk_name,
                instance_name,
                propolis_zone: format!("oxz_propolis-server_{}", propolis_id),
                volume_id: disk.volume_id.to_string(),
                disk_state: disk.runtime_state.disk_state.to_string(),
            }
        } else {
            UpstairsRow {
                host_serial: NOT_ON_SLED_MSG.to_string(),
                disk_name,
                instance_name,
                propolis_zone: NO_ACTIVE_PROPOLIS_MSG.to_string(),
                volume_id: disk.volume_id.to_string(),
                disk_state: disk.runtime_state.disk_state.to_string(),
            }
        }
    } else {
        // If the disk is not attached to anything, just print empty
        // fields.
        UpstairsRow {
            host_serial: "-".to_string(),
            disk_name: disk.name().to_string(),
            instance_name: "-".to_string(),
            propolis_zone: "-".to_string(),
            volume_id: disk.volume_id.to_string(),
            disk_state: disk.runtime_state.disk_state.to_string(),
        }
    };
    rows.push(usr);

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    // Get the dataset backing this volume.
    let regions = datastore.get_allocated_regions(disk.volume_id).await?;

    let mut rows = Vec::with_capacity(3);
    for (dataset, region) in regions {
        let my_pool_id = dataset.pool_id;
        let (_, my_zpool) = LookupPath::new(opctx, datastore)
            .zpool_id(my_pool_id)
            .fetch()
            .await
            .context("failed to look up zpool")?;

        let my_sled_id = my_zpool.sled_id;

        let (_, my_sled) = LookupPath::new(opctx, datastore)
            .sled_id(my_sled_id)
            .fetch()
            .await
            .context("failed to look up sled")?;

        rows.push(DownstairsRow {
            host_serial: my_sled.serial_number().to_string(),
            region: region.id().to_string(),
            zone: format!("oxz_crucible_{}", dataset.id()),
            physical_disk: my_zpool.physical_disk_id.to_string(),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Run `omdb db disk physical <UUID>`.
async fn cmd_db_disk_physical(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &DiskPhysicalArgs,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    // We start by finding any zpools that are using the physical disk.
    use db::schema::zpool::dsl as zpool_dsl;
    let mut query = zpool_dsl::zpool.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(zpool_dsl::time_deleted.is_null());
    }

    let zpools = query
        .filter(zpool_dsl::physical_disk_id.eq(args.uuid))
        .select(Zpool::as_select())
        .load_async(&*conn)
        .await
        .context("loading zpool from pysical disk id")?;

    let mut sled_ids = HashSet::new();
    let mut dataset_ids = HashSet::new();

    if zpools.is_empty() {
        println!("Found no zpools on physical disk UUID {}", args.uuid);
        return Ok(());
    }

    // The current plan is a single zpool per physical disk, so we expect that
    // this will have a single item.  However, If single zpool per disk ever
    // changes, this code will still work.
    for zp in zpools {
        // zpool has the sled id, record that so we can find the serial number.
        sled_ids.insert(zp.sled_id);

        // Next, we find all the datasets that are on our zpool.
        use db::schema::dataset::dsl as dataset_dsl;
        let mut query = dataset_dsl::dataset.into_boxed();
        if !fetch_opts.include_deleted {
            query = query.filter(dataset_dsl::time_deleted.is_null());
        }

        let datasets = query
            .filter(dataset_dsl::pool_id.eq(zp.id()))
            .select(Dataset::as_select())
            .load_async(&*conn)
            .await
            .context("loading dataset")?;

        // Add all the datasets ids that are using this pool.
        for ds in datasets {
            dataset_ids.insert(ds.id());
        }
    }

    // If we do have more than one sled ID, then something is wrong, but
    // go ahead and print out whatever we have found.
    for sid in sled_ids {
        let (_, my_sled) = LookupPath::new(opctx, datastore)
            .sled_id(sid)
            .fetch()
            .await
            .context("failed to look up sled")?;

        println!(
            "Physical disk: {} found on sled: {}",
            args.uuid,
            my_sled.serial_number()
        );
    }
    println!("DATASETS: {:?}", dataset_ids);

    let mut volume_ids = HashSet::new();
    // Now, take the list of datasets we found and search all the regions
    // to see if any of them are on the dataset.  If we find a region that
    // is on one of our datasets, then record the volume ID of that region.
    for did in dataset_ids.clone().into_iter() {
        use db::schema::region::dsl as region_dsl;
        let regions = region_dsl::region
            .filter(region_dsl::dataset_id.eq(did))
            .select(Region::as_select())
            .load_async(&*conn)
            .await
            .context("loading region")?;

        for rs in regions {
            volume_ids.insert(rs.volume_id());
        }
    }

    // At this point, we have a list of volume IDs that contain a region
    // that is part of a dataset on a pool on our disk.  The next step is
    // to find the virtual disks associated with these volume IDs and
    // display information about those disks.
    use db::schema::disk::dsl;
    let mut query = dsl::disk.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let disks = query
        .filter(dsl::volume_id.eq_any(volume_ids))
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(Disk::as_select())
        .load_async(&*conn)
        .await
        .context("loading disks")?;

    check_limit(&disks, fetch_opts.fetch_limit, || "listing disks".to_string());

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DiskRow {
        disk_name: String,
        id: String,
        state: String,
        instance_name: String,
    }

    let mut rows = Vec::new();

    for disk in disks {
        // If the disk is attached to an instance, determine the name of the
        // instance.
        let instance_name =
            if let Some(instance_uuid) = disk.runtime().attach_instance_id {
                // Get the instance this disk is attached to
                use db::schema::instance::dsl as instance_dsl;
                let instance = instance_dsl::instance
                    .filter(instance_dsl::id.eq(instance_uuid))
                    .limit(1)
                    .select(Instance::as_select())
                    .load_async(&*conn)
                    .await
                    .context("loading requested instance")?;

                if let Some(instance) = instance.into_iter().next() {
                    instance.name().to_string()
                } else {
                    "???".to_string()
                }
            } else {
                "-".to_string()
            };

        rows.push(DiskRow {
            disk_name: disk.name().to_string(),
            id: disk.id().to_string(),
            state: disk.runtime().disk_state,
            instance_name,
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    // Collect the region_snapshots associated with the dataset IDs
    let limit = fetch_opts.fetch_limit;
    use db::schema::region_snapshot::dsl as region_snapshot_dsl;
    let region_snapshots = region_snapshot_dsl::region_snapshot
        .filter(region_snapshot_dsl::dataset_id.eq_any(dataset_ids))
        .limit(i64::from(u32::from(limit)))
        .select(RegionSnapshot::as_select())
        .load_async(&*conn)
        .await
        .context("loading region snapshots")?;

    check_limit(&region_snapshots, limit, || {
        "listing region snapshots".to_string()
    });

    // The row describing the region_snapshot.
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RegionSnapshotRow {
        dataset_id: String,
        region_id: String,
        snapshot_id: String,
        volume_references: String,
    }
    let mut rsnap = Vec::new();

    // From each region snapshot:
    // Collect the snapshot IDs for later use.
    // Display the region snapshot rows.
    let mut snapshot_ids = HashSet::new();
    for rs in region_snapshots {
        snapshot_ids.insert(rs.snapshot_id);
        let rs = RegionSnapshotRow {
            dataset_id: rs.dataset_id.to_string(),
            region_id: rs.region_id.to_string(),
            snapshot_id: rs.snapshot_id.to_string(),
            volume_references: rs.volume_references.to_string(),
        };
        rsnap.push(rs);
    }
    let table = tabled::Table::new(rsnap)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    // Get the snapshots from the list of IDs we built above.
    // Display information about those snapshots.
    use db::schema::snapshot::dsl as snapshot_dsl;
    let mut query = snapshot_dsl::snapshot.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(snapshot_dsl::time_deleted.is_null());
    }

    let snapshots = query
        .filter(snapshot_dsl::id.eq_any(snapshot_ids))
        .limit(i64::from(u32::from(limit)))
        .select(Snapshot::as_select())
        .load_async(&*conn)
        .await
        .context("loading snapshots")?;

    check_limit(&snapshots, limit, || "listing snapshots".to_string());

    let rows =
        snapshots.into_iter().map(|snapshot| SnapshotRow::from(snapshot));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);
    Ok(())
}

// SERVICES

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct ServiceInstanceRow {
    #[tabled(rename = "SERVICE")]
    kind: String,
    instance_id: Uuid,
    addr: String,
    sled_serial: String,
}

// Snapshots
fn format_snapshot(state: &SnapshotState) -> impl Display {
    match state {
        SnapshotState::Creating => "creating".to_string(),
        SnapshotState::Ready => "ready".to_string(),
        SnapshotState::Faulted => "faulted".to_string(),
        SnapshotState::Destroyed => "destroyed".to_string(),
    }
}

// The row describing the snapshot
#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct SnapshotRow {
    snap_name: String,
    id: String,
    state: String,
    size: String,
    source_disk_id: String,
    source_volume_id: String,
    destination_volume_id: String,
}

impl From<Snapshot> for SnapshotRow {
    fn from(s: Snapshot) -> Self {
        SnapshotRow {
            snap_name: s.name().to_string(),
            id: s.id().to_string(),
            state: format_snapshot(&s.state).to_string(),
            size: s.size.to_string(),
            source_disk_id: s.disk_id.to_string(),
            source_volume_id: s.volume_id.to_string(),
            destination_volume_id: s.destination_volume_id.to_string(),
        }
    }
}

/// Run `omdb db snapshot list`.
async fn cmd_db_snapshot_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    let ctx = || "listing snapshots".to_string();
    let limit = fetch_opts.fetch_limit;

    use db::schema::snapshot::dsl;
    let mut query = dsl::snapshot.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let snapshots = query
        .limit(i64::from(u32::from(limit)))
        .select(Snapshot::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading snapshots")?;

    check_limit(&snapshots, limit, ctx);

    let rows =
        snapshots.into_iter().map(|snapshot| SnapshotRow::from(snapshot));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Run `omdb db snapshot info <UUID>`.
async fn cmd_db_snapshot_info(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &SnapshotInfoArgs,
) -> Result<(), anyhow::Error> {
    // The rows describing the downstairs regions for this snapshot/volume
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DownstairsRow {
        host_serial: String,
        region: String,
        zone: String,
        physical_disk: String,
    }

    use db::schema::snapshot::dsl as snapshot_dsl;
    let snapshots = snapshot_dsl::snapshot
        .filter(snapshot_dsl::id.eq(args.uuid))
        .limit(1)
        .select(Snapshot::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading requested snapshot")?;

    let mut dest_volume_ids = Vec::new();
    let rows = snapshots.into_iter().map(|snapshot| {
        dest_volume_ids.push(snapshot.destination_volume_id);
        SnapshotRow::from(snapshot)
    });
    if rows.len() == 0 {
        bail!("No snapshout with UUID: {} found", args.uuid);
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    for vol_id in dest_volume_ids {
        // Get the dataset backing this volume.
        let regions = datastore.get_allocated_regions(vol_id).await?;

        let mut rows = Vec::with_capacity(3);
        for (dataset, region) in regions {
            let my_pool_id = dataset.pool_id;
            let (_, my_zpool) = LookupPath::new(opctx, datastore)
                .zpool_id(my_pool_id)
                .fetch()
                .await
                .context("failed to look up zpool")?;

            let my_sled_id = my_zpool.sled_id;

            let (_, my_sled) = LookupPath::new(opctx, datastore)
                .sled_id(my_sled_id)
                .fetch()
                .await
                .context("failed to look up sled")?;

            rows.push(DownstairsRow {
                host_serial: my_sled.serial_number().to_string(),
                region: region.id().to_string(),
                zone: format!("oxz_crucible_{}", dataset.id()),
                physical_disk: my_zpool.physical_disk_id.to_string(),
            });
        }

        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();

        println!("{}", table);
    }

    Ok(())
}

// SLEDS

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct ServiceInstanceSledRow {
    #[tabled(rename = "SERVICE")]
    kind: String,
    instance_id: Uuid,
    addr: String,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct SledRow {
    serial: String,
    ip: String,
    role: &'static str,
    policy: SledPolicy,
    state: SledState,
    id: Uuid,
}

impl From<Sled> for SledRow {
    fn from(s: Sled) -> Self {
        SledRow {
            id: s.id(),
            serial: s.serial_number().to_string(),
            ip: s.address().to_string(),
            role: if s.is_scrimlet() { "scrimlet" } else { "-" },
            policy: s.policy(),
            state: s.state().into(),
        }
    }
}

/// Run `omdb db sleds`.
async fn cmd_db_sleds(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &SledsArgs,
) -> Result<(), anyhow::Error> {
    let limit = fetch_opts.fetch_limit;
    let filter = match args.filter {
        Some(filter) => filter,
        None => {
            eprintln!(
                "note: listing all commissioned sleds \
                 (use -F to filter, e.g. -F in-service)"
            );
            SledFilter::Commissioned
        }
    };

    let sleds = datastore
        .sled_list(&opctx, &first_page(limit), filter)
        .await
        .context("listing sleds")?;
    check_limit(&sleds, limit, || String::from("listing sleds"));

    let rows = sleds.into_iter().map(|s| SledRow::from(s));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(1, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct CustomerInstanceRow {
    id: String,
    name: String,
    state: String,
    propolis_id: MaybePropolisId,
    sled_id: MaybeSledId,
    host_serial: String,
}

/// Run `omdb db instances`: list data about customer VMs.
async fn cmd_db_instances(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    running: bool,
) -> Result<(), anyhow::Error> {
    use db::schema::instance::dsl;
    use db::schema::vmm::dsl as vmm_dsl;

    let limit = fetch_opts.fetch_limit;
    let mut query = dsl::instance.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let instances: Vec<InstanceAndActiveVmm> = query
        .left_join(
            vmm_dsl::vmm.on(vmm_dsl::id
                .nullable()
                .eq(dsl::active_propolis_id)
                .and(vmm_dsl::time_deleted.is_null())),
        )
        .limit(i64::from(u32::from(limit)))
        .select((Instance::as_select(), Option::<Vmm>::as_select()))
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading instances")?
        .into_iter()
        .map(|i: (Instance, Option<Vmm>)| i.into())
        .collect();

    let ctx = || "listing instances".to_string();
    check_limit(&instances, limit, ctx);

    let mut rows = Vec::new();
    let mut h_to_s: HashMap<Uuid, String> = HashMap::new();

    for i in instances {
        let host_serial = if i.vmm().is_some() {
            if let std::collections::hash_map::Entry::Vacant(e) =
                h_to_s.entry(i.sled_id().unwrap())
            {
                let (_, my_sled) = LookupPath::new(opctx, datastore)
                    .sled_id(i.sled_id().unwrap())
                    .fetch()
                    .await
                    .context("failed to look up sled")?;

                let host_serial = my_sled.serial_number().to_string();
                e.insert(host_serial.to_string());
                host_serial.to_string()
            } else {
                h_to_s.get(&i.sled_id().unwrap()).unwrap().to_string()
            }
        } else {
            "-".to_string()
        };

        if running && i.effective_state() != InstanceState::Running {
            continue;
        }

        let cir = CustomerInstanceRow {
            id: i.instance().id().to_string(),
            name: i.instance().name().to_string(),
            state: i.effective_state().to_string(),
            propolis_id: (&i).into(),
            sled_id: (&i).into(),
            host_serial,
        };

        rows.push(cir);
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

// DNS

/// Run `omdb db dns show`.
async fn cmd_db_dns_show(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ZoneRow {
        group: String,
        zone: String,
        #[tabled(rename = "ver")]
        version: String,
        updated: String,
        reason: String,
    }

    let limit = fetch_opts.fetch_limit;
    let mut rows = Vec::with_capacity(2);
    for group in [DnsGroup::Internal, DnsGroup::External] {
        let ctx = || format!("listing DNS zones for DNS group {:?}", group);
        let group_zones = datastore
            .dns_zones_list(opctx, group, &first_page(limit))
            .await
            .with_context(ctx)?;
        check_limit(&group_zones, limit, ctx);

        let version = datastore
            .dns_group_latest_version(opctx, group)
            .await
            .with_context(|| {
                format!("fetching latest version for DNS group {:?}", group)
            })?;

        rows.extend(group_zones.into_iter().map(|zone| ZoneRow {
            group: group.to_string(),
            zone: zone.zone_name,
            version: version.version.0.to_string(),
            updated:
                version.time_created.to_rfc3339_opts(SecondsFormat::Secs, true),
            reason: version.comment.clone(),
        }));
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
    println!("{}", table);
    Ok(())
}

async fn load_zones_version(
    opctx: &OpContext,
    datastore: &DataStore,
    limit: NonZeroU32,
    args: &DnsVersionArgs,
) -> Result<(Vec<DnsZone>, DnsVersion), anyhow::Error> {
    // The caller gave us a DNS group.  First we need to find the zones.
    let group = args.group.dns_group();
    let ctx = || format!("listing DNS zones for DNS group {:?}", group);
    let group_zones = datastore
        .dns_zones_list(opctx, group, &first_page(limit))
        .await
        .with_context(ctx)?;
    check_limit(&group_zones, limit, ctx);

    // Now load the full version info.
    use nexus_db_queries::db::schema::dns_version::dsl;
    let version = Generation::try_from(i64::from(args.version)).unwrap();
    let versions = dsl::dns_version
        .filter(dsl::dns_group.eq(group))
        .filter(dsl::version.eq(nexus_db_model::Generation::from(version)))
        .limit(1)
        .select(DnsVersion::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading requested version")?;

    let Some(version) = versions.into_iter().next() else {
        bail!("no such DNS version: {}", args.version);
    };

    Ok((group_zones, version))
}

/// Run `omdb db dns diff`.
async fn cmd_db_dns_diff(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &DnsVersionArgs,
) -> Result<(), anyhow::Error> {
    let limit = fetch_opts.fetch_limit;
    let (dns_zones, version) =
        load_zones_version(opctx, datastore, limit, args).await?;

    for zone in dns_zones {
        println!(
            "DNS zone:                   {} ({:?})",
            zone.zone_name, args.group
        );
        println!(
            "requested version:          {} (created at {})",
            *version.version,
            version.time_created.to_rfc3339_opts(SecondsFormat::Secs, true)
        );
        println!("version created by Nexus:   {}", version.creator);
        println!("version created because:    {}", version.comment);

        // Load the added and removed items.
        use nexus_db_queries::db::schema::dns_name::dsl;

        let added = dsl::dns_name
            .filter(dsl::dns_zone_id.eq(zone.id))
            .filter(dsl::version_added.eq(version.version))
            .limit(i64::from(u32::from(limit)))
            .select(DnsName::as_select())
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await
            .context("loading added names")?;
        check_limit(&added, limit, || "loading added names");

        let removed = dsl::dns_name
            .filter(dsl::dns_zone_id.eq(zone.id))
            .filter(dsl::version_removed.eq(version.version))
            .limit(i64::from(u32::from(limit)))
            .select(DnsName::as_select())
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await
            .context("loading added names")?;
        check_limit(&added, limit, || "loading removed names");
        println!(
            "changes:                    names added: {}, names removed: {}",
            added.len(),
            removed.len()
        );
        println!("");

        for a in added {
            print_name("+", &a.name, a.records().context("parsing records"));
        }

        for r in removed {
            print_name("-", &r.name, r.records().context("parsing records"));
        }
    }

    Ok(())
}

/// Run `omdb db dns names`.
async fn cmd_db_dns_names(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &DnsVersionArgs,
) -> Result<(), anyhow::Error> {
    let limit = fetch_opts.fetch_limit;
    let (group_zones, version) =
        load_zones_version(opctx, datastore, limit, args).await?;

    if group_zones.is_empty() {
        println!("no DNS zones found for group {:?}", args.group);
        return Ok(());
    }

    // There will almost never be more than one zone.  But just in case, we'll
    // iterate over whatever we find and print all the names in each one.
    for zone in group_zones {
        println!("{:?} zone: {}", args.group, zone.zone_name);
        println!("  {:50} {}", "NAME", "RECORDS");
        let ctx = || format!("listing names for zone {:?}", zone.zone_name);
        let mut names = datastore
            .dns_names_list(opctx, zone.id, version.version, &first_page(limit))
            .await
            .with_context(ctx)?;
        check_limit(&names, limit, ctx);
        names.sort_by(|(n1, _), (n2, _)| {
            // A natural sort by name puts records starting with numbers first
            // (which will be some of the uuids), then underscores (the SRV
            // names), and then the letters (the rest of the uuids).  This is
            // ugly.  Put the SRV records last (based on the underscore).  (We
            // could look at the record type instead, but that's just as cheesy:
            // names can in principle have multiple different kinds of records,
            // and we'd still want records of the same type to be sorted by
            // name.)
            match (n1.chars().next(), n2.chars().next()) {
                (Some('_'), Some(c)) if c != '_' => Ordering::Greater,
                (Some(c), Some('_')) if c != '_' => Ordering::Less,
                _ => n1.cmp(n2),
            }
        });

        for (name, records) in names {
            print_name("", &name, Ok(records));
        }
    }

    Ok(())
}

async fn cmd_db_eips(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    verbose: bool,
) -> Result<(), anyhow::Error> {
    use db::schema::external_ip::dsl;
    let mut query = dsl::external_ip.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let ips: Vec<ExternalIp> = query
        .select(ExternalIp::as_select())
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .get_results_async(&*datastore.pool_connection_for_tests().await?)
        .await?;

    check_limit(&ips, fetch_opts.fetch_limit, || {
        String::from("listing external ips")
    });

    struct PortRange {
        first: u16,
        last: u16,
    }

    impl Display for PortRange {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}/{}", self.first, self.last)
        }
    }

    enum Owner {
        Instance {
            id: Uuid,
            project: String,
            name: String,
        },
        Service {
            id: Uuid,
            kind: String,
            disposition: Option<BlueprintZoneDisposition>,
        },
        Project {
            id: Uuid,
            name: String,
        },
        None,
    }

    impl Owner {
        fn kind(&self) -> &'static str {
            match self {
                Owner::Instance { .. } => "instance",
                Owner::Service { .. } => "service",
                Owner::Project { .. } => "project",
                Owner::None => "none",
            }
        }

        fn id(&self) -> String {
            match self {
                Owner::Instance { id, .. }
                | Owner::Service { id, .. }
                | Owner::Project { id, .. } => id.to_string(),
                Owner::None => "none".to_string(),
            }
        }

        fn name(&self) -> String {
            match self {
                Self::Instance { project, name, .. } => {
                    format!("{project}/{name}")
                }
                Self::Service { kind, .. } => kind.to_string(),
                Self::Project { name, .. } => name.to_string(),
                Self::None => "none".to_string(),
            }
        }

        fn disposition(&self) -> Option<BlueprintZoneDisposition> {
            match self {
                Self::Service { disposition, .. } => *disposition,
                _ => None,
            }
        }
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct IpRow {
        ip: ipnetwork::IpNetwork,
        ports: PortRange,
        kind: IpKind,
        state: IpAttachState,
        owner_kind: &'static str,
        owner_id: String,
        owner_name: String,
        #[tabled(display_with = "display_option_blank")]
        owner_disposition: Option<BlueprintZoneDisposition>,
    }

    // Display an empty cell for an Option<T> if it's None.
    fn display_option_blank<T: Display>(opt: &Option<T>) -> String {
        opt.as_ref().map(|x| x.to_string()).unwrap_or_else(|| "".to_string())
    }

    if verbose {
        for ip in &ips {
            if verbose {
                println!("{ip:#?}");
            }
        }
        return Ok(());
    }

    let mut rows = Vec::new();

    let (_, current_target_blueprint) = datastore
        .blueprint_target_get_current_full(opctx)
        .await
        .context("loading current target blueprint")?;

    for ip in &ips {
        let owner = if let Some(owner_id) = ip.parent_id {
            if ip.is_service {
                let (kind, disposition) = match lookup_service_info(
                    owner_id,
                    &current_target_blueprint,
                )
                .await?
                {
                    Some(info) => (
                        format!("{:?}", info.service_kind),
                        Some(info.disposition),
                    ),
                    None => {
                        ("UNKNOWN (service ID not found)".to_string(), None)
                    }
                };
                Owner::Service { id: owner_id, kind, disposition }
            } else {
                let instance =
                    match lookup_instance(datastore, owner_id).await? {
                        Some(instance) => instance,
                        None => {
                            eprintln!("instance with id {owner_id} not found");
                            continue;
                        }
                    };

                let project =
                    match lookup_project(datastore, instance.project_id).await?
                    {
                        Some(project) => project,
                        None => {
                            eprintln!(
                                "project with id {} not found",
                                instance.project_id
                            );
                            continue;
                        }
                    };

                Owner::Instance {
                    id: owner_id,
                    project: project.name().to_string(),
                    name: instance.name().to_string(),
                }
            }
        } else if let Some(project_id) = ip.project_id {
            use db::schema::project::dsl as project_dsl;
            let project = match project_dsl::project
                .filter(project_dsl::id.eq(project_id))
                .limit(1)
                .select(Project::as_select())
                .load_async(&*datastore.pool_connection_for_tests().await?)
                .await
                .context("loading requested project")?
                .pop()
            {
                Some(project) => project,
                None => {
                    eprintln!("project with id {} not found", project_id);
                    continue;
                }
            };

            Owner::Project { id: project_id, name: project.name().to_string() }
        } else {
            Owner::None
        };

        let row = IpRow {
            ip: ip.ip,
            ports: PortRange {
                first: ip.first_port.into(),
                last: ip.last_port.into(),
            },
            state: ip.state,
            kind: ip.kind,
            owner_kind: owner.kind(),
            owner_id: owner.id(),
            owner_name: owner.name(),
            owner_disposition: owner.disposition(),
        };
        rows.push(row);
    }

    rows.sort_by(|a, b| a.ip.cmp(&b.ip));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_network_list_vnics(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    verbose: bool,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct NicRow {
        ip: IpNetwork,
        mac: MacAddr,
        slot: u8,
        primary: bool,
        kind: &'static str,
        subnet: String,
        parent_id: Uuid,
        parent_name: String,
    }
    use db::schema::network_interface::dsl;
    let mut query = dsl::network_interface.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let nics: Vec<NetworkInterface> = query
        .select(NetworkInterface::as_select())
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .get_results_async(&*datastore.pool_connection_for_tests().await?)
        .await?;

    check_limit(&nics, fetch_opts.fetch_limit, || {
        String::from("listing network interfaces")
    });

    if verbose {
        for nic in &nics {
            if verbose {
                println!("{nic:#?}");
            }
        }
        return Ok(());
    }

    let mut rows = Vec::new();

    for nic in &nics {
        let (kind, parent_name) = match nic.kind {
            NetworkInterfaceKind::Instance => {
                match lookup_instance(datastore, nic.parent_id).await? {
                    Some(instance) => {
                        match lookup_project(datastore, instance.project_id)
                            .await?
                        {
                            Some(project) => (
                                "instance",
                                format!(
                                    "{}/{}",
                                    project.name(),
                                    instance.name()
                                ),
                            ),
                            None => {
                                eprintln!(
                                    "project with id {} not found",
                                    instance.project_id
                                );
                                continue;
                            }
                        }
                    }
                    None => {
                        ("instance?", "parent instance not found".to_string())
                    }
                }
            }
            NetworkInterfaceKind::Probe => {
                match lookup_probe(datastore, nic.parent_id).await? {
                    Some(probe) => {
                        match lookup_project(datastore, probe.project_id)
                            .await?
                        {
                            Some(project) => (
                                "probe",
                                format!("{}/{}", project.name(), probe.name()),
                            ),
                            None => {
                                eprintln!(
                                    "project with id {} not found",
                                    probe.project_id
                                );
                                continue;
                            }
                        }
                    }
                    None => ("probe?", "parent probe not found".to_string()),
                }
            }
            NetworkInterfaceKind::Service => {
                // We create service NICs named after the service, so we can use
                // the nic name instead of looking up the service.
                ("service", nic.name().to_string())
            }
        };

        let subnet = {
            use db::schema::vpc_subnet::dsl;
            let subnet = match dsl::vpc_subnet
                .filter(dsl::id.eq(nic.subnet_id))
                .limit(1)
                .select(VpcSubnet::as_select())
                .load_async(&*datastore.pool_connection_for_tests().await?)
                .await
                .context("loading requested subnet")?
                .pop()
            {
                Some(subnet) => subnet,
                None => {
                    eprintln!("subnet with id {} not found", nic.subnet_id);
                    continue;
                }
            };

            if nic.ip.is_ipv4() {
                subnet.ipv4_block.to_string()
            } else {
                subnet.ipv6_block.to_string()
            }
        };

        let row = NicRow {
            ip: nic.ip,
            mac: *nic.mac,
            slot: *nic.slot,
            primary: nic.primary,
            kind,
            subnet,
            parent_id: nic.parent_id,
            parent_name,
        };
        rows.push(row);
    }

    rows.sort_by(|a, b| a.ip.cmp(&b.ip));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Validate the `volume_references` column of the region snapshots table
async fn cmd_db_validate_volume_references(
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    // First, get all region snapshot records
    let region_snapshots: Vec<RegionSnapshot> = {
        let region_snapshots: Vec<RegionSnapshot> = datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(|conn| async move {
                // Selecting all region snapshots requires a full table scan
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                use db::schema::region_snapshot::dsl;
                dsl::region_snapshot
                    .select(RegionSnapshot::as_select())
                    .get_results_async(&conn)
                    .await
            })
            .await?;

        region_snapshots
    };

    #[derive(Tabled)]
    struct Row {
        dataset_id: Uuid,
        region_id: Uuid,
        snapshot_id: Uuid,
        error: String,
    }

    let mut rows = Vec::new();

    // Then, for each, make sure that the `volume_references` matches what is in
    // the volume table
    for region_snapshot in region_snapshots {
        let matching_volumes: Vec<Volume> = {
            let snapshot_addr = region_snapshot.snapshot_addr.clone();

            let matching_volumes = datastore
                .pool_connection_for_tests()
                .await?
                .transaction_async(|conn| async move {
                    // Selecting all volumes based on the data column requires a
                    // full table scan
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                    let pattern = format!("%{}%", &snapshot_addr);

                    use db::schema::volume::dsl;

                    // Find all volumes that have not been deleted that contain
                    // this snapshot_addr. If a Volume has been soft deleted,
                    // then the region snapshot record should have had its
                    // volume references column updated accordingly.
                    dsl::volume
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::data.like(pattern))
                        .select(Volume::as_select())
                        .get_results_async(&conn)
                        .await
                })
                .await?;

            matching_volumes
        };

        // The Crucible Agent will reuse ports for regions and running snapshots
        // when they're deleted. Check that the matching volume construction requests
        // reference this snapshot addr as a read-only target.
        let matching_volumes = matching_volumes
            .into_iter()
            .filter(|volume| {
                let vcr: VolumeConstructionRequest =
                    serde_json::from_str(&volume.data()).unwrap();

                let mut targets = CrucibleTargets::default();
                read_only_resources_associated_with_volume(&vcr, &mut targets);

                targets
                    .read_only_targets
                    .contains(&region_snapshot.snapshot_addr)
            })
            .count();

        if matching_volumes != region_snapshot.volume_references as usize {
            rows.push(Row {
                dataset_id: region_snapshot.dataset_id,
                region_id: region_snapshot.region_id,
                snapshot_id: region_snapshot.snapshot_id,
                error: format!(
                    "record has {} volume references when it should be {}!",
                    region_snapshot.volume_references, matching_volumes,
                ),
            });
        } else {
            // The volume references are correct, but additionally check to see
            // deleting is true when matching_volumes is 0. Be careful: in the
            // snapshot create saga, the region snapshot record is created
            // before the snapshot's volume is inserted into the DB. There's a
            // time between these operations that this function would flag that
            // this region snapshot should have `deleting` set to true.

            if matching_volumes == 0 && !region_snapshot.deleting {
                rows.push(Row {
                    dataset_id: region_snapshot.dataset_id,
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                    error: String::from(
                        "record has 0 volume references but deleting is false!",
                    ),
                });
            }
        }
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_validate_region_snapshots(
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let mut regions_to_snapshots_map: BTreeMap<Uuid, HashSet<Uuid>> =
        BTreeMap::default();

    // First, get all region snapshot records (with their corresponding dataset)
    let datasets_and_region_snapshots: Vec<(Dataset, RegionSnapshot)> = {
        let datasets_region_snapshots: Vec<(Dataset, RegionSnapshot)> =
            datastore
                .pool_connection_for_tests()
                .await?
                .transaction_async(|conn| async move {
                    // Selecting all datasets and region snapshots requires a full table scan
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                    use db::schema::dataset::dsl as dataset_dsl;
                    use db::schema::region_snapshot::dsl;

                    dsl::region_snapshot
                        .inner_join(
                            dataset_dsl::dataset
                                .on(dsl::dataset_id.eq(dataset_dsl::id)),
                        )
                        .select((
                            Dataset::as_select(),
                            RegionSnapshot::as_select(),
                        ))
                        .get_results_async(&conn)
                        .await
                })
                .await?;

        datasets_region_snapshots
    };

    #[derive(Tabled)]
    struct Row {
        dataset_id: Uuid,
        region_id: Uuid,
        snapshot_id: Uuid,
        dataset_addr: std::net::SocketAddrV6,
        error: String,
    }

    let mut rows = Vec::new();

    // Then, for each one, reconcile with the corresponding Crucible Agent: do
    // the region_snapshot records match reality?
    for (dataset, region_snapshot) in datasets_and_region_snapshots {
        regions_to_snapshots_map
            .entry(region_snapshot.region_id)
            .or_default()
            .insert(region_snapshot.snapshot_id);

        use crucible_agent_client::types::RegionId;
        use crucible_agent_client::types::State;
        use crucible_agent_client::Client as CrucibleAgentClient;

        let url = format!("http://{}", dataset.address());
        let client = CrucibleAgentClient::new(&url);

        let actual_region_snapshots = client
            .region_get_snapshots(&RegionId(
                region_snapshot.region_id.to_string(),
            ))
            .await?;

        let snapshot_id = region_snapshot.snapshot_id.to_string();

        if actual_region_snapshots
            .snapshots
            .iter()
            .any(|x| x.name == snapshot_id)
        {
            // A snapshot currently exists, matching the database entry
        } else {
            // In this branch, there's a database entry for a snapshot that was
            // deleted. Due to how the snapshot create saga is currently
            // written, a database entry would not have been created unless a
            // snapshot was successfully made: unless that saga changes, we can
            // be reasonably sure that this snapshot existed at some point.

            match actual_region_snapshots.running_snapshots.get(&snapshot_id) {
                Some(running_snapshot) => {
                    match running_snapshot.state {
                        State::Destroyed | State::Failed => {
                            // In this branch, we can be sure a snapshot previously
                            // existed and was deleted: a running snapshot was made
                            // from it, then deleted, and the snapshot does not
                            // currently exist in the list of snapshots for this
                            // region. This record should be deleted.

                            // Before recommending anything, validate the higher
                            // level Snapshot object too: it should have been
                            // destroyed.

                            let snapshot: Snapshot = {
                                use db::schema::snapshot::dsl;

                                dsl::snapshot
                                    .filter(
                                        dsl::id.eq(region_snapshot.snapshot_id),
                                    )
                                    .select(Snapshot::as_select())
                                    .first_async(
                                        &*datastore
                                            .pool_connection_for_tests()
                                            .await?,
                                    )
                                    .await?
                            };

                            if snapshot.time_deleted().is_some() {
                                // This is ok - Nexus currently soft-deletes its
                                // resource records.
                                rows.push(Row {
                                    dataset_id: region_snapshot.dataset_id,
                                    region_id: region_snapshot.region_id,
                                    snapshot_id: region_snapshot.snapshot_id,
                                    dataset_addr: dataset.address(),
                                    error: String::from(
                                        "region snapshot was deleted, please remove its record",
                                    ),
                                });
                            } else {
                                // If the higher level Snapshot was _not_
                                // deleted, this is a Nexus bug: something told
                                // the Agent to delete the snapshot when the
                                // higher level Snapshot was not deleted!

                                rows.push(Row {
                                    dataset_id: region_snapshot.dataset_id,
                                    region_id: region_snapshot.region_id,
                                    snapshot_id: region_snapshot.snapshot_id,
                                    dataset_addr: dataset.address(),
                                    error: String::from(
                                        "NEXUS BUG: region snapshot was deleted, but the higher level snapshot was not!",
                                    ),
                                });
                            }
                        }

                        State::Requested
                        | State::Created
                        | State::Tombstoned => {
                            // The agent is in a bad state: we did not find the
                            // snapshot in the list of snapshots for this
                            // region, but either:
                            //
                            // - there's a requested or existing running
                            //   snapshot for it, or
                            //
                            // - there's a running snapshot that should have
                            //   been completely deleted before the snapshot
                            //   itself was deleted.
                            //
                            // This should have never been allowed to happen by
                            // the Agent, so it's a bug.

                            rows.push(Row {
                                dataset_id: region_snapshot.dataset_id,
                                region_id: region_snapshot.region_id,
                                snapshot_id: region_snapshot.snapshot_id,
                                dataset_addr: dataset.address(),
                                error: format!(
                                    "AGENT BUG: region snapshot was deleted but has a running snapshot in state {:?}!",
                                    running_snapshot.state,
                                ),
                            });
                        }
                    }
                }

                None => {
                    // A running snapshot never existed for this snapshot
                }
            }
        }
    }

    // Second, get all regions
    let datasets_and_regions: Vec<(Dataset, Region)> = {
        let datasets_and_regions: Vec<(Dataset, Region)> = datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(|conn| async move {
                // Selecting all datasets and regions requires a full table scan
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                use db::schema::dataset::dsl as dataset_dsl;
                use db::schema::region::dsl;

                dsl::region
                    .inner_join(
                        dataset_dsl::dataset
                            .on(dsl::dataset_id.eq(dataset_dsl::id)),
                    )
                    .select((Dataset::as_select(), Region::as_select()))
                    .get_results_async(&conn)
                    .await
            })
            .await?;

        datasets_and_regions
    };

    // Reconcile with the Crucible agents: are there snapshots that Nexus does
    // not know about?
    for (dataset, region) in datasets_and_regions {
        use crucible_agent_client::types::RegionId;
        use crucible_agent_client::types::State;
        use crucible_agent_client::Client as CrucibleAgentClient;

        let url = format!("http://{}", dataset.address());
        let client = CrucibleAgentClient::new(&url);

        let actual_region_snapshots = client
            .region_get_snapshots(&RegionId(region.id().to_string()))
            .await?;

        let default = HashSet::default();
        let nexus_region_snapshots: &HashSet<Uuid> =
            regions_to_snapshots_map.get(&region.id()).unwrap_or(&default);

        for actual_region_snapshot in &actual_region_snapshots.snapshots {
            let snapshot_id: Uuid = actual_region_snapshot.name.parse()?;
            if !nexus_region_snapshots.contains(&snapshot_id) {
                rows.push(Row {
                    dataset_id: dataset.id(),
                    region_id: region.id(),
                    snapshot_id,
                    dataset_addr: dataset.address(),
                    error: String::from(
                        "Nexus does not know about this snapshot!",
                    ),
                });
            }
        }

        for (_, actual_region_running_snapshot) in
            &actual_region_snapshots.running_snapshots
        {
            let snapshot_id: Uuid =
                actual_region_running_snapshot.name.parse()?;

            match actual_region_running_snapshot.state {
                State::Destroyed | State::Failed | State::Tombstoned => {
                    // don't check, Nexus would consider this gone
                }

                State::Requested | State::Created => {
                    if !nexus_region_snapshots.contains(&snapshot_id) {
                        rows.push(Row {
                            dataset_id: dataset.id(),
                            region_id: region.id(),
                            snapshot_id,
                            dataset_addr: dataset.address(),
                            error: String::from(
                                "Nexus does not know about this running snapshot!"
                            ),
                        });
                    }
                }
            }
        }
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .to_string();

    println!("{}", table);

    Ok(())
}

fn print_name(
    prefix: &str,
    name: &str,
    maybe_records: Result<Vec<DnsRecord>, anyhow::Error>,
) {
    let records = match maybe_records {
        Ok(records) => records,
        Err(error) => {
            println!(
                "{}  {:50} (failed to parse record data: {:#})",
                prefix, name, error
            );
            return;
        }
    };

    if records.len() == 1 {
        match &records[0] {
            DnsRecord::Srv(_) => (),
            DnsRecord::Aaaa(_) | DnsRecord::A(_) => {
                println!(
                    "{}  {:50} {}",
                    prefix,
                    name,
                    format_record(&records[0])
                );
                return;
            }
        }
    }

    println!("{}  {:50} (records: {})", prefix, name, records.len());
    for r in &records {
        println!("{}      {}", prefix, format_record(r));
    }
}

fn format_record(record: &DnsRecord) -> impl Display {
    match record {
        DnsRecord::A(addr) => format!("A    {}", addr),
        DnsRecord::Aaaa(addr) => format!("AAAA {}", addr),
        DnsRecord::Srv(Srv { port, target, .. }) => {
            format!("SRV  port {:5} {}", port, target)
        }
    }
}

// Inventory

async fn cmd_db_inventory(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    inventory_args: &InventoryArgs,
) -> Result<(), anyhow::Error> {
    let limit = fetch_opts.fetch_limit;
    let conn = datastore.pool_connection_for_tests().await?;
    match inventory_args.command {
        InventoryCommands::BaseboardIds => {
            cmd_db_inventory_baseboard_ids(&conn, limit).await
        }
        InventoryCommands::Cabooses => {
            cmd_db_inventory_cabooses(&conn, limit).await
        }
        InventoryCommands::Collections(CollectionsArgs {
            command: CollectionsCommands::List,
        }) => cmd_db_inventory_collections_list(&conn, limit).await,
        InventoryCommands::Collections(CollectionsArgs {
            command:
                CollectionsCommands::Show(CollectionsShowArgs {
                    id,
                    show_long_strings,
                }),
        }) => {
            let long_string_formatter =
                LongStringFormatter { show_long_strings };
            cmd_db_inventory_collections_show(
                opctx,
                datastore,
                id,
                long_string_formatter,
            )
            .await
        }
        InventoryCommands::RotPages => {
            cmd_db_inventory_rot_pages(&conn, limit).await
        }
    }
}

async fn cmd_db_inventory_baseboard_ids(
    conn: &DataStoreConnection<'_>,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BaseboardRow {
        id: Uuid,
        part_number: String,
        serial_number: String,
    }

    use db::schema::hw_baseboard_id::dsl;
    let baseboard_ids = dsl::hw_baseboard_id
        .order_by((dsl::part_number, dsl::serial_number))
        .limit(i64::from(u32::from(limit)))
        .select(HwBaseboardId::as_select())
        .load_async(&**conn)
        .await
        .context("loading baseboard ids")?;
    check_limit(&baseboard_ids, limit, || "loading baseboard ids");

    let rows = baseboard_ids.into_iter().map(|baseboard_id| BaseboardRow {
        id: baseboard_id.id,
        part_number: baseboard_id.part_number,
        serial_number: baseboard_id.serial_number,
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_inventory_cabooses(
    conn: &DataStoreConnection<'_>,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct CabooseRow {
        id: Uuid,
        board: String,
        git_commit: String,
        name: String,
        version: String,
    }

    use db::schema::sw_caboose::dsl;
    let mut cabooses = dsl::sw_caboose
        .limit(i64::from(u32::from(limit)))
        .select(SwCaboose::as_select())
        .load_async(&**conn)
        .await
        .context("loading cabooses")?;
    check_limit(&cabooses, limit, || "loading cabooses");
    cabooses.sort();

    let rows = cabooses.into_iter().map(|caboose| CabooseRow {
        id: caboose.id,
        board: caboose.board,
        name: caboose.name,
        version: caboose.version,
        git_commit: caboose.git_commit,
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_inventory_rot_pages(
    conn: &DataStoreConnection<'_>,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RotPageRow {
        id: Uuid,
        data_base64: String,
    }

    use db::schema::sw_root_of_trust_page::dsl;
    let mut rot_pages = dsl::sw_root_of_trust_page
        .limit(i64::from(u32::from(limit)))
        .select(SwRotPage::as_select())
        .load_async(&**conn)
        .await
        .context("loading rot_pages")?;
    check_limit(&rot_pages, limit, || "loading rot_pages");
    rot_pages.sort();

    let rows = rot_pages.into_iter().map(|rot_page| RotPageRow {
        id: rot_page.id,
        data_base64: rot_page.data_base64,
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_inventory_collections_list(
    conn: &DataStoreConnection<'_>,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct CollectionRow {
        id: CollectionUuid,
        started: String,
        took: String,
        nsps: i64,
        nerrors: i64,
    }

    let collections = {
        use db::schema::inv_collection::dsl;
        dsl::inv_collection
            .order_by(dsl::time_started)
            .limit(i64::from(u32::from(limit)))
            .select(InvCollection::as_select())
            .load_async(&**conn)
            .await
            .context("loading collections")?
    };
    check_limit(&collections, limit, || "loading collections");

    let mut rows = Vec::new();
    for collection in collections {
        let nerrors = {
            use db::schema::inv_collection_error::dsl;
            dsl::inv_collection_error
                .filter(dsl::inv_collection_id.eq(collection.id))
                .select(diesel::dsl::count_star())
                .first_async(&**conn)
                .await
                .context("counting errors")?
        };

        let nsps = {
            use db::schema::inv_service_processor::dsl;
            dsl::inv_service_processor
                .filter(dsl::inv_collection_id.eq(collection.id))
                .select(diesel::dsl::count_star())
                .first_async(&**conn)
                .await
                .context("counting SPs")?
        };

        let took = format!(
            "{} ms",
            collection
                .time_done
                .signed_duration_since(&collection.time_started)
                .num_milliseconds()
        );
        rows.push(CollectionRow {
            id: collection.id.into(),
            started: humantime::format_rfc3339_seconds(
                collection.time_started.into(),
            )
            .to_string(),
            took,
            nsps,
            nerrors,
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_inventory_collections_show(
    opctx: &OpContext,
    datastore: &DataStore,
    id: CollectionUuid,
    long_string_formatter: LongStringFormatter,
) -> Result<(), anyhow::Error> {
    let collection = datastore
        .inventory_collection_read(opctx, id)
        .await
        .context("reading collection")?;

    inv_collection_print(&collection).await?;
    let nerrors = inv_collection_print_errors(&collection).await?;
    inv_collection_print_devices(&collection, &long_string_formatter).await?;
    inv_collection_print_sleds(&collection);

    if nerrors > 0 {
        eprintln!(
            "warning: {} collection error{} {} reported above",
            nerrors,
            if nerrors == 1 { "" } else { "s" },
            if nerrors == 1 { "was" } else { "were" },
        );
    }

    Ok(())
}

async fn inv_collection_print(
    collection: &Collection,
) -> Result<(), anyhow::Error> {
    println!("collection: {}", collection.id);
    println!(
        "collector:  {}{}",
        collection.collector,
        if collection.collector.parse::<Uuid>().is_ok() {
            " (likely a Nexus instance)"
        } else {
            ""
        }
    );
    println!(
        "started:    {}",
        humantime::format_rfc3339_millis(collection.time_started.into())
    );
    println!(
        "done:       {}",
        humantime::format_rfc3339_millis(collection.time_done.into())
    );

    Ok(())
}

async fn inv_collection_print_errors(
    collection: &Collection,
) -> Result<u32, anyhow::Error> {
    println!("errors:     {}", collection.errors.len());
    for (index, message) in collection.errors.iter().enumerate() {
        println!("  error {}: {}", index, message);
    }

    Ok(collection
        .errors
        .len()
        .try_into()
        .expect("could not convert error count into u32 (yikes)"))
}

async fn inv_collection_print_devices(
    collection: &Collection,
    long_string_formatter: &LongStringFormatter,
) -> Result<(), anyhow::Error> {
    // Assemble a list of baseboard ids, sorted first by device type (sled,
    // switch, power), then by slot number.  This is the order in which we will
    // print everything out.
    let mut sorted_baseboard_ids: Vec<_> =
        collection.sps.keys().cloned().collect();
    sorted_baseboard_ids.sort_by(|s1, s2| {
        let sp1 = collection.sps.get(s1).unwrap();
        let sp2 = collection.sps.get(s2).unwrap();
        sp1.sp_type.cmp(&sp2.sp_type).then(sp1.sp_slot.cmp(&sp2.sp_slot))
    });

    // Now print them.
    for baseboard_id in &sorted_baseboard_ids {
        // This unwrap should not fail because the collection we're iterating
        // over came from the one we're looking into now.
        let sp = collection.sps.get(baseboard_id).unwrap();
        let baseboard = collection.baseboards.get(baseboard_id);
        let rot = collection.rots.get(baseboard_id);

        println!("");
        match baseboard {
            None => {
                // It should be impossible to find an SP whose baseboard
                // information we didn't previously fetch.  That's either a bug
                // in this tool (for failing to fetch or find the right
                // baseboard information) or the inventory system (for failing
                // to insert a record into the hw_baseboard_id table).
                println!(
                    "{:?} (serial number unknown -- this is a bug)",
                    sp.sp_type
                );
                println!("    part number: unknown");
            }
            Some(baseboard) => {
                println!("{:?} {}", sp.sp_type, baseboard.serial_number);
                println!("    part number: {}", baseboard.part_number);
            }
        };

        println!("    power:    {:?}", sp.power_state);
        println!("    revision: {}", sp.baseboard_revision);
        print!("    MGS slot: {:?} {}", sp.sp_type, sp.sp_slot);
        if let SpType::Sled = sp.sp_type {
            print!(" (cubby {})", sp.sp_slot);
        }
        println!("");
        println!("    found at: {} from {}", sp.time_collected, sp.source);

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct CabooseRow<'a> {
            slot: String,
            board: &'a str,
            name: &'a str,
            version: &'a str,
            git_commit: &'a str,
        }

        println!("    cabooses:");
        let caboose_rows: Vec<_> = CabooseWhich::iter()
            .filter_map(|c| {
                collection.caboose_for(c, baseboard_id).map(|d| (c, d))
            })
            .map(|(c, found_caboose)| CabooseRow {
                slot: format!("{:?}", c),
                board: &found_caboose.caboose.board,
                name: &found_caboose.caboose.name,
                version: &found_caboose.caboose.version,
                git_commit: &found_caboose.caboose.git_commit,
            })
            .collect();
        let table = tabled::Table::new(caboose_rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        println!("{}", textwrap::indent(&table.to_string(), "        "));

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct RotPageRow<'a> {
            slot: String,
            data_base64: Cow<'a, str>,
        }

        println!("    RoT pages:");
        let rot_page_rows: Vec<_> = RotPageWhich::iter()
            .filter_map(|which| {
                collection.rot_page_for(which, baseboard_id).map(|d| (which, d))
            })
            .map(|(which, found_page)| RotPageRow {
                slot: format!("{which:?}"),
                data_base64: long_string_formatter
                    .maybe_truncate(&found_page.page.data_base64),
            })
            .collect();
        let table = tabled::Table::new(rot_page_rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        println!("{}", textwrap::indent(&table.to_string(), "        "));

        if let Some(rot) = rot {
            println!("    RoT: active slot: slot {:?}", rot.active_slot);
            println!(
                "    RoT: persistent boot preference: slot {:?}",
                rot.persistent_boot_preference,
            );
            println!(
                "    RoT: pending persistent boot preference: {}",
                rot.pending_persistent_boot_preference
                    .map(|s| format!("slot {:?}", s))
                    .unwrap_or_else(|| String::from("-"))
            );
            println!(
                "    RoT: transient boot preference: {}",
                rot.transient_boot_preference
                    .map(|s| format!("slot {:?}", s))
                    .unwrap_or_else(|| String::from("-"))
            );

            println!(
                "    RoT: slot A SHA3-256: {}",
                rot.slot_a_sha3_256_digest
                    .clone()
                    .unwrap_or_else(|| String::from("-"))
            );

            println!(
                "    RoT: slot B SHA3-256: {}",
                rot.slot_b_sha3_256_digest
                    .clone()
                    .unwrap_or_else(|| String::from("-"))
            );
        } else {
            println!("    RoT: no information found");
        }
    }

    println!("");
    for sp_missing_rot in collection
        .sps
        .keys()
        .collect::<BTreeSet<_>>()
        .difference(&collection.rots.keys().collect::<BTreeSet<_>>())
    {
        // It's not a bug in either omdb or the inventory system to find an SP
        // with no RoT.  It just means that when we collected inventory from the
        // SP, it couldn't communicate with its RoT.
        let sp = collection.sps.get(*sp_missing_rot).unwrap();
        println!(
            "warning: found SP with no RoT: {:?} slot {}",
            sp.sp_type, sp.sp_slot
        );
    }

    for rot_missing_sp in collection
        .rots
        .keys()
        .collect::<BTreeSet<_>>()
        .difference(&collection.sps.keys().collect::<BTreeSet<_>>())
    {
        // It *is* a bug in the inventory system (or omdb) to find an RoT with
        // no SP, since we get the RoT information from the SP in the first
        // place.
        println!(
            "error: found RoT with no SP: \
            hw_baseboard_id {:?} -- this is a bug",
            rot_missing_sp
        );
    }

    Ok(())
}

fn inv_collection_print_sleds(collection: &Collection) {
    println!("SLED AGENTS");
    for sled in collection.sled_agents.values() {
        println!(
            "\nsled {} (role = {:?}, serial {})",
            sled.sled_id,
            sled.sled_role,
            match &sled.baseboard_id {
                Some(baseboard_id) => &baseboard_id.serial_number,
                None => "unknown",
            },
        );
        println!(
            "    found at:    {} from {}",
            sled.time_collected, sled.source
        );
        println!("    address:     {}", sled.sled_agent_address);
        println!("    usable hw threads:   {}", sled.usable_hardware_threads);
        println!(
            "    usable memory (GiB): {}",
            sled.usable_physical_ram.to_whole_gibibytes()
        );
        println!(
            "    reservoir (GiB):     {}",
            sled.reservoir_size.to_whole_gibibytes()
        );

        if let Some(zones) = collection.omicron_zones.get(&sled.sled_id) {
            println!(
                "    zones collected from {} at {}",
                zones.source, zones.time_collected,
            );
            println!(
                "    zones generation: {} (count: {})",
                zones.zones.generation,
                zones.zones.zones.len()
            );

            if zones.zones.zones.is_empty() {
                continue;
            }

            println!("    ZONES FOUND");
            for z in &zones.zones.zones {
                println!("      zone {} (type {})", z.id, z.zone_type.kind());
            }
        } else {
            println!("  warning: no zone information found");
        }
    }
}

#[derive(Debug)]
struct LongStringFormatter {
    show_long_strings: bool,
}

impl LongStringFormatter {
    fn maybe_truncate<'a>(&self, s: &'a str) -> Cow<'a, str> {
        use unicode_width::UnicodeWidthChar;

        // pick an arbitrary width at which we'll truncate, knowing that these
        // strings are probably contained in tables with other columns
        const TRUNCATE_AT_WIDTH: usize = 32;

        // quick check for short strings or if we should show long strings in
        // their entirety
        if self.show_long_strings || s.len() <= TRUNCATE_AT_WIDTH {
            return s.into();
        }

        // longer check; we'll do the proper thing here and check the unicode
        // width, and we don't really care about speed, so we can just iterate
        // over chars
        let mut width = 0;
        for (pos, ch) in s.char_indices() {
            let ch_width = UnicodeWidthChar::width(ch).unwrap_or(0);
            if width + ch_width > TRUNCATE_AT_WIDTH {
                let (prefix, _) = s.split_at(pos);
                return format!("{prefix}...").into();
            }
            width += ch_width;
        }

        // if we didn't break out of the loop, `s` in its entirety is not too
        // wide, so return it as-is
        s.into()
    }
}

// Reconfigurator

/// Packages up database state that's used as input to the Reconfigurator
/// planner into a file so that it can be loaded into `reconfigurator-cli`
async fn cmd_db_reconfigurator_save(
    opctx: &OpContext,
    datastore: &DataStore,
    reconfig_save_args: &ReconfiguratorSaveArgs,
) -> Result<(), anyhow::Error> {
    // See Nexus::blueprint_planning_context().
    eprint!("assembling reconfigurator state ... ");
    let state = nexus_reconfigurator_preparation::reconfigurator_state_load(
        opctx, datastore,
    )
    .await?;
    eprintln!("done");

    let output_path = &reconfig_save_args.output_file;
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&output_path)
        .with_context(|| format!("open {:?}", output_path))?;
    serde_json::to_writer_pretty(&file, &state)
        .with_context(|| format!("write {:?}", output_path))?;
    eprintln!("wrote {}", output_path);
    Ok(())
}
