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
// NOTE: allowing "transaction_async" without retry
#![allow(clippy::disallowed_methods)]

use crate::Omdb;
use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::ereport::cmd_db_ereport;
use crate::helpers::CONNECTION_OPTIONS_HEADING;
use crate::helpers::DATABASE_OPTIONS_HEADING;
use crate::helpers::const_max_len;
use crate::helpers::display_option_blank;
use alert::AlertArgs;
use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use clap::ArgAction;
use clap::ArgGroup;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use clap::builder::PossibleValue;
use clap::builder::PossibleValuesParser;
use clap::builder::TypedValueParser;
use db_metadata::DbMetadataArgs;
use db_metadata::DbMetadataCommands;
use db_metadata::cmd_db_metadata_list_nexus;
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use diesel::OptionalExtension;
use diesel::TextExpressionMethods;
use diesel::expression::SelectableHelper;
use diesel::query_dsl::QueryDsl;
use indicatif::ProgressBar;
use indicatif::ProgressDrawTarget;
use indicatif::ProgressStyle;
use internal_dns_types::names::ServiceName;
use ipnetwork::IpNetwork;
use nexus_config::PostgresConfigWithUrl;
use nexus_config::RegionAllocationStrategy;
use nexus_db_errors::OptionalError;
use nexus_db_lookup::DataStoreConnection;
use nexus_db_lookup::LookupPath;
use nexus_db_model::CrucibleDataset;
use nexus_db_model::Disk;
use nexus_db_model::DnsGroup;
use nexus_db_model::DnsName;
use nexus_db_model::DnsVersion;
use nexus_db_model::DnsZone;
use nexus_db_model::ExternalIp;
use nexus_db_model::HwBaseboardId;
use nexus_db_model::Image;
use nexus_db_model::Instance;
use nexus_db_model::InstanceIntendedState;
use nexus_db_model::InvCollection;
use nexus_db_model::InvNvmeDiskFirmware;
use nexus_db_model::InvPhysicalDisk;
use nexus_db_model::IpAttachState;
use nexus_db_model::IpKind;
use nexus_db_model::Migration;
use nexus_db_model::MigrationState;
use nexus_db_model::NetworkInterface;
use nexus_db_model::NetworkInterfaceKind;
use nexus_db_model::PhysicalDisk;
use nexus_db_model::Probe;
use nexus_db_model::Project;
use nexus_db_model::ReadOnlyTargetReplacement;
use nexus_db_model::Region;
use nexus_db_model::RegionReplacement;
use nexus_db_model::RegionReplacementState;
use nexus_db_model::RegionReplacementStep;
use nexus_db_model::RegionReplacementStepType;
use nexus_db_model::RegionSnapshot;
use nexus_db_model::RegionSnapshotReplacement;
use nexus_db_model::RegionSnapshotReplacementState;
use nexus_db_model::RegionSnapshotReplacementStep;
use nexus_db_model::Sled;
use nexus_db_model::Snapshot;
use nexus_db_model::SnapshotState;
use nexus_db_model::SwCaboose;
use nexus_db_model::SwRotPage;
use nexus_db_model::UpstairsRepairNotification;
use nexus_db_model::UpstairsRepairProgress;
use nexus_db_model::UserDataExportRecord;
use nexus_db_model::UserDataExportResource;
use nexus_db_model::Vmm;
use nexus_db_model::Volume;
use nexus_db_model::VolumeRepair;
use nexus_db_model::VolumeResourceUsage;
use nexus_db_model::VpcSubnet;
use nexus_db_model::Zpool;
use nexus_db_model::to_db_typed_uuid;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore::CrucibleTargets;
use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
use nexus_db_queries::db::datastore::InstanceStateComputer;
use nexus_db_queries::db::datastore::SQL_BATCH_SIZE;
use nexus_db_queries::db::datastore::VolumeCookedResult;
use nexus_db_queries::db::datastore::read_only_resources_associated_with_volume;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::model::ServiceKind;
use nexus_db_queries::db::pagination::Paginator;
use nexus_db_queries::db::pagination::paginated;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use nexus_db_queries::db::queries::region_allocation;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::DiskFilter;
use nexus_types::deployment::SledFilter;
use nexus_types::external_api::params;
use nexus_types::external_api::views::PhysicalDiskPolicy;
use nexus_types::external_api::views::PhysicalDiskState;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params::DnsRecord;
use nexus_types::internal_api::params::Srv;
use nexus_types::inventory::Collection;
use nexus_types::inventory::CollectionDisplayCliFilter;
use omicron_common::api::external;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Generation;
use omicron_common::api::external::InstanceState;
use omicron_common::api::external::MacAddr;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::DownstairsRegionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::ParseError;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::PropolisUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::VolumeUuid;
use omicron_uuid_kinds::ZpoolUuid;
use sled_agent_client::VolumeConstructionRequest;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::future::Future;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;
use tabled::Tabled;
use uuid::Uuid;

mod alert;
mod db_metadata;
mod ereport;
mod saga;
mod user_data_export;

const NO_ACTIVE_PROPOLIS_MSG: &str = "<no active Propolis>";
const NOT_ON_SLED_MSG: &str = "<not on any sled>";

struct MaybePropolisId(Option<PropolisUuid>);
struct MaybeSledId(Option<SledUuid>);

impl From<&InstanceAndActiveVmm> for MaybePropolisId {
    fn from(value: &InstanceAndActiveVmm) -> Self {
        Self(
            value
                .instance()
                .runtime()
                .propolis_id
                .map(PropolisUuid::from_untyped_uuid),
        )
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
                    .dns_lookup_all(log.clone(), ServiceName::Cockroach)
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
        let pool =
            Arc::new(db::Pool::new_single_host(&log.clone(), &db_config));

        // Being a dev tool, we want to try this operation even if the schema
        // doesn't match what we expect.  So we use `DataStore::new_unchecked()`
        // here.  We will then check the schema version explicitly and warn the
        // user if it doesn't match.
        let datastore = Arc::new(DataStore::new_unchecked(log.clone(), pool));
        check_schema_version(&datastore).await;
        Ok(datastore)
    }

    pub async fn with_datastore<F, Fut, T>(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
        f: F,
    ) -> anyhow::Result<T>
    where
        F: FnOnce(OpContext, Arc<DataStore>) -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        let datastore = self.connect(omdb, log).await?;
        let opctx = OpContext::for_tests(log.clone(), datastore.clone());
        let result = f(opctx, datastore.clone()).await;
        datastore.terminate().await;
        result
    }
}

#[derive(Debug, Args, Clone)]
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
#[derive(Debug, Subcommand, Clone)]
enum DbCommands {
    /// Commands for database metadata
    DbMetadata(DbMetadataArgs),
    /// Commands relevant to Crucible datasets
    CrucibleDataset(CrucibleDatasetArgs),
    /// Print any Crucible resources that are located on expunged physical disks
    ReplacementsToDo,
    /// Print information about the rack
    Rack(RackArgs),
    /// Print information about virtual disks
    Disks(DiskArgs),
    /// Print information about internal and external DNS
    Dns(DnsArgs),
    /// Query and display error reports
    Ereport(ereport::EreportArgs),
    /// Print information about collected hardware/software inventory
    Inventory(InventoryArgs),
    /// Print information about physical disks
    PhysicalDisks(PhysicalDisksArgs),
    /// Print information about regions
    Region(RegionArgs),
    /// Query for information about region replacements, optionally manually
    /// triggering one.
    RegionReplacement(RegionReplacementArgs),
    /// Query for information about region snapshot replacements, optionally
    /// manually triggering one.
    RegionSnapshotReplacement(RegionSnapshotReplacementArgs),
    /// Commands for querying and interacting with sagas
    Saga(saga::SagaArgs),
    /// Print information about sleds
    Sleds(SledsArgs),
    /// Print information about customer instances.
    Instance(InstanceArgs),
    /// Alias to `omdb instance list`.
    Instances(InstanceListArgs),
    /// Print information about the network
    Network(NetworkArgs),
    /// Print information about migrations
    #[clap(alias = "migration")]
    Migrations(MigrationsArgs),
    /// Print information about snapshots
    Snapshots(SnapshotArgs),
    /// Validate the contents of the database
    Validate(ValidateArgs),
    /// Print information about volumes
    Volumes(VolumeArgs),
    /// Print information about Propolis virtual machine manager (VMM)
    /// processes.
    Vmm(VmmArgs),
    /// Alias to `omdb db vmm list`.
    Vmms(VmmListArgs),
    /// Print information about the oximeter collector.
    Oximeter(OximeterArgs),
    /// Print information about alerts
    Alert(AlertArgs),
    /// Commands for querying and interacting with pools
    Zpool(ZpoolArgs),
    /// Commands for querying and interacting with user data export objects
    UserDataExport(user_data_export::UserDataExportArgs),
}

#[derive(Debug, Args, Clone)]
struct CrucibleDatasetArgs {
    #[command(subcommand)]
    command: CrucibleDatasetCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum CrucibleDatasetCommands {
    List,

    ShowOverprovisioned,

    MarkNonProvisionable(MarkNonProvisionableArgs),

    MarkProvisionable(MarkProvisionableArgs),
}

#[derive(Debug, Args, Clone)]
struct MarkNonProvisionableArgs {
    /// The UUID of the Crucible dataset
    dataset_id: DatasetUuid,
}

#[derive(Debug, Args, Clone)]
struct MarkProvisionableArgs {
    /// The UUID of the Crucible dataset
    dataset_id: DatasetUuid,
}

#[derive(Debug, Args, Clone)]
struct RackArgs {
    #[command(subcommand)]
    command: RackCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum RackCommands {
    /// Summarize current racks
    List,
}

#[derive(Debug, Args, Clone)]
struct DiskArgs {
    #[command(subcommand)]
    command: DiskCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum DiskCommands {
    /// Get info for a specific disk
    Info(DiskInfoArgs),
    /// Summarize current disks
    List,
    /// Determine what crucible resources are on the given physical disk.
    Physical(DiskPhysicalArgs),
}

#[derive(Debug, Args, Clone)]
struct DiskInfoArgs {
    /// The UUID of the disk
    uuid: Uuid,
}

#[derive(Debug, Args, Clone)]
struct DiskPhysicalArgs {
    /// The UUID of the physical disk
    uuid: PhysicalDiskUuid,
}

#[derive(Debug, Args, Clone)]
struct DnsArgs {
    #[command(subcommand)]
    command: DnsCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum DnsCommands {
    /// Summarize current version of all DNS zones
    Show,
    /// Show what changed in a given DNS version
    Diff(DnsVersionArgs),
    /// Show the full contents of a given DNS zone and version
    Names(DnsVersionArgs),
}

#[derive(Debug, Args, Clone)]
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

#[derive(Debug, Args, Clone)]
struct InstanceArgs {
    #[command(subcommand)]
    command: InstanceCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum InstanceCommands {
    /// list instances
    #[clap(alias = "ls")]
    List(InstanceListArgs),
    /// show detailed output for the selected instance.
    #[clap(alias = "show")]
    Info(InstanceInfoArgs),
}

#[derive(Debug, Args, Clone)]
struct InstanceListArgs {
    /// Only show the running instances
    #[arg(short, long, action=ArgAction::SetTrue)]
    running: bool,

    /// Only show instances in the provided state(s).
    ///
    /// By default, all instances are selected.
    #[arg(
        short,
        long = "state",
        conflicts_with = "running",
        value_parser = PossibleValuesParser::new(
            db::model::InstanceState::ALL_STATES
                .iter()
                .map(|v| PossibleValue::new(v.label()))
        ).try_map(|s| s.parse::<db::model::InstanceState>()),
        action = ArgAction::Append,
    )]
    states: Vec<db::model::InstanceState>,
}

#[derive(Debug, Args, Clone)]
struct InstanceInfoArgs {
    /// the UUID of the instance to show details for
    #[clap(value_name = "UUID")]
    id: InstanceUuid,

    /// include a list of VMMs and migrations previously associated with this
    /// instance.
    ///
    /// note that this is not exhaustive, as some VMM or migration records may
    /// have been hard-deleted.
    ///
    /// this is also enabled by `--all`.
    #[arg(short = 'i', long)]
    history: bool,

    /// include virtual resources provisioned by this instance.
    ///
    /// this is also enabled by `--all`.
    #[arg(short = 'r', long)]
    resources: bool,

    /// include all optional output.
    #[arg(short = 'a', long)]
    all: bool,
}

#[derive(Debug, Args, Clone)]
struct InventoryArgs {
    #[command(subcommand)]
    command: InventoryCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum InventoryCommands {
    /// list all baseboards ever found
    BaseboardIds,
    /// list all cabooses ever found
    Cabooses,
    /// list and show details from particular collections
    Collections(CollectionsArgs),
    /// show all physical disks ever found
    PhysicalDisks(InvPhysicalDisksArgs),
    /// list all root of trust pages ever found
    RotPages,
}

#[derive(Debug, Args, Clone)]
struct CollectionsArgs {
    #[command(subcommand)]
    command: CollectionsCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum CollectionsCommands {
    /// list collections
    List,
    /// show what was found in a particular collection
    Show(CollectionsShowArgs),
}

#[derive(Debug, Args, Clone)]
struct CollectionsShowArgs {
    /// id of the collection (or `latest`)
    id_or_latest: CollectionIdOrLatest,
    /// show long strings in their entirety
    #[clap(long)]
    show_long_strings: bool,

    #[clap(subcommand)]
    filter: Option<CollectionDisplayCliFilter>,
}

#[derive(Debug, Clone, Copy, Args)]
struct CollectionIdArgs {
    /// id of collection (or `latest` for the latest one)
    collection_id: CollectionIdOrLatest,
}

#[derive(Debug, Clone, Copy)]
enum CollectionIdOrLatest {
    Latest,
    CollectionId(CollectionUuid),
}

impl FromStr for CollectionIdOrLatest {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "latest" {
            Ok(Self::Latest)
        } else {
            let id = s.parse()?;
            Ok(Self::CollectionId(id))
        }
    }
}

impl CollectionIdOrLatest {
    async fn to_collection(
        &self,
        opctx: &OpContext,
        datastore: &DataStore,
    ) -> anyhow::Result<Collection> {
        match self {
            CollectionIdOrLatest::Latest => datastore
                .inventory_get_latest_collection(opctx)
                .await
                .context("fetching latest collection")?
                .ok_or_else(|| anyhow!("no inventory collections found")),
            CollectionIdOrLatest::CollectionId(id) => datastore
                .inventory_collection_read(opctx, *id)
                .await
                .with_context(|| format!("fetching collection {id}")),
        }
    }
}

#[derive(Debug, Args, Clone, Copy)]
struct InvPhysicalDisksArgs {
    #[clap(long)]
    collection_id: Option<CollectionUuid>,

    #[clap(long, requires("collection_id"))]
    sled_id: Option<SledUuid>,
}

#[derive(Debug, Args, Clone)]
struct PhysicalDisksArgs {
    /// Show disks that match the given filter
    #[clap(short = 'F', long, value_enum)]
    filter: Option<DiskFilter>,
}

#[derive(Debug, Args, Clone)]
struct SledsArgs {
    /// Show sleds that match the given filter
    #[clap(short = 'F', long, value_enum)]
    filter: Option<SledFilter>,
}

#[derive(Debug, Args, Clone)]
struct RegionArgs {
    #[command(subcommand)]
    command: RegionCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum RegionCommands {
    /// List regions that are still missing ports
    ListRegionsMissingPorts,

    /// List all regions
    List(RegionListArgs),

    /// Find what is using a region
    UsedBy(RegionUsedByArgs),

    /// Find deleted volume regions
    FindDeletedVolumeRegions,

    /// Perform an dry-run allocation and return what was selected
    DryRunRegionAllocation(DryRunRegionAllocationArgs),
}

#[derive(Debug, Args, Clone)]
struct RegionListArgs {
    /// Print region IDs only
    #[arg(long, short)]
    id_only: bool,

    /// List regions only in a certain dataset
    #[arg(long, short)]
    dataset_id: Option<DatasetUuid>,
}

#[derive(Debug, Args, Clone)]
struct RegionUsedByArgs {
    region_id: Vec<Uuid>,
}

#[derive(Debug, Args, Clone)]
struct DryRunRegionAllocationArgs {
    /// Specify to consider associated region snapshots as existing region
    /// allocations (i.e. do not allocate a new read-only region on the same
    /// sled as a related region snapshot)
    #[arg(long)]
    snapshot_id: Option<Uuid>,

    #[arg(long)]
    block_size: u32,

    /// The size of the virtual disk
    #[arg(long)]
    size: i64,

    /// Should the allocated regions be restricted to distinct sleds?
    #[arg(long)]
    distinct_sleds: bool,

    /// How many regions are required?
    #[arg(long, short, default_value_t = 3)]
    num_regions_required: usize,

    /// the (optional) Volume to associate the new regions with (defaults to a
    /// random ID)
    #[arg(long, short)]
    volume_id: Option<VolumeUuid>,
}

#[derive(Debug, Args, Clone)]
struct RegionReplacementArgs {
    #[command(subcommand)]
    command: RegionReplacementCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum RegionReplacementCommands {
    /// List region replacement requests
    List(RegionReplacementListArgs),
    /// Show current region replacements and their status
    Status,
    /// Show detailed information for a region replacement
    Info(RegionReplacementInfoArgs),
    /// Manually request a region replacement
    Request(RegionReplacementRequestArgs),
}

#[derive(Debug, Args, Clone)]
struct RegionReplacementListArgs {
    /// Only show region replacement requests in this state
    #[clap(long)]
    state: Option<RegionReplacementState>,

    /// Only show region replacement requests after a certain date
    #[clap(long)]
    after: Option<DateTime<Utc>>,
}

#[derive(Debug, Args, Clone)]
struct RegionReplacementInfoArgs {
    /// The UUID of the region replacement request
    replacement_id: Uuid,
}

#[derive(Debug, Args, Clone)]
struct RegionReplacementRequestArgs {
    /// The UUID of the region to replace
    region_id: Uuid,
}

#[derive(Debug, Args, Clone)]
struct NetworkArgs {
    #[command(subcommand)]
    command: NetworkCommands,

    /// Print out raw data structures from the data store.
    #[clap(long, global = true)]
    verbose: bool,
}

#[derive(Debug, Subcommand, Clone)]
enum NetworkCommands {
    /// List external IPs
    ListEips,
    /// List virtual network interfaces
    ListVnics,
}

#[derive(Debug, Args, Clone)]
struct MigrationsArgs {
    #[command(subcommand)]
    command: MigrationsCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum MigrationsCommands {
    /// List migrations
    #[clap(alias = "ls")]
    List(MigrationsListArgs),
    // N.B. I'm making this a subcommand not because there are currently any
    // other subcommands, but to reserve the optionality to add things other
    // than `list`...
}

#[derive(Debug, Args, Clone)]
struct MigrationsListArgs {
    /// Include only migrations where at least one side reports the migration
    /// is in progress.
    ///
    /// By default, migrations in all states are included. This can be combined
    /// with the `--pending`, `--completed`, and `--failed` arguments to include
    /// migrations in  multiple states.
    #[arg(short = 'r', long, action = ArgAction::SetTrue)]
    in_progress: bool,

    /// Include only migrations where at least one side is still pending (the
    /// sled-agent hasn't reported in yet).
    ///
    /// By default, migrations in all states are included. This can be combined
    /// with the `--in-progress`, `--completed` and `--failed` arguments to
    /// include migrations in multiple states.
    #[arg(short = 'p', long, action = ArgAction::SetTrue)]
    pending: bool,

    /// Include only migrations where at least one side reports the migration
    /// has completed.
    ///
    /// By default, migrations in all states are included. This can be combined
    /// with the `--in-progress`, `--pending`, and `--failed` arguments to
    /// include migrations in multiple states.
    #[arg(short = 'c', long, action = ArgAction::SetTrue)]
    completed: bool,

    /// Include only migrations where at least one side reports the migration
    /// has failed.
    ///
    /// By default, migrations in all states are included. This can be combined
    /// with the `--pending`, `--in-progress`, and --completed` arguments to
    /// include migrations  in multiple states.
    #[arg(short = 'f', long, action = ArgAction::SetTrue)]
    failed: bool,

    /// Show only migrations for this instance ID.
    ///
    /// By default, all instances are shown. This argument be repeated to select
    /// other instances.
    #[arg(short = 'i', long = "instance-id")]
    instance_ids: Vec<Uuid>,

    /// Output all data from the migration record.
    ///
    /// This includes update and deletion timestamps, the source and target
    /// generation numbers.
    #[arg(short, long, action = ArgAction::SetTrue)]
    verbose: bool,
}

#[derive(Debug, Args, Clone)]
struct SnapshotArgs {
    #[command(subcommand)]
    command: SnapshotCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum SnapshotCommands {
    /// Get info for a specific snapshot
    Info(SnapshotInfoArgs),
    /// Summarize current snapshots
    List,
}

#[derive(Debug, Args, Clone)]
struct SnapshotInfoArgs {
    /// The UUID of the snapshot
    uuid: Uuid,
}

#[derive(Debug, Args, Clone)]
struct RegionSnapshotReplacementArgs {
    #[command(subcommand)]
    command: RegionSnapshotReplacementCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum RegionSnapshotReplacementCommands {
    /// List region snapshot replacement requests
    List(RegionSnapshotReplacementListArgs),
    /// Show current region snapshot replacements and their status
    Status,
    /// Show detailed information for a region snapshot replacement
    Info(RegionSnapshotReplacementInfoArgs),
    /// Manually request a region snapshot replacement
    Request(RegionSnapshotReplacementRequestArgs),
    /// Try to determine what replacements are waiting on
    Waiting,
}

#[derive(Debug, Args, Clone)]
struct RegionSnapshotReplacementListArgs {
    /// Only show region snapshot replacement requests in this state
    #[clap(long)]
    state: Option<RegionSnapshotReplacementState>,

    /// Only show region snapshot replacement requests after a certain date
    #[clap(long)]
    after: Option<DateTime<Utc>>,
}

#[derive(Debug, Args, Clone)]
struct RegionSnapshotReplacementInfoArgs {
    /// The UUID of the region snapshot replacement request
    replacement_id: Uuid,
}

#[derive(Debug, Args, Clone)]
struct RegionSnapshotReplacementRequestArgs {
    /// The dataset id for a given region snapshot
    dataset_id: DatasetUuid,

    /// The region id for a given region snapshot
    region_id: Uuid,

    /// The snapshot id for a given region snapshot
    snapshot_id: Uuid,
}

#[derive(Debug, Args, Clone)]
struct ValidateArgs {
    #[command(subcommand)]
    command: ValidateCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum ValidateCommands {
    /// Validate each `volume_references` column in the region snapshots table
    ValidateVolumeReferences,

    /// Find either regions Nexus knows about that the corresponding Crucible
    /// agent says were deleted, or regions that Nexus doesn't know about.
    ValidateRegions(ValidateRegionsArgs),

    /// Find either region snapshots Nexus knows about that the corresponding
    /// Crucible agent says were deleted, or region snapshots that Nexus doesn't
    /// know about.
    ValidateRegionSnapshots,
}

#[derive(Debug, Args, Clone)]
struct ValidateRegionsArgs {
    /// Delete Regions Nexus is unaware of
    #[clap(long, default_value_t = false)]
    clean_up_orphaned_regions: bool,
}

#[derive(Debug, Args, Clone)]
struct VolumeArgs {
    #[command(subcommand)]
    command: VolumeCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum VolumeCommands {
    /// Get info for a specific volume
    Info(VolumeInfoArgs),
    /// Summarize current volumes
    List,
    /// What is holding the lock?
    LockHolder(VolumeLockHolderArgs),
    /// What volumes cannot activate?
    CannotActivate,
    /// What volumes reference a thing?
    Reference(VolumeReferenceArgs),
}

#[derive(Debug, Args, Clone)]
struct VolumeInfoArgs {
    /// The UUID of the volume
    uuid: Uuid,
}

#[derive(Debug, Args, Clone)]
struct VolumeLockHolderArgs {
    /// The UUID of the volume
    uuid: Uuid,
}

#[derive(Debug, Args, Clone)]
#[clap(group(
ArgGroup::new("volume-reference-group")
    .required(true)
    .args(&["ip", "net", "read_only_region", "region_snapshot"])
))]
struct VolumeReferenceArgs {
    #[clap(long, conflicts_with_all = ["net", "read_only_region", "region_snapshot"])]
    ip: Option<std::net::Ipv6Addr>,

    #[clap(long, conflicts_with_all = ["ip", "read_only_region", "region_snapshot"])]
    net: Option<oxnet::Ipv6Net>,

    #[clap(long, conflicts_with_all = ["ip", "net", "region_snapshot"])]
    read_only_region: Option<Uuid>,

    #[clap(
        long,
        conflicts_with_all = ["ip", "net", "read_only_region"],
        num_args = 3,
        value_names = ["DATASET ID", "REGION ID", "SNAPSHOT ID"],
    )]
    region_snapshot: Option<Vec<Uuid>>,
}

#[derive(Debug, Args, Clone)]
struct VmmArgs {
    #[command(subcommand)]
    command: VmmCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum VmmCommands {
    /// Get info for a specific VMM process
    #[clap(alias = "show")]
    Info(VmmInfoArgs),
    /// List VMM processes
    #[clap(alias = "ls")]
    List(VmmListArgs),
}

#[derive(Debug, Args, Clone)]
struct VmmInfoArgs {
    /// The UUID of the VMM process.
    uuid: Uuid,
}

#[derive(Debug, Args, Clone)]
struct VmmListArgs {
    /// Enable verbose output.
    ///
    /// You may need a really wide monitor for this!
    #[arg(long, short)]
    verbose: bool,

    /// Only show VMMs in the provided state(s).
    ///
    /// By default, all VMM states are selected.
    #[arg(
        short,
        long = "state",
        value_parser = PossibleValuesParser::new(
            db::model::VmmState::ALL_STATES
                .iter()
                .map(|v| PossibleValue::new(v.label()))
        )
        .try_map(|s| s.parse::<db::model::VmmState>()),
        action = ArgAction::Append,
    )]
    states: Vec<db::model::VmmState>,
}

#[derive(Debug, Args, Clone)]
struct ZpoolArgs {
    #[command(subcommand)]
    command: ZpoolCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum ZpoolCommands {
    /// List pools
    List(ZpoolListArgs),

    /// Set the control plane storage buffer for a pool
    SetStorageBuffer(SetStorageBufferArgs),
}

#[derive(Debug, Args, Clone)]
struct ZpoolListArgs {
    /// Only output zpool ids
    #[clap(short, long)]
    id_only: bool,
}

#[derive(Debug, Args, Clone)]
struct SetStorageBufferArgs {
    /// The UUID of Pool
    id: Uuid,

    /// How many bytes to set the buffer to
    storage_buffer: i64,
}

impl DbArgs {
    /// Run a `omdb db` subcommand.
    ///
    /// Mostly delegates to the async block in this function, taking care to
    /// properly terminate the database connection.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let fetch_opts = &self.fetch_opts;
        self.db_url_opts.with_datastore(omdb, log, |opctx, datastore| {
            async move {
                match &self.command {
                    DbCommands::DbMetadata(DbMetadataArgs {
                        command: DbMetadataCommands::ListNexus,
                    }) => {
                        cmd_db_metadata_list_nexus(&opctx, &datastore).await
                    }
                    DbCommands::CrucibleDataset(CrucibleDatasetArgs {
                        command: CrucibleDatasetCommands::List,
                    }) => {
                        cmd_crucible_dataset_list(&opctx, &datastore).await
                    }
                    DbCommands::CrucibleDataset(CrucibleDatasetArgs {
                        command: CrucibleDatasetCommands::ShowOverprovisioned,
                    }) => {
                        cmd_crucible_dataset_show_overprovisioned(
                            &opctx, &datastore,
                        ).await
                    }
                    DbCommands::CrucibleDataset(CrucibleDatasetArgs {
                        command: CrucibleDatasetCommands::MarkNonProvisionable(args),
                    }) => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_crucible_dataset_mark_non_provisionable(
                            &opctx, &datastore, args, token,
                        ).await
                    }
                    DbCommands::CrucibleDataset(CrucibleDatasetArgs {
                        command: CrucibleDatasetCommands::MarkProvisionable(args),
                    }) => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_crucible_dataset_mark_provisionable(
                            &opctx, &datastore, args, token,
                        ).await
                    }
                    DbCommands::ReplacementsToDo => {
                        replacements_to_do(&opctx, &datastore).await
                    }
                    DbCommands::Rack(RackArgs { command: RackCommands::List }) => {
                        cmd_db_rack_list(&opctx, &datastore, &fetch_opts).await
                    }
                    DbCommands::Disks(DiskArgs {
                        command: DiskCommands::Info(uuid),
                    }) => cmd_db_disk_info(&opctx, &datastore, uuid).await,
                    DbCommands::Disks(DiskArgs { command: DiskCommands::List }) => {
                        cmd_db_disk_list(&datastore, &fetch_opts).await
                    }
                    DbCommands::Disks(DiskArgs {
                        command: DiskCommands::Physical(uuid),
                    }) => {
                        cmd_db_disk_physical(&opctx, &datastore, &fetch_opts, uuid)
                            .await
                    }
                    DbCommands::Dns(DnsArgs { command: DnsCommands::Show }) => {
                        cmd_db_dns_show(&opctx, &datastore, &fetch_opts).await
                    }
                    DbCommands::Dns(DnsArgs { command: DnsCommands::Diff(args) }) => {
                        cmd_db_dns_diff(&opctx, &datastore, &fetch_opts, args)
                            .await
                    }
                    DbCommands::Dns(DnsArgs { command: DnsCommands::Names(args) }) => {
                        cmd_db_dns_names(&opctx, &datastore, &fetch_opts, args)
                            .await
                    }
                    DbCommands::Inventory(inventory_args) => {
                        cmd_db_inventory(
                            &opctx,
                            &datastore,
                            &fetch_opts,
                            inventory_args,
                        )
                        .await
                    }
                    DbCommands::PhysicalDisks(args) => {
                        cmd_db_physical_disks(
                            &opctx,
                            &datastore,
                            &fetch_opts,
                            args,
                        )
                        .await
                    }
                    DbCommands::Region(RegionArgs {
                        command: RegionCommands::ListRegionsMissingPorts,
                    }) => cmd_db_region_missing_ports(&opctx, &datastore).await,
                    DbCommands::Region(RegionArgs {
                        command: RegionCommands::List(region_list_args),
                    }) => {
                        cmd_db_region_list(
                            &datastore,
                            &fetch_opts,
                            region_list_args,
                        )
                        .await
                    }
                    DbCommands::Region(RegionArgs {
                        command: RegionCommands::UsedBy(region_used_by_args),
                    }) => {
                        cmd_db_region_used_by(
                            &datastore,
                            &fetch_opts,
                            region_used_by_args,
                        )
                        .await
                    }
                    DbCommands::Region(RegionArgs {
                        command: RegionCommands::FindDeletedVolumeRegions,
                    }) => cmd_db_region_find_deleted(&datastore).await,
                    DbCommands::Region(RegionArgs {
                        command: RegionCommands::DryRunRegionAllocation(args),
                    }) => cmd_db_dry_run_region_allocation(&opctx, &datastore, args).await,
                    DbCommands::RegionReplacement(RegionReplacementArgs {
                        command: RegionReplacementCommands::List(args),
                    }) => {
                        cmd_db_region_replacement_list(
                            &datastore,
                            &fetch_opts,
                            args,
                        )
                        .await
                    }
                    DbCommands::RegionReplacement(RegionReplacementArgs {
                        command: RegionReplacementCommands::Status,
                    }) => {
                        cmd_db_region_replacement_status(
                            &opctx,
                            &datastore,
                            &fetch_opts,
                        )
                        .await
                    }
                    DbCommands::RegionReplacement(RegionReplacementArgs {
                        command: RegionReplacementCommands::Info(args),
                    }) => {
                        cmd_db_region_replacement_info(&opctx, &datastore, args).await
                    }
                    DbCommands::RegionReplacement(RegionReplacementArgs {
                        command: RegionReplacementCommands::Request(args),
                    }) => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_db_region_replacement_request(
                            &opctx, &datastore, args, token,
                        )
                        .await
                    }
                    DbCommands::Saga(args) => {
                        args.exec(&omdb, &opctx, &datastore).await
                    }
                    DbCommands::Sleds(args) => {
                        cmd_db_sleds(&opctx, &datastore, &fetch_opts, args).await
                    }
                    DbCommands::Instance(InstanceArgs {
                        command: InstanceCommands::List(args),
                    }) => {
                        cmd_db_instances(&opctx, &datastore, &fetch_opts, args)
                            .await
                    }
                    DbCommands::Instance(InstanceArgs {
                        command: InstanceCommands::Info(args),
                    }) => {
                        cmd_db_instance_info(&opctx, &datastore, &fetch_opts, args)
                            .await
                    }
                    DbCommands::Instances(instances_options) => {
                        cmd_db_instances(
                            &opctx,
                            &datastore,
                            &fetch_opts,
                            instances_options,
                        )
                        .await
                    }
                    DbCommands::Network(NetworkArgs {
                        command: NetworkCommands::ListEips,
                        verbose,
                    }) => {
                        cmd_db_eips(&opctx, &datastore, &fetch_opts, *verbose)
                            .await
                    }
                    DbCommands::Network(NetworkArgs {
                        command: NetworkCommands::ListVnics,
                        verbose,
                    }) => {
                        cmd_db_network_list_vnics(
                            &datastore,
                            &fetch_opts,
                            *verbose,
                        )
                        .await
                    }
                    DbCommands::Migrations(MigrationsArgs {
                        command: MigrationsCommands::List(args),
                    }) => {
                        cmd_db_migrations_list(&datastore, &fetch_opts, args).await
                    }
                    DbCommands::Snapshots(SnapshotArgs {
                        command: SnapshotCommands::Info(uuid),
                    }) => cmd_db_snapshot_info(&opctx, &datastore, uuid).await,
                    DbCommands::Snapshots(SnapshotArgs {
                        command: SnapshotCommands::List,
                    }) => cmd_db_snapshot_list(&datastore, &fetch_opts).await,
                    DbCommands::RegionSnapshotReplacement(
                        RegionSnapshotReplacementArgs {
                            command: RegionSnapshotReplacementCommands::List(args),
                        },
                    ) => {
                        cmd_db_region_snapshot_replacement_list(
                            &datastore,
                            &fetch_opts,
                            args,
                        )
                        .await
                    }
                    DbCommands::RegionSnapshotReplacement(
                        RegionSnapshotReplacementArgs {
                            command: RegionSnapshotReplacementCommands::Status,
                        },
                    ) => {
                        cmd_db_region_snapshot_replacement_status(
                            &opctx,
                            &datastore,
                            &fetch_opts,
                        )
                        .await
                    }
                    DbCommands::RegionSnapshotReplacement(
                        RegionSnapshotReplacementArgs {
                            command: RegionSnapshotReplacementCommands::Info(args),
                        },
                    ) => {
                        cmd_db_region_snapshot_replacement_info(
                            &opctx, &datastore, args,
                        )
                        .await
                    }
                    DbCommands::RegionSnapshotReplacement(
                        RegionSnapshotReplacementArgs {
                            command: RegionSnapshotReplacementCommands::Request(args),
                        },
                    ) => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_db_region_snapshot_replacement_request(
                            &opctx, &datastore, args, token,
                        )
                        .await
                    }
                    DbCommands::RegionSnapshotReplacement(
                        RegionSnapshotReplacementArgs {
                            command: RegionSnapshotReplacementCommands::Waiting,
                        },
                    ) => {
                        cmd_db_region_snapshot_replacement_waiting(
                            &opctx, &datastore
                        )
                        .await
                    }
                    DbCommands::Validate(ValidateArgs {
                        command: ValidateCommands::ValidateVolumeReferences,
                    }) => cmd_db_validate_volume_references(&datastore).await,
                    DbCommands::Validate(ValidateArgs {
                        command: ValidateCommands::ValidateRegions(args),
                    }) => {
                        let clean_up_orphaned_regions =
                            if args.clean_up_orphaned_regions {
                                let token = omdb.check_allow_destructive()?;
                                CleanUpOrphanedRegions::Yes { _token: token }
                            } else {
                                CleanUpOrphanedRegions::No
                            };

                        cmd_db_validate_regions(&datastore, clean_up_orphaned_regions)
                            .await
                    }
                    DbCommands::Validate(ValidateArgs {
                        command: ValidateCommands::ValidateRegionSnapshots,
                    }) => cmd_db_validate_region_snapshots(&datastore).await,
                    DbCommands::Volumes(VolumeArgs {
                        command: VolumeCommands::Info(args),
                    }) => cmd_db_volume_info(&datastore, args).await,
                    DbCommands::Volumes(VolumeArgs {
                        command: VolumeCommands::List,
                    }) => cmd_db_volume_list(&datastore, &fetch_opts).await,
                    DbCommands::Volumes(VolumeArgs {
                        command: VolumeCommands::LockHolder(args),
                    }) => cmd_db_volume_lock_holder(&datastore, args).await,
                    DbCommands::Volumes(VolumeArgs {
                        command: VolumeCommands::CannotActivate,
                    }) => cmd_db_volume_cannot_activate(&opctx, &datastore).await,
                    DbCommands::Volumes(VolumeArgs {
                        command: VolumeCommands::Reference(args),
                    }) => cmd_db_volume_reference(&opctx, &datastore, &fetch_opts, &args).await,

                    DbCommands::Vmm(VmmArgs { command: VmmCommands::Info(args) }) => {
                        cmd_db_vmm_info(&opctx, &datastore, &fetch_opts, &args)
                            .await
                    }
                    DbCommands::Vmm(VmmArgs { command: VmmCommands::List(args) })
                    | DbCommands::Vmms(args) => {
                        cmd_db_vmm_list(&datastore, &fetch_opts, args).await
                    }
                    DbCommands::Oximeter(OximeterArgs {
                        command: OximeterCommands::ListProducers
                    }) => cmd_db_oximeter_list_producers(&datastore, fetch_opts).await,

                    DbCommands::Alert(args) => alert::cmd_db_alert(&opctx, &datastore, &fetch_opts, &args).await,
                    DbCommands::Zpool(ZpoolArgs {
                        command: ZpoolCommands::List(args)
                    }) => cmd_db_zpool_list(&opctx, &datastore, &args).await,
                    DbCommands::Zpool(ZpoolArgs {
                        command: ZpoolCommands::SetStorageBuffer(args)
                    }) => {
                        let token = omdb.check_allow_destructive()?;
                        cmd_db_zpool_set_storage_buffer(
                            &opctx,
                            &datastore,
                            &args,
                            token,
                        ).await
                    },
                    DbCommands::Ereport(args) => {
                        cmd_db_ereport(&datastore, &fetch_opts, &args).await
                    }
                    DbCommands::UserDataExport(args) => {
                        args.exec(&omdb, &opctx, &datastore).await
                    }
                }
            }
        }).await
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
    use nexus_db_schema::schema::instance::dsl;

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
        .all_omicron_zones(BlueprintZoneDisposition::any)
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
        BlueprintZoneType::ClickhouseServer(_) => ServiceKind::ClickhouseServer,
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
    use nexus_db_schema::schema::probe::dsl;

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
    use nexus_db_schema::schema::project::dsl;

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

// Crucible datasets

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct CrucibleDatasetRow {
    // dataset fields
    id: DatasetUuid,
    time_deleted: String,
    pool_id: ZpoolUuid,
    address: String,
    size_used: i64,
    no_provision: bool,

    // zpool fields
    #[tabled(display_with = "option_impl_display")]
    control_plane_storage_buffer: Option<i64>,
    #[tabled(display_with = "option_impl_display")]
    pool_total_size: Option<i64>,

    // computed fields
    #[tabled(display_with = "option_impl_display")]
    size_left: Option<i128>,
}

pub fn option_impl_display<T: std::fmt::Display>(t: &Option<T>) -> String {
    match t {
        Some(v) => format!("{v}"),
        None => String::from("n/a"),
    }
}

async fn get_crucible_dataset_rows(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<Vec<CrucibleDatasetRow>, anyhow::Error> {
    let crucible_datasets =
        datastore.crucible_dataset_list_all_batched(opctx).await?;

    let Some(latest_collection) =
        datastore.inventory_get_latest_collection(opctx).await?
    else {
        bail!("no latest inventory found!");
    };

    let mut zpool_total_size: HashMap<ZpoolUuid, i64> = HashMap::new();

    for sled_agent in latest_collection.sled_agents {
        for zpool in sled_agent.zpools {
            zpool_total_size.insert(zpool.id, zpool.total_size.into());
        }
    }

    let zpools: HashMap<ZpoolUuid, Zpool> = datastore
        .zpool_list_all_external_batched(opctx)
        .await?
        .into_iter()
        .map(|(zpool, _)| (zpool.id(), zpool))
        .collect();

    let mut result: Vec<CrucibleDatasetRow> =
        Vec::with_capacity(crucible_datasets.len());

    for d in crucible_datasets {
        let control_plane_storage_buffer: Option<i64> = match zpools
            .get(&d.pool_id())
        {
            Some(zpool) => Some(zpool.control_plane_storage_buffer().into()),
            None => None,
        };

        let pool_total_size = zpool_total_size.get(&d.pool_id());

        result.push(CrucibleDatasetRow {
            // dataset fields
            id: d.id(),
            time_deleted: match d.time_deleted() {
                Some(t) => t.to_string(),
                None => String::from(""),
            },
            pool_id: d.pool_id(),
            address: d.address().to_string(),
            size_used: d.size_used,
            no_provision: d.no_provision(),

            // zpool fields
            control_plane_storage_buffer,
            pool_total_size: pool_total_size.cloned(),

            // computed fields
            size_left: match (pool_total_size, control_plane_storage_buffer) {
                (Some(total_size), Some(control_plane_storage_buffer)) => Some(
                    i128::from(*total_size)
                        - i128::from(control_plane_storage_buffer)
                        - i128::from(d.size_used),
                ),

                _ => None,
            },
        });
    }

    Ok(result)
}

async fn cmd_crucible_dataset_list(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let rows: Vec<_> = get_crucible_dataset_rows(opctx, datastore).await?;

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::psql())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_crucible_dataset_show_overprovisioned(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    // A Crucible dataset is overprovisioned if size_used (amount taken up by
    // Crucible region reservations) plus the control plane storage buffer
    // (note this is _not_ a ZFS reservation! it's currently just a per-pool
    // value in the database) is larger than the backing pool's total size.

    let rows: Vec<_> = get_crucible_dataset_rows(opctx, datastore).await?;
    let rows: Vec<_> = rows
        .into_iter()
        .filter(|row| {
            match (row.pool_total_size, row.control_plane_storage_buffer) {
                (Some(pool_total_size), Some(control_plane_storage_buffer)) => {
                    (i128::from(row.size_used)
                        + i128::from(control_plane_storage_buffer))
                        >= i128::from(pool_total_size)
                }

                _ => {
                    // Without the total size or control plane storage buffer, we
                    // can't determine if the dataset is overprovisioned or not.
                    // Filter it out.
                    false
                }
            }
        })
        .collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::psql())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_crucible_dataset_mark_non_provisionable(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &MarkNonProvisionableArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    datastore
        .mark_crucible_dataset_not_provisionable(opctx, args.dataset_id)
        .await?;

    println!("marked {:?} as non-provisionable", args.dataset_id);

    Ok(())
}

async fn cmd_crucible_dataset_mark_provisionable(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &MarkProvisionableArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    datastore
        .mark_crucible_dataset_provisionable(opctx, args.dataset_id)
        .await?;

    println!("marked {:?} as provisionable", args.dataset_id);

    Ok(())
}

// Disks

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct DiskIdentity {
    id: Uuid,
    size: String,
    state: String,
    name: String,
}

impl From<&'_ db::model::Disk> for DiskIdentity {
    fn from(disk: &db::model::Disk) -> Self {
        Self {
            name: disk.name().to_string(),
            id: disk.id(),
            size: disk.size.to_string(),
            state: disk.runtime().disk_state,
        }
    }
}

/// Run `omdb db disk list`.
async fn cmd_db_disk_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    let ctx = || "listing disks".to_string();

    use nexus_db_schema::schema::disk::dsl;
    let mut query = dsl::disk.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DiskRow {
        #[tabled(inline)]
        identity: DiskIdentity,
        attached_to: String,
    }

    impl From<&'_ db::model::Disk> for DiskRow {
        fn from(disk: &db::model::Disk) -> Self {
            Self {
                identity: disk.into(),
                attached_to: match disk.runtime().attach_instance_id {
                    Some(uuid) => uuid.to_string(),
                    None => "-".to_string(),
                },
            }
        }
    }

    let disks = query
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(Disk::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading disks")?;

    check_limit(&disks, fetch_opts.fetch_limit, ctx);

    let rows = disks.iter().map(DiskRow::from);
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn replacements_to_do(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RegionRow {
        id: String,
        dataset_id: String,
        resource: String,
        #[tabled(display_with = "option_datetime_rfc3339_concise")]
        existing_request_time: Option<DateTime<Utc>>,
        existing_request: String,
    }

    let regions: Vec<Region> = vec![
        datastore
            .find_read_only_regions_on_expunged_physical_disks(opctx)
            .await?,
        datastore
            .find_read_write_regions_on_expunged_physical_disks(opctx)
            .await?,
    ]
    .into_iter()
    .flatten()
    .collect();

    let mut table_rows: Vec<RegionRow> = vec![];
    for region in regions {
        let maybe_request = datastore
            .lookup_region_replacement_request_by_old_region_id(
                opctx,
                DownstairsRegionUuid::from_untyped_uuid(region.id()),
            )
            .await?;

        table_rows.push(RegionRow {
            id: region.id().to_string(),
            dataset_id: region.dataset_id().to_string(),
            resource: if region.read_only() {
                String::from("read-only region")
            } else {
                String::from("read/write region")
            },
            existing_request_time: maybe_request
                .as_ref()
                .map(|x| x.request_time),
            existing_request: {
                if let Some(request) = &maybe_request {
                    format!(
                        "{} (state {:?})",
                        request.id, request.replacement_state
                    )
                } else {
                    String::from("")
                }
            },
        });
    }

    table_rows.sort_by_key(|x| x.existing_request_time);

    let table = tabled::Table::new(table_rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    println!("");

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RegionSnapshotRow {
        dataset_id: String,
        region_id: String,
        snapshot_id: String,
        #[tabled(display_with = "option_datetime_rfc3339_concise")]
        existing_request_time: Option<DateTime<Utc>>,
        existing_request: String,
    }

    let rs_rows: Vec<RegionSnapshot> = datastore
        .find_region_snapshots_on_expunged_physical_disks(opctx)
        .await?;

    let mut table_rows: Vec<RegionSnapshotRow> = vec![];
    for rs in rs_rows {
        let maybe_request = datastore
            .lookup_region_snapshot_replacement_request(opctx, &rs)
            .await?;

        table_rows.push(RegionSnapshotRow {
            dataset_id: rs.dataset_id().to_string(),
            region_id: rs.region_id.to_string(),
            snapshot_id: rs.snapshot_id.to_string(),
            existing_request_time: maybe_request
                .as_ref()
                .map(|x| x.request_time),
            existing_request: {
                if let Some(request) = maybe_request {
                    format!(
                        "{} (state {:?})",
                        request.id, request.replacement_state
                    )
                } else {
                    String::from("")
                }
            },
        });
    }

    table_rows.sort_by_key(|x| x.existing_request_time);

    let table = tabled::Table::new(table_rows)
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
        import_address: String,
    }

    // The rows describing the downstairs regions for this disk/volume
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DownstairsRow {
        host_serial: String,
        region: String,
        dataset: String,
        physical_disk: String,
    }

    use nexus_db_schema::schema::disk::dsl as disk_dsl;

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
        use nexus_db_schema::schema::instance::dsl as instance_dsl;
        use nexus_db_schema::schema::vmm::dsl as vmm_dsl;
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

            let import_address = match disk.pantry_address {
                Some(ref pa) => pa.clone().to_string(),
                None => "-".to_string(),
            };
            UpstairsRow {
                host_serial: my_sled.serial_number().to_string(),
                disk_name,
                instance_name,
                propolis_zone: format!("oxz_propolis-server_{}", propolis_id),
                volume_id: disk.volume_id().to_string(),
                disk_state: disk.runtime_state.disk_state.to_string(),
                import_address,
            }
        } else {
            let import_address = match disk.pantry_address {
                Some(ref pa) => pa.clone().to_string(),
                None => "-".to_string(),
            };
            UpstairsRow {
                host_serial: NOT_ON_SLED_MSG.to_string(),
                disk_name,
                instance_name,
                propolis_zone: NO_ACTIVE_PROPOLIS_MSG.to_string(),
                volume_id: disk.volume_id().to_string(),
                disk_state: disk.runtime_state.disk_state.to_string(),
                import_address,
            }
        }
    } else {
        // If the disk is not attached to anything, just print empty
        // fields.
        let import_address = match disk.pantry_address {
            Some(ref pa) => pa.clone().to_string(),
            None => "-".to_string(),
        };
        UpstairsRow {
            host_serial: "-".to_string(),
            disk_name: disk.name().to_string(),
            instance_name: "-".to_string(),
            propolis_zone: "-".to_string(),
            volume_id: disk.volume_id().to_string(),
            disk_state: disk.runtime_state.disk_state.to_string(),
            import_address,
        }
    };
    rows.push(usr);

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    // Get the dataset backing this volume.
    let regions = datastore.get_allocated_regions(disk.volume_id()).await?;

    let mut rows = Vec::with_capacity(3);
    for (dataset, region) in regions {
        let my_pool_id = dataset.pool_id();
        let (_, my_zpool) = LookupPath::new(opctx, datastore)
            .zpool_id(my_pool_id)
            .fetch()
            .await
            .context("failed to look up zpool")?;

        let my_sled_id = my_zpool.sled_id();

        let (_, my_sled) = LookupPath::new(opctx, datastore)
            .sled_id(my_sled_id)
            .fetch()
            .await
            .context("failed to look up sled")?;

        rows.push(DownstairsRow {
            host_serial: my_sled.serial_number().to_string(),
            region: region.id().to_string(),
            dataset: dataset.id().to_string(),
            physical_disk: my_zpool.physical_disk_id().to_string(),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    get_and_display_vcr(disk.volume_id(), datastore).await?;
    Ok(())
}

// Given a UUID, search the database for a volume with that ID
// If found, attempt to parse the .data field into a VolumeConstructionRequest
// and display it if successful.
async fn get_and_display_vcr(
    volume_id: VolumeUuid,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    // Get the VCR from the volume and display selected parts.
    use nexus_db_schema::schema::volume::dsl as volume_dsl;
    let volumes = volume_dsl::volume
        .filter(volume_dsl::id.eq(to_db_typed_uuid(volume_id)))
        .limit(1)
        .select(Volume::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading requested volume")?;

    for v in volumes {
        match serde_json::from_str(&v.data()) {
            Ok(vcr) => {
                println!("VCR from volume ID {volume_id}");
                print_vcr(vcr, 0);
            }
            Err(e) => {
                println!("Volume had invalid VCR in data field: {e}");
            }
        }
    }
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
    use nexus_db_schema::schema::zpool::dsl as zpool_dsl;
    let mut query = zpool_dsl::zpool.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(zpool_dsl::time_deleted.is_null());
    }

    let zpools = query
        .filter(zpool_dsl::physical_disk_id.eq(to_db_typed_uuid(args.uuid)))
        .select(Zpool::as_select())
        .load_async(&*conn)
        .await
        .context("loading zpool from pysical disk id")?;

    let mut sled_ids = HashSet::new();
    let mut dataset_ids: HashSet<DatasetUuid> = HashSet::new();

    if zpools.is_empty() {
        println!("Found no zpools on physical disk UUID {}", args.uuid);
        return Ok(());
    }

    // The current plan is a single zpool per physical disk, so we expect that
    // this will have a single item.  However, If single zpool per disk ever
    // changes, this code will still work.
    for zp in zpools {
        // zpool has the sled id, record that so we can find the serial number.
        sled_ids.insert(zp.sled_id());

        // Next, we find all the Crucible datasets that are on our zpool.
        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        let mut query = dataset_dsl::crucible_dataset.into_boxed();
        if !fetch_opts.include_deleted {
            query = query.filter(dataset_dsl::time_deleted.is_null());
        }

        let datasets = query
            .filter(dataset_dsl::pool_id.eq(to_db_typed_uuid(zp.id())))
            .select(CrucibleDataset::as_select())
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
    println!("CRUCIBLE DATASETS: {:?}", dataset_ids);

    let mut volume_ids = HashSet::new();
    // Now, take the list of datasets we found and search all the regions
    // to see if any of them are on the dataset.  If we find a region that
    // is on one of our datasets, then record the volume ID of that region.
    for did in dataset_ids.clone().into_iter() {
        use nexus_db_schema::schema::region::dsl as region_dsl;
        let regions = region_dsl::region
            .filter(region_dsl::dataset_id.eq(to_db_typed_uuid(did)))
            .select(Region::as_select())
            .load_async(&*conn)
            .await
            .context("loading region")?;

        for rs in regions {
            volume_ids.insert(rs.volume_id().into_untyped_uuid());
        }
    }

    // At this point, we have a list of volume IDs that contain a region
    // that is part of a dataset on a pool on our disk.  The next step is
    // to find the virtual disks associated with these volume IDs and
    // display information about those disks.
    use nexus_db_schema::schema::disk::dsl;
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
                use nexus_db_schema::schema::instance::dsl as instance_dsl;
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
    let dataset_ids: Vec<_> =
        dataset_ids.into_iter().map(|did| to_db_typed_uuid(did)).collect();

    let limit = fetch_opts.fetch_limit;
    use nexus_db_schema::schema::region_snapshot::dsl as region_snapshot_dsl;
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
    use nexus_db_schema::schema::snapshot::dsl as snapshot_dsl;
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

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct PhysicalDiskRow {
    id: PhysicalDiskUuid,
    serial: String,
    vendor: String,
    model: String,
    sled_id: SledUuid,
    policy: PhysicalDiskPolicy,
    state: PhysicalDiskState,
}

impl From<PhysicalDisk> for PhysicalDiskRow {
    fn from(d: PhysicalDisk) -> Self {
        PhysicalDiskRow {
            id: d.id(),
            serial: d.serial.clone(),
            vendor: d.vendor.clone(),
            model: d.model.clone(),
            sled_id: d.sled_id(),
            policy: d.disk_policy.into(),
            state: d.disk_state.into(),
        }
    }
}

/// Run `omdb db physical-disks`.
async fn cmd_db_physical_disks(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &PhysicalDisksArgs,
) -> Result<(), anyhow::Error> {
    let limit = fetch_opts.fetch_limit;
    let filter = match args.filter {
        Some(filter) => filter,
        None => {
            eprintln!(
                "note: listing all in-service disks \
                 (use -F to filter, e.g. -F in-service)"
            );
            DiskFilter::InService
        }
    };

    let sleds = datastore
        .physical_disk_list(&opctx, &first_page(limit), filter)
        .await
        .context("listing physical disks")?;
    check_limit(&sleds, limit, || String::from("listing physical disks"));

    let rows = sleds.into_iter().map(|s| PhysicalDiskRow::from(s));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(1, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

// SERVICES

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
            source_volume_id: s.volume_id().to_string(),
            destination_volume_id: s.destination_volume_id().to_string(),
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

    use nexus_db_schema::schema::snapshot::dsl;
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
        dataset: String,
        physical_disk: String,
    }

    use nexus_db_schema::schema::snapshot::dsl as snapshot_dsl;
    let mut snapshots = snapshot_dsl::snapshot
        .filter(snapshot_dsl::id.eq(args.uuid))
        .limit(1)
        .select(Snapshot::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading requested snapshot")?;

    if snapshots.is_empty() {
        bail!("No snapshot with UUID: {} found", args.uuid);
    }
    let snap = snapshots.pop().expect("Found more that one snapshot");

    let dest_vol_id = snap.destination_volume_id;
    let source_vol_id = snap.volume_id;
    let snap = SnapshotRow::from(snap);

    println!("                 Name: {}", snap.snap_name);
    println!("                   id: {}", snap.id);
    println!("                state: {}", snap.state);
    println!("                 size: {}", snap.size);
    println!("       source_disk_id: {}", snap.source_disk_id);
    println!("     source_volume_id: {}", snap.source_volume_id);
    println!("destination_volume_id: {}", snap.destination_volume_id);

    use nexus_db_schema::schema::region_snapshot::dsl as region_snapshot_dsl;
    let region_snapshots = region_snapshot_dsl::region_snapshot
        .filter(region_snapshot_dsl::snapshot_id.eq(args.uuid))
        .select(RegionSnapshot::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading region snapshots")?;

    println!();
    if region_snapshots.is_empty() {
        println!("No region snapshot info found");
    } else {
        // The row describing the region_snapshot.
        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct RegionSnapshotRow {
            dataset_id: String,
            region_id: String,
            snapshot_id: String,
            snapshot_addr: String,
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
                snapshot_addr: rs.snapshot_addr.to_string(),
                volume_references: rs.volume_references.to_string(),
            };
            rsnap.push(rs);
        }
        let table = tabled::Table::new(rsnap)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();

        println!("REGION SNAPSHOT INFO:");
        println!("{}", table);
    }

    let regions = datastore.get_allocated_regions(source_vol_id.into()).await?;
    if regions.is_empty() {
        println!("\nNo source region info found");
    } else {
        let mut rows = Vec::with_capacity(3);
        for (dataset, region) in regions {
            let my_pool_id = dataset.pool_id();
            let (_, my_zpool) = LookupPath::new(opctx, datastore)
                .zpool_id(my_pool_id)
                .fetch()
                .await
                .context("failed to look up zpool")?;

            let my_sled_id = my_zpool.sled_id();

            let (_, my_sled) = LookupPath::new(opctx, datastore)
                .sled_id(my_sled_id)
                .fetch()
                .await
                .context("failed to look up sled")?;

            rows.push(DownstairsRow {
                host_serial: my_sled.serial_number().to_string(),
                region: region.id().to_string(),
                dataset: dataset.id().to_string(),
                physical_disk: my_zpool.physical_disk_id().to_string(),
            });
        }

        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();

        println!("\nSOURCE REGION INFO:");
        println!("{}", table);
    }

    println!("SOURCE VOLUME VCR:");
    get_and_display_vcr(source_vol_id.into(), datastore).await?;

    // Get the dataset backing this volume.
    let regions = datastore.get_allocated_regions(dest_vol_id.into()).await?;

    let mut rows = Vec::with_capacity(3);
    for (dataset, region) in regions {
        let my_pool_id = dataset.pool_id();
        let (_, my_zpool) = LookupPath::new(opctx, datastore)
            .zpool_id(my_pool_id)
            .fetch()
            .await
            .context("failed to look up zpool")?;

        let my_sled_id = my_zpool.sled_id();

        let (_, my_sled) = LookupPath::new(opctx, datastore)
            .sled_id(my_sled_id)
            .fetch()
            .await
            .context("failed to look up sled")?;

        rows.push(DownstairsRow {
            host_serial: my_sled.serial_number().to_string(),
            region: region.id().to_string(),
            dataset: dataset.id().to_string(),
            physical_disk: my_zpool.physical_disk_id().to_string(),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("DESTINATION REGION INFO:");
    println!("{}", table);
    println!("DESTINATION VOLUME VCR:");
    get_and_display_vcr(dest_vol_id.into(), datastore).await?;

    Ok(())
}

// Volumes
/// Run `omdb db volume list`.
async fn cmd_db_volume_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct VolumeRow {
        id: String,
        created: String,
        modified: String,
        deleted: String,
    }

    let ctx = || "listing volumes".to_string();

    use nexus_db_schema::schema::volume::dsl;
    let mut query = dsl::volume.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    let volumes = query
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(Volume::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading volumes")?;

    check_limit(&volumes, fetch_opts.fetch_limit, ctx);

    let rows = volumes.into_iter().map(|volume| VolumeRow {
        id: volume.id().to_string(),
        created: volume.time_created().to_string(),
        modified: volume.time_modified().to_string(),
        deleted: match volume.time_deleted {
            Some(time) => time.to_string(),
            None => "NULL".to_string(),
        },
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Run `omdb db volume info <UUID>`.
async fn cmd_db_volume_info(
    datastore: &DataStore,
    args: &VolumeInfoArgs,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct VolumeRow {
        id: String,
        created: String,
        modified: String,
        deleted: String,
    }

    use nexus_db_schema::schema::volume::dsl as volume_dsl;

    let volumes = volume_dsl::volume
        .filter(volume_dsl::id.eq(args.uuid))
        .limit(1)
        .select(Volume::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading requested volume")?;

    let mut vcrs = Vec::new();
    let rows = volumes.into_iter().map(|volume| {
        match serde_json::from_str(&volume.data()) {
            Ok(vcr) => {
                vcrs.push(vcr);
            }
            Err(e) => {
                println!("Volume had invalid VCR in data field: {e}");
            }
        }

        VolumeRow {
            id: volume.id().to_string(),
            created: volume.time_created().to_string(),
            modified: volume.time_modified().to_string(),
            deleted: match volume.time_deleted {
                Some(time) => time.to_string(),
                None => "NULL".to_string(),
            },
        }
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    for vcr in vcrs {
        print_vcr(vcr, 0);
    }
    Ok(())
}

// Print the fields that I want to see of a VolumeConstructionRequests
// This will call itself on all sub_volumes and read_only_parents it finds.
// We use the pad variable to indicate how much indent we want to display
// information at.  Each time we recurse into another VCR layer, we increase
// the amount of indention.
fn print_vcr(vcr: VolumeConstructionRequest, pad: usize) {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct VCRVolume {
        id: String,
        bs: String,
        sub_volumes: usize,
        read_only_parent: bool,
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct VCRRegion {
        id: String,
        bs: String,
        bpe: u64,
        ec: u32,
        gen: u64,
        read_only: bool,
    }

    let indent = " ".repeat(pad);
    match vcr {
        VolumeConstructionRequest::Volume {
            id,
            block_size,
            sub_volumes,
            read_only_parent,
        } => {
            let row = VCRVolume {
                id: id.to_string(),
                bs: block_size.to_string(),
                sub_volumes: sub_volumes.len(),
                read_only_parent: read_only_parent.is_some(),
            };
            let table = tabled::Table::new(&[row])
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();

            // Shift the entire table over our indent amount.
            let indented_table: String = table
                .lines()
                .map(|line| format!("{}{}", indent, line))
                .collect::<Vec<String>>()
                .join("\n");
            println!("{}\n", indented_table);

            for (index, sv) in sub_volumes.iter().enumerate() {
                println!("{indent}SUB VOLUME {index}");
                print_vcr(sv.clone(), pad + 4);
                println!("");
            }

            if let Some(rop) = read_only_parent {
                println!("{indent}READ ONLY PARENT:");
                print_vcr(*rop, pad + 4);
            }
        }
        VolumeConstructionRequest::Region {
            block_size,
            blocks_per_extent,
            extent_count,
            gen,
            opts,
        } => {
            let row = VCRRegion {
                id: opts.id.to_string(),
                bs: block_size.to_string(),
                bpe: blocks_per_extent,
                ec: extent_count,
                gen,
                read_only: opts.read_only,
            };
            let table = tabled::Table::new(&[row])
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();

            // Shift the entire table over our indent amount.
            let indented_table: String = table
                .lines()
                .map(|line| format!("{}{}", indent, line))
                .collect::<Vec<String>>()
                .join("\n");
            println!("{}", indented_table);
            for target in opts.target {
                println!("{indent}{target}");
            }
        }
        _ => {
            println!("{indent}Unsupported volume type");
        }
    }
}

enum VolumeLockHolder {
    RegionReplacement { id: Uuid },
    RegionSnapshotReplacement { id: Uuid },
    RegionSnapshotReplacementStep { id: Uuid, request_id: Uuid },
    Unknown,
}

impl VolumeLockHolder {
    pub fn type_string(&self) -> String {
        match self {
            VolumeLockHolder::RegionReplacement { .. } => {
                String::from("region replacement")
            }

            VolumeLockHolder::RegionSnapshotReplacement { .. } => {
                String::from("region snapshot replacement")
            }

            VolumeLockHolder::RegionSnapshotReplacementStep { .. } => {
                String::from("region snapshot replacement step")
            }

            VolumeLockHolder::Unknown => String::from("unknown"),
        }
    }

    pub fn details(&self) -> String {
        match self {
            VolumeLockHolder::RegionReplacement { id } => id.to_string(),

            VolumeLockHolder::RegionSnapshotReplacement { id } => {
                id.to_string()
            }

            VolumeLockHolder::RegionSnapshotReplacementStep {
                id,
                request_id,
            } => format!("{id} (request {request_id})"),

            VolumeLockHolder::Unknown => String::from("n/a"),
        }
    }

    pub fn id(&self) -> Option<Uuid> {
        match self {
            VolumeLockHolder::RegionReplacement { id } => Some(*id),

            VolumeLockHolder::RegionSnapshotReplacement { id } => Some(*id),

            VolumeLockHolder::RegionSnapshotReplacementStep { id, .. } => {
                Some(*id)
            }

            VolumeLockHolder::Unknown => None,
        }
    }
}

async fn get_volume_lock_holder(
    conn: &DataStoreConnection,
    repair_id: Uuid,
) -> Result<VolumeLockHolder, anyhow::Error> {
    let maybe_region_replacement = {
        use nexus_db_schema::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(dsl::id.eq(repair_id))
            .select(RegionReplacement::as_select())
            .first_async(&**conn)
            .await
            .optional()?
    };

    if let Some(r) = maybe_region_replacement {
        return Ok(VolumeLockHolder::RegionReplacement { id: r.id });
    }

    let maybe_region_snapshot_replacement = {
        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(dsl::id.eq(repair_id))
            .select(RegionSnapshotReplacement::as_select())
            .first_async(&**conn)
            .await
            .optional()?
    };

    if let Some(r) = maybe_region_snapshot_replacement {
        return Ok(VolumeLockHolder::RegionSnapshotReplacement { id: r.id });
    }

    let maybe_region_snapshot_replacement_step = {
        use nexus_db_schema::schema::region_snapshot_replacement_step::dsl;

        dsl::region_snapshot_replacement_step
            .filter(dsl::id.eq(repair_id))
            .select(RegionSnapshotReplacementStep::as_select())
            .first_async(&**conn)
            .await
            .optional()?
    };

    if let Some(s) = maybe_region_snapshot_replacement_step {
        return Ok(VolumeLockHolder::RegionSnapshotReplacementStep {
            id: s.id,
            request_id: s.request_id,
        });
    }

    // It's possible that the `snapshot_create` saga has taken the lock, but
    // there's no way to know what that lock id is as it is randomly
    // generated during the saga.
    //
    // TODO with a better interface for querying sagas, one could:
    //
    // - scan for all the currently running `snapshot_create` sagas
    // - deserialize the output (if it's there) of the "lock_id" nodes
    // - match against that

    Ok(VolumeLockHolder::Unknown)
}

/// What is holding the volume lock?
async fn cmd_db_volume_lock_holder(
    datastore: &DataStore,
    args: &VolumeLockHolderArgs,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct HolderRow {
        volume_id: String,
        lock_id: String,
        holder_type: String,
        holder_details: String,
    }

    let mut rows = vec![];

    let volume_id = VolumeUuid::from_untyped_uuid(args.uuid);

    let maybe_volume_repair_record = datastore
        .pool_connection_for_tests()
        .await?
        .transaction_async(async move |conn| {
            use nexus_db_schema::schema::volume_repair::dsl;

            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

            dsl::volume_repair
                .filter(dsl::volume_id.eq(to_db_typed_uuid(volume_id)))
                .select(VolumeRepair::as_select())
                .first_async(&conn)
                .await
        })
        .await
        .optional()?;

    if let Some(volume_repair_record) = maybe_volume_repair_record {
        let conn = datastore.pool_connection_for_tests().await?;

        let lock_holder =
            get_volume_lock_holder(&conn, volume_repair_record.repair_id)
                .await?;

        rows.push(HolderRow {
            volume_id: volume_id.to_string(),
            lock_id: volume_repair_record.repair_id.to_string(),
            holder_type: lock_holder.type_string(),
            holder_details: lock_holder.details(),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_volume_cannot_activate(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    let mut paginator =
        Paginator::new(SQL_BATCH_SIZE, dropshot::PaginationOrder::Ascending);
    while let Some(p) = paginator.next() {
        use nexus_db_schema::schema::volume::dsl;
        let batch = paginated(dsl::volume, dsl::id, &p.current_pagparams())
            .filter(dsl::time_deleted.is_null())
            .select(Volume::as_select())
            .load_async(&*conn)
            .await
            .context("fetching volumes")?;

        paginator =
            p.found_batch(&batch, &|v: &Volume| v.id().into_untyped_uuid());

        for volume in batch {
            match datastore.volume_cooked(opctx, volume.id()).await? {
                VolumeCookedResult::HardDeleted => {
                    println!("{} hard deleted!", volume.id());
                }

                VolumeCookedResult::Ok => {}

                VolumeCookedResult::RegionSetWithAllExpungedMembers {
                    region_set,
                } => {
                    println!(
                        "volume {} is cooked: {region_set:?} are all expunged!",
                        volume.id(),
                    );
                }

                VolumeCookedResult::MultipleSomeReturned { target } => {
                    println!(
                        "target {target} does not uniquely identify a \
                        resource, please run `omdb db validate` sub-commands \
                        related to volumes!"
                    );
                }

                VolumeCookedResult::TargetNotFound { target } => {
                    println!("target {target} not found")
                }
            }
        }
    }

    Ok(())
}

/// What volumes are referenced an IP, netmask, region, or region snapshot?
async fn cmd_db_volume_reference(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &VolumeReferenceArgs,
) -> Result<(), anyhow::Error> {
    let volume_ids: Vec<Uuid> = if let Some(ip) = args.ip {
        datastore
            .find_volumes_referencing_ipv6_addr(opctx, ip)
            .await?
            .into_iter()
            .map(|v| v.id().into_untyped_uuid())
            .collect()
    } else if let Some(net) = args.net {
        datastore
            .find_volumes_referencing_ipv6_net(opctx, net)
            .await?
            .into_iter()
            .map(|v| v.id().into_untyped_uuid())
            .collect()
    } else if let Some(region_id) = args.read_only_region {
        datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::ReadOnlyRegion { region_id },
            )
            .await?
            .into_iter()
            .map(|vrur| vrur.volume_id.into_untyped_uuid())
            .collect()
    } else if let Some(region_snapshot_ids) = &args.region_snapshot {
        if region_snapshot_ids.len() != 3 {
            bail!(
                "three IDs required to uniquely identify a region snapshot: \
                dataset, region, and snapshot"
            );
        }

        datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: DatasetUuid::from_untyped_uuid(
                        region_snapshot_ids[0],
                    ),
                    region_id: region_snapshot_ids[1],
                    snapshot_id: region_snapshot_ids[2],
                },
            )
            .await?
            .into_iter()
            .map(|vrur| vrur.volume_id.into_untyped_uuid())
            .collect()
    } else {
        bail!("clap should not allow us to reach here!");
    };

    let volumes_used_by =
        volume_used_by(datastore, fetch_opts, &volume_ids).await?;

    let table = tabled::Table::new(volumes_used_by)
        .with(tabled::settings::Style::psql())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .with(tabled::settings::Panel::header("Referenced volumes"))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// List all regions still missing ports
async fn cmd_db_region_missing_ports(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let regions: Vec<Region> = datastore.regions_missing_ports(opctx).await?;

    for region in regions {
        println!("{:?}", region.id());
    }

    Ok(())
}

/// List all regions
async fn cmd_db_region_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &RegionListArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::region::dsl;

    let mut query = paginated(
        dsl::region,
        dsl::id,
        &first_page::<dsl::id>(fetch_opts.fetch_limit),
    );

    if let Some(dataset_id) = args.dataset_id {
        query = query.filter(dsl::dataset_id.eq(to_db_typed_uuid(dataset_id)));
    }

    let regions: Vec<Region> = query
        .select(Region::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await?;

    check_limit(&regions, fetch_opts.fetch_limit, || {
        String::from("listing regions")
    });

    if args.id_only {
        for region in regions {
            println!("{}", region.id());
        }
    } else {
        #[derive(Tabled)]
        struct RegionRow {
            id: Uuid,
            dataset_id: DatasetUuid,
            volume_id: VolumeUuid,
            block_size: i64,
            blocks_per_extent: u64,
            extent_count: u64,
            read_only: bool,
            deleting: bool,
        }

        let rows: Vec<_> = regions
            .into_iter()
            .map(|region: Region| RegionRow {
                id: region.id(),
                dataset_id: region.dataset_id(),
                volume_id: region.volume_id(),
                block_size: region.block_size().into(),
                blocks_per_extent: region.blocks_per_extent(),
                extent_count: region.extent_count(),
                read_only: region.read_only(),
                deleting: region.deleting(),
            })
            .collect();

        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::psql())
            .to_string();

        println!("{}", table);
    }

    Ok(())
}

#[derive(Tabled)]
struct VolumeUsedBy {
    volume_id: VolumeUuid,
    usage_type: String,
    usage_id: String,
    usage_name: String,
    deleted: bool,
}

async fn volume_used_by(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    volumes: &[Uuid],
) -> Result<Vec<VolumeUsedBy>, anyhow::Error> {
    let disks_used: Vec<Disk> = {
        let volumes = volumes.to_vec();
        datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(async move |conn| {
                use nexus_db_schema::schema::disk::dsl;

                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                paginated(
                    dsl::disk,
                    dsl::id,
                    &first_page::<dsl::id>(fetch_opts.fetch_limit),
                )
                .filter(dsl::volume_id.eq_any(volumes))
                .select(Disk::as_select())
                .load_async(&conn)
                .await
            })
            .await?
    };

    check_limit(&disks_used, fetch_opts.fetch_limit, || {
        String::from("listing disks used")
    });

    let snapshots_used: Vec<Snapshot> = {
        let volumes = volumes.to_vec();
        datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(async move |conn| {
                use nexus_db_schema::schema::snapshot::dsl;

                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                paginated(
                    dsl::snapshot,
                    dsl::id,
                    &first_page::<dsl::id>(fetch_opts.fetch_limit),
                )
                .filter(
                    dsl::volume_id
                        .eq_any(volumes.clone())
                        .or(dsl::destination_volume_id.eq_any(volumes.clone())),
                )
                .select(Snapshot::as_select())
                .load_async(&conn)
                .await
            })
            .await?
    };

    check_limit(&snapshots_used, fetch_opts.fetch_limit, || {
        String::from("listing snapshots used")
    });

    let images_used: Vec<Image> = {
        let volumes = volumes.to_vec();
        datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(async move |conn| {
                use nexus_db_schema::schema::image::dsl;

                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                paginated(
                    dsl::image,
                    dsl::id,
                    &first_page::<dsl::id>(fetch_opts.fetch_limit),
                )
                .filter(dsl::volume_id.eq_any(volumes))
                .select(Image::as_select())
                .load_async(&conn)
                .await
            })
            .await?
    };

    check_limit(&images_used, fetch_opts.fetch_limit, || {
        String::from("listing images used")
    });

    let export_used: Vec<UserDataExportRecord> = {
        let volumes = volumes.to_vec();
        datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(async move |conn| {
                use nexus_db_schema::schema::user_data_export::dsl;

                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                paginated(
                    dsl::user_data_export,
                    dsl::id,
                    &first_page::<dsl::id>(fetch_opts.fetch_limit),
                )
                .filter(dsl::volume_id.eq_any(volumes))
                .select(UserDataExportRecord::as_select())
                .load_async(&conn)
                .await
            })
            .await?
    };

    check_limit(&export_used, fetch_opts.fetch_limit, || {
        String::from("listing user data export used")
    });

    Ok(volumes
        .iter()
        .map(|volume_id| {
            let volume_id = VolumeUuid::from_untyped_uuid(*volume_id);

            let maybe_image =
                images_used.iter().find(|x| x.volume_id() == volume_id);

            let maybe_snapshot =
                snapshots_used.iter().find(|x| x.volume_id() == volume_id);

            let maybe_snapshot_dest = snapshots_used
                .iter()
                .find(|x| x.destination_volume_id() == volume_id);

            let maybe_disk =
                disks_used.iter().find(|x| x.volume_id() == volume_id);

            let maybe_export =
                export_used.iter().find(|x| x.volume_id() == Some(volume_id));

            if let Some(image) = maybe_image {
                VolumeUsedBy {
                    volume_id,
                    usage_type: String::from("image"),
                    usage_id: image.id().to_string(),
                    usage_name: image.name().to_string(),
                    deleted: image.time_deleted().is_some(),
                }
            } else if let Some(snapshot) = maybe_snapshot {
                VolumeUsedBy {
                    volume_id,
                    usage_type: String::from("snapshot"),
                    usage_id: snapshot.id().to_string(),
                    usage_name: snapshot.name().to_string(),
                    deleted: snapshot.time_deleted().is_some(),
                }
            } else if let Some(snapshot) = maybe_snapshot_dest {
                VolumeUsedBy {
                    volume_id,
                    usage_type: String::from("snapshot dest"),
                    usage_id: snapshot.id().to_string(),
                    usage_name: snapshot.name().to_string(),
                    deleted: snapshot.time_deleted().is_some(),
                }
            } else if let Some(disk) = maybe_disk {
                VolumeUsedBy {
                    volume_id,
                    usage_type: String::from("disk"),
                    usage_id: disk.id().to_string(),
                    usage_name: disk.name().to_string(),
                    deleted: disk.time_deleted().is_some(),
                }
            } else if let Some(export) = maybe_export {
                match export.resource() {
                    UserDataExportResource::Snapshot { id } => VolumeUsedBy {
                        volume_id,
                        usage_type: String::from("export"),
                        usage_id: id.to_string(),
                        usage_name: String::from("snapshot"),
                        deleted: export.deleted(),
                    },
                    UserDataExportResource::Image { id } => VolumeUsedBy {
                        volume_id,
                        usage_type: String::from("export"),
                        usage_id: id.to_string(),
                        usage_name: String::from("image"),
                        deleted: export.deleted(),
                    },
                }
            } else {
                VolumeUsedBy {
                    volume_id,
                    usage_type: String::from("unknown!"),
                    usage_id: String::from(""),
                    usage_name: String::from(""),
                    deleted: false,
                }
            }
        })
        .collect())
}

/// Find what is using a region
async fn cmd_db_region_used_by(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &RegionUsedByArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::region::dsl;

    let regions: Vec<Region> = paginated(
        dsl::region,
        dsl::id,
        &first_page::<dsl::id>(fetch_opts.fetch_limit),
    )
    .filter(dsl::id.eq_any(args.region_id.clone()))
    .select(Region::as_select())
    .load_async(&*datastore.pool_connection_for_tests().await?)
    .await?;

    check_limit(&regions, fetch_opts.fetch_limit, || {
        String::from("listing regions")
    });

    let volumes: Vec<Uuid> =
        regions.iter().map(|x| x.volume_id().into_untyped_uuid()).collect();

    let volumes_used_by =
        volume_used_by(datastore, fetch_opts, &volumes).await?;

    #[derive(Tabled)]
    struct RegionRow {
        id: Uuid,
        volume_id: VolumeUuid,
        usage_type: String,
        usage_id: String,
        usage_name: String,
        deleted: bool,
    }

    let rows: Vec<_> = regions
        .into_iter()
        .zip(volumes_used_by.into_iter())
        .map(|(region, volume_used_by)| RegionRow {
            id: region.id(),
            volume_id: volume_used_by.volume_id,
            usage_type: volume_used_by.usage_type,
            usage_id: volume_used_by.usage_id,
            usage_name: volume_used_by.usage_name,
            deleted: volume_used_by.deleted,
        })
        .collect();

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::psql())
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Find deleted volume regions
async fn cmd_db_region_find_deleted(
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let freed_crucible_resources =
        datastore.find_deleted_volume_regions().await?;

    #[derive(Tabled)]
    struct RegionRow {
        dataset_id: DatasetUuid,
        region_id: Uuid,
    }

    #[derive(Tabled)]
    struct VolumeRow {
        volume_id: VolumeUuid,
    }

    let region_rows: Vec<RegionRow> = freed_crucible_resources
        .datasets_and_regions
        .iter()
        .map(|row| {
            let (dataset, region) = row;

            RegionRow { dataset_id: dataset.id(), region_id: region.id() }
        })
        .collect();

    let table = tabled::Table::new(region_rows)
        .with(tabled::settings::Style::psql())
        .to_string();

    println!("{}", table);

    let volume_rows: Vec<VolumeRow> = freed_crucible_resources
        .volumes
        .iter()
        .map(|volume_id| VolumeRow { volume_id: *volume_id })
        .collect();

    let volume_table = tabled::Table::new(volume_rows)
        .with(tabled::settings::Style::psql())
        .to_string();

    println!("{}", volume_table);

    Ok(())
}

#[derive(Debug)]
enum DryRunRegionAllocationResult {
    QueryError { e: region_allocation::AllocationQueryError },

    Success { datasets_and_regions: Vec<(CrucibleDataset, Region)> },
}

async fn cmd_db_dry_run_region_allocation(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &DryRunRegionAllocationArgs,
) -> Result<(), anyhow::Error> {
    let volume_id = match args.volume_id {
        Some(v) => v,
        None => VolumeUuid::new_v4(),
    };

    let size: external::ByteCount = args.size.try_into()?;
    let block_size: params::BlockSize = args.block_size.try_into()?;

    let (blocks_per_extent, extent_count) = DataStore::get_crucible_allocation(
        &block_size.try_into().unwrap(),
        size,
    );

    let allocation_strategy = if args.distinct_sleds {
        RegionAllocationStrategy::RandomWithDistinctSleds { seed: None }
    } else {
        RegionAllocationStrategy::Random { seed: None }
    };

    let err = OptionalError::<DryRunRegionAllocationResult>::new();
    let conn = datastore.pool_connection_for_tests().await?;

    let result: Result<std::convert::Infallible, diesel::result::Error> =
        datastore
            .transaction_retry_wrapper("dry_run_region_allocation")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let allocation_strategy = allocation_strategy.clone();

                async move {
                    let query = region_allocation::allocation_query(
                        volume_id,
                        args.snapshot_id,
                        region_allocation::RegionParameters {
                            block_size: args.block_size.into(),
                            blocks_per_extent,
                            extent_count,
                            read_only: args.snapshot_id.is_some(),
                        },
                        &allocation_strategy,
                        args.num_regions_required,
                    )
                    .map_err(|e| {
                        err.bail(DryRunRegionAllocationResult::QueryError { e })
                    })?;

                    let datasets_and_regions: Vec<(CrucibleDataset, Region)> =
                        query.get_results_async(&conn).await?;

                    Err(err.bail(DryRunRegionAllocationResult::Success {
                        datasets_and_regions,
                    }))
                }
            })
            .await;

    let datasets_and_regions = match result {
        Ok(_) => {
            panic!("should not have succeeded!");
        }

        Err(e) => {
            if let Some(result) = err.take() {
                match result {
                    DryRunRegionAllocationResult::QueryError { e } => {
                        let err: external::Error = e.into();
                        Err(err)?
                    }

                    DryRunRegionAllocationResult::Success {
                        datasets_and_regions,
                    } => datasets_and_regions,
                }
            } else {
                Err(e)?
            }
        }
    };

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct Row {
        pub region_id: Uuid,

        pub dataset_id: DatasetUuid,
        pub size_used: i64,

        pub pool_id: ZpoolUuid,

        #[tabled(display_with = "option_impl_display")]
        pub total_size: Option<i64>,

        #[tabled(display_with = "option_impl_display")]
        pub size_left: Option<i64>,
    }

    let mut rows = Vec::with_capacity(datasets_and_regions.len());

    let Some(latest_collection) =
        datastore.inventory_get_latest_collection(opctx).await?
    else {
        bail!(
            "failing due to missing inventory - we rely on inventory to \
            calculate zpool sizing info"
        );
    };

    let mut zpool_total_size: HashMap<ZpoolUuid, i64> = HashMap::new();

    for sled_agent in latest_collection.sled_agents {
        for zpool in sled_agent.zpools {
            zpool_total_size.insert(zpool.id, zpool.total_size.into());
        }
    }

    for (dataset, region) in datasets_and_regions {
        let pool_id = dataset.pool_id();
        let total_size = zpool_total_size.get(&pool_id);
        rows.push(Row {
            region_id: region.id(),

            dataset_id: dataset.id(),
            size_used: dataset.size_used,

            pool_id,
            total_size: total_size.copied(),

            size_left: match total_size {
                Some(total_size) => Some(total_size - dataset.size_used),
                None => None,
            },
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::psql())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .with(tabled::settings::Panel::header("Allocation results"))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// List all region replacement requests
async fn cmd_db_region_replacement_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &RegionReplacementListArgs,
) -> Result<(), anyhow::Error> {
    let ctx = || "listing region replacement requests".to_string();
    let limit = fetch_opts.fetch_limit;

    let requests: Vec<RegionReplacement> = {
        let conn = datastore.pool_connection_for_tests().await?;

        use nexus_db_schema::schema::region_replacement::dsl;

        match (args.state, args.after) {
            (Some(state), Some(after)) => {
                dsl::region_replacement
                    .filter(dsl::replacement_state.eq(state))
                    .filter(dsl::request_time.gt(after))
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }

            (Some(state), None) => {
                dsl::region_replacement
                    .filter(dsl::replacement_state.eq(state))
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }

            (None, Some(after)) => {
                dsl::region_replacement
                    .filter(dsl::request_time.gt(after))
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }

            (None, None) => {
                dsl::region_replacement
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }
        }
    };

    check_limit(&requests, limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct Row {
        pub id: Uuid,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        pub request_time: DateTime<Utc>,
        pub replacement_state: String,
    }

    let mut rows = Vec::with_capacity(requests.len());

    for request in requests {
        rows.push(Row {
            id: request.id,
            request_time: request.request_time,
            replacement_state: format!("{:?}", request.replacement_state),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .with(tabled::settings::Panel::header("Region replacement requests"))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Display all non-complete region replacements
async fn cmd_db_region_replacement_status(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    let ctx = || "listing region replacement requests".to_string();
    let limit = fetch_opts.fetch_limit;

    let requests: Vec<RegionReplacement> = {
        let conn = datastore.pool_connection_for_tests().await?;

        use nexus_db_schema::schema::region_replacement::dsl;

        dsl::region_replacement
            .filter(dsl::replacement_state.ne(RegionReplacementState::Complete))
            .limit(i64::from(u32::from(limit)))
            .select(RegionReplacement::as_select())
            .get_results_async(&*conn)
            .await?
    };

    check_limit(&requests, limit, ctx);

    for request in requests {
        println!("{}:", request.id);
        println!();

        println!("      started: {}", request.request_time);
        println!("        state: {:?}", request.replacement_state);
        println!("old region id: {}", request.old_region_id);
        println!("new region id: {:?}", request.new_region_id);
        println!();

        if let Some(new_region_id) = request.new_region_id {
            // Find the most recent upstairs repair notification where the
            // downstairs being repaired is a "new" region id. This will give us
            // the most recent repair id.
            let maybe_repair: Option<UpstairsRepairNotification> = datastore
                .most_recent_started_repair_notification(opctx, new_region_id)
                .await?;

            if let Some(repair) = maybe_repair {
                let maybe_repair_progress: Option<UpstairsRepairProgress> =
                    datastore
                        .most_recent_repair_progress(
                            opctx,
                            repair.repair_id.into(),
                        )
                        .await?;

                if let Some(repair_progress) = maybe_repair_progress {
                    let bar = ProgressBar::with_draw_target(
                        Some(repair_progress.total_items as u64),
                        ProgressDrawTarget::stdout(),
                    )
                    .with_style(ProgressStyle::with_template(
                        "progress:\t{wide_bar:.green} [{pos:>7}/{len:>7}]",
                    )?)
                    .with_position(repair_progress.current_item as u64);

                    bar.abandon();

                    println!();
                }
            }
        }

        println!();
    }

    Ok(())
}

/// Show details for a single region replacement
async fn cmd_db_region_replacement_info(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &RegionReplacementInfoArgs,
) -> Result<(), anyhow::Error> {
    let request = datastore
        .get_region_replacement_request_by_id(opctx, args.replacement_id)
        .await?;

    // Show details
    println!("      started: {}", request.request_time);
    println!("        state: {:?}", request.replacement_state);
    println!("old region id: {}", request.old_region_id);
    println!("new region id: {:?}", request.new_region_id);
    println!();

    if let Some(new_region_id) = request.new_region_id {
        // Find all related notifications
        let notifications: Vec<UpstairsRepairNotification> = datastore
            .repair_notifications_for_region(opctx, new_region_id)
            .await?;

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct Row {
            #[tabled(display_with = "datetime_rfc3339_concise")]
            pub time: DateTime<Utc>,

            pub repair_id: String,
            pub repair_type: String,

            pub upstairs_id: String,
            pub session_id: String,

            pub notification_type: String,
        }

        let mut rows = Vec::with_capacity(notifications.len());

        for notification in &notifications {
            rows.push(Row {
                time: notification.time,
                repair_id: notification.repair_id.to_string(),
                repair_type: format!("{:?}", notification.repair_type),
                upstairs_id: notification.upstairs_id.to_string(),
                session_id: notification.session_id.to_string(),
                notification_type: format!(
                    "{:?}",
                    notification.notification_type
                ),
            });
        }

        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .with(tabled::settings::Panel::header("Repair notifications"))
            .to_string();

        println!("{}", table);

        println!();

        // Use the most recent notification to get the most recent repair ID,
        // and use that to search for progress.

        let maybe_repair: Option<UpstairsRepairNotification> = datastore
            .most_recent_started_repair_notification(opctx, new_region_id)
            .await?;

        if let Some(repair) = maybe_repair {
            let maybe_repair_progress: Option<UpstairsRepairProgress> =
                datastore
                    .most_recent_repair_progress(opctx, repair.repair_id.into())
                    .await?;

            if let Some(repair_progress) = maybe_repair_progress {
                let bar = ProgressBar::with_draw_target(
                    Some(repair_progress.total_items as u64),
                    ProgressDrawTarget::stdout(),
                )
                .with_style(ProgressStyle::with_template(
                    "progress:\t{wide_bar:.green} [{pos:>7}/{len:>7}]",
                )?)
                .with_position(repair_progress.current_item as u64);

                bar.abandon();

                println!();
            }
        }

        // Find the steps that the driver saga has committed to the DB.

        let steps: Vec<RegionReplacementStep> = datastore
            .region_replacement_request_steps(opctx, args.replacement_id)
            .await?;

        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct StepRow {
            #[tabled(display_with = "datetime_rfc3339_concise")]
            pub time: DateTime<Utc>,
            pub step_type: String,
            pub details: String,
        }

        let mut rows = Vec::with_capacity(steps.len());

        for step in steps {
            rows.push(StepRow {
                time: step.step_time,
                step_type: format!("{:?}", step.step_type),
                details: match step.step_type {
                    RegionReplacementStepType::Propolis => {
                        format!(
                            "instance {:?} vmm {:?}",
                            step.step_associated_instance_id,
                            step.step_associated_vmm_id,
                        )
                    }

                    RegionReplacementStepType::Pantry => {
                        format!(
                            "address {:?}:{:?} job {:?}",
                            step.step_associated_pantry_ip,
                            step.step_associated_pantry_port,
                            step.step_associated_pantry_job_id,
                        )
                    }
                },
            });
        }

        println!();

        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .with(tabled::settings::Panel::header("Repair steps"))
            .to_string();

        println!("{}", table);
    }

    Ok(())
}

/// Manually request a region replacement
async fn cmd_db_region_replacement_request(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &RegionReplacementRequestArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let region = datastore.get_region(args.region_id).await?;

    if region.read_only() {
        let request_id = datastore
            .create_read_only_region_replacement_request(opctx, region.id())
            .await?;

        println!("region snapshot replacement {request_id} created");
    } else {
        let request_id = datastore
            .create_region_replacement_request_for_region(opctx, &region)
            .await?;

        println!("region replacement {request_id} created");
    }

    Ok(())
}

// SLEDS

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct SledRow {
    serial: String,
    ip: String,
    role: &'static str,
    policy: SledPolicy,
    state: SledState,
    id: SledUuid,
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

// INSTANCES

/// Run `omdb db instance info`: show details about a customer VM.
async fn cmd_db_instance_info(
    _: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &InstanceInfoArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_model::{
        Instance, InstanceKarmicStatus, InstanceRuntimeState, Migration,
        Reincarnatability,
    };
    use nexus_db_schema::schema::{
        disk::dsl as disk_dsl, instance::dsl as instance_dsl,
        migration::dsl as migration_dsl, vmm::dsl as vmm_dsl,
    };
    let &InstanceInfoArgs { ref id, history, resources, all } = args;

    let instance = instance_dsl::instance
        .filter(instance_dsl::id.eq(id.into_untyped_uuid()))
        .select(Instance::as_select())
        .limit(1)
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .with_context(|| format!("failed to fetch instance record for {id}"))?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no instance found with ID {id}"))?;

    let active_vmm = if let Some(id) = instance.runtime_state.propolis_id {
        let fetch_result = vmm_dsl::vmm
            .filter(vmm_dsl::id.eq(id))
            .select(Vmm::as_select())
            .limit(1)
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await;
        let vmm = match fetch_result {
            Ok(rs) => rs.into_iter().next(),
            Err(e) => {
                eprintln!(
                    "warning: failed to look up active VMM record {id}: {e}"
                );
                None
            }
        };
        if vmm.is_none() {
            eprintln!(
                " /!\\ BAD: instance has an active VMM ({id}) but no matching VMM record was found!"
            );
        }
        vmm
    } else {
        None
    };
    // Manually format the instance struct because using its `fmt::Debug` impl
    // will print out the user-data array one byte per line, which is horrible.
    //
    // /!\ WARNING /!\
    // This does mean that anyone who adds new fields to the
    // `nexus_db_model::Instance` type will want to make sure to update this
    // code as well. Unfortunately, we can't just destructure the struct here to
    // make sure this code breaks, since the `identity` field isn't public.
    // So...just don't forget to do that, I guess.
    const ID: &'static str = "ID";
    const PROJECT_ID: &'static str = "project ID";
    const NAME: &'static str = "name";
    const DESCRIPTION: &'static str = "description";
    const CREATED: &'static str = "created at";
    const DELETED: &'static str = "deleted at";
    const API_STATE: &'static str = "external API state";
    const VCPUS: &'static str = "vCPUs";
    const MEMORY: &'static str = "memory";
    const HOSTNAME: &'static str = "hostname";
    const BOOT_DISK: &'static str = "boot disk";
    const AUTO_RESTART: &'static str = "auto-restart";
    const STATE: &'static str = "nexus state";
    const INTENDED_STATE: &'static str = "intended state";
    const LAST_MODIFIED: &'static str = "last modified at";
    const LAST_UPDATED: &'static str = "last updated at";
    const LAST_AUTO_RESTART: &'static str = "  last reincarnated at";
    const KARMIC_STATUS: &'static str = "  karmic status";
    const NEEDS_REINCARNATION: &'static str = "needs reincarnation";
    const ACTIVE_VMM: &'static str = "active VMM ID";
    const TARGET_VMM: &'static str = "target VMM ID";
    const MIGRATION_ID: &'static str = "migration ID";
    const UPDATER_LOCK: &'static str = "updater lock";
    const ACTIVE_VMM_RECORD: &'static str = "active VMM record";
    const MIGRATION_RECORD: &'static str = "migration record";
    const TARGET_VMM_RECORD: &'static str = "target VMM record";
    const WIDTH: usize = crate::helpers::const_max_len(&[
        ID,
        NAME,
        DESCRIPTION,
        CREATED,
        DELETED,
        VCPUS,
        MEMORY,
        BOOT_DISK,
        HOSTNAME,
        AUTO_RESTART,
        STATE,
        API_STATE,
        INTENDED_STATE,
        LAST_UPDATED,
        LAST_MODIFIED,
        LAST_AUTO_RESTART,
        KARMIC_STATUS,
        NEEDS_REINCARNATION,
        ACTIVE_VMM,
        TARGET_VMM,
        MIGRATION_ID,
        UPDATER_LOCK,
        ACTIVE_VMM_RECORD,
        MIGRATION_RECORD,
        TARGET_VMM_RECORD,
    ]);

    fn print_multiline_debug(slug: &str, thing: &impl core::fmt::Debug) {
        println!(
            "    {slug:>WIDTH$}:\n{}",
            textwrap::indent(
                &format!("{thing:#?}"),
                &" ".repeat(WIDTH - slug.len() + 8)
            )
        );
    }

    println!("\n{:=<80}", "== INSTANCE ");
    println!("    {ID:>WIDTH$}: {}", instance.id());
    println!("    {PROJECT_ID:>WIDTH$}: {}", instance.project_id);
    println!("    {NAME:>WIDTH$}: {}", instance.name());
    println!("    {DESCRIPTION:>WIDTH$}: {}", instance.description());
    println!("    {CREATED:>WIDTH$}: {}", instance.time_created());
    println!("    {LAST_MODIFIED:>WIDTH$}: {}", instance.time_modified());
    if let Some(deleted) = instance.time_deleted() {
        println!("/!\\ {DELETED:>WIDTH$}: {deleted}");
    }

    println!("\n{:=<80}", "== CONFIGURATION ");
    println!("    {VCPUS:>WIDTH$}: {}", instance.ncpus.0.0);
    println!("    {MEMORY:>WIDTH$}: {}", instance.memory.0);
    println!("    {HOSTNAME:>WIDTH$}: {}", instance.hostname);
    println!("    {BOOT_DISK:>WIDTH$}: {:?}", instance.boot_disk_id);
    print_multiline_debug(AUTO_RESTART, &instance.auto_restart);
    println!("\n{:=<80}", "== RUNTIME STATE ");
    let InstanceRuntimeState {
        time_updated,
        propolis_id,
        dst_propolis_id,
        migration_id,
        nexus_state,
        r#gen,
        time_last_auto_restarted,
    } = instance.runtime_state;
    println!("    {STATE:>WIDTH$}: {nexus_state:?}");
    let effective_state =
        InstanceStateComputer::new(&instance, active_vmm.as_ref())
            .compute_state();
    println!(
        "{} {API_STATE:>WIDTH$}: {effective_state:?}",
        if effective_state == InstanceState::Failed { "/!\\" } else { "(i)" }
    );
    println!("    {INTENDED_STATE:>WIDTH$}: {}", instance.intended_state);
    println!(
        "    {LAST_UPDATED:>WIDTH$}: {time_updated:?} (generation {})",
        r#gen.0
    );

    // Reincarnation status
    let InstanceKarmicStatus { needs_reincarnation, can_reincarnate } =
        instance.auto_restart_status(active_vmm.as_ref());
    println!(
        "{} {NEEDS_REINCARNATION:>WIDTH$}: {needs_reincarnation}",
        if needs_reincarnation { "(i)" } else { "   " }
    );
    match can_reincarnate {
        Reincarnatability::WillReincarnate => {
            println!(
                "    {KARMIC_STATUS:>WIDTH$}: saṃsāra (reincarnation enabled)"
            );
        }
        Reincarnatability::Nirvana => {
            println!(
                "    {KARMIC_STATUS:>WIDTH$}: nirvāṇa (reincarnation disabled)"
            );
        }
        Reincarnatability::CoolingDown(remaining) => {
            println!(
                "/!\\ {KARMIC_STATUS:>WIDTH$}: cooling down \
                 ({remaining:?} remaining)"
            );
        }
    }
    println!("    {LAST_AUTO_RESTART:>WIDTH$}: {time_last_auto_restarted:?}");

    println!("    {ACTIVE_VMM:>WIDTH$}: {propolis_id:?}");
    println!("    {TARGET_VMM:>WIDTH$}: {dst_propolis_id:?}");

    println!(
        "{}{MIGRATION_ID:>WIDTH$}: {migration_id:?}",
        if migration_id.is_some() { "(i) " } else { "    " },
    );
    if let Some(id) = instance.updater_id {
        print!("(i) {UPDATER_LOCK:>WIDTH$}: LOCKED by {id}")
    } else {
        print!("    {UPDATER_LOCK:>WIDTH$}: UNLOCKED");
    }
    println!(" at generation: {}", instance.updater_gen.0);

    fn print_vmm(kind: &str, id: Uuid, vmm: Option<&Vmm>) {
        match vmm {
            Some(vmm) => {
                println!(
                    "\n{:=<80}",
                    format!("== {} VMM ", kind.to_ascii_uppercase())
                );
                prettyprint_vmm("    ", vmm, Some(WIDTH), None, true);
                if vmm.time_deleted.is_some() {
                    eprintln!(
                        "\n/!\\ BAD: dangling foreign key to deleted {kind} \
                         VMM {id}"
                    );
                }
            }
            None => {
                eprintln!(
                    "\n/!\\ BAD: instance has a {kind} VMM with ID {id}, \
                     but no such VMM record exists in the database!"
                );
            }
        }
    }

    if let Some(id) = propolis_id {
        print_vmm("active", id, active_vmm.as_ref());
    }

    if let Some(id) = dst_propolis_id {
        let fetch_result = vmm_dsl::vmm
            .filter(vmm_dsl::id.eq(id))
            .select(Vmm::as_select())
            .limit(1)
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await;
        match fetch_result {
            Ok(rs) => {
                let vmm = rs.into_iter().next();
                print_vmm("target", id, vmm.as_ref());
            }
            Err(e) => {
                eprintln!("error looking up target VMM record {id}: {e}");
            }
        }
    }

    if let Some(id) = migration_id {
        let fetch_result = migration_dsl::migration
            .filter(migration_dsl::id.eq(id))
            .select(Migration::as_select())
            .limit(1)
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await;
        match fetch_result.map(|mut rs| rs.pop()) {
            Ok(Some(migration)) => {
                println!(
                    "\n    {MIGRATION_RECORD:>WIDTH$}:\n{}",
                    textwrap::indent(
                        &format!("{migration:#?}"),
                        &" ".repeat(WIDTH - MIGRATION_RECORD.len() + 8)
                    )
                );
                if migration.time_deleted.is_some() {
                    eprintln!(
                        "\n/!\\ BAD: dangling foreign key to deleted active \
                         migration {id}"
                    );
                }
            }
            Ok(None) => {
                eprintln!(
                    "\n/!\\ BAD: instance has a current migration with ID \
                    {id}, but no such migration exists in the database!"
                );
            }

            Err(e) => {
                eprintln!("error looking up migration record {id}: {e}");
            }
        }
    }

    let ctx = || "listing attached disks";
    let mut query = disk_dsl::disk
        .filter(disk_dsl::attach_instance_id.eq(id.into_untyped_uuid()))
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .order_by(disk_dsl::time_created.desc())
        .into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(disk_dsl::time_deleted.is_null());
    }

    let disks = query
        .select(Disk::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .with_context(ctx)?;

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DiskRow {
        #[tabled(rename = "#", display_with = "display_option_blank")]
        slot: Option<u8>,
        #[tabled(inline)]
        identity: DiskIdentity,
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct MaybeDeletedDiskRow {
        #[tabled(inline)]
        r: DiskRow,
        #[tabled(display_with = "datetime_opt_rfc3339_concise")]
        time_deleted: Option<DateTime<Utc>>,
    }

    impl From<&'_ db::model::Disk> for DiskRow {
        fn from(disk: &db::model::Disk) -> Self {
            Self { slot: disk.slot.map(|s| s.into()), identity: disk.into() }
        }
    }

    impl From<&'_ db::model::Disk> for MaybeDeletedDiskRow {
        fn from(disk: &db::model::Disk) -> Self {
            Self { r: disk.into(), time_deleted: disk.time_deleted() }
        }
    }

    if !disks.is_empty() {
        println!("\n{:=<80}", "== ATTACHED DISKS ");

        check_limit(&disks, fetch_opts.fetch_limit, ctx);
        let mut table = if fetch_opts.include_deleted {
            tabled::Table::new(disks.iter().map(MaybeDeletedDiskRow::from))
        } else {
            tabled::Table::new(disks.iter().map(DiskRow::from))
        };
        table
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0));
        println!("{table}");
    }

    if resources || all {
        use nexus_db_schema::schema::virtual_provisioning_resource::dsl as resource_dsl;
        let resources = resource_dsl::virtual_provisioning_resource
            .filter(resource_dsl::id.eq(id.into_untyped_uuid()))
            .select(db::model::VirtualProvisioningResource::as_select())
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await
            .context("fetching instance virtual provisioning record")?;
        println!("\n{:=<80}", "== VIRTUAL RESOURCES PROVISIONED ");
        if resources.is_empty() {
            println!("(i) no virtual resources provisioned for this instance")
        } else {
            if resources.len() > 1 {
                println!(
                    "/!\\ there should only be one virtual resource record \
                     for a given UUID! this is a bug!",
                );
            }
            for resource in resources {
                let db::model::VirtualProvisioningResource {
                    id: _,
                    time_modified,
                    resource_type,
                    virtual_disk_bytes_provisioned: db::model::ByteCount(disk),
                    cpus_provisioned,
                    ram_provisioned: db::model::ByteCount(ram),
                } = resource;
                const DISK: &'static str = "virtual disk";
                const RAM: &'static str = "RAM";
                const WIDTH: usize = crate::helpers::const_max_len(&[
                    VCPUS,
                    DISK,
                    RAM,
                    LAST_UPDATED,
                ]);
                if resource_type != "instance" {
                    println!(
                        "/!\\ virtual provisioning resource type is \
                 {resource_type:?} (expected \"instance\")",
                    );
                }
                println!("    {VCPUS:>WIDTH$}: {cpus_provisioned}");
                println!("    {RAM:>WIDTH$}: {ram}");
                println!("    {DISK:>WIDTH$}: {disk}");
                if let Some(modified) = time_modified {
                    println!("    {LAST_UPDATED:>WIDTH$}: {modified}")
                }
            }
        }
    }

    if history || all {
        let ctx = || "listing migrations";
        let past_migrations = migration_dsl::migration
            .filter(migration_dsl::instance_id.eq(id.into_untyped_uuid()))
            .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
            .order_by(migration_dsl::time_created.desc())
            // This is just to prove to CRDB that it can use the
            // migrations-by-time-created index, it doesn't actually do anything.
            .filter(
                migration_dsl::time_created.gt(chrono::DateTime::UNIX_EPOCH),
            )
            .select(Migration::as_select())
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await
            .with_context(ctx)?;

        if !past_migrations.is_empty() {
            println!("\n{:=<80}\n", "== MIGRATION HISTORY");

            check_limit(&past_migrations, fetch_opts.fetch_limit, ctx);

            let rows = past_migrations.into_iter().map(|m| {
                SingleInstanceMigrationRow {
                    created: m.time_created,
                    vmms: MigrationVmms::from(&m),
                }
            });

            let table = tabled::Table::new(rows)
                .with(tabled::settings::Style::empty())
                .with(tabled::settings::Padding::new(0, 1, 0, 0))
                .to_string();

            println!("{table}");
        }

        let ctx = || "listing past VMMs";
        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct VmmRow {
            #[tabled(inline)]
            state: VmmStateRow,
            sled_id: SledUuid,
            #[tabled(display_with = "datetime_rfc3339_concise")]
            time_created: chrono::DateTime<Utc>,
            #[tabled(display_with = "datetime_opt_rfc3339_concise")]
            time_deleted: Option<chrono::DateTime<Utc>>,
        }
        let vmms = vmm_dsl::vmm
            .filter(vmm_dsl::instance_id.eq(id.into_untyped_uuid()))
            .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
            .order_by(vmm_dsl::time_created.desc())
            .select(Vmm::as_select())
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await
            .with_context(ctx)?;

        if !vmms.is_empty() {
            println!("\n{:=<80}", "== VMM HISTORY ");

            check_limit(&vmms, fetch_opts.fetch_limit, ctx);

            let table = tabled::Table::new(vmms.iter().map(|vmm| {
                let &Vmm {
                    id,
                    sled_id,
                    propolis_ip: _,
                    propolis_port: _,
                    instance_id: _,
                    cpu_platform: _,
                    time_created,
                    time_deleted,
                    runtime:
                        db::model::VmmRuntimeState {
                            time_state_updated: _,
                            r#gen,
                            state,
                        },
                } = vmm;
                VmmRow {
                    state: VmmStateRow {
                        id,
                        state,
                        generation: r#gen.0.into(),
                    },
                    sled_id: sled_id.into(),
                    time_created,
                    time_deleted,
                }
            }))
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
            println!("{table}");
        }
    }

    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct VmmStateRow {
    id: Uuid,
    state: db::model::VmmState,
    #[tabled(rename = "GEN")]
    generation: u64,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct CustomerInstanceRow {
    id: String,
    state: String,
    intent: InstanceIntendedState,
    propolis_id: MaybePropolisId,
    sled_id: MaybeSledId,
    host_serial: String,
    name: String,
}

/// Run `omdb db instances`: list data about customer VMs.
async fn cmd_db_instances(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    &InstanceListArgs { running, ref states }: &InstanceListArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::instance::dsl;
    use nexus_db_schema::schema::vmm::dsl as vmm_dsl;

    let limit = fetch_opts.fetch_limit;
    let mut query = dsl::instance.into_boxed();
    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    if !states.is_empty() {
        query = query.filter(dsl::state.eq_any(states.clone()));
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
    let mut h_to_s: HashMap<SledUuid, String> = HashMap::new();

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
            intent: i.instance().intended_state,
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
    use nexus_db_schema::schema::dns_version::dsl;
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
        use nexus_db_schema::schema::dns_name::dsl;

        let mut added = dsl::dns_name
            .filter(dsl::dns_zone_id.eq(zone.id))
            .filter(dsl::version_added.eq(version.version))
            .limit(i64::from(u32::from(limit)))
            .select(DnsName::as_select())
            .load_async(&*datastore.pool_connection_for_tests().await?)
            .await
            .context("loading added names")?;
        check_limit(&added, limit, || "loading added names");

        let mut removed = dsl::dns_name
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

        // This is kind of stupid-expensive, but there aren't a lot of records
        // here and it's helpful for this output to be stable.
        added.sort_by_cached_key(|k| format!("{} {:?}", k.name, k.records()));
        removed.sort_by_cached_key(|k| format!("{} {:?}", k.name, k.records()));

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

        for (name, mut records) in names {
            records.sort();
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
    use nexus_db_schema::schema::external_ip::dsl;
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
            use nexus_db_schema::schema::project::dsl as project_dsl;
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
    use nexus_db_schema::schema::network_interface::dsl;
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
            use nexus_db_schema::schema::vpc_subnet::dsl;
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

// REGION SNAPSHOT REPLACEMENTS

/// List all region snapshot replacement requests
async fn cmd_db_region_snapshot_replacement_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &RegionSnapshotReplacementListArgs,
) -> Result<(), anyhow::Error> {
    let ctx = || "listing region snapshot replacement requests".to_string();
    let limit = fetch_opts.fetch_limit;

    let requests: Vec<RegionSnapshotReplacement> = {
        let conn = datastore.pool_connection_for_tests().await?;

        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

        match (args.state, args.after) {
            (Some(state), Some(after)) => {
                dsl::region_snapshot_replacement
                    .filter(dsl::replacement_state.eq(state))
                    .filter(dsl::request_time.gt(after))
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionSnapshotReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }

            (Some(state), None) => {
                dsl::region_snapshot_replacement
                    .filter(dsl::replacement_state.eq(state))
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionSnapshotReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }

            (None, Some(after)) => {
                dsl::region_snapshot_replacement
                    .filter(dsl::request_time.gt(after))
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionSnapshotReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }

            (None, None) => {
                dsl::region_snapshot_replacement
                    .limit(i64::from(u32::from(limit)))
                    .select(RegionSnapshotReplacement::as_select())
                    .get_results_async(&*conn)
                    .await?
            }
        }
    };

    check_limit(&requests, limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct Row {
        pub id: Uuid,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        pub request_time: DateTime<Utc>,
        pub replacement_state: String,
    }

    let mut rows = Vec::with_capacity(requests.len());

    for request in requests {
        rows.push(Row {
            id: request.id,
            request_time: request.request_time,
            replacement_state: format!("{:?}", request.replacement_state),
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .with(tabled::settings::Panel::header(
            "Region snapshot replacement requests",
        ))
        .to_string();

    println!("{}", table);

    Ok(())
}

/// Display all non-complete region snapshot replacements
async fn cmd_db_region_snapshot_replacement_status(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    let ctx = || "listing region snapshot replacement requests".to_string();
    let limit = fetch_opts.fetch_limit;

    let requests: Vec<RegionSnapshotReplacement> = {
        let conn = datastore.pool_connection_for_tests().await?;

        use nexus_db_schema::schema::region_snapshot_replacement::dsl;

        dsl::region_snapshot_replacement
            .filter(
                dsl::replacement_state
                    .ne(RegionSnapshotReplacementState::Complete),
            )
            .limit(i64::from(u32::from(limit)))
            .select(RegionSnapshotReplacement::as_select())
            .get_results_async(&*conn)
            .await?
    };

    check_limit(&requests, limit, ctx);

    for request in requests {
        let steps_left = datastore
            .in_progress_region_snapshot_replacement_steps(opctx, request.id)
            .await?;

        println!("{}:", request.id);
        println!();

        println!("                    started: {}", request.request_time);
        println!(
            "                      state: {:?}",
            request.replacement_state
        );
        match request.replacement_type() {
            ReadOnlyTargetReplacement::RegionSnapshot {
                dataset_id,
                region_id,
                snapshot_id,
            } => {
                println!(
                    "            region snapshot: {} {} {}",
                    dataset_id, region_id, snapshot_id,
                );
            }

            ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
                println!("           read-only region: {}", region_id);
            }
        }
        println!("              new region id: {:?}", request.new_region_id);
        println!("     in-progress steps left: {:?}", steps_left);
        println!();
    }

    Ok(())
}

/// Show details for a single region snapshot replacement
async fn cmd_db_region_snapshot_replacement_info(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &RegionSnapshotReplacementInfoArgs,
) -> Result<(), anyhow::Error> {
    let request = datastore
        .get_region_snapshot_replacement_request_by_id(
            opctx,
            args.replacement_id,
        )
        .await?;

    // Show details
    let steps_left = datastore
        .in_progress_region_snapshot_replacement_steps(opctx, request.id)
        .await?;

    println!("{}:", request.id);
    println!();

    println!("                    started: {}", request.request_time);
    println!("                      state: {:?}", request.replacement_state);
    match request.replacement_type() {
        ReadOnlyTargetReplacement::RegionSnapshot {
            dataset_id,
            region_id,
            snapshot_id,
        } => {
            println!(
                "            region snapshot: {} {} {}",
                dataset_id, region_id, snapshot_id,
            );
        }

        ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
            println!("           read-only region: {}", region_id);
        }
    }
    println!("              new region id: {:?}", request.new_region_id);
    println!("     in-progress steps left: {:?}", steps_left);
    println!();

    Ok(())
}

/// Manually request a region snapshot replacement
async fn cmd_db_region_snapshot_replacement_request(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &RegionSnapshotReplacementRequestArgs,
    _destruction_token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    let Some(region_snapshot) = datastore
        .region_snapshot_get(args.dataset_id, args.region_id, args.snapshot_id)
        .await?
    else {
        bail!("region snapshot not found!");
    };

    let request =
        RegionSnapshotReplacement::for_region_snapshot(&region_snapshot);
    let request_id = request.id;

    // If this function indirectly uses
    // `insert_region_snapshot_replacement_request`, there could be an authz
    // related `ObjectNotFound` due to the opctx being for the privileged test
    // user. Lookup the snapshot here, and directly use
    // `insert_snapshot_replacement_request_with_volume_id` instead.

    let db_snapshots = {
        use nexus_db_schema::schema::snapshot::dsl;
        let conn = datastore.pool_connection_for_tests().await?;
        dsl::snapshot
            .filter(dsl::id.eq(args.snapshot_id))
            .limit(1)
            .select(Snapshot::as_select())
            .load_async(&*conn)
            .await
            .context("loading requested snapshot")?
    };

    assert_eq!(db_snapshots.len(), 1);

    datastore
        .insert_region_snapshot_replacement_request_with_volume_id(
            opctx,
            request,
            db_snapshots[0].volume_id(),
        )
        .await?;

    println!("region snapshot replacement {request_id} created");

    Ok(())
}

async fn cmd_db_region_snapshot_replacement_waiting(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    // A region snapshot replacement is "stuck" if:
    //
    // 1. it is in state "Running", meaning that region snapshot replacement
    //    steps should be created for each volume that references the read-only
    //    target being replaced, and
    //
    // 2. something else has locked one of those volumes
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct HolderRow {
        region_snapshot_replacement_id: String,
        volume_id: String,
        holder_type: String,
        holder_details: String,
        edges: String,
    }

    let mut rows = vec![];
    let mut edges = vec![];

    let running_replacements =
        datastore.get_running_region_snapshot_replacements(opctx).await?;

    for replacement in running_replacements {
        let read_only_target_volume_references =
            match replacement.replacement_type() {
                ReadOnlyTargetReplacement::RegionSnapshot {
                    dataset_id,
                    region_id,
                    snapshot_id,
                } => {
                    datastore
                        .volume_usage_records_for_resource(
                            VolumeResourceUsage::RegionSnapshot {
                                dataset_id: dataset_id.into(),
                                region_id,
                                snapshot_id,
                            },
                        )
                        .await?
                }

                ReadOnlyTargetReplacement::ReadOnlyRegion { region_id } => {
                    datastore
                        .volume_usage_records_for_resource(
                            VolumeResourceUsage::ReadOnlyRegion { region_id },
                        )
                        .await?
                }
            };

        // There should be a step record created for each of these referenced
        // volumes. Check if there's a lock holder for these volumes that isn't
        // the replacement itself.

        let conn = datastore.pool_connection_for_tests().await?;

        for reference in read_only_target_volume_references {
            let maybe_volume_repair_record = {
                use nexus_db_schema::schema::volume_repair::dsl;

                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                dsl::volume_repair
                    .filter(dsl::volume_id.eq(reference.volume_id))
                    .select(VolumeRepair::as_select())
                    .first_async(&*conn)
                    .await
                    .optional()?
            };

            if let Some(volume_repair_record) = maybe_volume_repair_record {
                let lock_holder = get_volume_lock_holder(
                    &conn,
                    volume_repair_record.repair_id,
                )
                .await?;

                match lock_holder {
                    VolumeLockHolder::RegionReplacement { .. } => {}

                    VolumeLockHolder::RegionSnapshotReplacement { id } => {
                        if replacement.id == id {
                            // It's us, skip to the next reference
                            continue;
                        }
                    }

                    VolumeLockHolder::RegionSnapshotReplacementStep {
                        request_id,
                        ..
                    } => {
                        if replacement.id == request_id {
                            // It's us, skip to the next reference
                            continue;
                        }
                    }

                    VolumeLockHolder::Unknown => {}
                }

                edges.push((replacement.id, lock_holder, reference.volume_id));
            }
        }
    }

    // A node with only one outgoing edges is what is holding up replacements
    // (it's waiting on only one thing, where many others might be waiting for
    // it)
    let mut g = petgraph::graph::DiGraph::new();
    let mut node_indices: HashMap<Uuid, petgraph::graph::NodeIndex> =
        HashMap::new();

    for (replacement_id, lock_holder, _) in &edges {
        node_indices.entry(*replacement_id).or_insert_with(|| g.add_node(1));

        if let Some(lock_holder_id) = lock_holder.id() {
            node_indices.entry(lock_holder_id).or_insert_with(|| g.add_node(1));

            let from_ix = node_indices[&replacement_id];
            let to_ix = node_indices[&lock_holder_id];

            g.add_edge(from_ix, to_ix, 1);
        }
    }

    for (replacement_id, lock_holder, reference_volume_id) in edges {
        let number_of_outgoing_edges = g
            .edges_directed(
                node_indices[&replacement_id],
                petgraph::Direction::Outgoing,
            )
            .count();

        rows.push(HolderRow {
            region_snapshot_replacement_id: replacement_id.to_string(),
            volume_id: reference_volume_id.to_string(),
            holder_type: lock_holder.type_string(),
            holder_details: lock_holder.details(),
            // whatever is backed up behind only one thing is probably holding
            // everything else up, so mark that
            edges: if number_of_outgoing_edges == 1 {
                format!("******** {number_of_outgoing_edges}")
            } else {
                format!("         {number_of_outgoing_edges}")
            },
        });
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

// VALIDATION

/// Validate the volume resource usage table
async fn cmd_db_validate_volume_references(
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    // First, get all region snapshot records
    let region_snapshots: Vec<RegionSnapshot> = {
        let region_snapshots: Vec<RegionSnapshot> = datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(async move |conn| {
                // Selecting all region snapshots requires a full table scan
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                use nexus_db_schema::schema::region_snapshot::dsl;
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
        dataset_id: DatasetUuid,
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
                .transaction_async(async move |conn| {
                    // Selecting all volumes based on the data column requires a
                    // full table scan
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                    let pattern = format!("%{}%", &snapshot_addr);

                    use nexus_db_schema::schema::volume::dsl;

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

        let volume_usage_records = datastore
            .volume_usage_records_for_resource(
                VolumeResourceUsage::RegionSnapshot {
                    dataset_id: region_snapshot.dataset_id.into(),
                    region_id: region_snapshot.region_id,
                    snapshot_id: region_snapshot.snapshot_id,
                },
            )
            .await?;

        if matching_volumes != volume_usage_records.len() {
            rows.push(Row {
                dataset_id: region_snapshot.dataset_id.into(),
                region_id: region_snapshot.region_id,
                snapshot_id: region_snapshot.snapshot_id,
                error: format!(
                    "record has {} volume usage records when it should be {}!",
                    volume_usage_records.len(),
                    matching_volumes,
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
                    dataset_id: region_snapshot.dataset_id.into(),
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

enum CleanUpOrphanedRegions {
    Yes { _token: DestructiveOperationToken },
    No,
}

async fn cmd_db_validate_regions(
    datastore: &DataStore,
    clean_up_orphaned_regions: CleanUpOrphanedRegions,
) -> Result<(), anyhow::Error> {
    // *Lifetime note*:
    //
    // The lifetime of the region record in cockroachdb is longer than the time
    // the Crucible agent's region is in a non-destroyed state: Nexus will
    // perform the query to allocate regions (inserting them into the database)
    // before it ensures those regions are created (i.e. making the POST request
    // to the appropriate Crucible agent to create them), and it will request
    // that the regions be deleted (then wait for that region to transition to
    // the destroyed state) before hard-deleting the records in the database.

    // First, get all region records (with their corresponding dataset)
    let datasets_and_regions: Vec<(CrucibleDataset, Region)> = datastore
        .pool_connection_for_tests()
        .await?
        .transaction_async(async move |conn| {
            // Selecting all datasets and regions requires a full table scan
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

            use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
            use nexus_db_schema::schema::region::dsl;

            dsl::region
                .inner_join(
                    dataset_dsl::crucible_dataset
                        .on(dsl::dataset_id.eq(dataset_dsl::id)),
                )
                .select((CrucibleDataset::as_select(), Region::as_select()))
                .get_results_async(&conn)
                .await
        })
        .await?;

    #[derive(Tabled)]
    struct Row {
        dataset_id: DatasetUuid,
        region_id: Uuid,
        dataset_addr: std::net::SocketAddrV6,
        error: String,
    }

    let mut rows = Vec::new();

    // Reconcile with the corresponding Crucible Agent: are they aware of each
    // region in the database?
    for (dataset, region) in &datasets_and_regions {
        // If the dataset was expunged, do not attempt to contact the Crucible
        // agent!
        let in_service = datastore
            .crucible_dataset_physical_disk_in_service(dataset.id())
            .await?;

        if !in_service {
            eprintln!(
                "dataset {} {:?} is not in service, skipping",
                dataset.id(),
                dataset.address(),
            );
            continue;
        }

        use crucible_agent_client::Client as CrucibleAgentClient;
        use crucible_agent_client::types::RegionId;
        use crucible_agent_client::types::State;

        let dataset_addr = dataset.address();
        let url = format!("http://{}", dataset_addr);
        let client = CrucibleAgentClient::new(&url);

        let actual_region =
            match client.region_get(&RegionId(region.id().to_string())).await {
                Ok(region) => region.into_inner(),

                Err(e) => {
                    // Either there was a communication error, or the agent is
                    // unaware of the Region (this would be a 404).
                    match e {
                        crucible_agent_client::Error::ErrorResponse(rv)
                            if rv.status() == http::StatusCode::NOT_FOUND =>
                        {
                            rows.push(Row {
                                dataset_id: dataset.id(),
                                region_id: region.id(),
                                dataset_addr,
                                error: String::from(
                                    "Agent does not know about this region!",
                                ),
                            });
                        }

                        _ => {
                            eprintln!(
                                "{} region_get {:?}: {e}",
                                dataset_addr,
                                region.id(),
                            );
                        }
                    }

                    continue;
                }
            };

        // The Agent is aware of this region, but is it in the appropriate
        // state?

        match actual_region.state {
            State::Destroyed => {
                // If it is destroyed, then this is invalid as the record should
                // be hard-deleted as well (see the lifetime note above). Note
                // that omdb could be racing a Nexus that is performing region
                // deletion: if the region transitioned to Destroyed but Nexus
                // is waiting to re-poll, it will not have hard-deleted the
                // region record yet.

                rows.push(Row {
                    dataset_id: dataset.id(),
                    region_id: region.id(),
                    dataset_addr,
                    error: String::from(
                        "region may need to be manually hard-deleted",
                    ),
                });
            }

            _ => {
                // ok
            }
        }
    }

    // Reconcile with the Crucible agents: are there regions that Nexus does not
    // know about? Ask each Crucible agent for its list of regions, then check
    // in the database: if that region is _not_ in the database, then either it
    // was never created by Nexus, or it was hard-deleted by Nexus. Either way,
    // omdb should (if the command line argument is supplied) request that the
    // orphaned region be deleted.
    //
    // Note: This should not delete what is actually a valid region, see the
    // lifetime note above.

    let mut orphaned_bytes: u64 = 0;

    let db_region_ids: BTreeSet<Uuid> =
        datasets_and_regions.iter().map(|(_, r)| r.id()).collect();

    // Find all the Crucible datasets
    let datasets: Vec<CrucibleDataset> = datastore
        .pool_connection_for_tests()
        .await?
        .transaction_async(async move |conn| {
            // Selecting all datasets and regions requires a full table scan
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

            use nexus_db_schema::schema::crucible_dataset::dsl;

            dsl::crucible_dataset
                .select(CrucibleDataset::as_select())
                .get_results_async(&conn)
                .await
        })
        .await?;

    for dataset in &datasets {
        // If the dataset was expunged, do not attempt to contact the Crucible
        // agent!
        let in_service = datastore
            .crucible_dataset_physical_disk_in_service(dataset.id())
            .await?;

        if !in_service {
            eprintln!(
                "dataset {} {:?} is not in service, skipping",
                dataset.id(),
                dataset.address(),
            );
            continue;
        }

        use crucible_agent_client::Client as CrucibleAgentClient;
        use crucible_agent_client::types::State;

        let dataset_addr = dataset.address();
        let url = format!("http://{}", dataset_addr);
        let client = CrucibleAgentClient::new(&url);

        let actual_regions = match client.region_list().await {
            Ok(v) => v.into_inner(),
            Err(e) => {
                eprintln!("{} region_list: {e}", dataset_addr);
                continue;
            }
        };

        for actual_region in actual_regions {
            // Skip doing anything if the region is already tombstoned or
            // destroyed
            match actual_region.state {
                State::Destroyed | State::Tombstoned => {
                    // the Crucible agent will eventually clean this up, or
                    // already has.
                    continue;
                }

                State::Failed | State::Requested | State::Created => {
                    // this region needs cleaning up if there isn't an
                    // associated db record
                }
            }

            let actual_region_id: Uuid = actual_region.id.0.parse().unwrap();
            if !db_region_ids.contains(&actual_region_id) {
                orphaned_bytes += actual_region.block_size
                    * actual_region.extent_size
                    * u64::from(actual_region.extent_count);

                match clean_up_orphaned_regions {
                    CleanUpOrphanedRegions::Yes { .. } => {
                        match client.region_delete(&actual_region.id).await {
                            Ok(_) => {
                                eprintln!(
                                    "{} region {} deleted ok",
                                    dataset_addr, actual_region.id,
                                );
                            }

                            Err(e) => {
                                eprintln!(
                                    "{} region_delete {:?}: {e}",
                                    dataset_addr, actual_region.id,
                                );
                            }
                        }
                    }

                    CleanUpOrphanedRegions::No => {
                        // Do not delete this region, just print a row
                        rows.push(Row {
                            dataset_id: dataset.id(),
                            region_id: actual_region_id,
                            dataset_addr,
                            error: String::from(
                                "Nexus does not know about this region!",
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

    eprintln!("found {} orphaned bytes", orphaned_bytes);

    Ok(())
}

async fn cmd_db_validate_region_snapshots(
    datastore: &DataStore,
) -> Result<(), anyhow::Error> {
    let mut regions_to_snapshots_map: BTreeMap<Uuid, HashSet<Uuid>> =
        BTreeMap::default();

    // First, get all region snapshot records (with their corresponding dataset)
    let datasets_and_region_snapshots: Vec<(CrucibleDataset, RegionSnapshot)> = {
        let datasets_region_snapshots: Vec<(CrucibleDataset, RegionSnapshot)> =
            datastore
                .pool_connection_for_tests()
                .await?
                .transaction_async(async move |conn| {
                    // Selecting all datasets and region snapshots requires a
                    // full table scan
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                    use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
                    use nexus_db_schema::schema::region_snapshot::dsl;

                    dsl::region_snapshot
                        .inner_join(
                            dataset_dsl::crucible_dataset
                                .on(dsl::dataset_id.eq(dataset_dsl::id)),
                        )
                        .select((
                            CrucibleDataset::as_select(),
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
        dataset_id: DatasetUuid,
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

        // If the dataset was expunged, do not attempt to contact the Crucible
        // agent!
        let in_service = datastore
            .crucible_dataset_physical_disk_in_service(dataset.id())
            .await?;

        if !in_service {
            continue;
        }

        use crucible_agent_client::Client as CrucibleAgentClient;
        use crucible_agent_client::types::RegionId;
        use crucible_agent_client::types::State;

        let dataset_addr = dataset.address();
        let url = format!("http://{}", dataset_addr);
        let client = CrucibleAgentClient::new(&url);

        let actual_region_snapshots = match client
            .region_get_snapshots(&RegionId(
                region_snapshot.region_id.to_string(),
            ))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "{} region_get_snapshots {:?}: {e}",
                    dataset_addr, region_snapshot.region_id,
                );
                continue;
            }
        };

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
                                use nexus_db_schema::schema::snapshot::dsl;

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
                                    dataset_id: region_snapshot.dataset_id.into(),
                                    region_id: region_snapshot.region_id,
                                    snapshot_id: region_snapshot.snapshot_id,
                                    dataset_addr,
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
                                    dataset_id: region_snapshot.dataset_id.into(),
                                    region_id: region_snapshot.region_id,
                                    snapshot_id: region_snapshot.snapshot_id,
                                    dataset_addr,
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
                                dataset_id: region_snapshot.dataset_id.into(),
                                region_id: region_snapshot.region_id,
                                snapshot_id: region_snapshot.snapshot_id,
                                dataset_addr,
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

        let snapshot: Snapshot = {
            use nexus_db_schema::schema::snapshot::dsl;

            dsl::snapshot
                .filter(dsl::id.eq(region_snapshot.snapshot_id))
                .select(Snapshot::as_select())
                .first_async(&*datastore.pool_connection_for_tests().await?)
                .await?
        };

        if datastore.volume_get(snapshot.volume_id()).await?.is_none() {
            rows.push(Row {
                dataset_id: region_snapshot.dataset_id.into(),
                region_id: region_snapshot.region_id,
                snapshot_id: region_snapshot.snapshot_id,
                dataset_addr,
                error: format!("volume {} hard deleted!", snapshot.volume_id,),
            });
        }
    }

    // Second, get all regions
    let datasets_and_regions: Vec<(CrucibleDataset, Region)> = {
        let datasets_and_regions: Vec<(CrucibleDataset, Region)> = datastore
            .pool_connection_for_tests()
            .await?
            .transaction_async(async move |conn| {
                // Selecting all datasets and regions requires a full table scan
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

                use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
                use nexus_db_schema::schema::region::dsl;

                dsl::region
                    .inner_join(
                        dataset_dsl::crucible_dataset
                            .on(dsl::dataset_id.eq(dataset_dsl::id)),
                    )
                    .select((CrucibleDataset::as_select(), Region::as_select()))
                    .get_results_async(&conn)
                    .await
            })
            .await?;

        datasets_and_regions
    };

    // Reconcile with the Crucible agents: are there snapshots that Nexus does
    // not know about?
    for (dataset, region) in datasets_and_regions {
        // If the dataset was expunged, do not attempt to contact the Crucible
        // agent!
        let in_service = datastore
            .crucible_dataset_physical_disk_in_service(dataset.id())
            .await?;

        if !in_service {
            continue;
        }

        use crucible_agent_client::Client as CrucibleAgentClient;
        use crucible_agent_client::types::RegionId;
        use crucible_agent_client::types::State;

        let dataset_addr = dataset.address();
        let url = format!("http://{}", dataset_addr);
        let client = CrucibleAgentClient::new(&url);

        let actual_region_snapshots = match client
            .region_get_snapshots(&RegionId(region.id().to_string()))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "{} region_get_snapshots {:?}: {e}",
                    dataset_addr,
                    region.id(),
                );
                continue;
            }
        };

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
                    dataset_addr,
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
                            dataset_addr,
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
        .with(tabled::settings::Style::psql())
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
            DnsRecord::Aaaa(_) | DnsRecord::A(_) | DnsRecord::Ns(_) => {
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
        DnsRecord::Ns(ns) => format!("NS   {}", ns),
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
                    id_or_latest,
                    show_long_strings,
                    ref filter,
                }),
        }) => {
            cmd_db_inventory_collections_show(
                opctx,
                datastore,
                id_or_latest,
                show_long_strings,
                filter.as_ref(),
            )
            .await
        }
        InventoryCommands::PhysicalDisks(args) => {
            cmd_db_inventory_physical_disks(&conn, limit, args).await
        }
        InventoryCommands::RotPages => {
            cmd_db_inventory_rot_pages(&conn, limit).await
        }
    }
}

async fn cmd_db_inventory_baseboard_ids(
    conn: &DataStoreConnection,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct BaseboardRow {
        id: Uuid,
        part_number: String,
        serial_number: String,
    }

    use nexus_db_schema::schema::hw_baseboard_id::dsl;
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
    conn: &DataStoreConnection,
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
        #[tabled(display_with = "option_impl_display")]
        sign: Option<String>,
    }

    use nexus_db_schema::schema::sw_caboose::dsl;
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
        sign: caboose.sign,
    });
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_inventory_physical_disks(
    conn: &DataStoreConnection,
    limit: NonZeroU32,
    args: InvPhysicalDisksArgs,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct DiskRow {
        inv_collection_id: Uuid,
        sled_id: Uuid,
        slot: i64,
        vendor: String,
        model: String,
        serial: String,
        variant: String,
        firmware: String,
        next_firmware: String,
    }

    use nexus_db_schema::schema::inv_nvme_disk_firmware::dsl as firmware_dsl;
    use nexus_db_schema::schema::inv_physical_disk::dsl as disk_dsl;

    let mut query = disk_dsl::inv_physical_disk.into_boxed();
    query = query.limit(i64::from(u32::from(limit)));

    if let Some(collection_id) = args.collection_id {
        query = query.filter(
            disk_dsl::inv_collection_id.eq(collection_id.into_untyped_uuid()),
        );
    }

    if let Some(sled_id) = args.sled_id {
        query = query.filter(disk_dsl::sled_id.eq(sled_id.into_untyped_uuid()));
    }

    let disks = query
        .left_join(
            firmware_dsl::inv_nvme_disk_firmware.on(
                firmware_dsl::inv_collection_id
                    .eq(disk_dsl::inv_collection_id)
                    .and(firmware_dsl::sled_id.eq(disk_dsl::sled_id))
                    .and(firmware_dsl::slot.eq(disk_dsl::slot)),
            ),
        )
        .select((
            InvPhysicalDisk::as_select(),
            Option::<InvNvmeDiskFirmware>::as_select(),
        ))
        .load_async(&**conn)
        .await
        .context("loading physical disks")?;

    let rows = disks.into_iter().map(|(disk, firmware)| {
        let (active_firmware, next_firmware) =
            if let Some(firmware) = firmware.as_ref() {
                (firmware.current_version(), firmware.next_version())
            } else {
                (None, None)
            };

        DiskRow {
            inv_collection_id: disk.inv_collection_id.into_untyped_uuid(),
            sled_id: disk.sled_id.into_untyped_uuid(),
            slot: disk.slot,
            vendor: disk.vendor,
            model: disk.model.clone(),
            serial: disk.serial.clone(),
            variant: format!("{:?}", disk.variant),
            firmware: active_firmware.unwrap_or("UNKNOWN").to_string(),
            next_firmware: next_firmware.unwrap_or("").to_string(),
        }
    });

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

async fn cmd_db_inventory_rot_pages(
    conn: &DataStoreConnection,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct RotPageRow {
        id: Uuid,
        data_base64: String,
    }

    use nexus_db_schema::schema::sw_root_of_trust_page::dsl;
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
    conn: &DataStoreConnection,
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
        use nexus_db_schema::schema::inv_collection::dsl;
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
            use nexus_db_schema::schema::inv_collection_error::dsl;
            dsl::inv_collection_error
                .filter(dsl::inv_collection_id.eq(collection.id))
                .select(diesel::dsl::count_star())
                .first_async(&**conn)
                .await
                .context("counting errors")?
        };

        let nsps = {
            use nexus_db_schema::schema::inv_service_processor::dsl;
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
    id_or_latest: CollectionIdOrLatest,
    show_long_strings: bool,
    filter: Option<&CollectionDisplayCliFilter>,
) -> Result<(), anyhow::Error> {
    let collection = id_or_latest.to_collection(opctx, datastore).await?;

    let mut display = collection.display();
    if let Some(filter) = filter {
        display.apply_cli_filter(filter);
    }
    display.show_long_strings(show_long_strings);
    println!("{}", display);

    Ok(())
}

// Migrations

async fn cmd_db_migrations_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    args: &MigrationsListArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::migration::dsl;
    use omicron_common::api::internal::nexus;

    let mut state_filters = Vec::new();
    if args.completed {
        state_filters.push(MigrationState(nexus::MigrationState::Completed));
    }
    if args.failed {
        state_filters.push(MigrationState(nexus::MigrationState::Failed));
    }
    if args.in_progress {
        state_filters.push(MigrationState(nexus::MigrationState::InProgress));
    }
    if args.pending {
        state_filters.push(MigrationState(nexus::MigrationState::Pending));
    }

    let mut query = dsl::migration.into_boxed();

    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());
    }

    if !state_filters.is_empty() {
        query = query.filter(
            dsl::source_state
                .eq_any(state_filters.clone())
                .or(dsl::target_state.eq_any(state_filters)),
        );
    }

    if !args.instance_ids.is_empty() {
        query =
            query.filter(dsl::instance_id.eq_any(args.instance_ids.clone()));
    }

    let migrations = query
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .order_by(dsl::time_created)
        // This is just to prove to CRDB that it can use the
        // migrations-by-time-created index, it doesn't actually do anything.
        .filter(dsl::time_created.gt(chrono::DateTime::UNIX_EPOCH))
        .select(Migration::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("listing migrations")?;

    check_limit(&migrations, fetch_opts.fetch_limit, || "listing migrations");

    let table = if args.verbose {
        // If verbose mode is enabled, include the migration's ID as well as the
        // source and target updated timestamps.
        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct VerboseMigrationRow {
            #[tabled(display_with = "datetime_rfc3339_concise")]
            created: chrono::DateTime<Utc>,
            id: Uuid,
            instance: Uuid,
            #[tabled(inline)]
            vmms: MigrationVmms,
            #[tabled(display_with = "datetime_opt_rfc3339_concise")]
            src_updated: Option<chrono::DateTime<Utc>>,
            #[tabled(display_with = "datetime_opt_rfc3339_concise")]
            tgt_updated: Option<chrono::DateTime<Utc>>,
            #[tabled(display_with = "datetime_opt_rfc3339_concise")]
            deleted: Option<chrono::DateTime<Utc>>,
        }

        let rows = migrations.into_iter().map(|m| VerboseMigrationRow {
            id: m.id,
            instance: m.instance_id,
            vmms: MigrationVmms::from(&m),
            src_updated: m.time_source_updated,
            tgt_updated: m.time_target_updated,
            created: m.time_created,
            deleted: m.time_deleted,
        });

        tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string()
    } else if args.instance_ids.len() == 1 {
        // If only the migrations for a single instance are shown, we omit the
        // instance ID row for conciseness sake.
        let rows = migrations.into_iter().map(|m| SingleInstanceMigrationRow {
            created: m.time_created,
            vmms: MigrationVmms::from(&m),
        });

        tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string()
    } else {
        // Otherwise, the default format includes the instance ID, but omits
        // most of the timestamps for brevity.
        #[derive(Tabled)]
        #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
        struct MigrationRow {
            #[tabled(display_with = "datetime_rfc3339_concise")]
            created: chrono::DateTime<Utc>,
            instance: Uuid,
            #[tabled(inline)]
            vmms: MigrationVmms,
        }

        let rows = migrations.into_iter().map(|m| MigrationRow {
            created: m.time_created,
            instance: m.instance_id,
            vmms: MigrationVmms::from(&m),
        });

        tabled::Table::new(rows)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string()
    };

    println!("{table}");

    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct SingleInstanceMigrationRow {
    #[tabled(display_with = "datetime_rfc3339_concise")]
    created: chrono::DateTime<Utc>,
    #[tabled(inline)]
    vmms: MigrationVmms,
}
#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct MigrationVmms {
    src_state: MigrationState,
    tgt_state: MigrationState,
    src_vmm: Uuid,
    tgt_vmm: Uuid,
}

impl From<&'_ Migration> for MigrationVmms {
    fn from(
        &Migration {
            source_propolis_id,
            target_propolis_id,
            source_state,
            target_state,
            ..
        }: &Migration,
    ) -> Self {
        Self {
            src_state: source_state,
            tgt_state: target_state,
            src_vmm: source_propolis_id,
            tgt_vmm: target_propolis_id,
        }
    }
}

impl From<&'_ Migration> for SingleInstanceMigrationRow {
    fn from(migration: &Migration) -> Self {
        Self {
            created: migration.time_created,
            vmms: MigrationVmms::from(migration),
        }
    }
}

// VMMs

async fn cmd_db_vmm_info(
    opctx: &OpContext,
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    &VmmInfoArgs { uuid }: &VmmInfoArgs,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::migration::dsl as migration_dsl;
    use nexus_db_schema::schema::sled_resource_vmm::dsl as resource_dsl;
    use nexus_db_schema::schema::vmm::dsl as vmm_dsl;

    let vmm = vmm_dsl::vmm
        .filter(vmm_dsl::id.eq(uuid))
        .select(Vmm::as_select())
        .limit(1)
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .with_context(|| format!("failed to fetch VMM record for {uuid}"))?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no VMM found with ID {uuid}"))?;
    let sled_result =
        LookupPath::new(opctx, datastore).sled_id(vmm.sled_id()).fetch().await;
    let sled = match sled_result {
        Ok((_, sled)) => Some(sled),
        Err(err) => {
            eprintln!(
                "WARN: failed to fetch sled with ID {}: {err}",
                vmm.sled_id()
            );
            None
        }
    };

    println!("\n{:=<80}", "== VMM ");
    prettyprint_vmm(
        "    ",
        &vmm,
        None,
        sled.as_ref().map(|sled| sled.serial_number()),
        true,
    );

    fn prettyprint_reservation(
        resource: db::model::SledResourceVmm,
        include_sled_id: bool,
    ) {
        use db::model::ByteCount;
        let db::model::SledResourceVmm {
            id: _,
            sled_id,
            resources:
                db::model::Resources {
                    hardware_threads,
                    rss_ram: ByteCount(rss),
                    reservoir_ram: ByteCount(reservoir),
                },
            instance_id: _,
        } = resource;
        const SLED_ID: &'static str = "sled ID";
        const THREADS: &'static str = "hardware threads";
        const RSS: &'static str = "RSS RAM";
        const RESERVOIR: &'static str = "reservoir RAM";
        const WIDTH: usize = const_max_len(&[SLED_ID, THREADS, RSS, RESERVOIR]);
        if include_sled_id {
            println!("    {SLED_ID:>WIDTH$}: {sled_id}");
        }
        println!("    {THREADS:>WIDTH$}: {hardware_threads}");
        println!("    {RSS:>WIDTH$}: {rss}");
        println!("    {RESERVOIR:>WIDTH$}: {reservoir}");
    }

    let reservations = resource_dsl::sled_resource_vmm
        .filter(resource_dsl::id.eq(uuid))
        .select(db::model::SledResourceVmm::as_select())
        .load_async::<db::model::SledResourceVmm>(
            &*datastore.pool_connection_for_tests().await?,
        )
        .await
        .with_context(|| {
            format!("failed to fetch sled resource records for {uuid}")
        })?;

    if !reservations.is_empty() {
        println!("\n{:=<80}", "== SLED RESOURCE RESERVATIONS ");

        let multiple_reservations = reservations.len() > 1;
        if multiple_reservations {
            println!(
                "/!\\ VMM has multiple sled resource reservation records! \
                 This is a bug; please open an issue about it here:\n\
                 https://github.com/oxidecomputer/omicron/issues/new?template=Blank+issue",
            );
        }
        for r in reservations {
            prettyprint_reservation(r, multiple_reservations);
            println!();
        }
    }

    let ctx = || format!("listing migrations involving VMM {uuid}");
    let migrations = migration_dsl::migration
        .filter(
            migration_dsl::source_propolis_id
                .eq(uuid)
                .or(migration_dsl::target_propolis_id.eq(uuid)),
        )
        // A single VMM will typically only have 0-1 migrations in, but it may
        // have any number of migrations out, since attempts to migrate out of
        // the VMM may have failed on the migration target's side.
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .order_by(migration_dsl::time_created)
        // This is just to prove to CRDB that it can use the
        // migrations-by-time-created index, it doesn't actually do anything.
        .filter(migration_dsl::time_created.gt(chrono::DateTime::UNIX_EPOCH))
        .select(db::model::Migration::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .with_context(ctx)?;

    check_limit(&migrations, fetch_opts.fetch_limit, ctx);

    if !migrations.is_empty() {
        println!("\n{:=<80}", "== MIGRATIONS ");
        // TODO: since this command is focused on the individual VMM, we could
        // potentially be a bit fancier when displaying migrations, and print
        // something like "IN"/"OUT" based on the VMM's role in that migration,
        // rather than just sticking its UUID in the source/target column as
        // appropriate.
        let table = tabled::Table::new(
            migrations.iter().map(SingleInstanceMigrationRow::from),
        )
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();
        println!("{table}");
    }

    Ok(())
}

fn prettyprint_vmm(
    indent: &str,
    vmm: &Vmm,
    width: Option<usize>,
    sled_serial: Option<&str>,
    inst_id: bool,
) {
    const ID: &'static str = "ID";
    const CREATED: &'static str = "created at";
    const DELETED: &'static str = "deleted at";
    const UPDATED: &'static str = "updated at";
    const INSTANCE_ID: &'static str = "instance ID";
    const SLED_ID: &'static str = "sled ID";
    const SLED_SERIAL: &'static str = "sled serial";
    const CPU_PLATFORM: &'static str = "CPU platform";
    const ADDRESS: &'static str = "propolis address";
    const STATE: &'static str = "state";
    const WIDTH: usize = const_max_len(&[
        ID,
        CREATED,
        DELETED,
        UPDATED,
        INSTANCE_ID,
        SLED_ID,
        SLED_SERIAL,
        CPU_PLATFORM,
        STATE,
        ADDRESS,
    ]);

    let width = std::cmp::max(width, Some(WIDTH)).unwrap_or(WIDTH);
    let Vmm {
        id,
        time_created,
        time_deleted,
        instance_id,
        sled_id,
        propolis_ip,
        propolis_port,
        cpu_platform,
        runtime: db::model::VmmRuntimeState { state, r#gen, time_state_updated },
    } = vmm;

    println!("{indent}{ID:>width$}: {id}");
    if inst_id {
        println!("{indent}{INSTANCE_ID:>width$}: {instance_id}");
    }
    println!("{indent}{CREATED:>width$}: {time_created}");
    if let Some(deleted) = time_deleted {
        println!("{indent}{DELETED:width$}: {deleted}");
    }
    println!("{indent}{STATE:>width$}: {state}");
    let g = u64::from(r#gen.0);
    println!(
        "{indent}{UPDATED:>width$}: {time_state_updated:?} (generation {g})"
    );

    println!(
        "{indent}{ADDRESS:>width$}: {}:{}",
        propolis_ip.ip(),
        propolis_port.0
    );
    println!("{indent}{SLED_ID:>width$}: {sled_id}");
    if let Some(serial) = sled_serial {
        println!("{indent}{SLED_SERIAL:>width$}: {serial}");
    }
    println!("{indent}{CPU_PLATFORM:>width$}: {cpu_platform}");
}

async fn cmd_db_vmm_list(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
    &VmmListArgs { ref states, verbose }: &VmmListArgs,
) -> Result<(), anyhow::Error> {
    use db::model::VmmState;
    use nexus_db_schema::schema::{sled::dsl as sled_dsl, vmm::dsl};

    let ctx = || "loading VMMs";
    let mut query = dsl::vmm.into_boxed();

    if !fetch_opts.include_deleted {
        query = query.filter(dsl::time_deleted.is_null());

        // If the user wanted to see VMMs in states that the control plane may
        // have soft-deleted, but didn't ask to include deleted records, let
        // them know that some stuff may be missing.
        let maybe_deleted_states =
            states.iter().filter(|s| VmmState::DESTROYABLE_STATES.contains(s));
        for state in maybe_deleted_states {
            eprintln!(
                "WARN: VMMs in the `{state:?}` state may have been deleted, \
                 but `--include-deleted` was not specified",
            );
        }
    }

    if !states.is_empty() {
        query = query.filter(dsl::state.eq_any(states.clone()));
    }

    let vmms = datastore
        .pool_connection_for_tests()
        .await?
        .transaction_async(async move |conn| {
            // If we are including deleted VMMs, we can no longer use indices on
            // the VMM table, which do not index deleted VMMs. Thus, we must
            // allow a full-table scan in that case.
            if fetch_opts.include_deleted {
                conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
            }

            query
                .left_join(sled_dsl::sled.on(sled_dsl::id.eq(dsl::sled_id)))
                .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
                .select((Vmm::as_select(), Option::<Sled>::as_select()))
                .load_async::<(Vmm, Option<Sled>)>(&conn)
                .await
        })
        .await
        .with_context(ctx)?;

    check_limit(&vmms, fetch_opts.fetch_limit, ctx);

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct VmmRow<'a> {
        instance_id: Uuid,
        #[tabled(inline)]
        state: VmmStateRow,
        sled: &'a str,
    }

    impl<'a> From<&'a (Vmm, Option<Sled>)> for VmmRow<'a> {
        fn from((ref vmm, ref sled): &'a (Vmm, Option<Sled>)) -> Self {
            let &Vmm {
                id,
                time_created: _,
                time_deleted: _,
                instance_id,
                sled_id,
                propolis_ip: _,
                propolis_port: _,
                cpu_platform: _,
                runtime:
                    db::model::VmmRuntimeState {
                        state,
                        r#gen,
                        time_state_updated: _,
                    },
            } = vmm;
            let sled = match sled {
                Some(sled) => sled.serial_number(),
                None => {
                    eprintln!("WARN: no sled found with ID {sled_id}");
                    "<unknown>"
                }
            };
            VmmRow {
                instance_id,
                state: VmmStateRow { id, state, generation: r#gen.0.into() },
                sled,
            }
        }
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct VerboseVmmRow<'a> {
        #[tabled(inline)]
        inner: VmmRow<'a>,
        sled_id: SledUuid,
        address: std::net::SocketAddr,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_created: DateTime<Utc>,
        #[tabled(display_with = "datetime_rfc3339_concise")]
        time_updated: DateTime<Utc>,
    }

    impl<'a> From<&'a (Vmm, Option<Sled>)> for VerboseVmmRow<'a> {
        fn from(it: &'a (Vmm, Option<Sled>)) -> Self {
            let Vmm {
                time_created,
                time_deleted: _,
                sled_id,
                propolis_ip,
                propolis_port,
                ref runtime,
                ..
            } = it.0;
            VerboseVmmRow {
                sled_id: sled_id.into(),
                inner: VmmRow::from(it),
                address: std::net::SocketAddr::new(
                    propolis_ip.ip(),
                    propolis_port.into(),
                ),
                time_created,
                time_updated: runtime.time_state_updated,
            }
        }
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct WithDeleted<T: Tabled> {
        #[tabled(inline)]
        inner: T,
        #[tabled(display_with = "datetime_opt_rfc3339_concise")]
        time_deleted: Option<DateTime<Utc>>,
    }

    impl<'a, T> From<&'a (Vmm, Option<Sled>)> for WithDeleted<T>
    where
        T: Tabled + From<&'a (Vmm, Option<Sled>)>,
    {
        fn from(it: &'a (Vmm, Option<Sled>)) -> Self {
            Self { inner: T::from(it), time_deleted: it.0.time_deleted }
        }
    }

    let mut table = match (verbose, fetch_opts.include_deleted) {
        (true, true) => tabled::Table::new(
            vmms.iter().map(WithDeleted::<VerboseVmmRow>::from),
        ),
        (true, false) => {
            tabled::Table::new(vmms.iter().map(VerboseVmmRow::from))
        }
        (false, true) => {
            tabled::Table::new(vmms.iter().map(WithDeleted::<VmmRow>::from))
        }
        (false, false) => tabled::Table::new(vmms.iter().map(VmmRow::from)),
    };
    table
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0));

    println!("{table}");

    Ok(())
}

#[derive(Debug, Args, Clone)]
struct OximeterArgs {
    #[command(subcommand)]
    command: OximeterCommands,
}

#[derive(Debug, Subcommand, Clone)]
enum OximeterCommands {
    /// List metric producers and their assigned collector.
    ListProducers,
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct ProducerRow {
    oximeter_id: Uuid,
    #[tabled(inline)]
    identity: ProducerEndpointIdentity,
    kind: String,
    ip: std::net::IpAddr,
    port: u16,
    interval: f64,
}

impl From<&'_ db::model::ProducerEndpoint> for ProducerRow {
    fn from(producer: &db::model::ProducerEndpoint) -> Self {
        Self {
            identity: producer.into(),
            kind: format!("{:?}", producer.kind),
            ip: producer.ip.ip(),
            port: *producer.port,
            interval: producer.interval,
            oximeter_id: producer.oximeter_id,
        }
    }
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct ProducerEndpointIdentity {
    id: Uuid,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    time_created: DateTime<Utc>,
    #[tabled(display_with = "datetime_rfc3339_concise")]
    time_modified: DateTime<Utc>,
}

impl From<&'_ db::model::ProducerEndpoint> for ProducerEndpointIdentity {
    fn from(producer: &db::model::ProducerEndpoint) -> Self {
        Self {
            id: producer.id(),
            time_created: producer.time_created(),
            time_modified: producer.time_modified(),
        }
    }
}

async fn cmd_db_oximeter_list_producers(
    datastore: &DataStore,
    fetch_opts: &DbFetchOptions,
) -> Result<(), anyhow::Error> {
    use nexus_db_schema::schema::metric_producer::dsl;
    let rows = dsl::metric_producer
        .order_by(dsl::oximeter_id)
        .limit(i64::from(u32::from(fetch_opts.fetch_limit)))
        .select(nexus_db_model::ProducerEndpoint::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading metric producers")?;
    check_limit(&rows, fetch_opts.fetch_limit, || "listing oximeter producers");
    let rows = rows.iter().map(ProducerRow::from);
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

    Ok(())
}

// Format a `chrono::DateTime` in RFC3339 with milliseconds precision and using
// `Z` rather than the UTC offset for UTC timestamps, to save a few characters
// of line width in tabular output.
fn datetime_rfc3339_concise(t: &DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::format::SecondsFormat::Millis, true)
}
fn option_datetime_rfc3339_concise(t: &Option<DateTime<Utc>>) -> String {
    if let Some(t) = t {
        t.to_rfc3339_opts(chrono::format::SecondsFormat::Millis, true)
    } else {
        String::from("")
    }
}

// Format an optional `chrono::DateTime` in RFC3339 with milliseconds precision
// and using `Z` rather than the UTC offset for UTC timestamps, to save a few
// characters of line width in tabular output.
fn datetime_opt_rfc3339_concise(t: &Option<DateTime<Utc>>) -> String {
    t.map(|t| t.to_rfc3339_opts(chrono::format::SecondsFormat::Millis, true))
        .unwrap_or_else(|| "-".to_string())
}

async fn cmd_db_zpool_list(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &ZpoolListArgs,
) -> Result<(), anyhow::Error> {
    let zpools = datastore.zpool_list_all_external_batched(opctx).await?;

    let Some(latest_collection) =
        datastore.inventory_get_latest_collection(opctx).await?
    else {
        bail!("no latest inventory found!");
    };

    let mut zpool_total_size: HashMap<ZpoolUuid, i64> = HashMap::new();

    for sled_agent in latest_collection.sled_agents {
        for zpool in sled_agent.zpools {
            zpool_total_size.insert(zpool.id, zpool.total_size.into());
        }
    }

    #[derive(Tabled)]
    #[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
    struct ZpoolRow {
        id: ZpoolUuid,
        time_deleted: String,
        sled_id: SledUuid,
        physical_disk_id: PhysicalDiskUuid,
        #[tabled(display_with = "option_impl_display")]
        total_size: Option<i64>,
        control_plane_storage_buffer: i64,
    }

    let rows: Vec<ZpoolRow> = zpools
        .into_iter()
        .map(|(p, _)| {
            let zpool_id = p.id();
            Ok(ZpoolRow {
                id: zpool_id,
                time_deleted: match p.time_deleted() {
                    Some(t) => t.to_string(),
                    None => String::from(""),
                },
                sled_id: p.sled_id(),
                physical_disk_id: p.physical_disk_id(),
                total_size: zpool_total_size.get(&zpool_id).cloned(),
                control_plane_storage_buffer: p
                    .control_plane_storage_buffer()
                    .into(),
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    if args.id_only {
        for row in rows {
            println!("{}", row.id);
        }
    } else {
        let table = tabled::Table::new(rows)
            .with(tabled::settings::Style::psql())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();

        println!("{}", table);
    }

    Ok(())
}

async fn cmd_db_zpool_set_storage_buffer(
    opctx: &OpContext,
    datastore: &DataStore,
    args: &SetStorageBufferArgs,
    _token: DestructiveOperationToken,
) -> Result<(), anyhow::Error> {
    datastore
        .zpool_set_control_plane_storage_buffer(
            opctx,
            ZpoolUuid::from_untyped_uuid(args.id),
            args.storage_buffer,
        )
        .await?;

    println!(
        "set pool {} control plane storage buffer bytes to {}",
        args.id, args.storage_buffer,
    );

    Ok(())
}
