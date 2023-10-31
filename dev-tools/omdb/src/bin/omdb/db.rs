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

// NOTE: eminates from Tabled macros
#![allow(clippy::useless_vec)]

use crate::Omdb;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::SecondsFormat;
use clap::Args;
use clap::Subcommand;
use clap::ValueEnum;
use diesel::expression::SelectableHelper;
use diesel::query_dsl::QueryDsl;
use diesel::BoolExpressionMethods;
use diesel::ExpressionMethods;
use diesel::JoinOnDsl;
use diesel::NullableExpressionMethods;
use nexus_db_model::Dataset;
use nexus_db_model::Disk;
use nexus_db_model::DnsGroup;
use nexus_db_model::DnsName;
use nexus_db_model::DnsVersion;
use nexus_db_model::DnsZone;
use nexus_db_model::ExternalIp;
use nexus_db_model::Instance;
use nexus_db_model::Project;
use nexus_db_model::Region;
use nexus_db_model::RegionSnapshot;
use nexus_db_model::Sled;
use nexus_db_model::Snapshot;
use nexus_db_model::SnapshotState;
use nexus_db_model::Vmm;
use nexus_db_model::Zpool;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db;
use nexus_db_queries::db::datastore::InstanceAndActiveVmm;
use nexus_db_queries::db::identity::Asset;
use nexus_db_queries::db::lookup::LookupPath;
use nexus_db_queries::db::model::ServiceKind;
use nexus_db_queries::db::DataStore;
use nexus_types::identity::Resource;
use nexus_types::internal_api::params::DnsRecord;
use nexus_types::internal_api::params::Srv;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Generation;
use omicron_common::postgres_config::PostgresConfigWithUrl;
use std::cmp::Ordering;
use std::collections::BTreeMap;
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
    /// URL of the database SQL interface
    #[clap(long, env("OMDB_DB_URL"))]
    db_url: Option<PostgresConfigWithUrl>,

    /// limit to apply to queries that fetch rows
    #[clap(
        long = "fetch-limit",
        default_value_t = NonZeroU32::new(500).unwrap()
    )]
    fetch_limit: NonZeroU32,

    #[command(subcommand)]
    command: DbCommands,
}

/// Subcommands that query or update the database
#[derive(Debug, Subcommand)]
enum DbCommands {
    /// Print information about disks
    Disks(DiskArgs),
    /// Print information about internal and external DNS
    Dns(DnsArgs),
    /// Print information about control plane services
    Services(ServicesArgs),
    /// Print information about sleds
    Sleds,
    /// Print information about customer instances
    Instances,
    /// Print information about the network
    Network(NetworkArgs),
    /// Print information about snapshots
    Snapshots(SnapshotArgs),
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
struct ServicesArgs {
    #[command(subcommand)]
    command: ServicesCommands,
}

#[derive(Debug, Subcommand)]
enum ServicesCommands {
    /// List service instances
    ListInstances,
    /// List service instances, grouped by sled
    ListBySled,
}

#[derive(Debug, Args)]
struct NetworkArgs {
    #[command(subcommand)]
    command: NetworkCommands,

    /// Print out raw data structures from the data store.
    #[clap(long)]
    verbose: bool,
}

#[derive(Debug, Subcommand)]
enum NetworkCommands {
    /// List external IPs
    ListEips,
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

impl DbArgs {
    /// Run a `omdb db` subcommand.
    pub(crate) async fn run_cmd(
        &self,
        omdb: &Omdb,
        log: &slog::Logger,
    ) -> Result<(), anyhow::Error> {
        let db_url = match &self.db_url {
            Some(cli_or_env_url) => cli_or_env_url.clone(),
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
                .context("failed to parse constructed postgres URL")?
            }
        };
        eprintln!("note: using database URL {}", &db_url);

        let db_config = db::Config { url: db_url.clone() };
        let pool = Arc::new(db::Pool::new(&log.clone(), &db_config));

        // Being a dev tool, we want to try this operation even if the schema
        // doesn't match what we expect.  So we use `DataStore::new_unchecked()`
        // here.  We will then check the schema version explicitly and warn the
        // user if it doesn't match.
        let datastore = Arc::new(
            DataStore::new_unchecked(pool)
                .map_err(|e| anyhow!(e).context("creating datastore"))?,
        );
        check_schema_version(&datastore).await;

        let opctx = OpContext::for_tests(log.clone(), datastore.clone());
        match &self.command {
            DbCommands::Disks(DiskArgs {
                command: DiskCommands::Info(uuid),
            }) => cmd_db_disk_info(&opctx, &datastore, uuid).await,
            DbCommands::Disks(DiskArgs { command: DiskCommands::List }) => {
                cmd_db_disk_list(&datastore, self.fetch_limit).await
            }
            DbCommands::Disks(DiskArgs {
                command: DiskCommands::Physical(uuid),
            }) => {
                cmd_db_disk_physical(&opctx, &datastore, self.fetch_limit, uuid)
                    .await
            }
            DbCommands::Dns(DnsArgs { command: DnsCommands::Show }) => {
                cmd_db_dns_show(&opctx, &datastore, self.fetch_limit).await
            }
            DbCommands::Dns(DnsArgs { command: DnsCommands::Diff(args) }) => {
                cmd_db_dns_diff(&opctx, &datastore, self.fetch_limit, args)
                    .await
            }
            DbCommands::Dns(DnsArgs { command: DnsCommands::Names(args) }) => {
                cmd_db_dns_names(&opctx, &datastore, self.fetch_limit, args)
                    .await
            }
            DbCommands::Services(ServicesArgs {
                command: ServicesCommands::ListInstances,
            }) => {
                cmd_db_services_list_instances(
                    &opctx,
                    &datastore,
                    self.fetch_limit,
                )
                .await
            }
            DbCommands::Services(ServicesArgs {
                command: ServicesCommands::ListBySled,
            }) => {
                cmd_db_services_list_by_sled(
                    &opctx,
                    &datastore,
                    self.fetch_limit,
                )
                .await
            }
            DbCommands::Sleds => {
                cmd_db_sleds(&opctx, &datastore, self.fetch_limit).await
            }
            DbCommands::Instances => {
                cmd_db_instances(&opctx, &datastore, self.fetch_limit).await
            }
            DbCommands::Network(NetworkArgs {
                command: NetworkCommands::ListEips,
                verbose,
            }) => {
                cmd_db_eips(&opctx, &datastore, self.fetch_limit, *verbose)
                    .await
            }
            DbCommands::Snapshots(SnapshotArgs {
                command: SnapshotCommands::Info(uuid),
            }) => cmd_db_snapshot_info(&opctx, &datastore, uuid).await,
            DbCommands::Snapshots(SnapshotArgs {
                command: SnapshotCommands::List,
            }) => cmd_db_snapshot_list(&datastore, self.fetch_limit).await,
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
    let expected_version = nexus_db_model::schema::SCHEMA_VERSION;
    let version_check = datastore.database_schema_version().await;

    match version_check {
        Ok(found_version) => {
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
        eprintln!(
            "WARN: {}: found {} items (the limit).  There may be more items \
            that were ignored.  Consider overriding with --fetch-limit.",
            context(),
            items.len(),
        );
    }
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

// Disks

/// Run `omdb db disk list`.
async fn cmd_db_disk_list(
    datastore: &DataStore,
    limit: NonZeroU32,
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
    let disks = dsl::disk
        .filter(dsl::time_deleted.is_null())
        .limit(i64::from(u32::from(limit)))
        .select(Disk::as_select())
        .load_async(&*datastore.pool_connection_for_tests().await?)
        .await
        .context("loading disks")?;

    check_limit(&disks, limit, ctx);

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
    limit: NonZeroU32,
    args: &DiskPhysicalArgs,
) -> Result<(), anyhow::Error> {
    let conn = datastore.pool_connection_for_tests().await?;

    // We start by finding any zpools that are using the physical disk.
    use db::schema::zpool::dsl as zpool_dsl;
    let zpools = zpool_dsl::zpool
        .filter(zpool_dsl::time_deleted.is_null())
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
        let datasets = dataset_dsl::dataset
            .filter(dataset_dsl::time_deleted.is_null())
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
    let disks = dsl::disk
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::volume_id.eq_any(volume_ids))
        .limit(i64::from(u32::from(limit)))
        .select(Disk::as_select())
        .load_async(&*conn)
        .await
        .context("loading disks")?;

    check_limit(&disks, limit, || "listing disks".to_string());

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
    let snapshots = snapshot_dsl::snapshot
        .filter(snapshot_dsl::time_deleted.is_null())
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
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    let ctx = || "listing snapshots".to_string();

    use db::schema::snapshot::dsl;
    let snapshots = dsl::snapshot
        .filter(dsl::time_deleted.is_null())
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

/// Run `omdb db services list-instances`.
async fn cmd_db_services_list_instances(
    opctx: &OpContext,
    datastore: &DataStore,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    let sled_list = datastore
        .sled_list(&opctx, &first_page(limit))
        .await
        .context("listing sleds")?;
    check_limit(&sled_list, limit, || String::from("listing sleds"));

    let sleds: BTreeMap<Uuid, Sled> =
        sled_list.into_iter().map(|s| (s.id(), s)).collect();

    let mut rows = vec![];

    for service_kind in ServiceKind::iter() {
        let context =
            || format!("listing instances of kind {:?}", service_kind);
        let instances = datastore
            .services_list_kind(&opctx, service_kind, &first_page(limit))
            .await
            .with_context(&context)?;
        check_limit(&instances, limit, &context);

        rows.extend(instances.into_iter().map(|instance| {
            let addr =
                std::net::SocketAddrV6::new(*instance.ip, *instance.port, 0, 0)
                    .to_string();

            ServiceInstanceRow {
                kind: format!("{:?}", service_kind),
                instance_id: instance.id(),
                addr,
                sled_serial: sleds
                    .get(&instance.sled_id)
                    .map(|s| s.serial_number())
                    .unwrap_or("unknown")
                    .to_string(),
            }
        }));
    }

    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
        .to_string();

    println!("{}", table);

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

/// Run `omdb db services list-by-sled`.
async fn cmd_db_services_list_by_sled(
    opctx: &OpContext,
    datastore: &DataStore,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    let sled_list = datastore
        .sled_list(&opctx, &first_page(limit))
        .await
        .context("listing sleds")?;
    check_limit(&sled_list, limit, || String::from("listing sleds"));

    let sleds: BTreeMap<Uuid, Sled> =
        sled_list.into_iter().map(|s| (s.id(), s)).collect();
    let mut services_by_sled: BTreeMap<Uuid, Vec<ServiceInstanceSledRow>> =
        BTreeMap::new();

    for service_kind in ServiceKind::iter() {
        let context =
            || format!("listing instances of kind {:?}", service_kind);
        let instances = datastore
            .services_list_kind(&opctx, service_kind, &first_page(limit))
            .await
            .with_context(&context)?;
        check_limit(&instances, limit, &context);

        for i in instances {
            let addr =
                std::net::SocketAddrV6::new(*i.ip, *i.port, 0, 0).to_string();
            let sled_instances =
                services_by_sled.entry(i.sled_id).or_insert_with(Vec::new);
            sled_instances.push(ServiceInstanceSledRow {
                kind: format!("{:?}", service_kind),
                instance_id: i.id(),
                addr,
            })
        }
    }

    for (sled_id, instances) in services_by_sled {
        println!(
            "sled: {} (id {})\n",
            sleds.get(&sled_id).map(|s| s.serial_number()).unwrap_or("unknown"),
            sled_id,
        );
        let table = tabled::Table::new(instances)
            .with(tabled::settings::Style::empty())
            .with(tabled::settings::Padding::new(0, 1, 0, 0))
            .to_string();
        println!("{}", textwrap::indent(&table.to_string(), "  "));
        println!("");
    }

    Ok(())
}

#[derive(Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct SledRow {
    serial: String,
    ip: String,
    role: &'static str,
    id: Uuid,
}

impl From<Sled> for SledRow {
    fn from(s: Sled) -> Self {
        SledRow {
            id: s.id(),
            serial: s.serial_number().to_string(),
            ip: s.address().to_string(),
            role: if s.is_scrimlet() { "scrimlet" } else { "-" },
        }
    }
}

/// Run `omdb db sleds`.
async fn cmd_db_sleds(
    opctx: &OpContext,
    datastore: &DataStore,
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    let sleds = datastore
        .sled_list(&opctx, &first_page(limit))
        .await
        .context("listing sleds")?;
    check_limit(&sleds, limit, || String::from("listing sleds"));

    let rows = sleds.into_iter().map(|s| SledRow::from(s));
    let table = tabled::Table::new(rows)
        .with(tabled::settings::Style::empty())
        .with(tabled::settings::Padding::new(0, 1, 0, 0))
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
    limit: NonZeroU32,
) -> Result<(), anyhow::Error> {
    use db::schema::instance::dsl;
    use db::schema::vmm::dsl as vmm_dsl;
    let instances: Vec<InstanceAndActiveVmm> = dsl::instance
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
    limit: NonZeroU32,
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
    limit: NonZeroU32,
    args: &DnsVersionArgs,
) -> Result<(), anyhow::Error> {
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
    limit: NonZeroU32,
    args: &DnsVersionArgs,
) -> Result<(), anyhow::Error> {
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
    limit: NonZeroU32,
    verbose: bool,
) -> Result<(), anyhow::Error> {
    use db::schema::external_ip::dsl;
    let ips: Vec<ExternalIp> = dsl::external_ip
        .filter(dsl::time_deleted.is_null())
        .select(ExternalIp::as_select())
        .get_results_async(&*datastore.pool_connection_for_tests().await?)
        .await?;

    check_limit(&ips, limit, || String::from("listing external ips"));

    struct PortRange {
        first: u16,
        last: u16,
    }

    impl Display for PortRange {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}/{}", self.first, self.last)
        }
    }

    #[derive(Tabled)]
    enum Owner {
        Instance { project: String, name: String },
        Service { kind: String },
        None,
    }

    impl Display for Owner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Instance { project, name } => {
                    write!(f, "Instance {project}/{name}")
                }
                Self::Service { kind } => write!(f, "Service {kind}"),
                Self::None => write!(f, "None"),
            }
        }
    }

    #[derive(Tabled)]
    struct IpRow {
        ip: ipnetwork::IpNetwork,
        ports: PortRange,
        kind: String,
        owner: Owner,
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

    for ip in &ips {
        let owner = if let Some(owner_id) = ip.parent_id {
            if ip.is_service {
                let service = match LookupPath::new(opctx, datastore)
                    .service_id(owner_id)
                    .fetch()
                    .await
                {
                    Ok(instance) => instance,
                    Err(e) => {
                        eprintln!(
                            "error looking up service with id {owner_id}: {e}"
                        );
                        continue;
                    }
                };
                Owner::Service { kind: format!("{:?}", service.1.kind) }
            } else {
                use db::schema::instance::dsl as instance_dsl;
                let instance = match instance_dsl::instance
                    .filter(instance_dsl::id.eq(owner_id))
                    .limit(1)
                    .select(Instance::as_select())
                    .load_async(&*datastore.pool_connection_for_tests().await?)
                    .await
                    .context("loading requested instance")?
                    .pop()
                {
                    Some(instance) => instance,
                    None => {
                        eprintln!("instance with id {owner_id} not found");
                        continue;
                    }
                };

                use db::schema::project::dsl as project_dsl;
                let project = match project_dsl::project
                    .filter(project_dsl::id.eq(instance.project_id))
                    .limit(1)
                    .select(Project::as_select())
                    .load_async(&*datastore.pool_connection_for_tests().await?)
                    .await
                    .context("loading requested project")?
                    .pop()
                {
                    Some(instance) => instance,
                    None => {
                        eprintln!(
                            "project with id {} not found",
                            instance.project_id
                        );
                        continue;
                    }
                };

                Owner::Instance {
                    project: project.name().to_string(),
                    name: instance.name().to_string(),
                }
            }
        } else {
            Owner::None
        };

        let row = IpRow {
            ip: ip.ip,
            ports: PortRange {
                first: ip.first_port.into(),
                last: ip.last_port.into(),
            },
            kind: format!("{:?}", ip.kind),
            owner,
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
