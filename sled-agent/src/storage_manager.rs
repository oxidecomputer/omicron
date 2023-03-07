// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Management of sled-local storage.

use crate::nexus::LazyNexusClient;
use crate::params::DatasetKind;
use crate::profile::*;
use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;
use illumos_utils::dladm::Etherstub;
use illumos_utils::link::VnicAllocator;
use illumos_utils::running_zone::{InstalledZone, RunningZone};
use illumos_utils::zone::AddressRequest;
use illumos_utils::zpool::ZpoolName;
use illumos_utils::{zfs::Mountpoint, zone::ZONE_PREFIX, zpool::ZpoolInfo};
use nexus_client::types::PhysicalDiskDeleteRequest;
use nexus_client::types::PhysicalDiskKind;
use nexus_client::types::PhysicalDiskPutRequest;
use nexus_client::types::ZpoolPutRequest;
use omicron_common::api::external::{ByteCount, ByteCountRangeError};
use omicron_common::backoff;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::{Disk, DiskIdentity, DiskVariant, UnparsedDisk};
use slog::Logger;
use std::collections::hash_map;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::{IpAddr, Ipv6Addr, SocketAddrV6};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(test)]
use illumos_utils::{zfs::MockZfs as Zfs, zpool::MockZpool as Zpool};
#[cfg(not(test))]
use illumos_utils::{zfs::Zfs, zpool::Zpool};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    DiskError(#[from] sled_hardware::DiskError),

    // TODO: We could add the context of "why are we doint this op", maybe?
    #[error(transparent)]
    ZfsListDataset(#[from] illumos_utils::zfs::ListDatasetsError),

    #[error(transparent)]
    ZfsEnsureFilesystem(#[from] illumos_utils::zfs::EnsureFilesystemError),

    #[error(transparent)]
    ZfsSetValue(#[from] illumos_utils::zfs::SetValueError),

    #[error(transparent)]
    ZfsGetValue(#[from] illumos_utils::zfs::GetValueError),

    #[error(transparent)]
    GetZpoolInfo(#[from] illumos_utils::zpool::GetInfoError),

    #[error(transparent)]
    Fstyp(#[from] illumos_utils::fstyp::Error),

    #[error(transparent)]
    ZoneCommand(#[from] illumos_utils::running_zone::RunCommandError),

    #[error(transparent)]
    ZoneBoot(#[from] illumos_utils::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] illumos_utils::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] illumos_utils::running_zone::InstallZoneError),

    #[error("Error parsing pool {name}'s size: {err}")]
    BadPoolSize {
        name: String,
        #[source]
        err: ByteCountRangeError,
    },

    #[error("Failed to parse the dataset {name}'s UUID: {err}")]
    ParseDatasetUuid {
        name: String,
        #[source]
        err: uuid::Error,
    },

    #[error("Zpool Not Found: {0}")]
    ZpoolNotFound(String),

    #[error("Failed to serialize toml (intended for {path:?}): {err}")]
    Serialize {
        path: PathBuf,
        #[source]
        err: toml::ser::Error,
    },

    #[error("Failed to deserialize toml from {path:?}: {err}")]
    Deserialize {
        path: PathBuf,
        #[source]
        err: toml::de::Error,
    },

    #[error("Failed to perform I/O: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },
}

/// A ZFS storage pool.
struct Pool {
    id: Uuid,
    info: ZpoolInfo,
    // ZFS filesytem UUID -> Zone.
    zones: HashMap<Uuid, RunningZone>,
}

impl Pool {
    /// Queries for an existing Zpool by name.
    ///
    /// Returns Ok if the pool exists.
    fn new(name: &ZpoolName) -> Result<Pool, Error> {
        let info = Zpool::get_info(&name.to_string())?;

        // NOTE: This relies on the name being a UUID exactly.
        // We could be more flexible...
        Ok(Pool { id: name.id(), info, zones: HashMap::new() })
    }

    /// Associate an already running zone with this pool object.
    ///
    /// Typically this is used when a dataset within the zone (identified
    /// by ID) has a running zone (e.g. Crucible, Cockroach) operating on
    /// behalf of that data.
    fn add_zone(&mut self, id: Uuid, zone: RunningZone) {
        self.zones.insert(id, zone);
    }

    /// Access a zone managing data within this pool.
    fn get_zone(&self, id: Uuid) -> Option<&RunningZone> {
        self.zones.get(&id)
    }

    /// Returns the ID of the pool itself.
    fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the path for the configuration of a particular
    /// dataset within the pool. This configuration file provides
    /// the necessary information for zones to "launch themselves"
    /// after a reboot.
    async fn dataset_config_path(
        &self,
        dataset_id: Uuid,
    ) -> Result<PathBuf, Error> {
        let path = std::path::Path::new(omicron_common::OMICRON_CONFIG_PATH)
            .join(self.id.to_string());
        create_dir_all(&path).await.map_err(|err| Error::Io {
            message: format!("creating config dir {path:?}, which would contain config for {dataset_id}"),
            err,
        })?;
        let mut path = path.join(dataset_id.to_string());
        path.set_extension("toml");
        Ok(path)
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
struct DatasetName {
    // A unique identifier for the Zpool on which the dataset is stored.
    pool_name: String,
    // A name for the dataset within the Zpool.
    dataset_name: String,
}

impl DatasetName {
    fn new(pool_name: &str, dataset_name: &str) -> Self {
        Self {
            pool_name: pool_name.to_string(),
            dataset_name: dataset_name.to_string(),
        }
    }

    fn full(&self) -> String {
        format!("{}/{}", self.pool_name, self.dataset_name)
    }
}

// Description of a dataset within a ZFS pool, which should be created
// by the Sled Agent.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
struct DatasetInfo {
    address: SocketAddrV6,
    kind: DatasetKind,
    name: DatasetName,
}

impl DatasetInfo {
    fn new(
        pool: &str,
        kind: DatasetKind,
        address: SocketAddrV6,
    ) -> DatasetInfo {
        match kind {
            DatasetKind::CockroachDb { .. } => DatasetInfo {
                name: DatasetName::new(pool, "cockroachdb"),
                address,
                kind,
            },
            DatasetKind::Clickhouse { .. } => DatasetInfo {
                name: DatasetName::new(pool, "clickhouse"),
                address,
                kind,
            },
            DatasetKind::Crucible { .. } => DatasetInfo {
                name: DatasetName::new(pool, "crucible"),
                address,
                kind,
            },
        }
    }

    fn zone_prefix(&self) -> String {
        format!("{}{}_", ZONE_PREFIX, self.name.full())
    }
}

async fn add_profile_to_zone(
    log: &Logger,
    installed_zone: &InstalledZone,
    profile: ProfileBuilder,
) -> Result<(), Error> {
    info!(log, "Profile for {}:\n{}", installed_zone.name(), profile);

    let profile_path = format!(
        "{zone_mountpoint}/{zone}/root/var/svc/profile/site.xml",
        zone_mountpoint = illumos_utils::zfs::ZONE_ZFS_DATASET_MOUNTPOINT,
        zone = installed_zone.name(),
    );

    std::fs::write(&profile_path, format!("{profile}").as_bytes()).map_err(
        |err| Error::Io {
            message: format!("Cannot write to {profile_path}"),
            err,
        },
    )?;
    Ok(())
}

// Ensures that a zone backing a particular dataset is running.
async fn ensure_running_zone(
    log: &Logger,
    vnic_allocator: &VnicAllocator<Etherstub>,
    dataset_info: &DatasetInfo,
    dataset_name: &DatasetName,
    do_format: bool,
    underlay_address: Ipv6Addr,
) -> Result<RunningZone, Error> {
    let address_request = AddressRequest::new_static(
        IpAddr::V6(*dataset_info.address.ip()),
        None,
    );

    let err =
        RunningZone::get(log, &dataset_info.zone_prefix(), address_request)
            .await;
    match err {
        Ok(zone) => {
            info!(log, "Zone for {} is already running", dataset_name.full());
            return Ok(zone);
        }
        Err(illumos_utils::running_zone::GetZoneError::NotFound { .. }) => {
            info!(log, "Zone for {} was not found", dataset_name.full());

            let installed_zone = InstalledZone::install(
                log,
                vnic_allocator,
                &dataset_info.name.dataset_name,
                Some(&dataset_name.pool_name),
                &[zone::Dataset { name: dataset_name.full() }],
                &[],
                vec![],
                None,
                None,
                vec![],
            )
            .await?;

            let datalink = installed_zone.get_control_vnic_name();
            let gateway = &underlay_address.to_string();
            let listen_addr = &dataset_info.address.ip().to_string();
            let listen_port = &dataset_info.address.port().to_string();

            let zone = match dataset_info.kind {
                DatasetKind::CockroachDb { .. } => {
                    let config = PropertyGroupBuilder::new("config")
                        .add_property("datalink", "astring", datalink)
                        .add_property("gateway", "astring", gateway)
                        .add_property("listen_addr", "astring", listen_addr)
                        .add_property("listen_port", "astring", listen_port)
                        .add_property("store", "astring", "/data");

                    let profile = ProfileBuilder::new("omicron").add_service(
                        ServiceBuilder::new("system/illumos/cockroachdb")
                            .add_property_group(config),
                    );
                    add_profile_to_zone(log, &installed_zone, profile).await?;
                    let zone = RunningZone::boot(installed_zone).await?;

                    // Await liveness of the cluster.
                    info!(log, "start_zone: awaiting liveness of CRDB");
                    let check_health = || async {
                        let http_addr = SocketAddrV6::new(
                            *dataset_info.address.ip(),
                            8080,
                            0,
                            0,
                        );
                        reqwest::get(format!(
                            "http://{}/health?ready=1",
                            http_addr
                        ))
                        .await
                        .map_err(backoff::BackoffError::transient)
                    };
                    let log_failure = |_, _| {
                        warn!(log, "cockroachdb not yet alive");
                    };
                    backoff::retry_notify(
                        backoff::internal_service_policy(),
                        check_health,
                        log_failure,
                    )
                    .await
                    .expect("expected an infinite retry loop waiting for crdb");

                    info!(log, "CRDB is online");
                    // If requested, format the cluster with the initial tables.
                    if do_format {
                        info!(log, "Formatting CRDB");
                        zone.run_cmd(&[
                            "/opt/oxide/cockroachdb/bin/cockroach",
                            "sql",
                            "--insecure",
                            "--host",
                            &dataset_info.address.to_string(),
                            "--file",
                            "/opt/oxide/cockroachdb/sql/dbwipe.sql",
                        ])?;
                        zone.run_cmd(&[
                            "/opt/oxide/cockroachdb/bin/cockroach",
                            "sql",
                            "--insecure",
                            "--host",
                            &dataset_info.address.to_string(),
                            "--file",
                            "/opt/oxide/cockroachdb/sql/dbinit.sql",
                        ])?;
                        info!(log, "Formatting CRDB - Completed");
                    }

                    zone
                }
                DatasetKind::Clickhouse { .. } => {
                    let config = PropertyGroupBuilder::new("config")
                        .add_property("datalink", "astring", datalink)
                        .add_property("gateway", "astring", gateway)
                        .add_property("listen_addr", "astring", listen_addr)
                        .add_property("listen_port", "astring", listen_port)
                        .add_property("store", "astring", "/data");

                    let profile = ProfileBuilder::new("omicron").add_service(
                        ServiceBuilder::new("system/illumos/clickhouse")
                            .add_property_group(config),
                    );
                    add_profile_to_zone(log, &installed_zone, profile).await?;
                    RunningZone::boot(installed_zone).await?
                }
                DatasetKind::Crucible => {
                    let dataset = &dataset_info.name.full();
                    let uuid = &Uuid::new_v4().to_string();
                    let config = PropertyGroupBuilder::new("config")
                        .add_property("datalink", "astring", datalink)
                        .add_property("gateway", "astring", gateway)
                        .add_property("dataset", "astring", dataset)
                        .add_property("listen_addr", "astring", listen_addr)
                        .add_property("listen_port", "astring", listen_port)
                        .add_property("uuid", "astring", uuid)
                        .add_property("store", "astring", "/data");

                    let profile = ProfileBuilder::new("omicron").add_service(
                        ServiceBuilder::new("oxide/crucible/agent")
                            .add_property_group(config),
                    );
                    add_profile_to_zone(log, &installed_zone, profile).await?;
                    RunningZone::boot(installed_zone).await?
                }
            };
            Ok(zone)
        }
        Err(illumos_utils::running_zone::GetZoneError::NotRunning {
            name,
            state,
        }) => {
            // TODO(https://github.com/oxidecomputer/omicron/issues/725):
            unimplemented!("Handle a zone which exists, but is not running: {name}, in {state:?}");
        }
        Err(err) => {
            // TODO(https://github.com/oxidecomputer/omicron/issues/725):
            unimplemented!(
                "Handle a zone which exists, has some other problem: {err}"
            );
        }
    }
}

// The type of a future which is used to send a notification to Nexus.
type NotifyFut =
    Pin<Box<dyn futures::Future<Output = Result<(), String>> + Send>>;

#[derive(Debug)]
struct NewFilesystemRequest {
    zpool_id: Uuid,
    dataset_kind: DatasetKind,
    address: SocketAddrV6,
    responder: oneshot::Sender<Result<(), Error>>,
}

#[derive(Clone)]
struct StorageResources {
    // A map of "devfs path" to "Disk" attached to this Sled.
    //
    // Note that we're keying off of devfs path, rather than device ID.
    // This should allow us to identify when a U.2 has been removed from
    // one slot and placed into another one.
    disks: Arc<Mutex<HashMap<PathBuf, Disk>>>,
    // A map of "zpool name" to "pool".
    pools: Arc<Mutex<HashMap<ZpoolName, Pool>>>,
}

// A worker that starts zones for pools as they are received.
struct StorageWorker {
    log: Logger,
    sled_id: Uuid,
    lazy_nexus_client: LazyNexusClient,
    nexus_notifications: FuturesOrdered<NotifyFut>,
    rx: mpsc::Receiver<StorageWorkerRequest>,
    vnic_allocator: VnicAllocator<Etherstub>,
    underlay_address: Ipv6Addr,
}

#[derive(Clone, Debug)]
enum NotifyDiskRequest {
    Add(Disk),
    Remove(DiskIdentity),
}

impl StorageWorker {
    // Ensures the named dataset exists as a filesystem with a UUID, optionally
    // creating it if `do_format` is true.
    //
    // Returns the UUID attached to the ZFS filesystem.
    fn ensure_dataset_with_id(
        dataset_name: &DatasetName,
        do_format: bool,
    ) -> Result<Uuid, Error> {
        let fs_name = &dataset_name.full();
        Zfs::ensure_zoned_filesystem(
            &fs_name,
            Mountpoint::Path(PathBuf::from("/data")),
            do_format,
        )?;
        // Ensure the dataset has a usable UUID.
        if let Ok(id_str) = Zfs::get_oxide_value(&fs_name, "uuid") {
            if let Ok(id) = id_str.parse::<Uuid>() {
                return Ok(id);
            }
        }
        let id = Uuid::new_v4();
        Zfs::set_oxide_value(&fs_name, "uuid", &id.to_string())?;
        Ok(id)
    }

    // Starts the zone for a dataset within a particular zpool.
    //
    // If requested via the `do_format` parameter, may also initialize
    // these resources.
    //
    // Returns the UUID attached to the underlying ZFS dataset.
    // Returns (was_inserted, Uuid).
    async fn initialize_dataset_and_zone(
        &mut self,
        pool: &mut Pool,
        dataset_info: &DatasetInfo,
        do_format: bool,
    ) -> Result<(bool, Uuid), Error> {
        // Ensure the underlying dataset exists before trying to poke at zones.
        let dataset_name = &dataset_info.name;
        info!(&self.log, "Ensuring dataset {} exists", dataset_name.full());
        let id =
            StorageWorker::ensure_dataset_with_id(&dataset_name, do_format)?;

        // If this zone has already been processed by us, return immediately.
        if let Some(_) = pool.get_zone(id) {
            return Ok((false, id));
        }
        // Otherwise, the zone may or may not exist.
        // We need to either look up or create the zone.
        info!(
            &self.log,
            "Ensuring zone for {} is running",
            dataset_name.full()
        );
        let zone = ensure_running_zone(
            &self.log,
            &self.vnic_allocator,
            dataset_info,
            &dataset_name,
            do_format,
            self.underlay_address,
        )
        .await?;

        info!(
            &self.log,
            "Zone {} with address {} is running",
            zone.name(),
            dataset_info.address,
        );
        pool.add_zone(id, zone);
        Ok((true, id))
    }

    // Adds a "notification to nexus" to `nexus_notifications`,
    // informing it about the addition of `pool_id` to this sled.
    fn add_zpool_notify(&mut self, pool_id: Uuid, size: ByteCount) {
        let sled_id = self.sled_id;
        let lazy_nexus_client = self.lazy_nexus_client.clone();
        let notify_nexus = move || {
            let zpool_request = ZpoolPutRequest { size: size.into() };
            let lazy_nexus_client = lazy_nexus_client.clone();
            async move {
                lazy_nexus_client
                    .get()
                    .await
                    .map_err(|e| {
                        backoff::BackoffError::transient(e.to_string())
                    })?
                    .zpool_put(&sled_id, &pool_id, &zpool_request)
                    .await
                    .map_err(|e| {
                        backoff::BackoffError::transient(e.to_string())
                    })?;
                Ok(())
            }
        };
        let log = self.log.clone();
        let log_post_failure = move |_, delay| {
            warn!(
                log,
                "failed to notify nexus, will retry in {:?}", delay;
            );
        };
        self.nexus_notifications.push_back(
            backoff::retry_notify(
                backoff::retry_policy_internal_service_aggressive(),
                notify_nexus,
                log_post_failure,
            )
            .boxed(),
        );
    }

    async fn ensure_using_exactly_these_disks(
        &mut self,
        resources: &StorageResources,
        unparsed_disks: Vec<UnparsedDisk>,
    ) -> Result<(), Error> {
        let mut new_disks = HashMap::new();

        // We may encounter errors while parsing any of the disks; keep track of
        // any errors that occur and return any of them if something goes wrong.
        //
        // That being said, we should not prevent access to the other disks if
        // only one failure occurs.
        let mut err: Option<Error> = None;

        // Ensure all disks conform to the expected partition layout.
        for disk in unparsed_disks.into_iter() {
            match sled_hardware::Disk::new(&self.log, disk).map_err(|err| {
                warn!(self.log, "Could not ensure partitions: {err}");
                err
            }) {
                Ok(disk) => {
                    let devfs_path = disk.devfs_path().clone();
                    new_disks.insert(devfs_path, disk);
                }
                Err(e) => {
                    warn!(self.log, "Cannot parse disk: {e}");
                    err = Some(e.into());
                }
            };
        }

        let mut disks = resources.disks.lock().await;

        // Remove disks that don't appear in the "new_disks" set.
        //
        // This also accounts for zpools and notifies Nexus.
        let disks_to_be_removed = disks
            .iter()
            .filter(|(key, old_disk)| {
                // If this disk appears in the "new" and "old" set, it should
                // only be removed if it has changed.
                //
                // This treats a disk changing in an unexpected way as a
                // "removal and re-insertion".
                if let Some(new_disk) = new_disks.get(*key) {
                    // Changed Disk -> Disk should be removed.
                    new_disk != *old_disk
                } else {
                    // Not in the new set -> Disk should be removed.
                    true
                }
            })
            .map(|(_key, disk)| disk.clone())
            .collect::<Vec<_>>();
        for disk in disks_to_be_removed {
            if let Err(e) = self
                .delete_disk_locked(&resources, &mut disks, disk.devfs_path())
                .await
            {
                warn!(self.log, "Failed to delete disk: {e}");
                err = Some(e);
            }
        }

        // Add new disks to `resources.disks`.
        //
        // This also accounts for zpools and notifies Nexus.
        for (key, new_disk) in new_disks {
            if let Some(old_disk) = disks.get(&key) {
                // In this case, the disk should be unchanged.
                //
                // This assertion should be upheld by the filter above, which
                // should remove disks that changed.
                assert!(old_disk == &new_disk);
            } else {
                if let Err(e) = self
                    .upsert_disk_locked(&resources, &mut disks, new_disk)
                    .await
                {
                    warn!(self.log, "Failed to upsert disk: {e}");
                    err = Some(e);
                }
            }
        }

        if let Some(err) = err {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn upsert_disk(
        &mut self,
        resources: &StorageResources,
        disk: UnparsedDisk,
    ) -> Result<(), Error> {
        info!(self.log, "Upserting disk: {disk:?}");

        // Ensure the disk conforms to an expected partition layout.
        let disk =
            sled_hardware::Disk::new(&self.log, disk).map_err(|err| {
                warn!(self.log, "Could not ensure partitions: {err}");
                err
            })?;

        let mut disks = resources.disks.lock().await;
        self.upsert_disk_locked(resources, &mut disks, disk).await
    }

    async fn upsert_disk_locked(
        &mut self,
        resources: &StorageResources,
        disks: &mut tokio::sync::MutexGuard<'_, HashMap<PathBuf, Disk>>,
        disk: Disk,
    ) -> Result<(), Error> {
        let zpool_name = disk.zpool_name().clone();
        disks.insert(disk.devfs_path().to_path_buf(), disk.clone());
        self.upsert_zpool(&resources, &zpool_name).await?;
        self.physical_disk_notify(NotifyDiskRequest::Add(disk));

        Ok(())
    }

    async fn delete_disk(
        &mut self,
        resources: &StorageResources,
        disk: UnparsedDisk,
    ) -> Result<(), Error> {
        info!(self.log, "Deleting disk: {disk:?}");

        let mut disks = resources.disks.lock().await;
        self.delete_disk_locked(resources, &mut disks, disk.devfs_path()).await
    }

    async fn delete_disk_locked(
        &mut self,
        resources: &StorageResources,
        disks: &mut tokio::sync::MutexGuard<'_, HashMap<PathBuf, Disk>>,
        key: &PathBuf,
    ) -> Result<(), Error> {
        if let Some(parsed_disk) = disks.remove(key) {
            resources.pools.lock().await.remove(&parsed_disk.zpool_name());
            self.physical_disk_notify(NotifyDiskRequest::Remove(
                parsed_disk.identity().clone(),
            ));
        }
        Ok(())
    }

    // Adds a "notification to nexus" to `self.nexus_notifications`, informing it
    // about the addition/removal of a physical disk to this sled.
    fn physical_disk_notify(&mut self, disk: NotifyDiskRequest) {
        let sled_id = self.sled_id;
        let lazy_nexus_client = self.lazy_nexus_client.clone();
        let notify_nexus = move || {
            let disk = disk.clone();
            let lazy_nexus_client = lazy_nexus_client.clone();
            async move {
                let nexus = lazy_nexus_client.get().await.map_err(|e| {
                    backoff::BackoffError::transient(e.to_string())
                })?;

                match disk {
                    NotifyDiskRequest::Add(disk) => {
                        let request = PhysicalDiskPutRequest {
                            model: disk.identity().model.clone(),
                            serial: disk.identity().serial.clone(),
                            vendor: disk.identity().vendor.clone(),
                            variant: match disk.variant() {
                                DiskVariant::U2 => PhysicalDiskKind::U2,
                                DiskVariant::M2 => PhysicalDiskKind::M2,
                            },
                            sled_id,
                        };
                        nexus.physical_disk_put(&request).await.map_err(
                            |e| backoff::BackoffError::transient(e.to_string()),
                        )?;
                    }
                    NotifyDiskRequest::Remove(disk_identity) => {
                        let request = PhysicalDiskDeleteRequest {
                            model: disk_identity.model.clone(),
                            serial: disk_identity.serial.clone(),
                            vendor: disk_identity.vendor.clone(),
                            sled_id,
                        };
                        nexus.physical_disk_delete(&request).await.map_err(
                            |e| backoff::BackoffError::transient(e.to_string()),
                        )?;
                    }
                }
                Ok(())
            }
        };
        let log = self.log.clone();
        let log_post_failure = move |_, delay| {
            warn!(
                log,
                "failed to notify nexus, will retry in {:?}", delay;
            );
        };
        self.nexus_notifications.push_back(
            backoff::retry_notify(
                backoff::retry_policy_internal_service_aggressive(),
                notify_nexus,
                log_post_failure,
            )
            .boxed(),
        );
    }

    async fn upsert_zpool(
        &mut self,
        resources: &StorageResources,
        pool_name: &ZpoolName,
    ) -> Result<(), Error> {
        let mut pools = resources.pools.lock().await;
        let zpool = Pool::new(pool_name)?;

        let pool = match pools.entry(pool_name.clone()) {
            hash_map::Entry::Occupied(mut entry) => {
                // The pool already exists.
                entry.get_mut().info = zpool.info;
                return Ok(());
            }
            hash_map::Entry::Vacant(entry) => entry.insert(zpool),
        };

        info!(&self.log, "Storage manager processing zpool: {:#?}", pool.info);

        let size = ByteCount::try_from(pool.info.size()).map_err(|err| {
            Error::BadPoolSize { name: pool_name.to_string(), err }
        })?;

        // If we find filesystems within our datasets, ensure their
        // zones are up-and-running.
        let mut datasets = vec![];
        let existing_filesystems = Zfs::list_datasets(&pool_name.to_string())?;
        for fs_name in existing_filesystems {
            info!(
                &self.log,
                "StorageWorker loading fs {} on zpool {}",
                fs_name,
                pool_name.to_string()
            );
            // We intentionally do not exit on error here -
            // otherwise, the failure of a single dataset would
            // stop the storage manager from processing all storage.
            //
            // Instead, we opt to log the failure.
            let dataset_name =
                DatasetName::new(&pool_name.to_string(), &fs_name);
            let result = self.load_dataset(pool, &dataset_name).await;
            match result {
                Ok(dataset) => datasets.push(dataset),
                Err(e) => warn!(
                    &self.log,
                    "StorageWorker Failed to load dataset: {}", e
                ),
            }
        }

        // Notify Nexus of the zpool.
        self.add_zpool_notify(pool.id(), size);
        Ok(())
    }

    // Attempts to add a dataset within a zpool, according to `request`.
    async fn add_dataset(
        &mut self,
        resources: &StorageResources,
        request: &NewFilesystemRequest,
    ) -> Result<(), Error> {
        info!(self.log, "add_dataset: {:?}", request);
        let mut pools = resources.pools.lock().await;
        let name = ZpoolName::new(request.zpool_id);
        let pool = pools.get_mut(&name).ok_or_else(|| {
            Error::ZpoolNotFound(format!(
                "{}, looked up while trying to add dataset",
                request.zpool_id
            ))
        })?;

        let pool_name = pool.info.name();
        let dataset_info = DatasetInfo::new(
            pool_name,
            request.dataset_kind.clone(),
            request.address,
        );
        let (is_new_dataset, id) = self
            .initialize_dataset_and_zone(
                pool,
                &dataset_info,
                // do_format=
                true,
            )
            .await?;

        if !is_new_dataset {
            return Ok(());
        }

        // Now that the dataset has been initialized, record the configuration
        // so it can re-initialize itself after a reboot.
        let path = pool.dataset_config_path(id).await?;
        let info_str = toml::to_string(&dataset_info)
            .map_err(|err| Error::Serialize { path: path.clone(), err })?;
        let pool_name = pool.info.name();
        let mut file = File::create(&path).await.map_err(|err| Error::Io {
            message: format!("Failed creating config file at {path:?} for pool {pool_name}, dataset: {id}"),
            err,
        })?;
        file.write_all(info_str.as_bytes()).await.map_err(|err| Error::Io {
            message: format!("Failed writing config to {path:?} for pool {pool_name}, dataset: {id}"),
            err,
        })?;

        Ok(())
    }

    async fn load_dataset(
        &mut self,
        pool: &mut Pool,
        dataset_name: &DatasetName,
    ) -> Result<(Uuid, SocketAddrV6, DatasetKind), Error> {
        let name = dataset_name.full();
        let id = Zfs::get_oxide_value(&name, "uuid")?
            .parse::<Uuid>()
            .map_err(|err| Error::ParseDatasetUuid { name, err })?;
        let config_path = pool.dataset_config_path(id).await?;
        info!(
            self.log,
            "Loading Dataset from {}",
            config_path.to_string_lossy()
        );
        let pool_name = pool.info.name();
        let dataset_info: DatasetInfo =
            toml::from_str(
                &tokio::fs::read_to_string(&config_path).await.map_err(|err| Error::Io {
                    message: format!("read config for pool {pool_name}, dataset {dataset_name:?} from {config_path:?}"),
                    err,
                })?
            ).map_err(|err| {
                Error::Deserialize {
                    path: config_path,
                    err,
                }
            })?;
        self.initialize_dataset_and_zone(
            pool,
            &dataset_info,
            // do_format=
            false,
        )
        .await?;

        Ok((id, dataset_info.address, dataset_info.kind))
    }

    // Small wrapper around `Self::do_work_internal` that ensures we always
    // emit info to the log when we exit.
    async fn do_work(
        &mut self,
        resources: StorageResources,
    ) -> Result<(), Error> {
        loop {
            match self.do_work_internal(&resources).await {
                Ok(()) => {
                    info!(self.log, "StorageWorker exited successfully");
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        self.log,
                        "StorageWorker encountered unexpected error: {}", e
                    );
                    // ... for now, keep trying.
                }
            }
        }
    }

    async fn do_work_internal(
        &mut self,
        resources: &StorageResources,
    ) -> Result<(), Error> {
        loop {
            tokio::select! {
                _ = self.nexus_notifications.next(), if !self.nexus_notifications.is_empty() => {},
                Some(request) = self.rx.recv() => {
                    use StorageWorkerRequest::*;
                    match request {
                        AddDisk(disk) => {
                            self.upsert_disk(&resources, disk).await?;
                        },
                        RemoveDisk(disk) => {
                            self.delete_disk(&resources, disk).await?;
                        },
                        NewPool(pool_name) => {
                            self.upsert_zpool(&resources, &pool_name).await?;
                        },
                        NewFilesystem(request) => {
                            let result = self.add_dataset(&resources, &request).await;
                            let _ = request.responder.send(result);
                        },
                        DisksChanged(disks) => {
                            self.ensure_using_exactly_these_disks(&resources, disks).await?;
                        },
                    }
                },
            }
        }
    }
}

#[derive(Debug)]
enum StorageWorkerRequest {
    AddDisk(UnparsedDisk),
    RemoveDisk(UnparsedDisk),
    DisksChanged(Vec<UnparsedDisk>),
    NewPool(ZpoolName),
    NewFilesystem(NewFilesystemRequest),
}

/// A sled-local view of all attached storage.
pub struct StorageManager {
    log: Logger,

    resources: StorageResources,

    tx: mpsc::Sender<StorageWorkerRequest>,

    // A handle to a worker which updates "pools".
    task: JoinHandle<Result<(), Error>>,
}

impl StorageManager {
    /// Creates a new [`StorageManager`] which should manage local storage.
    pub async fn new(
        log: &Logger,
        sled_id: Uuid,
        lazy_nexus_client: LazyNexusClient,
        etherstub: Etherstub,
        underlay_address: Ipv6Addr,
    ) -> Self {
        let log = log.new(o!("component" => "StorageManager"));
        let resources = StorageResources {
            disks: Arc::new(Mutex::new(HashMap::new())),
            pools: Arc::new(Mutex::new(HashMap::new())),
        };
        let (tx, rx) = mpsc::channel(30);
        let vnic_allocator = VnicAllocator::new("Storage", etherstub);

        StorageManager {
            log: log.clone(),
            resources: resources.clone(),
            tx,
            task: tokio::task::spawn(async move {
                let mut worker = StorageWorker {
                    log,
                    sled_id,
                    lazy_nexus_client,
                    nexus_notifications: FuturesOrdered::new(),
                    rx,
                    vnic_allocator,
                    underlay_address,
                };

                worker.do_work(resources).await
            }),
        }
    }

    /// Ensures that the storage manager tracks exactly the provided disks.
    ///
    /// This acts similar to a batch [Self::upsert_disk] for all new disks, and
    /// [Self::delete_disk] for all removed disks.
    ///
    /// If errors occur, an arbitrary "one" of them will be returned, but a
    /// best-effort attempt to add all disks will still be attempted.
    // Receiver implemented by [StorageWorker::ensure_using_exactly_these_disks]
    pub async fn ensure_using_exactly_these_disks<I>(&self, unparsed_disks: I)
    where
        I: IntoIterator<Item = UnparsedDisk>,
    {
        self.tx
            .send(StorageWorkerRequest::DisksChanged(
                unparsed_disks.into_iter().collect::<Vec<_>>(),
            ))
            .await
            .unwrap();
    }

    /// Adds a disk and associated zpool to the storage manager.
    // Receiver implemented by [StorageWorker::upsert_disk].
    pub async fn upsert_disk(&self, disk: UnparsedDisk) {
        info!(self.log, "Upserting disk: {disk:?}");
        self.tx.send(StorageWorkerRequest::AddDisk(disk)).await.unwrap();
    }

    /// Removes a disk, if it's tracked by the storage manager, as well
    /// as any associated zpools.
    // Receiver implemented by [StorageWorker::delete_disk].
    pub async fn delete_disk(&self, disk: UnparsedDisk) {
        info!(self.log, "Deleting disk: {disk:?}");
        self.tx.send(StorageWorkerRequest::RemoveDisk(disk)).await.unwrap();
    }

    /// Adds a zpool to the storage manager.
    // Receiver implemented by [StorageWorker::upsert_zpool].
    pub async fn upsert_zpool(&self, name: ZpoolName) {
        info!(self.log, "Inserting zpool: {name:?}");
        self.tx.send(StorageWorkerRequest::NewPool(name)).await.unwrap();
    }

    pub async fn get_zpools(&self) -> Result<Vec<crate::params::Zpool>, Error> {
        let pools = self.resources.pools.lock().await;
        Ok(pools
            .keys()
            .map(|zpool| crate::params::Zpool { id: zpool.id() })
            .collect())
    }

    pub async fn upsert_filesystem(
        &self,
        zpool_id: Uuid,
        dataset_kind: DatasetKind,
        address: SocketAddrV6,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let request = NewFilesystemRequest {
            zpool_id,
            dataset_kind,
            address,
            responder: tx,
        };

        self.tx
            .send(StorageWorkerRequest::NewFilesystem(request))
            .await
            .expect("Storage worker bug (not alive)");
        rx.await.expect(
            "Storage worker bug (dropped responder without responding)",
        )?;

        Ok(())
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        // NOTE: Ideally, with async drop, we'd await completion of the worker
        // somehow.
        //
        // Without that option, we instead opt to simply cancel the worker
        // task to ensure it does not remain alive beyond the StorageManager
        // itself.
        self.task.abort();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn serialize_dataset_info() {
        let dataset_info = DatasetInfo {
            address: "[::1]:8080".parse().unwrap(),
            kind: DatasetKind::Crucible,
            name: DatasetName::new("pool", "dataset"),
        };

        toml::to_string(&dataset_info).unwrap();
    }
}
