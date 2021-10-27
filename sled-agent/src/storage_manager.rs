//! Management of sled-local storage.

use crate::illumos::{
    svc::wait_for_service,
    zfs::Mountpoint,
    zone::{
        COCKROACH_ZONE_PREFIX, CRUCIBLE_SVC_DIRECTORY, CRUCIBLE_ZONE_PREFIX,
    },
    zpool::ZpoolInfo,
};
use crate::vnic::{interface_name, IdAllocator, Vnic};
use ipnetwork::IpNetwork;
use omicron_common::api::external::{ByteCount, Error};
use omicron_common::api::internal::nexus::{DatasetInfo, SledAgentPoolInfo};
use slog::Logger;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zfs::Zfs, zone::Zones, zpool::Zpool};
#[cfg(test)]
use crate::illumos::{
    dladm::MockDladm as Dladm, zfs::MockZfs as Zfs, zone::MockZones as Zones,
    zpool::MockZpool as Zpool,
};

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use omicron_common::NexusClient;

/// A ZFS filesystem dataset within a zpool.
#[allow(dead_code)]
struct Filesystem {
    name: String,
    address: SocketAddr,
}

/// A ZFS storage pool.
struct Pool {
    id: Uuid,
    info: ZpoolInfo,
    filesystems: HashMap<Uuid, Filesystem>,
}

impl Pool {
    /// Queries for an existing Zpool by name.
    ///
    /// Returns Ok if the pool exists.
    fn new(name: &str) -> Result<Pool, Error> {
        let info = Zpool::get_info(name)?;

        // NOTE: This relies on the name being a UUID exactly.
        // We could be more flexible...
        let id: Uuid =
            info.name().parse().map_err(|e| Error::InternalError {
                message: format!(
                    "Zpool's name cannot be parsed as UUID: {}",
                    e
                ),
            })?;
        Ok(Pool { id, info, filesystems: HashMap::new() })
    }

    fn add_filesystem(&mut self, id: Uuid, fs: Filesystem) {
        self.filesystems.insert(id, fs);
    }

    fn id(&self) -> Uuid {
        self.id
    }
}

// Description of a dataset within a ZFS pool, which should be created
// by the Sled Agent.
struct PartitionInfo<'a> {
    name: &'a str,
    zone_prefix: &'a str,
    data_directory: &'a str,
    svc_directory: &'a str,
    port: u16,
}

const PARTITIONS: &[PartitionInfo<'static>] = &[
    PartitionInfo {
        name: "crucible",
        zone_prefix: CRUCIBLE_ZONE_PREFIX,
        data_directory: "/data",
        svc_directory: CRUCIBLE_SVC_DIRECTORY,
        // TODO: Ensure crucible agent uses this port
        port: 8080,
    },
    PartitionInfo {
        name: "cockroach",
        zone_prefix: COCKROACH_ZONE_PREFIX,
        data_directory: "/data",
        svc_directory: CRUCIBLE_SVC_DIRECTORY, // XXX Replace me
        // TODO: Ensure cockroach uses this port
        port: 8080,
    },
];

// A worker that starts zones for pools as they are received.
struct StorageWorker {
    log: Logger,
    sled_id: Uuid,
    nexus_client: Arc<NexusClient>,
    pools: Arc<Mutex<HashMap<String, Pool>>>,
    new_pools_rx: mpsc::Receiver<String>,
    vnic_id_allocator: IdAllocator,
}

impl StorageWorker {
    // Idempotently ensure the named filesystem exists with a UUID.
    fn ensure_filesystem_with_id(fs_name: &str) -> Result<Uuid, Error> {
        Zfs::ensure_filesystem(&fs_name, Mountpoint::Legacy)?;
        // Ensure the filesystem has a usable UUID.
        if let Ok(id_str) = Zfs::get_oxide_value(&fs_name, "uuid") {
            if let Ok(id) = id_str.parse::<Uuid>() {
                return Ok(id);
            }
        }
        let id = Uuid::new_v4();
        Zfs::set_oxide_value(&fs_name, "uuid", &id.to_string())?;
        Ok(id)
    }

    async fn do_work(&mut self) -> Result<(), Error> {
        self.do_work_internal()
            .await
            .map(|()| {
                info!(self.log, "StorageWorker exited successfully");
            })
            .map_err(|e| {
                warn!(self.log, "StorageWorker exited unexpectedly: {}", e);
                e
            })
    }

    async fn do_work_internal(&mut self) -> Result<(), Error> {
        info!(self.log, "StorageWorker creating storage base zone");
        // Create a base zone, from which all running storage zones are cloned.
        Zones::create_storage_base(&self.log)?;
        info!(self.log, "StorageWorker creating storage base zone - DONE");

        while let Some(pool_name) = self.new_pools_rx.recv().await {
            let mut pools = self.pools.lock().await;
            // TODO: when would this not exist? It really should...
            let pool = pools.get_mut(&pool_name).unwrap();

            info!(
                &self.log,
                "Storage manager processing zpool: {:#?}", pool.info
            );

            // For now, we place all "expected" filesystems on each new zpool
            // we see. The decision of "whether or not to actually use the
            // filesystem" is a decision left to both the bootstrapping protocol
            // and Nexus.
            //
            // If we had a better signal - from the bootstrapping system - about
            // where Cockroach nodes should exist, we could be more selective
            // about this placement.

            // Ensure the necessary filesystems exist (Cockroach, Crucible, etc),
            // and start zones for each of them.
            let mut datasets = vec![];
            for partition in PARTITIONS {
                let name = format!("{}/{}", pool.info.name(), partition.name);

                info!(&self.log, "Ensuring filesystem {} exists", name);
                let id = StorageWorker::ensure_filesystem_with_id(&name)?;

                info!(&self.log, "Creating zone for {}", name);
                let network = self
                    .create_zone(
                        partition.zone_prefix,
                        &name,
                        partition.data_directory,
                        partition.svc_directory,
                    )
                    .await?;

                let address = SocketAddr::new(network.ip(), partition.port);
                info!(&self.log, "Created zone with address {}", address);
                pool.add_filesystem(id, Filesystem { name, address });
                datasets.push(DatasetInfo { id });
            }

            let size = ByteCount::try_from(pool.info.size()).map_err(|e| {
                Error::InternalError {
                    message: format!("Invalid pool size: {}", e),
                }
            })?;

            // TODO: Notify nexus!
            let _allocation_info = self
                .nexus_client
                .zpool_post(
                    pool.id(),
                    self.sled_id,
                    SledAgentPoolInfo { id: pool.id(), size, datasets },
                )
                .await;

            // TODO: Apply allocation advice from Nexus (setting reservation /
            // quota properties).
        }
        Ok(())
    }

    // Creates a new zone, injecting the associated ZFS filesystem.
    //
    // * zone_name_prefix: Describes the newly created zone name (prefix + ID)
    // * filesystem_name: Name of ZFS filesystem to inject.
    // * data_directory: Path to mounted ZFS filesytem within the zone.
    // * svc_directory: Auxiliary filesystem to inject as read-only loopback.
    //      (This is useful for injecting paths to binaries).
    //
    // TODO: Should this be idempotent? Can we "create a zone if it doesn't
    // already exist"?
    async fn create_zone(
        &self,
        zone_name_prefix: &str,
        filesystem_name: &str,
        data_directory: &str,
        svc_directory: &str,
    ) -> Result<IpNetwork, Error> {
        let physical_dl = Dladm::find_physical()?;
        let nic =
            Vnic::new_control(&self.vnic_id_allocator, &physical_dl, None)?;
        let id = Uuid::new_v4();
        let zname = format!("{}{}", zone_name_prefix, id);

        // Configure the new zone - this should be identical to the base zone,
        // but with a specified VNIC and pool.
        Zones::configure_zone(
            &self.log,
            &zname,
            &[
                zone::Fs {
                    ty: "lofs".to_string(),
                    dir: svc_directory.to_string(),
                    special: svc_directory.to_string(),
                    options: vec!["ro".to_string()],
                    ..Default::default()
                },
                zone::Fs {
                    ty: "zfs".to_string(),
                    dir: data_directory.to_string(),
                    special: filesystem_name.to_string(),
                    options: vec!["rw".to_string()],
                    ..Default::default()
                },
            ],
            &[],
            vec![nic.name().to_string()],
        )?;

        // Clone from the base zone installation.
        Zones::clone_from_base_storage(&zname)?;

        // Boot the new zone.
        info!(&self.log, "Zone {} booting", zname);
        Zones::boot(&zname)?;

        // Wait for the network services to come online, then create an address
        // to use for communicating with the newly created zone.
        wait_for_service(Some(&zname), "svc:/milestone/network:default")
            .await?;

        let network =
            Zones::create_address(&zname, &interface_name(&nic.name()))?;
        Ok(network)
    }
}

/// A sled-local view of all attached storage.
pub struct StorageManager {
    // A map of "zpool name" to "pool".
    // TODO: Does this need to be arc / mutexed? who needs access other than the
    // worker?
    pools: Arc<Mutex<HashMap<String, Pool>>>,
    new_pools_tx: mpsc::Sender<String>,

    // A handle to a worker which updates "pools".
    task: JoinHandle<Result<(), Error>>,
}

impl StorageManager {
    /// Creates a new [`StorageManager`] which should manage local storage.
    pub async fn new(
        log: &Logger,
        sled_id: Uuid,
        nexus_client: Arc<NexusClient>,
    ) -> Result<Self, Error> {
        let log = log.new(o!("component" => "sled agent storage manager"));
        info!(log, "SLED AGENT: Creating storage manager");
        let pools = Arc::new(Mutex::new(HashMap::new()));
        let (new_pools_tx, new_pools_rx) = mpsc::channel(10);
        let mut worker = StorageWorker {
            log,
            sled_id,
            nexus_client,
            pools: pools.clone(),
            new_pools_rx,
            vnic_id_allocator: IdAllocator::new(),
        };
        Ok(StorageManager {
            pools,
            new_pools_tx,
            task: tokio::task::spawn(async move { worker.do_work().await }),
        })
    }

    /// Adds a zpool to the storage manager.
    pub async fn upsert_zpool(&self, name: &str) -> Result<(), Error> {
        let zpool = Pool::new(name)?;

        let is_new = {
            let mut pools = self.pools.lock().await;
            let entry = pools.entry(name.to_string());
            let is_new =
                matches!(entry, std::collections::hash_map::Entry::Vacant(_));

            // Ensure that the pool info is up-to-date.
            entry
                .and_modify(|e| {
                    e.info = zpool.info.clone();
                })
                .or_insert_with(|| zpool);
            is_new
        };

        // If we hadn't previously been handling this zpool, hand it off to the
        // worker for management (zone creation).
        if is_new {
            self.new_pools_tx.send(name.to_string()).await.unwrap();
        }
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
