//! Management of sled-local storage.

use crate::illumos::{
    svc::wait_for_service,
    zfs::Mountpoint,
    zone::CRUCIBLE_ZONE_PREFIX,
    zpool::ZpoolInfo,
};
use crate::vnic::{interface_name, IdAllocator, Vnic};
use ipnetwork::IpNetwork;
use omicron_common::api::external::{ByteCount, Error};
use omicron_common::api::internal::nexus::SledAgentPoolInfo;
use slog::Logger;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zpool::Zpool, zfs::Zfs, zone::Zones};
#[cfg(test)]
use crate::illumos::{
    dladm::MockDladm as Dladm, zfs::MockZfs as Zfs, zpool::MockZpool as Zpool, zone::MockZones as Zones
};

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use omicron_common::NexusClient;

fn crucible_zone_name(id: &Uuid) -> String {
    format!("{}{}", CRUCIBLE_ZONE_PREFIX, id)
}

enum PoolManagementState {
    // No assigned agents.
    Unmanaged,
    // The pool is actively managed by a Crucible Agent.
    Managed {
        address: SocketAddr
    },
}

/// A ZFS storage pool.
struct Pool {
    id: Uuid,
    info: ZpoolInfo,
    state: PoolManagementState,
}

impl Pool {
    /// Queries for an existing Zpool by name.
    ///
    /// Returns Ok if the pool exists.
    fn new(name: &str) -> Result<Pool, Error> {
        let info = Zpool::get_info(name)?;

        // NOTE: This relies on the name being a UUID exactly.
        // We could be more flexible...
        let id: Uuid = info.name().parse()
            .map_err(|e| Error::InternalError {
                message: format!("Zpool's name cannot be parsed as UUID: {}", e),
            })?;
        Ok(Pool { id, info, state: PoolManagementState::Unmanaged })
    }

    fn id(&self) -> Uuid {
        self.id
    }
}

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
        // Create a base zone, from which all running storage zones are cloned.
        Zones::create_crucible_base(&self.log)?;

        while let Some(pool_name) = self.new_pools_rx.recv().await {
            info!(&self.log, "creating crucible zone for zpool: {}", pool_name);

            let mut pools = self.pools.lock().await;
            // TODO: when would this not exist? It really should...
            let mut pool = pools.get_mut(&pool_name).unwrap();

            // For now, we place all "expected" filesystems on each new zpool
            // we see. The decision of "whether or not to actually use the
            // filesystem" is a decision left to both the bootstrapping protocol
            // and Nexus.
            //
            // If we had a better signal - from the bootstrapping system - about
            // where Cockroach nodes should exist, we could be more selective
            // about this placement.

            // Ensure the necessary filesystems exist (Cockroach, Crucible, etc).
            let crucible_fs = format!("{}/{}", pool.info.name(), "crucible");
            let crucible_id = StorageWorker::ensure_filesystem_with_id(&crucible_fs)?;

            // TODO: We need to ensure the crucible agent server starts
            // running with this address / port combo too.
            let network = self.create_crucible_zone(crucible_fs).await?;
            pool.state = PoolManagementState::Managed {
                address: SocketAddr::new(
                    network.ip(),
                    8080,
                ),
            };

            let cockroach_fs = format!("{}/{}", pool.info.name(), "cockroach");
            let cockroach_id = StorageWorker::ensure_filesystem_with_id(&cockroach_fs)?;

            // TODO: Notify nexus!
            let allocation_info = self.nexus_client.zpool_post(
                pool.id(),
                self.sled_id,
                SledAgentPoolInfo {
                    // TODO: No unwrap
                    size: ByteCount::try_from(pool.info.size()).unwrap(),
                },
            ).await;

            // TODO: Ensure the datasets are of the requested size

            // TODO: Notify nexus that the datasets are up too
        }
        Ok(())
    }

    // TODO TODO:
    //
    // Pool ID != Crucible ID
    //
    // We should carve a dataset *out* of the pool for Crucible
    // Could also carve a dataset out for other stuff
    // TODO: ASK NEXUS WHAT TO DO

    // Configures and starts a zone which contains a Crucible Agent,
    // responsible for managing the provided ZFS filesystem.
    async fn create_crucible_zone(&self, filesystem_name: String) -> Result<IpNetwork, Error> {
        let physical_dl = Dladm::find_physical()?;
        let nic = Vnic::new_control(&self.vnic_id_allocator, &physical_dl, None)?;

        let id = Uuid::new_v4();
        let zname = crucible_zone_name(&id);

        // Configure the new zone - this should be identical to the base zone,
        // but with a specified VNIC and pool.
        Zones::configure_crucible_zone(
            &self.log,
            &zname,
            nic.name().to_string(),
            filesystem_name,
        )?;

        // Clone from the base crucible installation.
        Zones::clone_from_base_crucible(&zname)?;

        // Boot the new zone.
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
