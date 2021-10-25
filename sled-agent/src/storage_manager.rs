//! Management of sled-local storage.

use crate::illumos::{
    svc::wait_for_service,
    zone::CRUCIBLE_ZONE_PREFIX,
    zpool::ZpoolInfo,
};
use crate::vnic::{interface_name, IdAllocator, Vnic};
use ipnetwork::IpNetwork;
use omicron_common::api::external::Error;
use slog::Logger;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

#[cfg(not(test))]
use crate::illumos::{dladm::Dladm, zpool::Zpool, zone::Zones};
#[cfg(test)]
use crate::illumos::{dladm::MockDladm as Dladm, zpool::MockZpool as Zpool, zone::MockZones as Zones};

fn crucible_zone_name(id: &Uuid) -> String {
    format!("{}{}", CRUCIBLE_ZONE_PREFIX, id)
}

enum PoolManagementState {
    // No assigned Crucible Agent.
    Unmanaged,
    // The pool is actively managed by a Crucible Agent.
    Managed { address: SocketAddr },
}

/// A ZFS storage pool.
struct Pool {
    info: ZpoolInfo,
    state: PoolManagementState,
}

impl Pool {
    /// Queries for an existing Zpool by name.
    ///
    /// Returns Ok if the pool exists.
    fn new(name: &str) -> Result<Pool, Error> {
        let info = Zpool::get_info(name)?;
        Ok(Pool { info, state: PoolManagementState::Unmanaged })
    }
}

// TODO: Add a task to continually query for disks
//
// TODO: there should also be a task that continually updates nexus
// on the pool situation, but idk if that should be here or in sled_agent.rs

// TODO: what is the structure of work? before we start throwing complex structs
// at stuff, what do we want dataflow to look like?
//
// - I want one task to query for disks (... won't exist). But it needs access
// to this stuff.
// - I want a queue of pools which need zones.
// - I want a worker to consume the "pools-needing-work" queue.

// A worker that starts zones for pools as they are received.
struct StorageWorker {
    log: Logger,
    pools: Arc<Mutex<HashMap<String, Pool>>>,
    new_pools_rx: mpsc::Receiver<String>,
    vnic_id_allocator: IdAllocator,
}

impl StorageWorker {
    async fn do_work(&mut self) -> Result<(), Error> {
        // Create a base zone, from which all running storage zones are cloned.
        Zones::create_crucible_base(&self.log)?;

        while let Some(pool_name) = self.new_pools_rx.recv().await {
            info!(&self.log, "creating crucible zone for zpool: {}", pool_name);

            let mut pools = self.pools.lock().await;
            // TODO: when would this not exist? It really should...
            let mut pool = pools.get_mut(&pool_name).unwrap();

            // TODO: We need to ensure the crucible agent server starts
            // running with this address / port combo too.
            let network = self.create_crucible_zone(pool_name).await?;
            pool.state = PoolManagementState::Managed {
                address: SocketAddr::new(
                    network.ip(),
                    8080,
                ),
            };
            drop(pools);

            // TODO: Notify nexus!
        }
        Ok(())
    }

    // Configures and starts a zone which contains a Crucible Agent,
    // responsible for managing the provided zpool.
    async fn create_crucible_zone(&self, pool_name: String) -> Result<IpNetwork, Error> {
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
            pool_name,
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
    /// Creates a new [`StorageManager`] which should monitor for local storage.
    ///
    /// NOTE: Monitoring is not yet implemented - this option simply reports
    /// "no observed local disks".
    pub fn new(log: &Logger) -> Result<Self, Error> {
        let pools = Arc::new(Mutex::new(HashMap::new()));
        let (new_pools_tx, new_pools_rx) = mpsc::channel(10);
        let log = log.new(o!("component" => "sled agent storage manager"));

        let mut worker = StorageWorker {
            log,
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

    /// Creates a new [`StorageManager`] from a list of pre-supplied zpools.
    pub async fn new_from_zpools(log: &Logger, zpools: Vec<String>) -> Result<Self, Error> {
        let pools = zpools
            .into_iter()
            .map(|name| {
                let pool = Pool::new(&name)?;
                Ok((name, pool))
            })
            .collect::<Result<HashMap<String, Pool>, Error>>()?;

        // Enqueue all supplied zpools to the worker.
        let (new_pools_tx, new_pools_rx) = mpsc::channel(pools.len());
        for name in pools.keys() {
            new_pools_tx.send(name.clone()).await.unwrap();
        }

        let pools = Arc::new(Mutex::new(pools));
        let log = log.new(o!("component" => "sled agent storage manager"));
        let mut worker = StorageWorker {
            log,
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
