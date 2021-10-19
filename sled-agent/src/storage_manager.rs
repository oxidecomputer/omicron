//! Management of sled-local storage.

use crate::illumos::zpool::ZpoolInfo;
use omicron_common::api::external::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tokio::sync::mpsc;

#[cfg(test)]
use crate::illumos::zpool::MockZpool as Zpool;
#[cfg(not(test))]
use crate::illumos::zpool::Zpool;

enum PoolManagementState {
    // No assigned Crucible Agent.
    Unmanaged,
    // The pool is actively managed by a Crucible Agent.
    Managed {
        address: SocketAddr,
    },
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
    pools: Arc<Mutex<HashMap<String, Pool>>>,
    new_pools_rx: mpsc::Receiver<String>,
}

impl StorageWorker {
    async fn do_work(&mut self) {
        while let Some(name) = self.new_pools_rx.recv().await {
            let mut pools = self.pools.lock().unwrap();

            // TODO: when would this not exist? It really should...
            let mut pool = pools.get_mut(&name).unwrap();

            // TODO: start zone, update state.
            println!("I should start a zone for {}", name);
            pool.state = PoolManagementState::Managed {
                address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            };
        }
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
    task: JoinHandle<()>,
}

impl StorageManager {
    /// Creates a new [`StorageManager`] which should monitor for local storage.
    ///
    /// NOTE: Monitoring is not yet implemented - this option simply reports
    /// "no observed local disks".
    pub fn new() -> Result<Self, Error> {
        let pools = Arc::new(Mutex::new(HashMap::new()));
        let (new_pools_tx, new_pools_rx) = mpsc::channel(10);

        let mut worker = StorageWorker {
            pools: pools.clone(),
            new_pools_rx,
        };
        Ok(StorageManager {
            pools,
            new_pools_tx,
            task: tokio::task::spawn(async move { worker.do_work().await }),
        })
    }

    /// Creates a new [`StorageManager`] from a list of pre-supplied zpools.
    pub async fn new_from_zpools(zpools: Vec<String>) -> Result<Self, Error> {
        let pools =
            zpools
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
        let mut worker = StorageWorker {
            pools: pools.clone(),
            new_pools_rx,
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
            let mut pools = self.pools.lock().unwrap();
            let entry = pools.entry(name.to_string());
            let is_new = matches!(entry, std::collections::hash_map::Entry::Vacant(_));

            // Ensure that the pool info is up-to-date.
            entry.and_modify(|e| {
                e.info = zpool.info.clone();
            }).or_insert_with(|| zpool);
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
