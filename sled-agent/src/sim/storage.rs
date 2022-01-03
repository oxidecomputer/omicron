// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent storage implementation

use futures::lock::Mutex;
use nexus_client::Client as NexusClient;
use nexus_client::types::{ByteCount, DatasetKind, DatasetPutRequest, ZpoolPutRequest};
use slog::Logger;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

// XXX Don't really like this import.
//
// Maybe refactor the "types" used by the HTTP
// service to a separate file.
use super::http_entrypoints_storage::{CreateRegion, Region, RegionId, State};

pub struct CrucibleDataInner {
    regions: HashMap<Uuid, Region>,
}

impl CrucibleDataInner {
    fn new() -> Self {
        Self { regions: HashMap::new() }
    }

    fn list(&self) -> Vec<Region> {
        self.regions.values().cloned().collect()
    }

    fn create(&mut self, params: CreateRegion) -> Region {
        let id = Uuid::from_str(&params.id.0).unwrap();
        let region = Region {
            id: params.id,
            volume_id: params.volume_id,
            block_size: params.block_size,
            extent_size: params.extent_size,
            extent_count: params.extent_count,
            // NOTE: This is a lie - no server is running.
            port_number: 0,

            // TODO: This should be "Started", we should control state
            // transitions.
//            state: State::Requested,
            state: State::Created,
        };
        self.regions.insert(id, region.clone());
        region
    }

    fn get(&self, id: RegionId) -> Option<Region> {
        let id = Uuid::from_str(&id.0).unwrap();
        self.regions.get(&id).cloned()
    }

    fn delete(&mut self, id: RegionId) -> Option<Region> {
        let id = Uuid::from_str(&id.0).unwrap();
        self.regions.remove(&id)
    }
}

pub struct CrucibleData {
    inner: Mutex<CrucibleDataInner>,
}

impl CrucibleData {
    fn new() -> Self {
        Self { inner: Mutex::new(CrucibleDataInner::new()) }
    }

    pub async fn list(&self) -> Vec<Region> {
        self.inner.lock().await.list()
    }

    pub async fn create(&self, params: CreateRegion) -> Region {
        self.inner.lock().await.create(params)
    }

    pub async fn get(&self, id: RegionId) -> Option<Region> {
        self.inner.lock().await.get(id)
    }

    pub async fn delete(&self, id: RegionId) -> Option<Region> {
        self.inner.lock().await.delete(id)
    }
}

/// A simulated Crucible Dataset.
///
/// Contains both the data and the HTTP server.
pub struct CrucibleDataset {
    server: dropshot::HttpServer<Arc<CrucibleData>>,
    _data: Arc<CrucibleData>,
}

impl CrucibleDataset {
    fn new(log: &Logger) -> Self {
        let data = Arc::new(CrucibleData::new());
        let config = dropshot::ConfigDropshot {
            bind_address: SocketAddr::new("127.0.0.1".parse().unwrap(), 0),
            ..Default::default()
        };
        let dropshot_log = log
            .new(o!("component" => "Simulated CrucibleAgent Dropshot Server"));
        let server = dropshot::HttpServerStarter::new(
            &config,
            super::http_entrypoints_storage::api(),
            data.clone(),
            &dropshot_log,
        )
        .expect("Could not initialize server")
        .start();

        CrucibleDataset { server, _data: data }
    }

    fn address(&self) -> SocketAddr {
        self.server.local_addr()
    }
}

pub struct Zpool {
    datasets: HashMap<Uuid, CrucibleDataset>,
}

impl Zpool {
    pub fn new() -> Self {
        Zpool { datasets: HashMap::new() }
    }

    pub fn insert_dataset(&mut self, log: &Logger, id: Uuid) -> &CrucibleDataset {
        self.datasets.insert(id, CrucibleDataset::new(log));
        self.datasets.get(&id).expect("Failed to get the dataset we just inserted")
    }
}

/// Simulated representation of all storage on a sled.
pub struct Storage {
    sled_id: Uuid,
    nexus_client: Arc<NexusClient>,
    log: Logger,
    zpools: HashMap<Uuid, Zpool>,
}

impl Storage {
    pub fn new(sled_id: Uuid, nexus_client: Arc<NexusClient>, log: Logger) -> Self {
        Self { sled_id, nexus_client, log, zpools: HashMap::new() }
    }

    /// Adds a Zpool to the sled's simulated storage and notifies Nexus.
    pub async fn insert_zpool(&mut self, zpool_id: Uuid, size: u64) {
        // Update our local data
        self.zpools.insert(zpool_id, Zpool::new());

        // Notify Nexus
        let request = ZpoolPutRequest {
            size: ByteCount(size),
        };
        self.nexus_client.zpool_put(&self.sled_id, &zpool_id, &request)
            .await
            .expect("Failed to notify Nexus about new Zpool");
    }

    /// Adds a Dataset to the sled's simulated storage and notifies Nexus.
    pub async fn insert_dataset(&mut self, zpool_id: Uuid, dataset_id: Uuid) {
        // Update our local data
        let dataset = self.zpools.get_mut(&zpool_id)
            .expect("Zpool does not exist")
            .insert_dataset(&self.log, dataset_id);

        // Notify Nexus
        let request = DatasetPutRequest {
            address: dataset.address().to_string(),
            kind: DatasetKind::Crucible,
        };
        self.nexus_client.dataset_put(&zpool_id, &dataset_id, &request)
            .await
            .expect("Failed to notify Nexus about new Dataset");
    }
}
