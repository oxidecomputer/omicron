// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent storage implementation

use futures::lock::Mutex;
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
            // NOTE: This is a lie, obviously. No server is running.
            port_number: 0,
            state: State::Requested,
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
    _server: dropshot::HttpServer<Arc<CrucibleData>>,
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

        CrucibleDataset { _server: server, _data: data }
    }
}

pub struct Zpool {
    datasets: HashMap<Uuid, CrucibleDataset>,
}

impl Zpool {
    pub fn new() -> Self {
        Zpool { datasets: HashMap::new() }
    }

    pub fn insert_dataset(&mut self, log: &Logger, id: Uuid) {
        self.datasets.insert(id, CrucibleDataset::new(log));
    }
}

/// Simulated representation of all storage on a sled.
pub struct Storage {
    log: Logger,
    zpools: HashMap<Uuid, Zpool>,
}

impl Storage {
    pub fn new(log: Logger) -> Self {
        Self { log, zpools: HashMap::new() }
    }

    pub fn log(&self) -> &Logger {
        &self.log
    }

    /// Adds a Zpool to the sled's simulated storage.
    ///
    /// The Zpool is originally empty.
    pub fn insert_zpool(&mut self, id: Uuid) {
        self.zpools.insert(id, Zpool::new());
    }

    pub fn get_zpool_mut(&mut self, id: Uuid) -> &mut Zpool {
        self.zpools.get_mut(&id).expect("Zpool does not exist")
    }
}
