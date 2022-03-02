// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent storage implementation
//!
//! Note, this refers to the "storage which exists on the Sled", rather
//! than the representation of "virtual disks" which would be presented
//! through Nexus' external API.

use crate::nexus::NexusClient;
use crucible_agent_client::types::{CreateRegion, Region, RegionId, State};
use futures::lock::Mutex;
use nexus_client::types::{
    ByteCount, DatasetKind, DatasetPutRequest, ZpoolPutRequest,
};
use slog::Logger;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

type CreateCallback = Box<dyn Fn(&CreateRegion) -> State + Send + 'static>;

struct CrucibleDataInner {
    regions: HashMap<Uuid, Region>,
    on_create: Option<CreateCallback>,
}

impl CrucibleDataInner {
    fn new() -> Self {
        Self { regions: HashMap::new(), on_create: None }
    }

    fn set_create_callback(&mut self, callback: CreateCallback) {
        self.on_create = Some(callback);
    }

    fn list(&self) -> Vec<Region> {
        self.regions.values().cloned().collect()
    }

    fn create(&mut self, params: CreateRegion) -> Region {
        let id = Uuid::from_str(&params.id.0).unwrap();

        let state = if let Some(on_create) = &self.on_create {
            on_create(&params)
        } else {
            State::Requested
        };

        let region = Region {
            id: params.id,
            volume_id: params.volume_id,
            block_size: params.block_size,
            extent_size: params.extent_size,
            extent_count: params.extent_count,
            // NOTE: This is a lie - no server is running.
            port_number: 0,
            state,
            encrypted: false,
            cert_pem: None,
            key_pem: None,
            root_pem: None,
        };
        let old = self.regions.insert(id, region.clone());
        if let Some(old) = old {
            assert_eq!(
                old.id.0, region.id.0,
                "Region already exists, but with a different ID"
            );
        }
        region
    }

    fn get(&self, id: RegionId) -> Option<Region> {
        let id = Uuid::from_str(&id.0).unwrap();
        self.regions.get(&id).cloned()
    }

    fn get_mut(&mut self, id: &RegionId) -> Option<&mut Region> {
        let id = Uuid::from_str(&id.0).unwrap();
        self.regions.get_mut(&id)
    }

    fn delete(&mut self, id: RegionId) -> Option<Region> {
        let id = Uuid::from_str(&id.0).unwrap();
        let mut region = self.regions.get_mut(&id)?;
        region.state = State::Destroyed;
        Some(region.clone())
    }
}

/// Represents a running Crucible Agent. Contains regions.
pub struct CrucibleData {
    inner: Mutex<CrucibleDataInner>,
}

impl CrucibleData {
    fn new() -> Self {
        Self { inner: Mutex::new(CrucibleDataInner::new()) }
    }

    pub async fn set_create_callback(&self, callback: CreateCallback) {
        self.inner.lock().await.set_create_callback(callback);
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

    pub async fn set_state(&self, id: &RegionId, state: State) {
        self.inner
            .lock()
            .await
            .get_mut(id)
            .expect("region does not exist")
            .state = state;
    }
}

/// A simulated Crucible Dataset.
///
/// Contains both the data and the HTTP server.
pub struct CrucibleServer {
    server: dropshot::HttpServer<Arc<CrucibleData>>,
    data: Arc<CrucibleData>,
}

impl CrucibleServer {
    fn new(log: &Logger, crucible_ip: IpAddr) -> Self {
        let data = Arc::new(CrucibleData::new());
        let config = dropshot::ConfigDropshot {
            bind_address: SocketAddr::new(crucible_ip, 0),
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
        info!(&log, "Created Simulated Crucible Server"; "address" => server.local_addr());

        CrucibleServer { server, data }
    }

    fn address(&self) -> SocketAddr {
        self.server.local_addr()
    }
}

struct Zpool {
    datasets: HashMap<Uuid, CrucibleServer>,
}

impl Zpool {
    fn new() -> Self {
        Zpool { datasets: HashMap::new() }
    }

    fn insert_dataset(
        &mut self,
        log: &Logger,
        id: Uuid,
        crucible_ip: IpAddr,
    ) -> &CrucibleServer {
        self.datasets.insert(id, CrucibleServer::new(log, crucible_ip));
        self.datasets
            .get(&id)
            .expect("Failed to get the dataset we just inserted")
    }
}

/// Simulated representation of all storage on a sled.
pub struct Storage {
    sled_id: Uuid,
    nexus_client: Arc<NexusClient>,
    log: Logger,
    zpools: HashMap<Uuid, Zpool>,
    crucible_ip: IpAddr,
}

impl Storage {
    pub fn new(
        sled_id: Uuid,
        nexus_client: Arc<NexusClient>,
        crucible_ip: IpAddr,
        log: Logger,
    ) -> Self {
        Self { sled_id, nexus_client, log, zpools: HashMap::new(), crucible_ip }
    }

    /// Adds a Zpool to the sled's simulated storage and notifies Nexus.
    pub async fn insert_zpool(&mut self, zpool_id: Uuid, size: u64) {
        // Update our local data
        self.zpools.insert(zpool_id, Zpool::new());

        // Notify Nexus
        let request = ZpoolPutRequest { size: ByteCount(size) };
        self.nexus_client
            .zpool_put(&self.sled_id, &zpool_id, &request)
            .await
            .expect("Failed to notify Nexus about new Zpool");
    }

    /// Adds a Dataset to the sled's simulated storage and notifies Nexus.
    pub async fn insert_dataset(&mut self, zpool_id: Uuid, dataset_id: Uuid) {
        // Update our local data
        let dataset = self
            .zpools
            .get_mut(&zpool_id)
            .expect("Zpool does not exist")
            .insert_dataset(&self.log, dataset_id, self.crucible_ip);

        // Notify Nexus
        let request = DatasetPutRequest {
            address: dataset.address().to_string(),
            kind: DatasetKind::Crucible,
        };
        self.nexus_client
            .dataset_put(&zpool_id, &dataset_id, &request)
            .await
            .expect("Failed to notify Nexus about new Dataset");
    }

    pub async fn get_dataset(
        &self,
        zpool_id: Uuid,
        dataset_id: Uuid,
    ) -> Arc<CrucibleData> {
        self.zpools
            .get(&zpool_id)
            .expect("Zpool does not exist")
            .datasets
            .get(&dataset_id)
            .expect("Dataset does not exist")
            .data
            .clone()
    }
}
