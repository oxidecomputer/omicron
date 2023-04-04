// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent storage implementation
//!
//! Note, this refers to the "storage which exists on the Sled", rather
//! than the representation of "virtual disks" which would be presented
//! through Nexus' external API.

use crate::nexus::NexusClient;
use crate::sim::http_entrypoints_pantry::ExpectedDigest;
use crate::sim::SledAgent;
use anyhow::{bail, Result};
use chrono::prelude::*;
use crucible_agent_client::types::{
    CreateRegion, Region, RegionId, RunningSnapshot, Snapshot, State,
};
use crucible_client_types::VolumeConstructionRequest;
use dropshot::HttpError;
use futures::lock::Mutex;
use nexus_client::types::{ByteCount, ZpoolPutRequest};
use slog::Logger;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

type CreateCallback = Box<dyn Fn(&CreateRegion) -> State + Send + 'static>;

struct CrucibleDataInner {
    regions: HashMap<Uuid, Region>,
    snapshots: HashMap<Uuid, Vec<Snapshot>>,
    running_snapshots: HashMap<Uuid, HashMap<String, RunningSnapshot>>,
    on_create: Option<CreateCallback>,
    next_port: u16,
}

impl CrucibleDataInner {
    fn new(crucible_port: u16) -> Self {
        Self {
            regions: HashMap::new(),
            snapshots: HashMap::new(),
            running_snapshots: HashMap::new(),
            on_create: None,
            next_port: crucible_port,
        }
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
            block_size: params.block_size,
            extent_size: params.extent_size,
            extent_count: params.extent_count,
            // NOTE: This is a lie - no server is running.
            port_number: self.next_port,
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
        self.next_port += 1;
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

    fn delete(&mut self, id: RegionId) -> Result<Option<Region>> {
        // Can't delete a ZFS dataset if there are snapshots
        if !self.snapshots_for_region(&id).is_empty() {
            bail!(
                "must delete snapshots {:?} first!",
                self.snapshots_for_region(&id)
                    .into_iter()
                    .map(|s| s.name)
                    .collect::<Vec<String>>(),
            );
        }

        let id = Uuid::from_str(&id.0).unwrap();
        if let Some(mut region) = self.regions.get_mut(&id) {
            region.state = State::Destroyed;
            Ok(Some(region.clone()))
        } else {
            Ok(None)
        }
    }

    fn create_snapshot(
        &mut self,
        id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<Snapshot> {
        let vec = self.snapshots.entry(id).or_insert_with(|| Vec::new());

        if vec.iter().any(|x| x.name == snapshot_id.to_string()) {
            bail!("region {} snapshot {} exists already", id, snapshot_id);
        }

        let snap =
            Snapshot { name: snapshot_id.to_string(), created: Utc::now() };

        vec.push(snap.clone());

        Ok(snap)
    }

    fn snapshots_for_region(&self, id: &RegionId) -> Vec<Snapshot> {
        let id = Uuid::from_str(&id.0).unwrap();
        match self.snapshots.get(&id) {
            Some(vec) => vec.clone(),
            None => vec![],
        }
    }

    fn running_snapshots_for_id(
        &self,
        id: &RegionId,
    ) -> HashMap<String, RunningSnapshot> {
        let id = Uuid::from_str(&id.0).unwrap();
        match self.running_snapshots.get(&id) {
            Some(map) => map.clone(),
            None => HashMap::new(),
        }
    }

    fn delete_snapshot(&mut self, id: &RegionId, name: &str) -> Result<()> {
        let running_snapshots_for_id = self.running_snapshots_for_id(id);
        if running_snapshots_for_id.contains_key(name) {
            bail!("downstairs running for region {} snapshot {}", id.0, name,);
        }

        let id = Uuid::from_str(&id.0).unwrap();
        if let Some(vec) = self.snapshots.get_mut(&id) {
            vec.retain(|x| x.name != name);
        }

        Ok(())
    }

    fn create_running_snapshot(
        &mut self,
        id: &RegionId,
        name: &str,
    ) -> Result<RunningSnapshot> {
        let id = Uuid::from_str(&id.0).unwrap();

        let map =
            self.running_snapshots.entry(id).or_insert_with(|| HashMap::new());

        // If a running snapshot exists already, return it - this endpoint must
        // be idempotent.
        if let Some(running_snapshot) = map.get(&name.to_string()) {
            return Ok(running_snapshot.clone());
        }

        let running_snapshot = RunningSnapshot {
            id: RegionId(Uuid::new_v4().to_string()),
            name: name.to_string(),
            port_number: self.next_port,
            state: State::Requested,
        };

        map.insert(name.to_string(), running_snapshot.clone());

        self.next_port += 1;

        Ok(running_snapshot)
    }

    fn delete_running_snapshot(
        &mut self,
        id: &RegionId,
        name: &str,
    ) -> Result<()> {
        let id = Uuid::from_str(&id.0).unwrap();

        let map =
            self.running_snapshots.entry(id).or_insert_with(|| HashMap::new());

        // If the running snapshot was already deleted, then return Ok
        if !map.contains_key(&name.to_string()) {
            return Ok(());
        }

        map.remove(&name.to_string());

        Ok(())
    }

    /// Return true if there are no undeleted Crucible resources
    pub fn is_empty(&self) -> bool {
        let non_destroyed_regions = self
            .regions
            .values()
            .filter(|r| r.state != State::Destroyed)
            .count();

        let snapshots = self.snapshots.values().flatten().count();

        let running_snapshots =
            self.running_snapshots.values().flat_map(|hm| hm.values()).count();

        non_destroyed_regions == 0 && snapshots == 0 && running_snapshots == 0
    }
}

/// Represents a running Crucible Agent. Contains regions.
pub struct CrucibleData {
    inner: Mutex<CrucibleDataInner>,
}

impl CrucibleData {
    fn new(crucible_port: u16) -> Self {
        Self { inner: Mutex::new(CrucibleDataInner::new(crucible_port)) }
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

    pub async fn delete(&self, id: RegionId) -> Result<Option<Region>> {
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

    pub async fn create_snapshot(
        &self,
        id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<Snapshot> {
        self.inner.lock().await.create_snapshot(id, snapshot_id)
    }

    pub async fn snapshots_for_region(&self, id: &RegionId) -> Vec<Snapshot> {
        self.inner.lock().await.snapshots_for_region(id)
    }

    pub async fn running_snapshots_for_id(
        &self,
        id: &RegionId,
    ) -> HashMap<String, RunningSnapshot> {
        self.inner.lock().await.running_snapshots_for_id(id)
    }

    pub async fn delete_snapshot(
        &self,
        id: &RegionId,
        name: &str,
    ) -> Result<()> {
        self.inner.lock().await.delete_snapshot(id, name)
    }

    pub async fn create_running_snapshot(
        &self,
        id: &RegionId,
        name: &str,
    ) -> Result<RunningSnapshot> {
        self.inner.lock().await.create_running_snapshot(id, name)
    }

    pub async fn delete_running_snapshot(
        &self,
        id: &RegionId,
        name: &str,
    ) -> Result<()> {
        self.inner.lock().await.delete_running_snapshot(id, name)
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.is_empty()
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
    fn new(log: &Logger, crucible_ip: IpAddr, crucible_port: u16) -> Self {
        // SocketAddr::new with port set to 0 will grab any open port to host
        // the emulated crucible agent, but set the fake downstairs listen ports
        // to start at `crucible_port`.
        let data = Arc::new(CrucibleData::new(crucible_port));
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

    pub fn data(&self) -> Arc<CrucibleData> {
        self.data.clone()
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
        crucible_port: u16,
    ) -> &CrucibleServer {
        self.datasets
            .insert(id, CrucibleServer::new(log, crucible_ip, crucible_port));
        self.datasets
            .get(&id)
            .expect("Failed to get the dataset we just inserted")
    }

    pub async fn get_dataset_for_region(
        &self,
        region_id: Uuid,
    ) -> Option<Arc<CrucibleData>> {
        for dataset in self.datasets.values() {
            for region in &dataset.data().list().await {
                let id = Uuid::from_str(&region.id.0).unwrap();
                if id == region_id {
                    return Some(dataset.data());
                }
            }
        }

        None
    }

    pub async fn get_dataset_for_port(
        &self,
        port: u16,
    ) -> Option<Arc<CrucibleData>> {
        for dataset in self.datasets.values() {
            for region in &dataset.data().list().await {
                if port == region.port_number {
                    return Some(dataset.data());
                }
            }
        }

        None
    }
}

/// Simulated representation of all storage on a sled.
pub struct Storage {
    sled_id: Uuid,
    nexus_client: Arc<NexusClient>,
    log: Logger,
    zpools: HashMap<Uuid, Zpool>,
    crucible_ip: IpAddr,
    next_crucible_port: u16,
}

impl Storage {
    pub fn new(
        sled_id: Uuid,
        nexus_client: Arc<NexusClient>,
        crucible_ip: IpAddr,
        log: Logger,
    ) -> Self {
        Self {
            sled_id,
            nexus_client,
            log,
            zpools: HashMap::new(),
            crucible_ip,
            next_crucible_port: 100,
        }
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

    /// Adds a Dataset to the sled's simulated storage.
    pub async fn insert_dataset(
        &mut self,
        zpool_id: Uuid,
        dataset_id: Uuid,
    ) -> SocketAddr {
        // Update our local data
        let dataset = self
            .zpools
            .get_mut(&zpool_id)
            .expect("Zpool does not exist")
            .insert_dataset(
                &self.log,
                dataset_id,
                self.crucible_ip,
                self.next_crucible_port,
            );

        self.next_crucible_port += 100;

        dataset.address()
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

    pub async fn get_dataset_for_region(
        &self,
        region_id: Uuid,
    ) -> Option<Arc<CrucibleData>> {
        for zpool in self.zpools.values() {
            if let Some(dataset) = zpool.get_dataset_for_region(region_id).await
            {
                return Some(dataset);
            }
        }

        None
    }

    pub async fn get_dataset_for_port(
        &self,
        port: u16,
    ) -> Option<Arc<CrucibleData>> {
        for zpool in self.zpools.values() {
            if let Some(dataset) = zpool.get_dataset_for_port(port).await {
                return Some(dataset);
            }
        }

        None
    }
}

/// Simulated crucible pantry
pub struct Pantry {
    pub id: Uuid,
    vcrs: Mutex<HashMap<String, VolumeConstructionRequest>>, // Please rewind!
    sled_agent: Arc<SledAgent>,
    jobs: Mutex<HashSet<String>>,
}

impl Pantry {
    pub fn new(sled_agent: Arc<SledAgent>) -> Self {
        Self {
            id: Uuid::new_v4(),
            vcrs: Mutex::new(HashMap::default()),
            sled_agent,
            jobs: Mutex::new(HashSet::default()),
        }
    }

    pub async fn entry(
        &self,
        volume_id: String,
    ) -> Result<VolumeConstructionRequest, HttpError> {
        let vcrs = self.vcrs.lock().await;
        match vcrs.get(&volume_id) {
            Some(entry) => Ok(entry.clone()),

            None => Err(HttpError::for_not_found(None, volume_id)),
        }
    }

    pub async fn attach(
        &self,
        volume_id: String,
        volume_construction_request: VolumeConstructionRequest,
    ) -> Result<()> {
        let mut vcrs = self.vcrs.lock().await;
        vcrs.insert(volume_id, volume_construction_request);
        Ok(())
    }

    pub async fn is_job_finished(
        &self,
        job_id: String,
    ) -> Result<bool, HttpError> {
        let jobs = self.jobs.lock().await;
        if !jobs.contains(&job_id) {
            return Err(HttpError::for_not_found(None, job_id));
        }
        Ok(true)
    }

    pub async fn get_job_result(
        &self,
        job_id: String,
    ) -> Result<Result<()>, HttpError> {
        let mut jobs = self.jobs.lock().await;
        if !jobs.contains(&job_id) {
            return Err(HttpError::for_not_found(None, job_id));
        }
        jobs.remove(&job_id);
        Ok(Ok(()))
    }

    pub async fn import_from_url(
        &self,
        volume_id: String,
        _url: String,
        _expected_digest: Option<ExpectedDigest>,
    ) -> Result<String, HttpError> {
        self.entry(volume_id).await?;

        // Make up job
        let mut jobs = self.jobs.lock().await;
        let job_id = Uuid::new_v4().to_string();
        jobs.insert(job_id.clone());

        Ok(job_id)
    }

    pub async fn snapshot(
        &self,
        volume_id: String,
        snapshot_id: String,
    ) -> Result<(), HttpError> {
        // Perform the disk id -> region id mapping just as was done by during
        // the simulated instance ensure, then call
        // [`instance_issue_disk_snapshot_request`] as the snapshot logic is the
        // same.
        let vcrs = self.vcrs.lock().await;
        let volume_construction_request = vcrs.get(&volume_id).unwrap();

        self.sled_agent
            .map_disk_ids_to_region_ids(&volume_construction_request)
            .await?;

        self.sled_agent
            .instance_issue_disk_snapshot_request(
                Uuid::new_v4(), // instance id, not used by function
                volume_id.parse().unwrap(),
                snapshot_id.parse().unwrap(),
            )
            .await
            .map_err(|e| HttpError::for_internal_error(e.to_string()))
    }

    pub async fn bulk_write(
        &self,
        volume_id: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), HttpError> {
        let vcr = self.entry(volume_id).await?;

        // Currently, Nexus will only make volumes where the first subvolume is
        // a Region. This will change in the future!
        let (region_block_size, region_size) = match vcr {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                match sub_volumes[0] {
                    VolumeConstructionRequest::Region {
                        block_size,
                        blocks_per_extent,
                        extent_count,
                        ..
                    } => (
                        block_size,
                        block_size * blocks_per_extent * (extent_count as u64),
                    ),

                    _ => {
                        panic!("unexpected Volume layout");
                    }
                }
            }

            _ => {
                panic!("unexpected Volume layout");
            }
        };

        if (offset % region_block_size) != 0 {
            return Err(HttpError::for_bad_request(
                None,
                "offset not multiple of block size!".to_string(),
            ));
        }

        if (data.len() as u64 % region_block_size) != 0 {
            return Err(HttpError::for_bad_request(
                None,
                "data length not multiple of block size!".to_string(),
            ));
        }

        if (offset + data.len() as u64) > region_size {
            return Err(HttpError::for_bad_request(
                None,
                "offset + data length off end of region!".to_string(),
            ));
        }

        Ok(())
    }

    pub async fn scrub(&self, volume_id: String) -> Result<String, HttpError> {
        self.entry(volume_id).await?;

        // Make up job
        let mut jobs = self.jobs.lock().await;
        let job_id = Uuid::new_v4().to_string();
        jobs.insert(job_id.clone());

        Ok(job_id)
    }

    pub async fn detach(&self, volume_id: String) -> Result<()> {
        let mut vcrs = self.vcrs.lock().await;
        vcrs.remove(&volume_id);
        Ok(())
    }
}

pub struct PantryServer {
    pub server: dropshot::HttpServer<Arc<Pantry>>,
    pub pantry: Arc<Pantry>,
}

impl PantryServer {
    pub async fn new(
        log: Logger,
        ip: IpAddr,
        sled_agent: Arc<SledAgent>,
    ) -> Self {
        let pantry = Arc::new(Pantry::new(sled_agent));

        let server = dropshot::HttpServerStarter::new(
            &dropshot::ConfigDropshot {
                bind_address: SocketAddr::new(ip, 0),
                // This has to be large enough to support:
                // - bulk writes into disks
                request_body_max_bytes: 8192 * 1024,
                ..Default::default()
            },
            super::http_entrypoints_pantry::api(),
            pantry.clone(),
            &log.new(o!("component" => "dropshot")),
        )
        .expect("Could not initialize pantry server")
        .start();

        info!(&log, "Started Simulated Crucible Pantry"; "address" => server.local_addr());

        PantryServer { server, pantry }
    }

    pub fn addr(&self) -> SocketAddr {
        self.server.local_addr()
    }
}
