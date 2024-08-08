// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent storage implementation
//!
//! Note, this refers to the "storage which exists on the Sled", rather
//! than the representation of "virtual disks" which would be presented
//! through Nexus' external API.

use crate::sim::http_entrypoints_pantry::ExpectedDigest;
use crate::sim::SledAgent;
use anyhow::{self, bail, Result};
use chrono::prelude::*;
use crucible_agent_client::types::{
    CreateRegion, Region, RegionId, RunningSnapshot, Snapshot, State,
};
use dropshot::HandlerTaskMode;
use dropshot::HttpError;
use futures::lock::Mutex;
use omicron_common::disk::DatasetsConfig;
use omicron_common::disk::DiskIdentity;
use omicron_common::disk::DiskVariant;
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::ZpoolUuid;
use propolis_client::types::VolumeConstructionRequest;
use sled_storage::resources::DatasetManagementStatus;
use sled_storage::resources::DatasetsManagementResult;
use sled_storage::resources::DiskManagementStatus;
use sled_storage::resources::DisksManagementResult;
use slog::Logger;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

type CreateCallback = Box<dyn Fn(&CreateRegion) -> State + Send + 'static>;

struct CrucibleDataInner {
    log: Logger,
    regions: HashMap<Uuid, Region>,
    snapshots: HashMap<Uuid, HashMap<String, Snapshot>>,
    running_snapshots: HashMap<Uuid, HashMap<String, RunningSnapshot>>,
    on_create: Option<CreateCallback>,
    region_creation_error: bool,
    region_deletion_error: bool,
    creating_a_running_snapshot_should_fail: bool,
    next_port: u16,
}

impl CrucibleDataInner {
    fn new(log: Logger, crucible_port: u16) -> Self {
        Self {
            log,
            regions: HashMap::new(),
            snapshots: HashMap::new(),
            running_snapshots: HashMap::new(),
            on_create: None,
            region_creation_error: false,
            region_deletion_error: false,
            creating_a_running_snapshot_should_fail: false,
            next_port: crucible_port,
        }
    }

    fn set_create_callback(&mut self, callback: CreateCallback) {
        self.on_create = Some(callback);
    }

    fn list(&self) -> Vec<Region> {
        self.regions.values().cloned().collect()
    }

    fn create(&mut self, params: CreateRegion) -> Result<Region> {
        let id = Uuid::from_str(&params.id.0).unwrap();

        let state = if let Some(on_create) = &self.on_create {
            on_create(&params)
        } else {
            State::Requested
        };

        if self.region_creation_error {
            bail!("region creation error!");
        }

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
            source: None,
            read_only: false,
        };

        let old = self.regions.insert(id, region.clone());

        if let Some(old) = old {
            assert_eq!(
                old.id.0, region.id.0,
                "Region already exists, but with a different ID"
            );
        }

        self.next_port += 1;

        Ok(region)
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

        if self.region_deletion_error {
            bail!("region deletion error!");
        }

        let id = Uuid::from_str(&id.0).unwrap();
        if let Some(region) = self.regions.get_mut(&id) {
            if region.state == State::Failed {
                // The real Crucible agent would not let a Failed region be
                // deleted
                bail!("cannot delete in state Failed");
            }

            region.state = State::Destroyed;
            Ok(Some(region.clone()))
        } else {
            Ok(None)
        }
    }

    fn create_snapshot(&mut self, id: Uuid, snapshot_id: Uuid) -> Snapshot {
        info!(self.log, "Creating region {} snapshot {}", id, snapshot_id);
        self.snapshots
            .entry(id)
            .or_insert_with(|| HashMap::new())
            .entry(snapshot_id.to_string())
            .or_insert_with(|| Snapshot {
                name: snapshot_id.to_string(),
                created: Utc::now(),
            })
            .clone()
    }

    fn snapshots_for_region(&self, id: &RegionId) -> Vec<Snapshot> {
        let id = Uuid::from_str(&id.0).unwrap();
        match self.snapshots.get(&id) {
            Some(map) => map.values().cloned().collect(),
            None => vec![],
        }
    }

    fn get_snapshot_for_region(
        &self,
        id: &RegionId,
        snapshot_id: &str,
    ) -> Option<Snapshot> {
        let id = Uuid::from_str(&id.0).unwrap();
        self.snapshots
            .get(&id)
            .and_then(|hm| hm.get(&snapshot_id.to_string()).cloned())
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
        if let Some(running_snapshot) = running_snapshots_for_id.get(name) {
            match &running_snapshot.state {
                State::Created | State::Requested | State::Tombstoned => {
                    bail!(
                        "downstairs running for region {} snapshot {}",
                        id.0,
                        name
                    );
                }

                State::Destroyed => {
                    // ok
                }

                State::Failed => {
                    bail!(
                        "failed downstairs running for region {} snapshot {}",
                        id.0,
                        name
                    );
                }
            }
        }

        info!(self.log, "Deleting region {} snapshot {}", id.0, name);
        let region_id = Uuid::from_str(&id.0).unwrap();
        if let Some(map) = self.snapshots.get_mut(&region_id) {
            map.remove(name);
        }
        Ok(())
    }

    fn set_creating_a_running_snapshot_should_fail(&mut self) {
        self.creating_a_running_snapshot_should_fail = true;
    }

    fn set_region_creation_error(&mut self, value: bool) {
        self.region_creation_error = value;
    }

    fn set_region_deletion_error(&mut self, value: bool) {
        self.region_deletion_error = value;
    }

    fn create_running_snapshot(
        &mut self,
        id: &RegionId,
        name: &str,
    ) -> Result<RunningSnapshot> {
        if self.creating_a_running_snapshot_should_fail {
            bail!("failure creating running snapshot");
        }

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
            state: State::Created,
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

        if let Some(running_snapshot) = map.get_mut(&name.to_string()) {
            running_snapshot.state = State::Destroyed;
        }

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

        let running_snapshots = self
            .running_snapshots
            .values()
            .flat_map(|hm| hm.values())
            .filter(|rs| rs.state != State::Destroyed)
            .count();

        info!(
            self.log,
            "is_empty non_destroyed_regions {} snapshots {} running_snapshots {}",
            non_destroyed_regions,
            snapshots,
            running_snapshots,
        );

        non_destroyed_regions == 0 && snapshots == 0 && running_snapshots == 0
    }
}

/// Represents a running Crucible Agent. Contains regions.
pub struct CrucibleData {
    inner: Mutex<CrucibleDataInner>,
}

impl CrucibleData {
    fn new(log: Logger, crucible_port: u16) -> Self {
        Self { inner: Mutex::new(CrucibleDataInner::new(log, crucible_port)) }
    }

    pub async fn set_create_callback(&self, callback: CreateCallback) {
        self.inner.lock().await.set_create_callback(callback);
    }

    pub async fn list(&self) -> Vec<Region> {
        self.inner.lock().await.list()
    }

    pub async fn create(&self, params: CreateRegion) -> Result<Region> {
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
    ) -> Snapshot {
        self.inner.lock().await.create_snapshot(id, snapshot_id)
    }

    pub async fn snapshots_for_region(&self, id: &RegionId) -> Vec<Snapshot> {
        self.inner.lock().await.snapshots_for_region(id)
    }

    pub async fn get_snapshot_for_region(
        &self,
        id: &RegionId,
        snapshot_id: &str,
    ) -> Option<Snapshot> {
        self.inner.lock().await.get_snapshot_for_region(id, snapshot_id)
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

    pub async fn set_creating_a_running_snapshot_should_fail(&self) {
        self.inner.lock().await.set_creating_a_running_snapshot_should_fail();
    }

    pub async fn set_region_creation_error(&self, value: bool) {
        self.inner.lock().await.set_region_creation_error(value);
    }

    pub async fn set_region_deletion_error(&self, value: bool) {
        self.inner.lock().await.set_region_deletion_error(value);
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
        let data = Arc::new(CrucibleData::new(
            log.new(slog::o!("port" => format!("{crucible_port}"))),
            crucible_port,
        ));
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

pub(crate) struct PhysicalDisk {
    pub(crate) identity: DiskIdentity,
    pub(crate) variant: DiskVariant,
    pub(crate) slot: i64,
}

pub(crate) struct Zpool {
    id: ZpoolUuid,
    physical_disk_id: Uuid,
    total_size: u64,
    datasets: HashMap<Uuid, CrucibleServer>,
}

impl Zpool {
    fn new(id: ZpoolUuid, physical_disk_id: Uuid, total_size: u64) -> Self {
        Zpool { id, physical_disk_id, total_size, datasets: HashMap::new() }
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

    pub fn total_size(&self) -> u64 {
        self.total_size
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

    pub fn drop_dataset(&mut self, id: Uuid) {
        let _ = self.datasets.remove(&id).expect("Failed to get the dataset");
    }
}

/// Simulated representation of all storage on a sled.
pub struct Storage {
    sled_id: Uuid,
    log: Logger,
    config: Option<OmicronPhysicalDisksConfig>,
    dataset_config: Option<DatasetsConfig>,
    physical_disks: HashMap<Uuid, PhysicalDisk>,
    next_disk_slot: i64,
    zpools: HashMap<ZpoolUuid, Zpool>,
    crucible_ip: IpAddr,
    next_crucible_port: u16,
}

impl Storage {
    pub fn new(sled_id: Uuid, crucible_ip: IpAddr, log: Logger) -> Self {
        Self {
            sled_id,
            log,
            config: None,
            dataset_config: None,
            physical_disks: HashMap::new(),
            next_disk_slot: 0,
            zpools: HashMap::new(),
            crucible_ip,
            next_crucible_port: 100,
        }
    }

    /// Returns an immutable reference to all (currently known) physical disks
    pub fn physical_disks(&self) -> &HashMap<Uuid, PhysicalDisk> {
        &self.physical_disks
    }

    pub async fn datasets_list(&self) -> Result<DatasetsConfig, HttpError> {
        let Some(config) = self.dataset_config.as_ref() else {
            return Err(HttpError::for_not_found(
                None,
                "No control plane datasets".into(),
            ));
        };
        Ok(config.clone())
    }

    pub async fn datasets_ensure(
        &mut self,
        config: DatasetsConfig,
    ) -> Result<DatasetsManagementResult, HttpError> {
        if let Some(stored_config) = self.dataset_config.as_ref() {
            if stored_config.generation < config.generation {
                return Err(HttpError::for_client_error(
                    None,
                    http::StatusCode::BAD_REQUEST,
                    "Generation number too old".to_string(),
                ));
            }
        }
        self.dataset_config.replace(config.clone());

        Ok(DatasetsManagementResult {
            status: config
                .datasets
                .values()
                .map(|config| DatasetManagementStatus {
                    dataset_name: config.name.clone(),
                    err: None,
                })
                .collect(),
        })
    }

    pub async fn omicron_physical_disks_list(
        &mut self,
    ) -> Result<OmicronPhysicalDisksConfig, HttpError> {
        let Some(config) = self.config.as_ref() else {
            return Err(HttpError::for_not_found(
                None,
                "No control plane disks".into(),
            ));
        };
        Ok(config.clone())
    }

    pub async fn omicron_physical_disks_ensure(
        &mut self,
        config: OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, HttpError> {
        if let Some(stored_config) = self.config.as_ref() {
            if stored_config.generation < config.generation {
                return Err(HttpError::for_client_error(
                    None,
                    http::StatusCode::BAD_REQUEST,
                    "Generation number too old".to_string(),
                ));
            }
        }
        self.config.replace(config.clone());

        Ok(DisksManagementResult {
            status: config
                .disks
                .into_iter()
                .map(|config| DiskManagementStatus {
                    identity: config.identity,
                    err: None,
                })
                .collect(),
        })
    }

    pub async fn insert_physical_disk(
        &mut self,
        id: Uuid,
        identity: DiskIdentity,
        variant: DiskVariant,
    ) {
        let slot = self.next_disk_slot;
        self.next_disk_slot += 1;
        self.physical_disks
            .insert(id, PhysicalDisk { identity, variant, slot });
    }

    /// Adds a Zpool to the sled's simulated storage.
    pub async fn insert_zpool(
        &mut self,
        zpool_id: ZpoolUuid,
        disk_id: Uuid,
        size: u64,
    ) {
        // Update our local data
        self.zpools.insert(zpool_id, Zpool::new(zpool_id, disk_id, size));
    }

    /// Returns an immutable reference to all zpools
    pub fn zpools(&self) -> &HashMap<ZpoolUuid, Zpool> {
        &self.zpools
    }

    /// Adds a Dataset to the sled's simulated storage.
    pub async fn insert_dataset(
        &mut self,
        zpool_id: ZpoolUuid,
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

    pub fn get_all_physical_disks(
        &self,
    ) -> Vec<nexus_client::types::PhysicalDiskPutRequest> {
        self.physical_disks
            .iter()
            .map(|(id, disk)| {
                let variant = match disk.variant {
                    DiskVariant::U2 => {
                        nexus_client::types::PhysicalDiskKind::U2
                    }
                    DiskVariant::M2 => {
                        nexus_client::types::PhysicalDiskKind::M2
                    }
                };

                nexus_client::types::PhysicalDiskPutRequest {
                    id: *id,
                    vendor: disk.identity.vendor.clone(),
                    serial: disk.identity.serial.clone(),
                    model: disk.identity.model.clone(),
                    variant,
                    sled_id: self.sled_id,
                }
            })
            .collect()
    }

    pub fn get_all_zpools(&self) -> Vec<nexus_client::types::ZpoolPutRequest> {
        self.zpools
            .values()
            .map(|pool| nexus_client::types::ZpoolPutRequest {
                id: pool.id.into_untyped_uuid(),
                sled_id: self.sled_id,
                physical_disk_id: pool.physical_disk_id,
            })
            .collect()
    }

    pub fn get_all_datasets(
        &self,
        zpool_id: ZpoolUuid,
    ) -> Vec<(Uuid, SocketAddr)> {
        let zpool = self.zpools.get(&zpool_id).expect("Zpool does not exist");

        zpool
            .datasets
            .iter()
            .map(|(id, server)| (*id, server.address()))
            .collect()
    }

    pub async fn get_dataset(
        &self,
        zpool_id: ZpoolUuid,
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

    pub fn drop_dataset(&mut self, zpool_id: ZpoolUuid, dataset_id: Uuid) {
        self.zpools
            .get_mut(&zpool_id)
            .expect("Zpool does not exist")
            .drop_dataset(dataset_id)
    }
}

/// Simulated crucible pantry
pub struct Pantry {
    pub id: OmicronZoneUuid,
    vcrs: Mutex<HashMap<String, VolumeConstructionRequest>>, // Please rewind!
    sled_agent: Arc<SledAgent>,
    jobs: Mutex<HashSet<String>>,
}

impl Pantry {
    pub fn new(sled_agent: Arc<SledAgent>) -> Self {
        Self {
            id: OmicronZoneUuid::new_v4(),
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
    ) -> Result<Result<bool>, HttpError> {
        let mut jobs = self.jobs.lock().await;
        if !jobs.contains(&job_id) {
            return Err(HttpError::for_not_found(None, job_id));
        }
        jobs.remove(&job_id);
        Ok(Ok(true))
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
                InstanceUuid::new_v4(), // instance id, not used by function
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
                        block_size
                            * blocks_per_extent
                            * u64::from(extent_count),
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
                default_handler_task_mode: HandlerTaskMode::Detached,
                log_headers: vec![],
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
