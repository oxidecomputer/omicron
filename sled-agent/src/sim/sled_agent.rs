// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use super::collection::{PokeMode, SimCollection};
use super::config::Config;
use super::disk::SimDisk;
use super::instance::SimInstance;
use super::storage::CrucibleData;
use super::storage::Storage;

use crate::nexus::NexusClient;
use crate::params::{
    DiskStateRequested, InstanceHardware, InstanceMigrationSourceParams,
    InstancePutStateResponse, InstanceStateRequested,
    InstanceUnregisterResponse,
};
use crate::sim::simulatable::Simulatable;
use crate::updates::UpdateManager;
use futures::lock::Mutex;
use omicron_common::api::external::{DiskState, Error, ResourceType};
use omicron_common::api::internal::nexus::{
    DiskRuntimeState, SledInstanceState,
};
use omicron_common::api::internal::nexus::{
    InstanceRuntimeState, VmmRuntimeState,
};
use slog::Logger;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use uuid::Uuid;

use std::collections::HashMap;
use std::str::FromStr;

use dropshot::HttpServer;
use illumos_utils::opte::params::{
    DeleteVirtualNetworkInterfaceHost, SetVirtualNetworkInterfaceHost,
};
use nexus_client::types::PhysicalDiskKind;
use omicron_common::address::PROPOLIS_PORT;
use propolis_client::{
    types::VolumeConstructionRequest, Client as PropolisClient,
};
use propolis_mock_server::Context as PropolisContext;

/// Simulates management of the control plane on a sled
///
/// The current implementation simulates a server directly in this program.
/// **It's important to be careful about the interface exposed by this struct.**
/// The intent is for it to eventually be implemented using requests to a remote
/// server.  The tighter the coupling that exists now, the harder this will be to
/// move later.
pub struct SledAgent {
    pub id: Uuid,
    pub ip: IpAddr,
    /// collection of simulated instances, indexed by instance uuid
    instances: Arc<SimCollection<SimInstance>>,
    /// collection of simulated disks, indexed by disk uuid
    disks: Arc<SimCollection<SimDisk>>,
    storage: Mutex<Storage>,
    updates: UpdateManager,
    nexus_address: SocketAddr,
    pub nexus_client: Arc<NexusClient>,
    disk_id_to_region_ids: Mutex<HashMap<String, Vec<Uuid>>>,
    pub v2p_mappings: Mutex<HashMap<Uuid, Vec<SetVirtualNetworkInterfaceHost>>>,
    mock_propolis:
        Mutex<Option<(HttpServer<Arc<PropolisContext>>, PropolisClient)>>,
}

fn extract_targets_from_volume_construction_request(
    vcr: &VolumeConstructionRequest,
) -> Result<Vec<SocketAddr>, std::net::AddrParseError> {
    // A snapshot is simply a flush with an extra parameter, and flushes are
    // only sent to sub volumes, not the read only parent. Flushes are only
    // processed by regions, so extract each region that would be affected by a
    // flush.

    let mut res = vec![];
    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes,
            read_only_parent: _,
        } => {
            for sub_volume in sub_volumes.iter() {
                res.extend(extract_targets_from_volume_construction_request(
                    sub_volume,
                )?);
            }
        }

        VolumeConstructionRequest::Url { .. } => {
            // noop
        }

        VolumeConstructionRequest::Region {
            block_size: _,
            blocks_per_extent: _,
            extent_count: _,
            opts,
            gen: _,
        } => {
            for target in &opts.target {
                res.push(SocketAddr::from_str(target)?);
            }
        }

        VolumeConstructionRequest::File { .. } => {
            // noop
        }
    }
    Ok(res)
}

impl SledAgent {
    // TODO-cleanup should this instantiate the NexusClient it needs?
    // Should it take a Config object instead of separate id, sim_mode, etc?
    /// Constructs a simulated SledAgent with the given uuid.
    pub async fn new_simulated_with_id(
        config: &Config,
        log: Logger,
        nexus_address: SocketAddr,
        nexus_client: Arc<NexusClient>,
    ) -> Arc<SledAgent> {
        let id = config.id;
        let sim_mode = config.sim_mode;
        info!(&log, "created simulated sled agent"; "sim_mode" => ?sim_mode);

        let instance_log = log.new(o!("kind" => "instances"));
        let disk_log = log.new(o!("kind" => "disks"));
        let storage_log = log.new(o!("kind" => "storage"));

        Arc::new(SledAgent {
            id,
            ip: config.dropshot.bind_address.ip(),
            instances: Arc::new(SimCollection::new(
                Arc::clone(&nexus_client),
                instance_log,
                sim_mode,
            )),
            disks: Arc::new(SimCollection::new(
                Arc::clone(&nexus_client),
                disk_log,
                sim_mode,
            )),
            storage: Mutex::new(Storage::new(
                id,
                Arc::clone(&nexus_client),
                config.storage.ip,
                storage_log,
            )),
            updates: UpdateManager::new(config.updates.clone()),
            nexus_address,
            nexus_client,
            disk_id_to_region_ids: Mutex::new(HashMap::new()),
            v2p_mappings: Mutex::new(HashMap::new()),
            mock_propolis: Mutex::new(None),
        })
    }

    /// Map disk id to regions for later lookup
    ///
    /// Crucible regions are returned with a port number, and volume
    /// construction requests contain a single Nexus region (which points to
    /// three crucible regions). Extract the region addresses, lookup the
    /// region from the port (which should be unique), and pair disk id with
    /// region ids. This map is referred to later when making snapshots.
    pub async fn map_disk_ids_to_region_ids(
        &self,
        volume_construction_request: &VolumeConstructionRequest,
    ) -> Result<(), Error> {
        let disk_id = match volume_construction_request {
            VolumeConstructionRequest::Volume { id, .. } => id,

            _ => {
                panic!("root of volume construction request not a volume!");
            }
        };

        let targets = extract_targets_from_volume_construction_request(
            &volume_construction_request,
        )
        .map_err(|e| {
            Error::invalid_request(&format!("bad socketaddr: {e:?}"))
        })?;

        let mut region_ids = Vec::new();

        let storage = self.storage.lock().await;
        for target in targets {
            let crucible_data = storage
                .get_dataset_for_port(target.port())
                .await
                .ok_or_else(|| {
                    Error::internal_error(&format!(
                        "no dataset for port {}",
                        target.port()
                    ))
                })?;

            for region in crucible_data.list().await {
                if region.port_number == target.port() {
                    let region_id = Uuid::from_str(&region.id.0).unwrap();
                    region_ids.push(region_id);
                }
            }
        }

        let mut disk_id_to_region_ids = self.disk_id_to_region_ids.lock().await;
        disk_id_to_region_ids.insert(disk_id.to_string(), region_ids.clone());

        Ok(())
    }

    /// Idempotently ensures that the given API Instance (described by
    /// `api_instance`) exists on this server in the given runtime state
    /// (described by `target`).
    pub async fn instance_register(
        self: &Arc<Self>,
        instance_id: Uuid,
        propolis_id: Uuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
    ) -> Result<SledInstanceState, Error> {
        // respond with a fake 500 level failure if asked to ensure an instance
        // with more than 16 CPUs.
        let ncpus: i64 = (&hardware.properties.ncpus).into();
        if ncpus > 16 {
            return Err(Error::internal_error(
                &"could not allocate an instance: ran out of CPUs!",
            ));
        };

        for disk in &hardware.disks {
            let initial_state = DiskRuntimeState {
                disk_state: DiskState::Attached(instance_id),
                gen: omicron_common::api::external::Generation::new(),
                time_updated: chrono::Utc::now(),
            };

            // Ensure that any disks that are in this request are attached to
            // this instance.
            let id = match disk.volume_construction_request {
                VolumeConstructionRequest::Volume { id, .. } => id,
                _ => panic!("Unexpected construction type"),
            };
            self.disks
                .sim_ensure(
                    &id,
                    initial_state,
                    Some(DiskStateRequested::Attached(instance_id)),
                )
                .await?;
            self.disks
                .sim_ensure_producer(&id, (self.nexus_address, id))
                .await?;
        }

        // If the user of this simulated agent previously requested a mock
        // Propolis server, start that server.
        //
        // N.B. The server serves on localhost and not on the per-sled IPv6
        //      address that Nexus chose when starting the instance. Tests that
        //      use the mock are expected to correct the contents of CRDB to
        //      point to the correct address.
        let mock_lock = self.mock_propolis.lock().await;
        if let Some((_srv, client)) = mock_lock.as_ref() {
            if !self.instances.contains_key(&instance_id).await {
                let properties = propolis_client::types::InstanceProperties {
                    id: propolis_id,
                    name: hardware.properties.hostname.clone(),
                    description: "sled-agent-sim created instance".to_string(),
                    image_id: Uuid::default(),
                    bootrom_id: Uuid::default(),
                    memory: hardware.properties.memory.to_whole_mebibytes(),
                    vcpus: hardware.properties.ncpus.0 as u8,
                };
                let body = propolis_client::types::InstanceEnsureRequest {
                    properties,
                    nics: vec![],
                    disks: vec![],
                    migrate: None,
                    cloud_init_bytes: None,
                };
                // Try to create the instance
                client.instance_ensure().body(body).send().await.map_err(
                    |e| {
                        Error::internal_error(&format!(
                            "propolis-client: {}",
                            e
                        ))
                    },
                )?;
            }
        }

        let instance_run_time_state = self
            .instances
            .sim_ensure(
                &instance_id,
                SledInstanceState {
                    instance_state: instance_runtime,
                    vmm_state: vmm_runtime,
                    propolis_id,
                },
                None,
            )
            .await?;

        for disk_request in &hardware.disks {
            let vcr = &disk_request.volume_construction_request;
            self.map_disk_ids_to_region_ids(&vcr).await?;
        }

        Ok(instance_run_time_state)
    }

    /// Forcibly unregisters an instance. To simulate the rude termination that
    /// this produces in the real sled agent, the instance's mock Propolis is
    /// not notified.
    pub async fn instance_unregister(
        self: &Arc<Self>,
        instance_id: Uuid,
    ) -> Result<InstanceUnregisterResponse, Error> {
        let instance =
            match self.instances.sim_get_cloned_object(&instance_id).await {
                Ok(instance) => instance,
                Err(Error::ObjectNotFound { .. }) => {
                    return Ok(InstanceUnregisterResponse {
                        updated_runtime: None,
                    })
                }
                Err(e) => return Err(e),
            };

        self.detach_disks_from_instance(instance_id).await?;
        let response = InstanceUnregisterResponse {
            updated_runtime: Some(instance.terminate()),
        };

        // Poke the now-destroyed instance to force it to be removed from the
        // collection.
        //
        // TODO: In the real sled agent, this happens inline without publishing
        // any other state changes, whereas this call causes any pending state
        // changes to be published. This can be fixed by adding a simulated
        // object collection function to forcibly remove an object from a
        // collection.
        self.instances.sim_poke(instance_id, PokeMode::Drain).await;
        Ok(response)
    }

    /// Asks the supplied instance to transition to the requested state.
    pub async fn instance_ensure_state(
        self: &Arc<Self>,
        instance_id: Uuid,
        state: InstanceStateRequested,
    ) -> Result<InstancePutStateResponse, Error> {
        let current =
            match self.instances.sim_get_cloned_object(&instance_id).await {
                Ok(i) => i.current().clone(),
                Err(_) => match state {
                    InstanceStateRequested::Stopped => {
                        return Ok(InstancePutStateResponse {
                            updated_runtime: None,
                        });
                    }
                    _ => {
                        return Err(Error::invalid_request(&format!(
                            "instance {} not registered on sled",
                            instance_id,
                        )));
                    }
                },
            };

        let mock_lock = self.mock_propolis.lock().await;
        if let Some((_srv, client)) = mock_lock.as_ref() {
            let body = match state {
                InstanceStateRequested::MigrationTarget(_) => {
                    return Err(Error::internal_error(
                        "migration not implemented for mock Propolis",
                    ));
                }
                InstanceStateRequested::Running => {
                    propolis_client::types::InstanceStateRequested::Run
                }
                InstanceStateRequested::Stopped => {
                    propolis_client::types::InstanceStateRequested::Stop
                }
                InstanceStateRequested::Reboot => {
                    propolis_client::types::InstanceStateRequested::Reboot
                }
            };
            client.instance_state_put().body(body).send().await.map_err(
                |e| Error::internal_error(&format!("propolis-client: {}", e)),
            )?;
        }

        let new_state = self
            .instances
            .sim_ensure(&instance_id, current, Some(state))
            .await?;

        // If this request will shut down the simulated instance, look for any
        // disks that are attached to it and drive them to the Detached state.
        if matches!(state, InstanceStateRequested::Stopped) {
            self.detach_disks_from_instance(instance_id).await?;
        }

        Ok(InstancePutStateResponse { updated_runtime: Some(new_state) })
    }

    async fn detach_disks_from_instance(
        &self,
        instance_id: Uuid,
    ) -> Result<(), Error> {
        self.disks
            .sim_ensure_for_each_where(
                |disk| match disk.current().disk_state {
                    DiskState::Attached(id) | DiskState::Attaching(id) => {
                        id == instance_id
                    }
                    _ => false,
                },
                &DiskStateRequested::Detached,
            )
            .await?;

        Ok(())
    }

    pub async fn instance_put_migration_ids(
        self: &Arc<Self>,
        instance_id: Uuid,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<SledInstanceState, Error> {
        let instance =
            self.instances.sim_get_cloned_object(&instance_id).await?;

        instance.put_migration_ids(old_runtime, migration_ids).await
    }

    /// Idempotently ensures that the given API Disk (described by `api_disk`)
    /// is attached (or not) as specified.  This simulates disk attach and
    /// detach, similar to instance boot and halt.
    pub async fn disk_ensure(
        self: &Arc<Self>,
        disk_id: Uuid,
        initial_state: DiskRuntimeState,
        target: DiskStateRequested,
    ) -> Result<DiskRuntimeState, Error> {
        self.disks.sim_ensure(&disk_id, initial_state, Some(target)).await
    }

    pub fn updates(&self) -> &UpdateManager {
        &self.updates
    }

    pub async fn instance_count(&self) -> usize {
        self.instances.size().await
    }

    pub async fn disk_count(&self) -> usize {
        self.disks.size().await
    }

    pub async fn instance_poke(&self, id: Uuid) {
        self.instances.sim_poke(id, PokeMode::Drain).await;
    }

    pub async fn disk_poke(&self, id: Uuid) {
        self.disks.sim_poke(id, PokeMode::SingleStep).await;
    }

    /// Adds a Physical Disk to the simulated sled agent.
    pub async fn create_external_physical_disk(
        &self,
        vendor: String,
        serial: String,
        model: String,
    ) {
        let variant = PhysicalDiskKind::U2;
        self.storage
            .lock()
            .await
            .insert_physical_disk(vendor, serial, model, variant)
            .await;
    }

    pub async fn get_zpools(&self) -> Vec<Uuid> {
        self.storage.lock().await.get_all_zpools()
    }

    pub async fn get_datasets(
        &self,
        zpool_id: Uuid,
    ) -> Vec<(Uuid, SocketAddr)> {
        self.storage.lock().await.get_all_datasets(zpool_id)
    }

    /// Adds a Zpool to the simulated sled agent.
    pub async fn create_zpool(
        &self,
        id: Uuid,
        vendor: String,
        serial: String,
        model: String,
        size: u64,
    ) {
        self.storage
            .lock()
            .await
            .insert_zpool(id, vendor, serial, model, size)
            .await;
    }

    /// Adds a Crucible Dataset within a zpool.
    pub async fn create_crucible_dataset(
        &self,
        zpool_id: Uuid,
        dataset_id: Uuid,
    ) -> SocketAddr {
        self.storage.lock().await.insert_dataset(zpool_id, dataset_id).await
    }

    /// Returns a crucible dataset within a particular zpool.
    pub async fn get_crucible_dataset(
        &self,
        zpool_id: Uuid,
        dataset_id: Uuid,
    ) -> Arc<CrucibleData> {
        self.storage.lock().await.get_dataset(zpool_id, dataset_id).await
    }

    /// Issue a snapshot request for a Crucible disk attached to an instance.
    ///
    /// The real sled agent simply sends this snapshot request to the
    /// instance's propolis server, which returns 200 OK and no body when the
    /// request is processed by the crucible backend. Later, Nexus will ask
    /// the crucible agent if the snapshot was created ok too.
    ///
    /// We're not simulating the propolis server, so directly create a
    /// snapshot here.
    pub async fn instance_issue_disk_snapshot_request(
        &self,
        _instance_id: Uuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        // In order to fulfill the snapshot request, emulate creating snapshots
        // for each region that makes up the disk. Use the disk_id_to_region_ids
        // map to perform lookup based on this function's disk id argument.

        let disk_id_to_region_ids = self.disk_id_to_region_ids.lock().await;
        let region_ids = disk_id_to_region_ids.get(&disk_id.to_string());

        let region_ids = region_ids.ok_or_else(|| {
            Error::not_found_by_id(ResourceType::Disk, &disk_id)
        })?;

        let storage = self.storage.lock().await;

        for region_id in region_ids {
            let crucible_data =
                storage.get_dataset_for_region(*region_id).await;

            if let Some(crucible_data) = crucible_data {
                crucible_data.create_snapshot(*region_id, snapshot_id).await;
            } else {
                return Err(Error::not_found_by_id(
                    ResourceType::Disk,
                    &disk_id,
                ));
            }
        }

        Ok(())
    }

    pub async fn set_virtual_nic_host(
        &self,
        interface_id: Uuid,
        mapping: &SetVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        let mut v2p_mappings = self.v2p_mappings.lock().await;
        let vec = v2p_mappings.entry(interface_id).or_default();
        vec.push(mapping.clone());
        Ok(())
    }

    pub async fn unset_virtual_nic_host(
        &self,
        interface_id: Uuid,
        mapping: &DeleteVirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        let mut v2p_mappings = self.v2p_mappings.lock().await;
        let vec = v2p_mappings.entry(interface_id).or_default();
        vec.retain(|x| {
            x.virtual_ip != mapping.virtual_ip || x.vni != mapping.vni
        });

        // If the last entry was removed, remove the entire interface ID so that
        // tests don't have to distinguish never-created entries from
        // previously-extant-but-now-empty entries.
        if vec.is_empty() {
            v2p_mappings.remove(&interface_id);
        }

        Ok(())
    }

    /// Used for integration tests that require a component to talk to a
    /// mocked propolis-server API.
    // TODO: fix schemas so propolis-server's port isn't hardcoded in nexus
    // such that we can run more than one of these.
    // (this is only needed by test_instance_serial at present)
    pub async fn start_local_mock_propolis_server(
        &self,
        log: &Logger,
    ) -> Result<(), Error> {
        let mut mock_lock = self.mock_propolis.lock().await;
        if mock_lock.is_some() {
            return Err(Error::ObjectAlreadyExists {
                type_name: ResourceType::Service,
                object_name: "mock propolis server".to_string(),
            });
        }
        let propolis_bind_address =
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), PROPOLIS_PORT);
        let dropshot_config = dropshot::ConfigDropshot {
            bind_address: propolis_bind_address,
            ..Default::default()
        };
        let propolis_log = log.new(o!("component" => "propolis-server-mock"));
        let private = Arc::new(PropolisContext::new(propolis_log));
        info!(log, "Starting mock propolis-server...");
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let mock_api = propolis_mock_server::api();

        let srv = dropshot::HttpServerStarter::new(
            &dropshot_config,
            mock_api,
            private,
            &dropshot_log,
        )
        .map_err(|error| {
            Error::unavail(&format!("initializing propolis-server: {}", error))
        })?
        .start();
        let client = propolis_client::Client::new(&format!(
            "http://{}",
            srv.local_addr()
        ));
        *mock_lock = Some((srv, client));
        Ok(())
    }
}
