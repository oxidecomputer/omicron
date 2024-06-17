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
use crate::bootstrap::early_networking::{
    EarlyNetworkConfig, EarlyNetworkConfigBody,
};
use crate::nexus::NexusClient;
use crate::params::{
    DiskStateRequested, InstanceExternalIpBody, InstanceHardware,
    InstanceMetadata, InstanceMigrationSourceParams, InstancePutStateResponse,
    InstanceStateRequested, InstanceUnregisterResponse, Inventory,
    OmicronPhysicalDisksConfig, OmicronZonesConfig, SledRole,
};
use crate::sim::simulatable::Simulatable;
use crate::updates::UpdateManager;
use anyhow::bail;
use anyhow::Context;
use dropshot::{HttpError, HttpServer};
use futures::lock::Mutex;
use illumos_utils::opte::params::VirtualNetworkInterfaceHost;
use omicron_common::api::external::{
    ByteCount, DiskState, Error, Generation, ResourceType,
};
use omicron_common::api::internal::nexus::{
    DiskRuntimeState, SledInstanceState,
};
use omicron_common::api::internal::nexus::{
    InstanceRuntimeState, VmmRuntimeState,
};
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid, PropolisUuid, ZpoolUuid};
use oxnet::Ipv6Net;
use propolis_client::{
    types::VolumeConstructionRequest, Client as PropolisClient,
};
use propolis_mock_server::Context as PropolisContext;
use sled_storage::resources::DisksManagementResult;
use slog::Logger;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

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
    pub v2p_mappings: Mutex<HashSet<VirtualNetworkInterfaceHost>>,
    mock_propolis:
        Mutex<Option<(HttpServer<Arc<PropolisContext>>, PropolisClient)>>,
    /// lists of external IPs assigned to instances
    pub external_ips: Mutex<HashMap<Uuid, HashSet<InstanceExternalIpBody>>>,
    config: Config,
    fake_zones: Mutex<OmicronZonesConfig>,
    instance_ensure_state_error: Mutex<Option<Error>>,
    pub bootstore_network_config: Mutex<EarlyNetworkConfig>,
    pub log: Logger,
}

fn extract_targets_from_volume_construction_request(
    vcr: &VolumeConstructionRequest,
) -> Result<Vec<SocketAddr>, std::net::AddrParseError> {
    // A snapshot is simply a flush with an extra parameter, and flushes are
    // only sent to sub volumes, not the read only parent. Flushes are only
    // processed by regions, so extract each region that would be affected by a
    // flush.

    let mut res = vec![];
    let mut parts: VecDeque<&VolumeConstructionRequest> = VecDeque::new();
    parts.push_back(&vcr);

    while let Some(vcr_part) = parts.pop_front() {
        match vcr_part {
            VolumeConstructionRequest::Volume { sub_volumes, .. } => {
                for sub_volume in sub_volumes {
                    parts.push_back(sub_volume);
                }
            }

            VolumeConstructionRequest::Url { .. } => {
                // noop
            }

            VolumeConstructionRequest::Region { opts, .. } => {
                for target in &opts.target {
                    res.push(SocketAddr::from_str(&target)?);
                }
            }

            VolumeConstructionRequest::File { .. } => {
                // noop
            }
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

        let bootstore_network_config = Mutex::new(EarlyNetworkConfig {
            generation: 0,
            schema_version: 1,
            body: EarlyNetworkConfigBody {
                ntp_servers: Vec::new(),
                rack_network_config: Some(RackNetworkConfig {
                    rack_subnet: Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 56)
                        .unwrap(),
                    infra_ip_first: Ipv4Addr::UNSPECIFIED,
                    infra_ip_last: Ipv4Addr::UNSPECIFIED,
                    ports: Vec::new(),
                    bgp: Vec::new(),
                    bfd: Vec::new(),
                }),
            },
        });

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
                config.storage.ip,
                storage_log,
            )),
            updates: UpdateManager::new(config.updates.clone()),
            nexus_address,
            nexus_client,
            disk_id_to_region_ids: Mutex::new(HashMap::new()),
            v2p_mappings: Mutex::new(HashSet::new()),
            external_ips: Mutex::new(HashMap::new()),
            mock_propolis: Mutex::new(None),
            config: config.clone(),
            fake_zones: Mutex::new(OmicronZonesConfig {
                generation: Generation::new(),
                zones: vec![],
            }),
            instance_ensure_state_error: Mutex::new(None),
            log,
            bootstore_network_config,
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
        instance_id: InstanceUuid,
        propolis_id: PropolisUuid,
        hardware: InstanceHardware,
        instance_runtime: InstanceRuntimeState,
        vmm_runtime: VmmRuntimeState,
        metadata: InstanceMetadata,
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
                disk_state: DiskState::Attached(
                    instance_id.into_untyped_uuid(),
                ),
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
                    Some(DiskStateRequested::Attached(
                        instance_id.into_untyped_uuid(),
                    )),
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
            if !self
                .instances
                .contains_key(&instance_id.into_untyped_uuid())
                .await
            {
                let properties = propolis_client::types::InstanceProperties {
                    id: propolis_id.into_untyped_uuid(),
                    name: hardware.properties.hostname.to_string(),
                    description: "sled-agent-sim created instance".to_string(),
                    image_id: Uuid::default(),
                    bootrom_id: Uuid::default(),
                    memory: hardware.properties.memory.to_whole_mebibytes(),
                    vcpus: hardware.properties.ncpus.0 as u8,
                    metadata: metadata.into(),
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
                &instance_id.into_untyped_uuid(),
                SledInstanceState {
                    instance_state: instance_runtime,
                    vmm_state: vmm_runtime,
                    propolis_id,
                    migration_state: None,
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
        instance_id: InstanceUuid,
    ) -> Result<InstanceUnregisterResponse, Error> {
        let instance = match self
            .instances
            .sim_get_cloned_object(&instance_id.into_untyped_uuid())
            .await
        {
            Ok(instance) => instance,
            Err(Error::ObjectNotFound { .. }) => {
                return Ok(InstanceUnregisterResponse { updated_runtime: None })
            }
            Err(e) => return Err(e),
        };

        self.detach_disks_from_instance(instance_id).await?;
        let response = InstanceUnregisterResponse {
            updated_runtime: Some(instance.terminate()),
        };

        self.instances.sim_force_remove(instance_id.into_untyped_uuid()).await;
        Ok(response)
    }

    /// Asks the supplied instance to transition to the requested state.
    pub async fn instance_ensure_state(
        self: &Arc<Self>,
        instance_id: InstanceUuid,
        state: InstanceStateRequested,
    ) -> Result<InstancePutStateResponse, Error> {
        if let Some(e) = self.instance_ensure_state_error.lock().await.as_ref()
        {
            return Err(e.clone());
        }

        let current = match self
            .instances
            .sim_get_cloned_object(&instance_id.into_untyped_uuid())
            .await
        {
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
                    let instances = self.instances.clone();
                    let log = self.log.new(
                        o!("component" => "SledAgent-insure_instance_state"),
                    );
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        match instances
                            .sim_ensure(
                                &instance_id.into_untyped_uuid(),
                                current,
                                Some(state),
                            )
                            .await
                        {
                            Ok(state) => {
                                let instance_state: nexus_client::types::SledInstanceState = state.into();
                                info!(log, "sim_ensure success"; "instance_state" => #?instance_state);
                            }
                            Err(instance_put_error) => {
                                error!(log, "sim_ensure failure"; "error" => #?instance_put_error);
                            }
                        }
                    });
                    return Ok(InstancePutStateResponse {
                        updated_runtime: None,
                    });
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
            .sim_ensure(&instance_id.into_untyped_uuid(), current, Some(state))
            .await?;

        // If this request will shut down the simulated instance, look for any
        // disks that are attached to it and drive them to the Detached state.
        if matches!(state, InstanceStateRequested::Stopped) {
            self.detach_disks_from_instance(instance_id).await?;
        }

        Ok(InstancePutStateResponse { updated_runtime: Some(new_state) })
    }

    pub async fn instance_get_state(
        &self,
        instance_id: InstanceUuid,
    ) -> Result<SledInstanceState, HttpError> {
        let instance = self
            .instances
            .sim_get_cloned_object(&instance_id.into_untyped_uuid())
            .await
            .map_err(|_| {
                crate::sled_agent::Error::Instance(
                    crate::instance_manager::Error::NoSuchInstance(instance_id),
                )
            })?;
        Ok(instance.current())
    }

    pub async fn set_instance_ensure_state_error(&self, error: Option<Error>) {
        *self.instance_ensure_state_error.lock().await = error;
    }

    async fn detach_disks_from_instance(
        &self,
        instance_id: InstanceUuid,
    ) -> Result<(), Error> {
        self.disks
            .sim_ensure_for_each_where(
                |disk| match disk.current().disk_state {
                    DiskState::Attached(id) | DiskState::Attaching(id) => {
                        id == instance_id.into_untyped_uuid()
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
        instance_id: InstanceUuid,
        old_runtime: &InstanceRuntimeState,
        migration_ids: &Option<InstanceMigrationSourceParams>,
    ) -> Result<SledInstanceState, Error> {
        let instance = self
            .instances
            .sim_get_cloned_object(&instance_id.into_untyped_uuid())
            .await?;

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

    pub async fn instance_poke(&self, id: InstanceUuid) {
        self.instances.sim_poke(id.into_untyped_uuid(), PokeMode::Drain).await;
    }

    pub async fn disk_poke(&self, id: Uuid) {
        self.disks.sim_poke(id, PokeMode::SingleStep).await;
    }

    /// Adds a Physical Disk to the simulated sled agent.
    pub async fn create_external_physical_disk(
        &self,
        id: Uuid,
        identity: DiskIdentity,
    ) {
        let variant = sled_hardware::DiskVariant::U2;
        self.storage
            .lock()
            .await
            .insert_physical_disk(id, identity, variant)
            .await;
    }

    pub async fn get_all_physical_disks(
        &self,
    ) -> Vec<nexus_client::types::PhysicalDiskPutRequest> {
        self.storage.lock().await.get_all_physical_disks()
    }

    pub async fn get_zpools(
        &self,
    ) -> Vec<nexus_client::types::ZpoolPutRequest> {
        self.storage.lock().await.get_all_zpools()
    }

    pub async fn get_datasets(
        &self,
        zpool_id: ZpoolUuid,
    ) -> Vec<(Uuid, SocketAddr)> {
        self.storage.lock().await.get_all_datasets(zpool_id)
    }

    /// Adds a Zpool to the simulated sled agent.
    pub async fn create_zpool(
        &self,
        id: ZpoolUuid,
        physical_disk_id: Uuid,
        size: u64,
    ) {
        self.storage
            .lock()
            .await
            .insert_zpool(id, physical_disk_id, size)
            .await;
    }

    /// Adds a Crucible Dataset within a zpool.
    pub async fn create_crucible_dataset(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: Uuid,
    ) -> SocketAddr {
        self.storage.lock().await.insert_dataset(zpool_id, dataset_id).await
    }

    /// Returns a crucible dataset within a particular zpool.
    pub async fn get_crucible_dataset(
        &self,
        zpool_id: ZpoolUuid,
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
        _instance_id: InstanceUuid,
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
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        let mut v2p_mappings = self.v2p_mappings.lock().await;
        v2p_mappings.insert(mapping.clone());
        Ok(())
    }

    pub async fn unset_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        let mut v2p_mappings = self.v2p_mappings.lock().await;
        v2p_mappings.remove(mapping);
        Ok(())
    }

    pub async fn list_virtual_nics(
        &self,
    ) -> Result<Vec<VirtualNetworkInterfaceHost>, Error> {
        let v2p_mappings = self.v2p_mappings.lock().await;
        Ok(Vec::from_iter(v2p_mappings.clone()))
    }

    pub async fn instance_put_external_ip(
        &self,
        instance_id: InstanceUuid,
        body_args: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        if !self.instances.contains_key(&instance_id.into_untyped_uuid()).await
        {
            return Err(Error::internal_error(
                "can't alter IP state for nonexistent instance",
            ));
        }

        let mut eips = self.external_ips.lock().await;
        let my_eips = eips.entry(instance_id.into_untyped_uuid()).or_default();

        // High-level behaviour: this should always succeed UNLESS
        // trying to add a double ephemeral.
        if let InstanceExternalIpBody::Ephemeral(curr_ip) = &body_args {
            if my_eips.iter().any(|v| {
                if let InstanceExternalIpBody::Ephemeral(other_ip) = v {
                    curr_ip != other_ip
                } else {
                    false
                }
            }) {
                return Err(Error::invalid_request("cannot replace existing ephemeral IP without explicit removal"));
            }
        }

        my_eips.insert(*body_args);

        Ok(())
    }

    pub async fn instance_delete_external_ip(
        &self,
        instance_id: InstanceUuid,
        body_args: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        if !self.instances.contains_key(&instance_id.into_untyped_uuid()).await
        {
            return Err(Error::internal_error(
                "can't alter IP state for nonexistent instance",
            ));
        }

        let mut eips = self.external_ips.lock().await;
        let my_eips = eips.entry(instance_id.into_untyped_uuid()).or_default();

        my_eips.remove(&body_args);

        Ok(())
    }

    /// Used for integration tests that require a component to talk to a
    /// mocked propolis-server API. Returns the socket on which the dropshot
    /// service is listening, which *must* be patched into Nexus with
    /// `nexus_db_queries::db::datastore::vmm_overwrite_addr_for_test` after
    /// the instance creation saga if functionality touching propolis-server
    /// is to be tested (e.g. serial console connection).
    pub async fn start_local_mock_propolis_server(
        &self,
        log: &Logger,
    ) -> Result<SocketAddr, Error> {
        let mut mock_lock = self.mock_propolis.lock().await;
        if mock_lock.is_some() {
            return Err(Error::ObjectAlreadyExists {
                type_name: ResourceType::Service,
                object_name: "mock propolis server".to_string(),
            });
        }
        let propolis_bind_address =
            SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 0);
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
        let addr = srv.local_addr();
        let client = propolis_client::Client::new(&format!("http://{}", addr));
        *mock_lock = Some((srv, client));
        Ok(addr)
    }

    pub async fn inventory(
        &self,
        addr: SocketAddr,
    ) -> anyhow::Result<Inventory> {
        let sled_agent_address = match addr {
            SocketAddr::V4(_) => {
                bail!("sled_agent_ip must be v6 for inventory")
            }
            SocketAddr::V6(v6) => v6,
        };

        let storage = self.storage.lock().await;
        Ok(Inventory {
            sled_id: self.id,
            sled_agent_address,
            sled_role: SledRole::Scrimlet,
            baseboard: self.config.hardware.baseboard.clone(),
            usable_hardware_threads: self.config.hardware.hardware_threads,
            usable_physical_ram: ByteCount::try_from(
                self.config.hardware.physical_ram,
            )
            .context("usable_physical_ram")?,
            reservoir_size: ByteCount::try_from(
                self.config.hardware.reservoir_ram,
            )
            .context("reservoir_size")?,
            disks: storage
                .physical_disks()
                .values()
                .map(|info| crate::params::InventoryDisk {
                    identity: info.identity.clone(),
                    variant: info.variant,
                    slot: info.slot,
                })
                .collect(),
            zpools: storage
                .zpools()
                .iter()
                .map(|(id, zpool)| {
                    Ok(crate::params::InventoryZpool {
                        id: *id,
                        total_size: ByteCount::try_from(zpool.total_size())?,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?,
        })
    }

    pub async fn omicron_physical_disks_list(
        &self,
    ) -> Result<OmicronPhysicalDisksConfig, HttpError> {
        self.storage.lock().await.omicron_physical_disks_list().await
    }

    pub async fn omicron_physical_disks_ensure(
        &self,
        config: OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, HttpError> {
        self.storage.lock().await.omicron_physical_disks_ensure(config).await
    }

    pub async fn omicron_zones_list(&self) -> OmicronZonesConfig {
        self.fake_zones.lock().await.clone()
    }

    pub async fn omicron_zones_ensure(
        &self,
        requested_zones: OmicronZonesConfig,
    ) {
        *self.fake_zones.lock().await = requested_zones;
    }

    pub async fn drop_dataset(&self, zpool_id: ZpoolUuid, dataset_id: Uuid) {
        self.storage.lock().await.drop_dataset(zpool_id, dataset_id)
    }
}
