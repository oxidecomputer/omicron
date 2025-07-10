// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use super::artifact_store::SimArtifactStorage;
use super::collection::{PokeMode, SimCollection};
use super::config::Config;
use super::disk::SimDisk;
use super::instance::{self, SimInstance};
use super::storage::CrucibleData;
use super::storage::Storage;
use crate::artifact_store::ArtifactStore;
use crate::nexus::NexusClient;
use crate::sim::SimulatedUpstairs;
use crate::sim::simulatable::Simulatable;
use crate::support_bundle::storage::SupportBundleQueryType;
use crate::updates::UpdateManager;
use anyhow::Context;
use anyhow::bail;
use bytes::Bytes;
use dropshot::Body;
use dropshot::HttpError;
use futures::Stream;
use nexus_sled_agent_shared::inventory::{
    ConfigReconcilerInventoryStatus, HostPhase2DesiredSlots, Inventory,
    InventoryDataset, InventoryDisk, InventoryZpool, OmicronSledConfig,
    OmicronZonesConfig, SledRole, ZoneImageResolverInventory,
};
use omicron_common::api::external::{
    ByteCount, DiskState, Error, Generation, ResourceType,
};
use omicron_common::api::internal::nexus::{
    DiskRuntimeState, MigrationRuntimeState, MigrationState, SledVmmState,
};
use omicron_common::api::internal::shared::{
    RackNetworkConfig, ResolvedVpcRoute, ResolvedVpcRouteSet,
    ResolvedVpcRouteState, RouterId, RouterKind, RouterVersion,
    VirtualNetworkInterfaceHost,
};
use omicron_common::disk::{
    DatasetsConfig, DatasetsManagementResult, DiskIdentity, DiskVariant,
    DisksManagementResult, OmicronPhysicalDisksConfig,
};
use omicron_uuid_kinds::{
    DatasetUuid, GenericUuid, PhysicalDiskUuid, PropolisUuid, SledUuid,
    SupportBundleUuid, ZpoolUuid,
};
use oxnet::Ipv6Net;
use propolis_client::instance_spec::SpecKey;
use propolis_client::{
    Client as PropolisClient, types::InstanceInitializationMethod,
};
use range_requests::PotentialRange;
use sled_agent_api::SupportBundleMetadata;
use sled_agent_types::disk::DiskStateRequested;
use sled_agent_types::early_networking::{
    EarlyNetworkConfig, EarlyNetworkConfigBody,
};
use sled_agent_types::instance::{
    InstanceEnsureBody, InstanceExternalIpBody, VmmPutStateResponse,
    VmmStateRequested, VmmUnregisterResponse,
};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tufaceous_artifact::ArtifactHash;
use uuid::Uuid;

/// Simulates management of the control plane on a sled
///
/// The current implementation simulates a server directly in this program.
/// **It's important to be careful about the interface exposed by this struct.**
/// The intent is for it to eventually be implemented using requests to a remote
/// server.  The tighter the coupling that exists now, the harder this will be to
/// move later.
pub struct SledAgent {
    pub id: SledUuid,
    pub ip: IpAddr,
    /// collection of simulated VMMs, indexed by Propolis uuid
    vmms: Arc<SimCollection<SimInstance>>,
    /// collection of simulated disks, indexed by disk uuid
    disks: Arc<SimCollection<SimDisk>>,
    storage: Storage,
    updates: UpdateManager,
    nexus_address: SocketAddr,
    pub nexus_client: Arc<NexusClient>,
    pub simulated_upstairs: Arc<SimulatedUpstairs>,
    pub v2p_mappings: Mutex<HashSet<VirtualNetworkInterfaceHost>>,
    mock_propolis: futures::lock::Mutex<
        Option<(propolis_mock_server::Server, PropolisClient)>,
    >,
    /// lists of external IPs assigned to instances
    pub external_ips:
        Mutex<HashMap<PropolisUuid, HashSet<InstanceExternalIpBody>>>,
    pub vpc_routes: Mutex<HashMap<RouterId, RouteSet>>,
    config: Config,
    fake_zones: Mutex<OmicronZonesConfig>,
    instance_ensure_state_error: Mutex<Option<Error>>,
    pub bootstore_network_config: Mutex<EarlyNetworkConfig>,
    pub(super) repo_depot:
        dropshot::HttpServer<ArtifactStore<SimArtifactStorage>>,
    pub log: Logger,
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
        simulated_upstairs: Arc<SimulatedUpstairs>,
        sled_index: u16,
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

        let storage = Storage::new(
            id.into_untyped_uuid(),
            sled_index,
            config.storage.ip,
            storage_log,
        );

        simulated_upstairs.register_storage(id, &storage);

        let repo_depot =
            ArtifactStore::new(&log, SimArtifactStorage::new(), None)
                .await
                .start(&log, &config.dropshot);

        Arc::new(SledAgent {
            id,
            ip: config.dropshot.bind_address.ip(),
            vmms: Arc::new(SimCollection::new(
                Arc::clone(&nexus_client),
                instance_log,
                sim_mode,
            )),
            disks: Arc::new(SimCollection::new(
                Arc::clone(&nexus_client),
                disk_log,
                sim_mode,
            )),
            storage,
            updates: UpdateManager::new(config.updates.clone()),
            nexus_address,
            nexus_client,
            simulated_upstairs,
            v2p_mappings: Mutex::new(HashSet::new()),
            external_ips: Mutex::new(HashMap::new()),
            vpc_routes: Mutex::new(HashMap::new()),
            mock_propolis: futures::lock::Mutex::new(None),
            config: config.clone(),
            fake_zones: Mutex::new(OmicronZonesConfig {
                generation: Generation::new(),
                zones: vec![],
            }),
            instance_ensure_state_error: Mutex::new(None),
            repo_depot,
            log,
            bootstore_network_config,
        })
    }

    /// Idempotently ensures that the given API Instance (described by
    /// `api_instance`) exists on this server in the given runtime state
    /// (described by `target`).
    pub async fn instance_register(
        self: &Arc<Self>,
        propolis_id: PropolisUuid,
        instance: InstanceEnsureBody,
    ) -> Result<SledVmmState, Error> {
        let InstanceEnsureBody {
            vmm_spec,
            local_config,
            instance_id,
            migration_id,
            vmm_runtime,
            metadata,
            ..
        } = instance;
        // respond with a fake 500 level failure if asked to ensure an instance
        // with more than 16 CPUs.
        let ncpus = vmm_spec.0.board.cpus;
        if ncpus > 16 {
            return Err(Error::internal_error(
                &"could not allocate an instance: ran out of CPUs!",
            ));
        };

        for (id, _disk) in vmm_spec.crucible_backends() {
            let SpecKey::Uuid(id) = id else {
                return Err(Error::invalid_value(
                    id.to_string(),
                    "Crucible disks in a Propolis spec must have UUID keys",
                ));
            };

            let initial_state = DiskRuntimeState {
                disk_state: DiskState::Attached(
                    instance_id.into_untyped_uuid(),
                ),
                gen: omicron_common::api::external::Generation::new(),
                time_updated: chrono::Utc::now(),
            };

            // Ensure that any disks that are in this request are attached to
            // this instance.
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
                .sim_ensure_producer(id, (self.nexus_address, *id))
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
            if !self.vmms.contains_key(&instance_id.into_untyped_uuid()).await {
                let metadata = propolis_client::types::InstanceMetadata {
                    project_id: metadata.project_id,
                    silo_id: metadata.silo_id,
                    sled_id: self.id.into_untyped_uuid(),
                    sled_model: self
                        .config
                        .hardware
                        .baseboard
                        .model()
                        .to_string(),
                    sled_revision: self.config.hardware.baseboard.revision(),
                    sled_serial: self
                        .config
                        .hardware
                        .baseboard
                        .identifier()
                        .to_string(),
                };
                let properties = propolis_client::types::InstanceProperties {
                    id: propolis_id.into_untyped_uuid(),
                    name: local_config.hostname.to_string(),
                    description: "sled-agent-sim created instance".to_string(),
                    metadata,
                };

                let body = propolis_client::types::InstanceEnsureRequest {
                    properties,
                    init: InstanceInitializationMethod::Spec {
                        spec: vmm_spec.0.clone(),
                    },
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

        let migration_in =
            migration_id.map(|migration_id| MigrationRuntimeState {
                migration_id,
                state: MigrationState::Pending,
                gen: Generation::new(),
                time_updated: chrono::Utc::now(),
            });

        let instance_run_time_state = self
            .vmms
            .sim_ensure(
                &propolis_id.into_untyped_uuid(),
                SledVmmState {
                    vmm_state: vmm_runtime,
                    migration_in,
                    migration_out: None,
                },
                None,
            )
            .await?;

        for (id, disk_request) in vmm_spec.crucible_backends() {
            let SpecKey::Uuid(id) = id else {
                unreachable!("already verified Crucible disks have UUID keys");
            };

            let vcr = serde_json::from_str(&disk_request.request_json)?;
            self.simulated_upstairs.map_id_to_vcr(*id, &vcr);
        }

        let mut routes = self.vpc_routes.lock().unwrap();
        for nic in &local_config.nics {
            let my_routers = [
                RouterId { vni: nic.vni, kind: RouterKind::System },
                RouterId { vni: nic.vni, kind: RouterKind::Custom(nic.subnet) },
            ];

            for router in my_routers {
                routes.entry(router).or_default();
            }
        }

        Ok(instance_run_time_state)
    }

    /// Forcibly unregisters an instance. To simulate the rude termination that
    /// this produces in the real sled agent, the instance's mock Propolis is
    /// not notified.
    pub async fn instance_unregister(
        self: &Arc<Self>,
        propolis_id: PropolisUuid,
    ) -> Result<VmmUnregisterResponse, Error> {
        let instance = match self
            .vmms
            .sim_get_cloned_object(&propolis_id.into_untyped_uuid())
            .await
        {
            Ok(instance) => instance,
            Err(Error::ObjectNotFound { .. }) => {
                return Ok(VmmUnregisterResponse { updated_runtime: None });
            }
            Err(e) => return Err(e),
        };

        let response = VmmUnregisterResponse {
            updated_runtime: Some(instance.terminate()),
        };

        self.vmms.sim_force_remove(propolis_id.into_untyped_uuid()).await;
        Ok(response)
    }

    /// Asks the supplied instance to transition to the requested state.
    pub async fn instance_ensure_state(
        self: &Arc<Self>,
        propolis_id: PropolisUuid,
        state: VmmStateRequested,
    ) -> Result<VmmPutStateResponse, HttpError> {
        if let Some(e) =
            self.instance_ensure_state_error.lock().unwrap().as_ref()
        {
            return Err(e.clone().into());
        }

        let current =
            self.get_sim_instance(propolis_id).await?.current().clone();

        let mock_lock = self.mock_propolis.lock().await;
        if let Some((_srv, client)) = mock_lock.as_ref() {
            let body = match state {
                VmmStateRequested::MigrationTarget(_) => {
                    return Err(Error::internal_error(
                        "migration not implemented for mock Propolis",
                    )
                    .into());
                }
                VmmStateRequested::Running => {
                    let vmms = self.vmms.clone();
                    let log = self.log.new(
                        o!("component" => "SledAgent-insure_instance_state"),
                    );
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        match vmms
                            .sim_ensure(
                                &propolis_id.into_untyped_uuid(),
                                current,
                                Some(state),
                            )
                            .await
                        {
                            Ok(state) => {
                                let vmm_state: nexus_client::types::SledVmmState = state.into();
                                info!(log, "sim_ensure success"; "vmm_state" => #?vmm_state);
                            }
                            Err(instance_put_error) => {
                                error!(log, "sim_ensure failure"; "error" => #?instance_put_error);
                            }
                        }
                    });
                    return Ok(VmmPutStateResponse { updated_runtime: None });
                }
                VmmStateRequested::Stopped => {
                    propolis_client::types::InstanceStateRequested::Stop
                }
                VmmStateRequested::Reboot => {
                    propolis_client::types::InstanceStateRequested::Reboot
                }
            };
            client.instance_state_put().body(body).send().await.map_err(
                |e| {
                    crate::sled_agent::Error::Instance(
                        crate::instance_manager::Error::Instance(
                            crate::instance::Error::Propolis(e), // whew!
                        ),
                    )
                },
            )?;
        }

        let new_state = self
            .vmms
            .sim_ensure(&propolis_id.into_untyped_uuid(), current, Some(state))
            .await?;

        Ok(VmmPutStateResponse { updated_runtime: Some(new_state) })
    }

    /// Wrapper around `sim_get_cloned_object` that returns the same error as
    /// the real sled-agent on an unknown VMM.
    async fn get_sim_instance(
        &self,
        propolis_id: PropolisUuid,
    ) -> Result<SimInstance, crate::sled_agent::Error> {
        self.vmms
            .sim_get_cloned_object(&propolis_id.into_untyped_uuid())
            .await
            .map_err(|_| {
                crate::sled_agent::Error::Instance(
                    crate::instance_manager::Error::NoSuchVmm(propolis_id),
                )
            })
    }

    pub async fn instance_get_state(
        &self,
        propolis_id: PropolisUuid,
    ) -> Result<SledVmmState, HttpError> {
        Ok(self.get_sim_instance(propolis_id).await?.current())
    }

    pub async fn instance_simulate_migration_source(
        &self,
        propolis_id: PropolisUuid,
        migration: instance::SimulateMigrationSource,
    ) -> Result<(), HttpError> {
        let instance = self.get_sim_instance(propolis_id).await?;
        instance.set_simulated_migration_source(migration);
        Ok(())
    }

    pub async fn set_instance_ensure_state_error(&self, error: Option<Error>) {
        *self.instance_ensure_state_error.lock().unwrap() = error;
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

    pub fn artifact_store(&self) -> &ArtifactStore<SimArtifactStorage> {
        self.repo_depot.app_private()
    }

    pub async fn vmm_count(&self) -> usize {
        self.vmms.size().await
    }

    pub async fn disk_count(&self) -> usize {
        self.disks.size().await
    }

    pub async fn vmm_poke(&self, id: PropolisUuid, mode: PokeMode) {
        self.vmms.sim_poke(id.into_untyped_uuid(), mode).await;
    }

    pub async fn disk_poke(&self, id: Uuid) {
        self.disks.sim_poke(id, PokeMode::SingleStep).await;
    }

    /// Adds a Physical Disk to the simulated sled agent.
    pub fn create_external_physical_disk(
        &self,
        id: PhysicalDiskUuid,
        identity: DiskIdentity,
    ) {
        let variant = DiskVariant::U2;
        self.storage.lock().insert_physical_disk(id, identity, variant);
    }

    pub fn get_all_physical_disks(
        &self,
    ) -> Vec<nexus_client::types::PhysicalDiskPutRequest> {
        self.storage.lock().get_all_physical_disks()
    }

    pub fn get_zpools(&self) -> Vec<nexus_client::types::ZpoolPutRequest> {
        self.storage.lock().get_all_zpools()
    }

    pub fn get_crucible_datasets(
        &self,
        zpool_id: ZpoolUuid,
    ) -> Vec<(DatasetUuid, SocketAddr)> {
        self.storage.lock().get_all_crucible_datasets(zpool_id)
    }

    /// Adds a Zpool to the simulated sled agent.
    pub fn create_zpool(
        &self,
        id: ZpoolUuid,
        physical_disk_id: PhysicalDiskUuid,
        size: u64,
    ) {
        self.storage.lock().insert_zpool(id, physical_disk_id, size);
    }

    pub fn has_zpool(&self, id: ZpoolUuid) -> bool {
        self.storage.lock().has_zpool(id)
    }

    /// Adds a Crucible Dataset within a zpool.
    pub fn create_crucible_dataset(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> SocketAddr {
        self.storage.lock().insert_crucible_dataset(zpool_id, dataset_id)
    }

    /// Returns a crucible dataset within a particular zpool.
    pub fn get_crucible_dataset(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Arc<CrucibleData> {
        self.storage.lock().get_crucible_dataset(zpool_id, dataset_id)
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
    pub fn instance_issue_disk_snapshot_request(
        &self,
        _propolis_id: PropolisUuid,
        disk_id: Uuid,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        self.simulated_upstairs.snapshot(disk_id, snapshot_id)
    }

    pub fn set_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        let mut v2p_mappings = self.v2p_mappings.lock().unwrap();
        v2p_mappings.insert(mapping.clone());
        Ok(())
    }

    pub fn unset_virtual_nic_host(
        &self,
        mapping: &VirtualNetworkInterfaceHost,
    ) -> Result<(), Error> {
        let mut v2p_mappings = self.v2p_mappings.lock().unwrap();
        v2p_mappings.remove(mapping);
        Ok(())
    }

    pub fn list_virtual_nics(
        &self,
    ) -> Result<Vec<VirtualNetworkInterfaceHost>, Error> {
        let v2p_mappings = self.v2p_mappings.lock().unwrap();
        Ok(Vec::from_iter(v2p_mappings.clone()))
    }

    pub async fn instance_put_external_ip(
        &self,
        propolis_id: PropolisUuid,
        body_args: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        if !self.vmms.contains_key(&propolis_id.into_untyped_uuid()).await {
            return Err(Error::internal_error(
                "can't alter IP state for VMM that's not registered",
            ));
        }

        let mut eips = self.external_ips.lock().unwrap();
        let my_eips = eips.entry(propolis_id).or_default();

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
                return Err(Error::invalid_request(
                    "cannot replace existing ephemeral IP without explicit removal",
                ));
            }
        }

        my_eips.insert(*body_args);

        Ok(())
    }

    pub async fn instance_delete_external_ip(
        &self,
        propolis_id: PropolisUuid,
        body_args: &InstanceExternalIpBody,
    ) -> Result<(), Error> {
        if !self.vmms.contains_key(&propolis_id.into_untyped_uuid()).await {
            return Err(Error::internal_error(
                "can't alter IP state for VMM that's not registered",
            ));
        }

        let mut eips = self.external_ips.lock().unwrap();
        let my_eips = eips.entry(propolis_id).or_default();

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
        let dropshot_config = propolis_mock_server::Config {
            bind_address: propolis_bind_address,
            default_request_body_max_bytes: 1024 * 1024,
            ..Default::default()
        };
        info!(log, "Starting mock propolis-server...");
        let srv = propolis_mock_server::start(dropshot_config, log.clone())
            .map_err(|error| {
                Error::unavail(&format!(
                    "initializing propolis-server: {}",
                    error
                ))
            })?;
        let addr = srv.local_addr();
        let client = propolis_client::Client::new(&format!("http://{}", addr));
        *mock_lock = Some((srv, client));
        Ok(addr)
    }

    pub fn inventory(&self, addr: SocketAddr) -> anyhow::Result<Inventory> {
        let sled_agent_address = match addr {
            SocketAddr::V4(_) => {
                bail!("sled_agent_ip must be v6 for inventory")
            }
            SocketAddr::V6(v6) => v6,
        };

        let storage = self.storage.lock();

        let disks_config =
            storage.omicron_physical_disks_list().unwrap_or_default();
        let datasets_config =
            storage.datasets_config_list().unwrap_or_default();
        let zones_config = self.fake_zones.lock().unwrap().clone();

        let sled_config = OmicronSledConfig {
            generation: zones_config.generation,
            disks: disks_config.disks.into_iter().collect(),
            datasets: datasets_config.datasets.into_values().collect(),
            zones: zones_config.zones.into_iter().collect(),
            remove_mupdate_override: None,
            host_phase_2: HostPhase2DesiredSlots::current_contents(),
        };

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
                .map(|info| InventoryDisk {
                    identity: info.identity.clone(),
                    variant: info.variant,
                    slot: info.slot,
                    active_firmware_slot: 1,
                    next_active_firmware_slot: None,
                    number_of_firmware_slots: 1,
                    slot1_is_read_only: true,
                    slot_firmware_versions: vec![Some("SIMUL1".to_string())],
                })
                .collect(),
            zpools: storage
                .zpools()
                .iter()
                .map(|(id, zpool)| {
                    Ok(InventoryZpool {
                        id: *id,
                        total_size: ByteCount::try_from(zpool.total_size())?,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?,
            // NOTE: We report the "configured" datasets as the "real" datasets
            // unconditionally here. No real datasets exist, so we're free
            // to lie here, but this information should be taken with a
            // particularly careful grain-of-salt -- it's supposed to
            // represent the "real" datasets the sled agent can observe.
            datasets: storage
                .datasets_config_list()
                .map(|config| {
                    config
                        .datasets
                        .into_iter()
                        .map(|(id, config)| InventoryDataset {
                            id: Some(id),
                            name: config.name.full_name(),
                            available: ByteCount::from_kibibytes_u32(0),
                            used: ByteCount::from_kibibytes_u32(0),
                            quota: config.inner.quota,
                            reservation: config.inner.reservation,
                            compression: config.inner.compression.to_string(),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|_| vec![]),
            ledgered_sled_config: Some(sled_config),
            reconciler_status: ConfigReconcilerInventoryStatus::NotYetRun,
            last_reconciliation: None,
            // TODO: simulate the zone image resolver with greater fidelity
            zone_image_resolver: ZoneImageResolverInventory::new_fake(),
        })
    }

    pub async fn support_bundle_list(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
    ) -> Result<Vec<SupportBundleMetadata>, HttpError> {
        self.storage
            .as_support_bundle_storage(&self.log)
            .list(zpool_id, dataset_id)
            .await
            .map_err(|err| err.into())
    }

    pub async fn support_bundle_create(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        expected_hash: ArtifactHash,
        stream: impl Stream<Item = Result<Bytes, HttpError>>,
    ) -> Result<SupportBundleMetadata, HttpError> {
        self.storage
            .as_support_bundle_storage(&self.log)
            .create(
                zpool_id,
                dataset_id,
                support_bundle_id,
                expected_hash,
                stream,
            )
            .await
            .map_err(|err| err.into())
    }

    pub(crate) async fn support_bundle_get(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        range: Option<PotentialRange>,
        query: SupportBundleQueryType,
    ) -> Result<http::Response<Body>, HttpError> {
        self.storage
            .as_support_bundle_storage(&self.log)
            .get(zpool_id, dataset_id, support_bundle_id, range, query)
            .await
            .map_err(|err| err.into())
    }

    pub(crate) async fn support_bundle_head(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
        range: Option<PotentialRange>,
        query: SupportBundleQueryType,
    ) -> Result<http::Response<Body>, HttpError> {
        self.storage
            .as_support_bundle_storage(&self.log)
            .head(zpool_id, dataset_id, support_bundle_id, range, query)
            .await
            .map_err(|err| err.into())
    }

    pub async fn support_bundle_delete(
        &self,
        zpool_id: ZpoolUuid,
        dataset_id: DatasetUuid,
        support_bundle_id: SupportBundleUuid,
    ) -> Result<(), HttpError> {
        self.storage
            .as_support_bundle_storage(&self.log)
            .delete(zpool_id, dataset_id, support_bundle_id)
            .await
            .map_err(|err| err.into())
    }

    pub fn datasets_ensure(
        &self,
        config: DatasetsConfig,
    ) -> Result<DatasetsManagementResult, HttpError> {
        self.storage.lock().datasets_ensure(config)
    }

    pub fn datasets_config_list(&self) -> Result<DatasetsConfig, HttpError> {
        self.storage.lock().datasets_config_list()
    }

    pub fn omicron_physical_disks_list(
        &self,
    ) -> Result<OmicronPhysicalDisksConfig, HttpError> {
        self.storage.lock().omicron_physical_disks_list()
    }

    pub fn omicron_physical_disks_ensure(
        &self,
        config: OmicronPhysicalDisksConfig,
    ) -> Result<DisksManagementResult, HttpError> {
        self.storage.lock().omicron_physical_disks_ensure(config)
    }

    pub fn set_omicron_config(
        &self,
        config: OmicronSledConfig,
    ) -> Result<(), HttpError> {
        // TODO Update the simulator to work on `OmicronSledConfig` instead of
        // the three separate legacy configs
        let disks_config = OmicronPhysicalDisksConfig {
            generation: config.generation,
            disks: config.disks.into_iter().collect(),
        };
        let datasets_config = DatasetsConfig {
            generation: config.generation,
            datasets: config.datasets.into_iter().map(|d| (d.id, d)).collect(),
        };
        let zones_config = OmicronZonesConfig {
            generation: config.generation,
            zones: config.zones.into_iter().collect(),
        };

        let mut storage = self.storage.lock();
        let _ = storage.omicron_physical_disks_ensure(disks_config)?;
        let _ = storage.datasets_ensure(datasets_config)?;
        *self.fake_zones.lock().unwrap() = zones_config;
        //*self.sled_config.lock().unwrap() = Some(config);

        Ok(())
    }

    pub fn omicron_zones_list(&self) -> OmicronZonesConfig {
        self.fake_zones.lock().unwrap().clone()
    }

    pub fn omicron_zones_ensure(&self, requested_zones: OmicronZonesConfig) {
        *self.fake_zones.lock().unwrap() = requested_zones;
    }

    pub fn drop_dataset(&self, zpool_id: ZpoolUuid, dataset_id: DatasetUuid) {
        self.storage.lock().drop_dataset(zpool_id, dataset_id)
    }

    pub fn list_vpc_routes(&self) -> Vec<ResolvedVpcRouteState> {
        let routes = self.vpc_routes.lock().unwrap();
        routes
            .iter()
            .map(|(k, v)| ResolvedVpcRouteState { id: *k, version: v.version })
            .collect()
    }

    pub fn set_vpc_routes(&self, new_routes: Vec<ResolvedVpcRouteSet>) {
        let mut routes = self.vpc_routes.lock().unwrap();
        for new in new_routes {
            // Disregard any route information for a subnet we don't have.
            let Some(old) = routes.get(&new.id) else {
                continue;
            };

            // We have to handle subnet router changes, as well as
            // spurious updates from multiple Nexus instances.
            // If there's a UUID match, only update if vers increased,
            // otherwise take the update verbatim (including loss of version).
            match (old.version, new.version) {
                (Some(old_vers), Some(new_vers))
                    if !old_vers.is_replaced_by(&new_vers) =>
                {
                    continue;
                }
                _ => {
                    println!(
                        "sled {} successfully installed routes {new:?}",
                        self.id
                    );
                }
            };

            routes.insert(
                new.id,
                RouteSet { version: new.version, routes: new.routes },
            );
        }
    }
}

/// Stored routes (and usage count) for a given VPC/subnet.
//  NB: We aren't doing post count tracking here to unsubscribe
//      from (VNI, subnet) pairs.
#[derive(Debug, Clone, Default)]
pub struct RouteSet {
    pub version: Option<RouterVersion>,
    pub routes: HashSet<ResolvedVpcRoute>,
}
