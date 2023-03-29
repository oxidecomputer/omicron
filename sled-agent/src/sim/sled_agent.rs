// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use crate::nexus::NexusClient;
use crate::params::{
    DiskStateRequested, InstanceHardware, InstanceRuntimeStateRequested,
    InstanceStateRequested,
};
use crate::updates::UpdateManager;
use futures::lock::Mutex;
use omicron_common::api::external::{Error, ResourceType};
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use slog::Logger;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use uuid::Uuid;

use std::collections::HashMap;
use std::str::FromStr;

use crucible_client_types::VolumeConstructionRequest;
use dropshot::HttpServer;
use omicron_common::address::PROPOLIS_PORT;
use propolis_client::Client as PropolisClient;
use propolis_server::mock_server::Context as PropolisContext;

use super::collection::SimCollection;
use super::config::Config;
use super::disk::SimDisk;
use super::instance::SimInstance;
use super::storage::CrucibleData;
use super::storage::Storage;

/// Simulates management of the control plane on a sled
///
/// The current implementation simulates a server directly in this program.
/// **It's important to be careful about the interface exposed by this struct.**
/// The intent is for it to eventually be implemented using requests to a remote
/// server.  The tighter the coupling that exists now, the harder this will be to
/// move later.
pub struct SledAgent {
    pub id: Uuid,
    /// collection of simulated instances, indexed by instance uuid
    instances: Arc<SimCollection<SimInstance>>,
    /// collection of simulated disks, indexed by disk uuid
    disks: Arc<SimCollection<SimDisk>>,
    storage: Mutex<Storage>,
    updates: UpdateManager,
    nexus_address: SocketAddr,
    pub nexus_client: Arc<NexusClient>,
    disk_id_to_region_ids: Mutex<HashMap<String, Vec<Uuid>>>,
    mock_propolis:
        Mutex<Option<(HttpServer<Arc<PropolisContext>>, PropolisClient)>>,
}

fn extract_targets_from_volume_construction_request(
    vec: &mut Vec<SocketAddr>,
    vcr: &VolumeConstructionRequest,
) {
    // A snapshot is simply a flush with an extra parameter, and flushes are
    // only sent to sub volumes, not the read only parent. Flushes are only
    // processed by regions, so extract each region that would be affected by a
    // flush.
    match vcr {
        VolumeConstructionRequest::Volume {
            id: _,
            block_size: _,
            sub_volumes,
            read_only_parent: _,
        } => {
            for sub_volume in sub_volumes.iter() {
                extract_targets_from_volume_construction_request(
                    vec, sub_volume,
                );
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
                vec.push(*target);
            }
        }

        VolumeConstructionRequest::File { .. } => {
            // noop
        }
    }
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
            VolumeConstructionRequest::Volume {
                id,
                block_size: _,
                sub_volumes: _,
                read_only_parent: _,
            } => id,

            _ => {
                panic!("root of volume construction request not a volume!");
            }
        };

        let mut targets = Vec::new();
        extract_targets_from_volume_construction_request(
            &mut targets,
            &volume_construction_request,
        );

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
    pub async fn instance_ensure(
        self: &Arc<Self>,
        instance_id: Uuid,
        mut initial_hardware: InstanceHardware,
        target: InstanceRuntimeStateRequested,
    ) -> Result<InstanceRuntimeState, Error> {
        // respond with a fake 500 level failure if asked to ensure an instance
        // with more than 16 CPUs.
        let ncpus: i64 = (&initial_hardware.runtime.ncpus).into();
        if ncpus > 16 {
            return Err(Error::internal_error(
                &"could not allocate an instance: ran out of CPUs!",
            ));
        };

        for disk in &initial_hardware.disks {
            let initial_state = DiskRuntimeState {
                disk_state: omicron_common::api::external::DiskState::Attached(
                    instance_id,
                ),
                gen: omicron_common::api::external::Generation::new(),
                time_updated: chrono::Utc::now(),
            };
            let target = DiskStateRequested::Attached(instance_id);

            let id = match disk.volume_construction_request {
                propolis_client::instance_spec::VolumeConstructionRequest::Volume { id, .. } => id,
                _ => panic!("Unexpected construction type"),
            };
            self.disks.sim_ensure(&id, initial_state, target).await?;
            self.disks
                .sim_ensure_producer(&id, (self.nexus_address, id))
                .await?;
        }

        // if we're making our first instance and a mock propolis-server
        // is running, interact with it, and patch the instance's
        // reported propolis-server IP for reports back to nexus.
        let mock_lock = self.mock_propolis.lock().await;
        if let Some((_srv, client)) = mock_lock.as_ref() {
            if let Some(addr) = initial_hardware.runtime.propolis_addr.as_mut()
            {
                addr.set_ip(Ipv6Addr::LOCALHOST.into());
            }
            if !self.instances.contains_key(&instance_id).await {
                let properties = propolis_client::types::InstanceProperties {
                    id: initial_hardware.runtime.propolis_id,
                    name: initial_hardware.runtime.hostname.clone(),
                    description: "sled-agent-sim created instance".to_string(),
                    image_id: Uuid::default(),
                    bootrom_id: Uuid::default(),
                    memory: initial_hardware
                        .runtime
                        .memory
                        .to_whole_mebibytes(),
                    vcpus: initial_hardware.runtime.ncpus.0 as u8,
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

            let body = match target.run_state {
                InstanceStateRequested::Running => {
                    propolis_client::types::InstanceStateRequested::Run
                }
                InstanceStateRequested::Destroyed
                | InstanceStateRequested::Stopped => {
                    propolis_client::types::InstanceStateRequested::Stop
                }
                InstanceStateRequested::Reboot => {
                    propolis_client::types::InstanceStateRequested::Reboot
                }
                InstanceStateRequested::Migrating => {
                    propolis_client::types::InstanceStateRequested::MigrateStart
                }
            };
            client.instance_state_put().body(body).send().await.map_err(
                |e| Error::internal_error(&format!("propolis-client: {}", e)),
            )?;
        }

        let instance_run_time_state = self
            .instances
            .sim_ensure(&instance_id, initial_hardware.runtime, target)
            .await?;

        for disk_request in &initial_hardware.disks {
            // disk_request.volume_construction_request is of type
            // propolis_client::instance_spec::VolumeConstructionRequest, where
            // map_disk_ids_to_region_ids expects
            // crucible_client_types::VolumeConstructionRequest, so take a round
            // trip through JSON serialization -> deserialization to make this
            // work.
            let vcr: crucible_client_types::VolumeConstructionRequest =
                serde_json::from_str(&serde_json::to_string(
                    &disk_request.volume_construction_request,
                )?)?;

            self.map_disk_ids_to_region_ids(&vcr).await?;
        }

        Ok(instance_run_time_state)
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
        self.disks.sim_ensure(&disk_id, initial_state, target).await
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
        self.instances.sim_poke(id).await;
    }

    pub async fn disk_poke(&self, id: Uuid) {
        self.disks.sim_poke(id).await;
    }

    /// Adds a Zpool to the simulated sled agent.
    pub async fn create_zpool(&self, id: Uuid, size: u64) {
        self.storage.lock().await.insert_zpool(id, size).await;
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
                crucible_data
                    .create_snapshot(*region_id, snapshot_id)
                    .await
                    .map_err(|e| Error::internal_error(&e.to_string()))?;
            } else {
                return Err(Error::not_found_by_id(
                    ResourceType::Disk,
                    &disk_id,
                ));
            }
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
        let config = propolis_server::config::Config {
            bootrom: Default::default(),
            pci_bridges: Default::default(),
            chipset: Default::default(),
            devices: Default::default(),
            block_devs: Default::default(),
        };
        let private = Arc::new(PropolisContext::new(config, propolis_log));
        info!(log, "Starting mock propolis-server...");
        let dropshot_log = log.new(o!("component" => "dropshot"));
        let mock_api = propolis_server::mock_server::api();

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
