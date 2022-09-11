// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulated sled agent implementation

use crate::nexus::NexusClient;
use crate::params::{
    DiskStateRequested, InstanceHardware, InstanceRuntimeStateRequested,
    InstanceSerialConsoleData,
};
use crate::serial::ByteOffset;
use futures::lock::Mutex;
use omicron_common::api::external::{Error, InstanceState, ResourceType};
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

use std::collections::HashMap;
use std::str::FromStr;

use propolis_client::api::VolumeConstructionRequest;

use super::collection::SimCollection;
use super::config::Config;
use super::disk::SimDisk;
use super::instance::SimInstance;
use super::storage::{CrucibleData, Storage};

/// Simulates management of the control plane on a sled
///
/// The current implementation simulates a server directly in this program.
/// **It's important to be careful about the interface exposed by this struct.**
/// The intent is for it to eventually be implemented using requests to a remote
/// server.  The tighter the coupling that exists now, the harder this will be to
/// move later.
pub struct SledAgent {
    /// collection of simulated instances, indexed by instance uuid
    instances: Arc<SimCollection<SimInstance>>,
    /// collection of simulated disks, indexed by disk uuid
    disks: Arc<SimCollection<SimDisk>>,
    storage: Mutex<Storage>,
    nexus_address: SocketAddr,
    pub nexus_client: Arc<NexusClient>,
    disk_id_to_region_ids: Mutex<HashMap<String, Vec<Uuid>>>,
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

        VolumeConstructionRequest::Region { block_size: _, opts, gen: _ } => {
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
    pub fn new_simulated_with_id(
        config: &Config,
        log: Logger,
        nexus_address: SocketAddr,
        nexus_client: Arc<NexusClient>,
    ) -> SledAgent {
        let id = config.id;
        let sim_mode = config.sim_mode;
        info!(&log, "created simulated sled agent"; "sim_mode" => ?sim_mode);

        let instance_log = log.new(o!("kind" => "instances"));
        let disk_log = log.new(o!("kind" => "disks"));
        let storage_log = log.new(o!("kind" => "storage"));

        SledAgent {
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
            nexus_address,
            nexus_client,
            disk_id_to_region_ids: Mutex::new(HashMap::new()),
        }
    }

    /// Map disk id to regions for later lookup
    ///
    /// Crucible regions are returned with a port number, and volume
    /// construction requests contain a single Nexus region (which points to
    /// three crucible regions). Extract the region addresses, lookup the
    /// region from the port (which should be unique), and pair disk id with
    /// region ids. This map is referred to later when making snapshots.
    async fn map_disk_ids_to_region_ids(
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
        initial_hardware: InstanceHardware,
        target: InstanceRuntimeStateRequested,
    ) -> Result<InstanceRuntimeState, Error> {
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
                VolumeConstructionRequest::Volume { id, .. } => id,
                _ => panic!("Unexpected construction type"),
            };
            self.disks.sim_ensure(&id, initial_state, target).await?;
            self.disks
                .sim_ensure_producer(&id, (self.nexus_address, id))
                .await?;
        }

        let instance_run_time_state = self
            .instances
            .sim_ensure(&instance_id, initial_hardware.runtime, target)
            .await?;

        for disk_request in &initial_hardware.disks {
            self.map_disk_ids_to_region_ids(
                &disk_request.volume_construction_request,
            )
            .await?;
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
    ) {
        self.storage.lock().await.insert_dataset(zpool_id, dataset_id).await;
    }

    /// Returns a crucible dataset within a particular zpool.
    pub async fn get_crucible_dataset(
        &self,
        zpool_id: Uuid,
        dataset_id: Uuid,
    ) -> Arc<CrucibleData> {
        self.storage.lock().await.get_dataset(zpool_id, dataset_id).await
    }

    /// Get contents of an instance's serial console.
    pub async fn instance_serial_console_data(
        &self,
        instance_id: Uuid,
        byte_offset: ByteOffset,
        max_bytes: Option<usize>,
    ) -> Result<InstanceSerialConsoleData, String> {
        if !self.instances.sim_contains(&instance_id).await {
            return Err(format!("No such instance {}", instance_id));
        }

        let current = self
            .instances
            .sim_get_current_state(&instance_id)
            .await
            .map_err(|e| format!("{}", e))?;
        if current.run_state != InstanceState::Running {
            return Ok(InstanceSerialConsoleData {
                data: vec![],
                last_byte_offset: 0,
            });
        }

        let gerunds = [
            "Loading",
            "Reloading",
            "Advancing",
            "Reticulating",
            "Defeating",
            "Spoiling",
            "Cooking",
            "Destroying",
            "Resenting",
            "Introducing",
            "Reiterating",
            "Blasting",
            "Tolling",
            "Delivering",
            "Engendering",
            "Establishing",
        ];
        let nouns = [
            "canon",
            "browsers",
            "meta",
            "splines",
            "villains",
            "plot",
            "books",
            "evidence",
            "decisions",
            "chaos",
            "points",
            "processors",
            "bells",
            "value",
            "gender",
            "shots",
        ];
        let mut entropy = instance_id.as_u128();
        let mut buf = format!(
            "This is simulated serial console output for {}.\n",
            instance_id
        );
        while entropy != 0 {
            let gerund = gerunds[entropy as usize % gerunds.len()];
            entropy /= gerunds.len() as u128;
            let noun = nouns[entropy as usize % nouns.len()];
            entropy /= nouns.len() as u128;
            buf += &format!(
                "{} {}... {}[\x1b[92m 0K \x1b[m]\n",
                gerund,
                noun,
                " ".repeat(40 - gerund.len() - noun.len())
            );
        }
        buf += "\x1b[2J\x1b[HOS/478 (localhorse) (ttyl)\n\nlocalhorse login: ";

        let start = match byte_offset {
            ByteOffset::FromStart(offset) => offset,
            ByteOffset::MostRecent(offset) => buf.len() - offset,
        };

        let start = start.min(buf.len());
        let end = (start + max_bytes.unwrap_or(16 * 1024)).min(buf.len());
        let data = buf[start..end].as_bytes().to_vec();

        let last_byte_offset = (start + data.len()) as u64;

        Ok(InstanceSerialConsoleData { data, last_byte_offset })
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

    /// Issue a snapshot request for a Crucible disk not attached to an
    /// instance.
    pub async fn issue_disk_snapshot_request(
        &self,
        disk_id: Uuid,
        volume_construction_request: VolumeConstructionRequest,
        snapshot_id: Uuid,
    ) -> Result<(), Error> {
        // Perform the disk id -> region id mapping just as was done by during
        // the simulated instance ensure, then call
        // [`instance_issue_disk_snapshot_request`] as the snapshot logic is the
        // same.
        self.map_disk_ids_to_region_ids(&volume_construction_request).await?;

        self.instance_issue_disk_snapshot_request(
            Uuid::new_v4(), // instance id, not used by function
            disk_id,
            snapshot_id,
        )
        .await
    }
}
