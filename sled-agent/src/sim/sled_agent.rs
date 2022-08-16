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
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use slog::Logger;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

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
        }
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
                crucible_client_types::VolumeConstructionRequest::Volume {
                    id,
                    ..
                } => id,
                _ => panic!("Unexpected construction type"),
            };
            self.disks.sim_ensure(&id, initial_state, target).await?;
            self.disks
                .sim_ensure_producer(&id, (self.nexus_address, id))
                .await?;
        }
        self.instances
            .sim_ensure(&instance_id, initial_hardware.runtime, target)
            .await
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

        // TODO: if instance state isn't running {
        //  return Ok(InstanceSerialConsoleData { data: vec![], last_byte_offset: 0 });
        // }

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
}
