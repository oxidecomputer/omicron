// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/*!
 * Simulated sled agent implementation
 */

use crate::params::DiskStateRequested;
use futures::lock::Mutex;
use nexus_client::Client as NexusClient;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::InstanceHardware;
use omicron_common::api::internal::sled_agent::InstanceRuntimeStateRequested;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

use super::collection::SimCollection;
use super::config::Config;
use super::disk::SimDisk;
use super::instance::SimInstance;
use super::storage::{CrucibleData, Storage};

/**
 * Simulates management of the control plane on a sled
 *
 * The current implementation simulates a server directly in this program.
 * **It's important to be careful about the interface exposed by this struct.**
 * The intent is for it to eventually be implemented using requests to a remote
 * server.  The tighter the coupling that exists now, the harder this will be to
 * move later.
 */
pub struct SledAgent {
    /** collection of simulated instances, indexed by instance uuid */
    instances: Arc<SimCollection<SimInstance>>,
    /** collection of simulated disks, indexed by disk uuid */
    disks: Arc<SimCollection<SimDisk>>,
    storage: Mutex<Storage>,
}

impl SledAgent {
    /*
     * TODO-cleanup should this instantiate the NexusClient it needs?
     * Should it take a Config object instead of separate id, sim_mode, etc?
     */
    /** Constructs a simulated SledAgent with the given uuid. */
    pub fn new_simulated_with_id(
        config: &Config,
        log: Logger,
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
        }
    }

    /**
     * Idempotently ensures that the given API Instance (described by
     * `api_instance`) exists on this server in the given runtime state
     * (described by `target`).
     */
    pub async fn instance_ensure(
        self: &Arc<Self>,
        instance_id: Uuid,
        initial_hardware: InstanceHardware,
        target: InstanceRuntimeStateRequested,
    ) -> Result<InstanceRuntimeState, Error> {
        Ok(self
            .instances
            .sim_ensure(&instance_id, initial_hardware.runtime, target)
            .await?)
    }

    /**
     * Idempotently ensures that the given API Disk (described by `api_disk`)
     * is attached (or not) as specified.  This simulates disk attach and
     * detach, similar to instance boot and halt.
     */
    pub async fn disk_ensure(
        self: &Arc<Self>,
        disk_id: Uuid,
        initial_state: DiskRuntimeState,
        target: DiskStateRequested,
    ) -> Result<DiskRuntimeState, Error> {
        Ok(self.disks.sim_ensure(&disk_id, initial_state, target).await?)
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
}
