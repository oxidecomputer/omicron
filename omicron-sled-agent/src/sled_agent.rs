/*!
 * Sled agent implementation
 */

use super::config::SimMode;

use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskStateRequested;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

use crate::sled_agent::sim::{SimCollection, SimDisk, SimInstance};

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
    /** unique id for this server */
    pub id: Uuid,

    /** collection of simulated instances, indexed by instance uuid */
    instances: Arc<SimCollection<SimInstance>>,
    /** collection of simulated disks, indexed by disk uuid */
    disks: Arc<SimCollection<SimDisk>>,
}

impl SledAgent {
    /*
     * TODO-cleanup should this instantiate the NexusClient it needs?
     * Should it take a Config object instead of separate id, sim_mode, etc?
     */
    /** Constructs a simulated SledAgent with the given uuid. */
    pub fn new_simulated_with_id(
        id: &Uuid,
        sim_mode: SimMode,
        log: Logger,
        ctlsc: Arc<NexusClient>,
    ) -> SledAgent {
        info!(&log, "created simulated sled agent"; "sim_mode" => ?sim_mode);

        let instance_log = log.new(o!("kind" => "instances"));
        let disk_log = log.new(o!("kind" => "disks"));

        SledAgent {
            id: *id,
            instances: Arc::new(SimCollection::new(
                Arc::clone(&ctlsc),
                instance_log,
                sim_mode,
            )),
            disks: Arc::new(SimCollection::new(
                Arc::clone(&ctlsc),
                disk_log,
                sim_mode,
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
        initial_runtime: ApiInstanceRuntimeState,
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        Ok(self
            .instances
            .sim_ensure(&instance_id, initial_runtime, target)
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
        initial_state: ApiDiskRuntimeState,
        target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        Ok(self.disks.sim_ensure(&disk_id, initial_state, target).await?)
    }

    pub async fn instance_poke(&self, id: Uuid) {
        self.instances.sim_poke(id).await;
    }

    pub async fn disk_poke(&self, id: Uuid) {
        self.disks.sim_poke(id).await;
    }
}
