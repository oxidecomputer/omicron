/*!
 * Sled agent implementation
 */

use super::config::SimMode;

use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskStateRequested;
use omicron_common::model::ApiInstanceMetrics;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::NexusClient;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

use crate::sim::{SimCollection, SimDisk, SimInstance};

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
        nexus_client: Arc<NexusClient>,
    ) -> SledAgent {
        info!(&log, "created simulated sled agent"; "sim_mode" => ?sim_mode);

        let instance_log = log.new(o!("kind" => "instances"));
        let disk_log = log.new(o!("kind" => "disks"));

        SledAgent {
            id: *id,
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
        }
    }

    /**
     * Idempotently ensures that the given API Instance (described by
     * `instance_id`) exists on this server in the given runtime state
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
     * Idempotently ensures that the given API Disk (described by `disk_id`)
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

    /**
     * Return the current metrics associated with the given API Instance.
     */
    pub async fn instance_get_metrics(
        self: &Arc<Self>,
        instance_id: Uuid,
    ) -> Result<ApiInstanceMetrics, ApiError> {
        // TODO(ben) Once we have the full state of the instance modeled
        // in the agent, fill this out with at least empty information
        // for each vCPU, disk, and network interface (in that order).
        Ok(ApiInstanceMetrics::empty(instance_id))
    }
}
