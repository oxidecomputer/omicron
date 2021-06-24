//! Sled agent implementation

use omicron_common::error::ApiError;
use omicron_common::model::{
    ApiDiskRuntimeState, ApiDiskStateRequested, ApiInstanceRuntimeState,
    ApiInstanceRuntimeStateRequested,
};
use omicron_common::NexusClient;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

use crate::instance_manager::InstanceManager;

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
pub struct SledAgent {
    instances: InstanceManager,
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub fn new(
        id: &Uuid,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> Result<SledAgent, ApiError> {
        info!(&log, "created sled agent"; "id" => ?id);

        let instances =
            InstanceManager::new(log.clone(), nexus_client.clone())?;
        Ok(SledAgent { instances })
    }

    pub async fn instance_ensure(
        &self,
        instance_id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        self.instances.ensure(instance_id, initial_runtime, target).await
    }

    /// Idempotently ensures that the given Disk is attached (or not) as
    /// specified.
    ///
    /// NOTE: Not yet implemented.
    pub async fn disk_ensure(
        &self,
        _disk_id: Uuid,
        _initial_state: ApiDiskRuntimeState,
        _target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        todo!("Disk attachment not yet implemented");
    }
}
