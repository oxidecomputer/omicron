//! Sled agent implementation

use omicron_common::api::ApiError;
use omicron_common::api::{
    ApiDiskRuntimeState, ApiDiskStateRequested, ApiInstanceRuntimeState,
    ApiInstanceRuntimeStateRequested,
};
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

#[cfg(test)]
use crate::mocks::MockNexusClient as NexusClient;
#[cfg(not(test))]
use omicron_common::NexusClient;

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

        Ok(SledAgent {
            instances: InstanceManager::new(log.clone(), nexus_client)?,
        })
    }

    /// Idempotently ensures that a given Instance is running on the sled.
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
