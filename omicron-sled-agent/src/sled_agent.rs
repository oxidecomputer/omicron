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

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as managed instances.
pub struct SledAgent {}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub fn new(
        id: &Uuid,
        log: Logger,
        _nexus_client: Arc<NexusClient>,
    ) -> Result<SledAgent, ApiError> {
        info!(&log, "created sled agent"; "id" => ?id);

        // TODO: Create an "instance manager" which handles all incoming
        // requests.
        Ok(SledAgent {})
    }

    /// Idempotently ensures that a given Instance is running on the sled.
    ///
    /// NOTE: Not yet implemented.
    pub async fn instance_ensure(
        &self,
        _instance_id: Uuid,
        _initial_runtime: ApiInstanceRuntimeState,
        _target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        todo!("Instance ensure not yet implemented");
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
