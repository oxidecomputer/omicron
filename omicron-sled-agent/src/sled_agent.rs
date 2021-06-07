//! Sled agent implementation

use futures::lock::Mutex;
use omicron_common::error::ApiError;
use omicron_common::model::{
    ApiDiskRuntimeState,
    ApiDiskStateRequested,
    ApiInstanceRuntimeState,
    ApiInstanceRuntimeStateRequested,
};
use omicron_common::NexusClient;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::instance::Instance;

/// Describes an executing Sled Agent object.
///
/// Contains both a connection to the Nexus, as well as a map of managed
/// instances.
pub struct SledAgent {
    log: Logger,
    nexus_client: Arc<NexusClient>,
    instances: Mutex<BTreeMap<Uuid, Instance>>,
}

impl SledAgent {
    /// Initializes a new [`SledAgent`] object.
    pub fn new(
        id: &Uuid,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> SledAgent {
        info!(&log, "created sled agent"; "id" => ?id);

        SledAgent { log, nexus_client, instances: Mutex::new(BTreeMap::new()) }
    }

    /// Idempotently ensures that the given API Instance (described by
    /// `api_instance`) exists on this server in the given runtime state
    /// (described by `target`).
    pub async fn instance_ensure(
        self: &Self,
        instance_id: Uuid,
        initial_runtime: ApiInstanceRuntimeState,
        target: ApiInstanceRuntimeStateRequested,
    ) -> Result<ApiInstanceRuntimeState, ApiError> {
        info!(&self.log, "instance_ensure {} -> {:#?}", instance_id, target);
        let mut instances = self.instances.lock().await;

        let instance = {
            if let Some(instance) = instances.get_mut(&instance_id) {
                // Instance already exists.
                info!(&self.log, "instance already exists");
                instance
            } else {
                // Instance does not exist - create it.
                info!(&self.log, "new instance");
                let instance_log =
                    self.log.new(o!("instance" => instance_id.to_string()));
                instances.insert(
                    instance_id,
                    Instance::new(
                        instance_log,
                        instance_id,
                        initial_runtime,
                        self.nexus_client.clone(),
                    )
                    .await?,
                );
                instances.get_mut(&instance_id).unwrap()
            }
        };

        instance.transition(target).await
    }

    /// Idempotently ensures that the given API Disk (described by `api_disk`)
    /// is attached (or not) as specified.
    ///
    /// NOTE: Not yet implemented.
    pub async fn disk_ensure(
        self: &Self,
        _disk_id: Uuid,
        _initial_state: ApiDiskRuntimeState,
        _target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        todo!("Disk attachment not yet implemented");
    }
}
