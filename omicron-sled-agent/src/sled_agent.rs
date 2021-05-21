/// Sled agent implementation

use futures::lock::Mutex;
use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskStateRequested;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::NexusClient;
use slog::Logger;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::instance::{InstanceState, Action as InstanceAction};

// TODO: Could easily refactor this elsewhere...
struct Instance {
    id: Uuid,
    state: InstanceState,
}

impl Instance {
    fn new(id: Uuid, initial_runtime: ApiInstanceRuntimeState) -> Result<Self, ApiError> {
        // TODO: Probably needs more config?
        // TODO: Launch in a Zone?

        // Launch a new SMF service running Propolis.
        smf::Config::add("svc:/system/illumos/propolis-server")
            .run(id.to_string())
            .map_err(|e|
                ApiError::InternalError {
                    message: format!("Cannot create propolis service: {}", e)
                }
            )?;

        // TODO: IP ADDRESS????

        Ok(Instance {
            id,
            state: InstanceState::new(initial_runtime),
        })
    }
}

pub struct SledAgent {
    id: Uuid,
    log: Logger,
    nexus_client: Arc<NexusClient>,
    instances: Mutex<BTreeMap<Uuid, Instance>>,
}

impl SledAgent {
    pub fn new(
        id: &Uuid,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> SledAgent {
        info!(&log, "created sled agent"; "id" => ?id);

        // TODO: Connection to propolis? Should this be done lazily?

        SledAgent {
            id: *id,
            log,
            nexus_client,
            instances: Mutex::new(BTreeMap::new()),
        }
    }

    /// Idempotently ensures that the given API Instance (described by
    /// `api_instance`) exists on this server in the given runtime state
    /// (described by `target`).
    pub async fn instance_ensure(
        self: &Arc<Self>,
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
                instances.insert(instance_id, Instance::new(instance_id, initial_runtime)?);
                instances.get_mut(&instance_id).unwrap()
            }
        };

        if let Some(action) = instance.state.request_transition(&target)? {
            match action {
                InstanceAction::Run => todo!(),
                InstanceAction::Stop => todo!(),
                InstanceAction::Reboot => todo!(),
                InstanceAction::Destroy => todo!(),
            }
        }
        Ok(instance.state.current().clone())
    }

    /// Idempotently ensures that the given API Disk (described by `api_disk`)
    /// is attached (or not) as specified.
    pub async fn disk_ensure(
        self: &Arc<Self>,
        disk_id: Uuid,
        initial_state: ApiDiskRuntimeState,
        target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        todo!("Disk attachment not yet implemented");
    }
}
