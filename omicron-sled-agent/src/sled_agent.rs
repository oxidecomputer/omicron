/*!
 * Sled agent implementation.
 */

use chrono::Utc;
use futures::lock::Mutex;
use std::collections::HashMap;
use omicron_common::error::ApiError;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskStateRequested;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceRuntimeStateRequested;
use omicron_common::NexusClient;
use slog::Logger;
use std::sync::Arc;
use uuid::Uuid;

/// TODO: Relocate?
pub struct Plot {
    // TODO: Zone info?

    client: Option<propolis_client::Client>,

    current_state: ApiInstanceRuntimeState,

    requested_state: Option<ApiInstanceRuntimeStateRequested>,
}

impl Plot {
    pub fn new(instance_id: Uuid, initial_state: ApiInstanceRuntimeState) -> Self {
        let service = "propolis-server";
        let manifest = format!("/opt/oxide/{}/pkg/manifest.xml", service);
        smf::Config::import().run(manifest).unwrap();
        let parent_fmri = format!("svc:/system/illumos/{}", service);
        let child_service = instance_id.to_string();
        smf::Config::add(parent_fmri).run(&child_service).unwrap();

        // TODO: Could disable service on drop?
        smf::Adm::new()
            .enable()
            .synchronous()
            .temporary()
            .run(smf::AdmSelection::ByPattern(&[child_service])).unwrap();

        Plot {
            client: None,
            current_state: initial_state,
            requested_state: None,
        }
    }

    fn transition(&mut self, target: ApiInstanceRuntimeStateRequested)
        -> Result<Option<ApiInstanceRuntimeStateRequested>, ApiError> {
        todo!();
    }
}

/**
 * TODO:
 */
pub struct SledAgent {
    /** unique id for this server */
    id: Uuid,

    log: Logger,

    nexus_client: Arc<NexusClient>,

    plots: Mutex<HashMap<Uuid, Plot>>,
}

impl SledAgent {
    /** Constructs a SledAgent with the given uuid. */
    pub fn new(
        id: &Uuid,
        log: Logger,
        nexus_client: Arc<NexusClient>,
    ) -> SledAgent {
        info!(&log, "creating sled agent");

        // TODO: We should probably check out the existing services to bring
        // propolis instances into a predictable state - what if the SA crashes
        // w/propolis clients still running?

        SledAgent {
            id: *id,
            log,
            nexus_client,
            plots: Mutex::new(HashMap::new()),
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
        info!(&self.log, "instance_ensure: {}", instance_id);

        let mut plots = self.plots.lock().await;

        if !plots.contains_key(&instance_id) {
            // Validate the initial state.
            if initial_runtime.reboot_in_progress ||
                initial_runtime.run_state != ApiInstanceState::Creating {
                return Err(ApiError::InvalidRequest {
                    message: format!(
                        "cannot create instance with state: {}", initial_runtime.run_state
                    ),
                });
            }

            info!(&self.log, "allocating instance {}", instance_id);
            let plot = Plot::new(instance_id, initial_runtime);
            info!(&self.log, "created new child service: {}", instance_id);
            plots.insert(instance_id, plot);
        }

        todo!();
        /*
        Ok(
            ApiInstanceRuntimeState {
                run_state: _,
                reboot_in_progress: false,
                sled_uuid: self.id,
                gen: _,
                time_updated: Utc::now(),
            }
        )
        */
    }

    /**
     * Idempotently ensures that the given API Disk (described by `api_disk`)
     * is attached (or not) as specified.
     */
    pub async fn disk_ensure(
        self: &Arc<Self>,
        disk_id: Uuid,
        initial_state: ApiDiskRuntimeState,
        target: ApiDiskStateRequested,
    ) -> Result<ApiDiskRuntimeState, ApiError> {
        todo!();
    }
}
