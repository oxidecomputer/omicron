/*!
 * Interfaces available to saga actions and undo actions
 */

use crate::api_error::ApiError;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiProject;
use crate::datastore::ControlDataStore;
use crate::oxide_controller::OxideController;
use crate::SledAgentClient;
use std::sync::Arc;
use uuid::Uuid;

/* XXX Compare to ControllerServerContext */
pub struct OxcSagaContext {
    controller: Arc<OxideController>,
}

impl OxcSagaContext {
    pub fn new(controller: Arc<OxideController>) -> OxcSagaContext {
        OxcSagaContext {
            controller
        }
    }

    /* XXX These interfaces need work, but they're a rough start. */
    pub async fn alloc_server(
        &self,
        project: &ApiProject,
        params: &ApiInstanceCreateParams,
    ) -> Result<Arc<SledAgentClient>, ApiError> {
        let sleds = self.controller.sled_agents.lock().await;
        let arc = self
            .controller
            .sled_allocate_instance(&sleds, project, params)
            .await?;
        Ok(Arc::clone(arc))
    }

    pub fn datastore(&self) -> &ControlDataStore {
        &self.controller.datastore
    }

    pub async fn sled_client(
        &self,
        sled_id: &Uuid,
    ) -> Result<Arc<SledAgentClient>, ApiError> {
        self.controller.sled_client(sled_id).await
    }
}
