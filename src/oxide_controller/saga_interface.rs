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
        OxcSagaContext { controller }
    }

    /*
     * TODO-design This interface should not exist.  Instead, sleds should be
     * represented in the database.  Reservations will wind up writing to the
     * database.  Allocating a server will thus be a saga action, complete with
     * an undo action.  The only thing needed at this layer is a way to read and
     * write to the database, which we already have.
     *
     * For now, sleds aren't in the database.  We rely on the fact that the
     * controller knows what sleds exist.
     *
     * Note: the parameters appear here (unused) to make sure callers make sure
     * to have them available.  They're not used now, but they will be in a real
     * implementation.
     */
    pub async fn alloc_server(
        &self,
        _project: &ApiProject,
        _params: &ApiInstanceCreateParams,
    ) -> Result<Uuid, ApiError> {
        self.controller.sled_allocate().await
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
