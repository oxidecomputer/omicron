/*!
 * Interfaces available to saga actions and undo actions
 */

use crate::api_error::ApiError;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiProject;
use crate::nexus::db;
use crate::nexus::Nexus;
use crate::sled_agent;
use std::sync::Arc;
use uuid::Uuid;

/*
 * TODO-design Should this be the same thing as ServerContext?  It's
 * very analogous, but maybe there's utility in having separate views for the
 * HTTP server and sagas.
 */
pub struct SagaContext {
    nexus: Arc<Nexus>,
}

impl SagaContext {
    pub fn new(nexus: Arc<Nexus>) -> SagaContext {
        SagaContext { nexus }
    }

    /*
     * TODO-design This interface should not exist.  Instead, sleds should be
     * represented in the database.  Reservations will wind up writing to the
     * database.  Allocating a server will thus be a saga action, complete with
     * an undo action.  The only thing needed at this layer is a way to read and
     * write to the database, which we already have.
     *
     * For now, sleds aren't in the database.  We rely on the fact that Nexus
     * knows what sleds exist.
     *
     * Note: the parameters appear here (unused) to make sure callers make sure
     * to have them available.  They're not used now, but they will be in a real
     * implementation.
     */
    pub async fn alloc_server(
        &self,
        _params: &ApiInstanceCreateParams,
    ) -> Result<Uuid, ApiError> {
        self.nexus.sled_allocate().await
    }

    pub fn datastore(&self) -> &db::DataStore {
        self.nexus.datastore()
    }

    pub async fn sled_client(
        &self,
        sled_id: &Uuid,
    ) -> Result<Arc<sled_agent::Client>, ApiError> {
        self.nexus.sled_client(sled_id).await
    }
}
