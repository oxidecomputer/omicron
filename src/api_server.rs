/*!
 * server-wide state and facilities
 */

use actix_web::web::Data;

use crate::api_model;
use crate::sim;
use sim::SimulatorBuilder;
use std::sync::Arc;

/**
 * Stores shared state used by API endpoints
 */
pub struct ApiServerState {
    /** the API backend to use for servicing requests */
    pub backend: Arc<dyn api_model::ApiBackend>
}

/**
 * Set up initial server-wide shared state.
 */
pub fn setup_server_state()
    -> Data<ApiServerState>
{
    let mut simbuilder = SimulatorBuilder::new();
    simbuilder.project_create("simproject1");
    simbuilder.project_create("simproject2");
    simbuilder.project_create("simproject3");

    Data::new(ApiServerState {
        backend: Arc::new(simbuilder.build())
    })
}
