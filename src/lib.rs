/*!
 * Library interfaces for this crate, intended for use only by the automated
 * test suite.  This crate does not define a Rust library API that's intended to
 * be consumed from the outside.
 *
 * TODO-cleanup is there a better way to do this?
 */

pub mod api_error;
mod api_http_entrypoints;
pub mod api_http_util;
pub mod api_model;
pub mod httpapi;
mod sim;

use std::net::SocketAddr;
use std::sync::Arc;

pub struct ApiServer {
    pub http_server: httpapi::HttpServer,
}

pub struct ApiServerState {
    pub backend: Arc<dyn api_model::ApiBackend>,
}

impl ApiServer {
    pub fn new(bind_address: &SocketAddr)
        -> Result<ApiServer, hyper::error::Error>
    {
        let mut simbuilder = sim::SimulatorBuilder::new();
        simbuilder.project_create("simproject1");
        simbuilder.project_create("simproject2");
        simbuilder.project_create("simproject3");

        let api_state = ApiServerState {
            backend: Arc::new(simbuilder.build()),
        };

        let http_server = httpapi::HttpServer::new(bind_address, api_state)?;
        let mut router = http_server.router();

        api_http_entrypoints::api_register_entrypoints(router);

        Ok(ApiServer {
            http_server: http_server
        })
    }
}
