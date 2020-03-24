/*!
 * Library interfaces for this crate, intended for use only by the automated
 * test suite.  This crate does not define a Rust library API that's intended to
 * be consumed from the outside.
 *
 * TODO-cleanup is there a better way to do this?
 */

pub mod api_config;
pub mod api_error;
mod api_http_entrypoints;
pub mod api_model;
pub mod httpapi;
mod sim;
pub mod test_common;

pub use api_config::ApiServerConfig;
use httpapi::RequestContext;
pub use httpapi::HEADER_REQUEST_ID;
use std::any::Any;
use std::sync::Arc;

#[macro_use]
extern crate slog;
use slog::Logger;

/**
 * Consumer handle for the API server.
 */
pub struct ApiServer {
    pub http_server: httpapi::HttpServer,
    pub log: Logger,
}

impl ApiServer {
    pub fn new(
        config: &ApiServerConfig,
    ) -> Result<ApiServer, api_error::InitError> {
        let mut simbuilder = sim::SimulatorBuilder::new();
        simbuilder.project_create("simproject1");
        simbuilder.project_create("simproject2");
        simbuilder.project_create("simproject3");

        let api_state = Arc::new(ApiRequestContext {
            backend: Arc::new(simbuilder.build()),
        });

        let mut router = httpapi::HttpRouter::new();

        let log = config.log.to_logger()?;
        api_http_entrypoints::api_register_entrypoints(&mut router);
        let http_server = httpapi::HttpServer::new(
            &config.bind_address,
            router,
            Box::new(api_state),
            log.new(slog::o!()),
        )
        .map_err(|error| api_error::InitError(format!("{}", error)))?;

        Ok(ApiServer {
            http_server,
            log,
        })
    }
}

/**
 * API-specific state that we'll associate with the server and make available to
 * API request handler functions.  See `api_backend()`.
 */
pub struct ApiRequestContext {
    pub backend: Arc<dyn api_model::ApiBackend>,
}

/**
 * This function gets our implementation-specific backend out of the
 * generic RequestContext structure.  We make use of 'dyn Any' here and
 * downcast.  It should not be possible for this downcast to fail unless the
 * caller has passed us a RequestContext from a totally different HttpServer
 * created with a different type for its private data, which we do not expect.
 * TODO-cleanup: can we make this API statically type-safe?
 */
pub fn api_backend(
    rqctx: &Arc<RequestContext>,
) -> Arc<dyn api_model::ApiBackend> {
    let maybectx: &(dyn Any + Send + Sync) = rqctx.server.private.as_ref();
    let apictx = maybectx
        .downcast_ref::<Arc<ApiRequestContext>>()
        .expect("api_backend(): wrong type for private data");
    return Arc::clone(&apictx.backend);
}
