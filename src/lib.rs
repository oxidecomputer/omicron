/*!
 * Library interfaces for this crate, intended for use only by the automated
 * test suite.  This crate does not define a Rust library API that's intended to
 * be consumed from the outside.
 *
 * TODO-cleanup is there a better way to do this?
 */

mod api_config;
mod api_error;
mod api_http_entrypoints;
pub mod api_model;
mod sim;

pub use api_config::ApiServerConfig;
use dropshot::ApiDescription;
use dropshot::RequestContext;
use std::any::Any;
use std::sync::Arc;

#[macro_use]
extern crate slog;

/**
 * Returns a Dropshot `ApiDescription` for our API.
 */
pub fn dropshot_api() -> ApiDescription {
    let mut api = ApiDescription::new();
    api_http_entrypoints::api_register_entrypoints(&mut api);
    api
}

/**
 * Run the OpenAPI generator, which emits the OpenAPI spec to stdout.
 */
pub fn run_openapi()
{
    dropshot_api().print_openapi();
}

/**
 * Returns the API-specific state object to be used in our API server.
 * TODO-cleanup it's clearer now how to get rid of one level of Arc.
 */
pub fn api_context() -> Box<Arc<ApiContext>>
{
    let mut simbuilder = sim::SimulatorBuilder::new();
    simbuilder.project_create("simproject1");
    simbuilder.project_create("simproject2");
    simbuilder.project_create("simproject3");

    let api_state = Arc::new(ApiContext {
        backend: Arc::new(simbuilder.build()),
    });

    Box::new(api_state)
}

/**
 * Run an instance of the API server.
 */
pub async fn run_server(config: &ApiServerConfig) -> Result<(), String> {
    let log = config
        .log
        .to_logger("oxide-api")
        .map_err(|message| format!("initializing logger: {}", message))?;
    info!(log, "starting server");

    let mut http_server = dropshot::HttpServer::new(
        &config.dropshot,
        dropshot_api(),
        api_context(),
        &log,
    )
    .map_err(|error| format!("initializing server: {}", error))?;

    let join_handle = http_server.run().await;
    let server_result = join_handle
        .map_err(|error| format!("waiting for server: {}", error))?;
    server_result.map_err(|error| format!("server stopped: {}", error))
}

/**
 * API-specific state that we'll associate with the server and make available to
 * API request handler functions.  See `api_backend()`.
 */
pub struct ApiContext {
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
        .downcast_ref::<Arc<ApiContext>>()
        .expect("api_backend(): wrong type for private data");
    return Arc::clone(&apictx.backend);
}
