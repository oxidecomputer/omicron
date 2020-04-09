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
mod api_impl;

pub use api_config::ApiServerConfig;
use dropshot::ApiDescription;
use dropshot::RequestContext;
use std::sync::Arc;
use api_impl::OxideRack;

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
pub fn run_openapi() {
    dropshot_api().print_openapi();
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
        ApiContext::new(),
        &log,
    )
    .map_err(|error| format!("initializing server: {}", error))?;

    let join_handle = http_server.run().await;
    let server_result = join_handle
        .map_err(|error| format!("waiting for server: {}", error))?;

    // XXX This is where we should initialize hardcoded data, I think?
    //simbuilder
    //    .project_create("1eb2b543-b199-405f-b705-1739d01a197c", "simproject1");
    //simbuilder
    //    .project_create("4f57c123-3bda-4fae-94a2-46a9632d40b6", "simproject2");
    //simbuilder
    //    .project_create("4aac89b0-df9a-441d-b050-f953476ea290", "simproject3");

    server_result.map_err(|error| format!("server stopped: {}", error))
}

/**
 * API-specific state that we'll associate with the server and make available to
 * API request handler functions.
 */
pub struct ApiContext {
    pub rack: Arc<OxideRack>,
}

impl ApiContext {
    pub fn new() -> Arc<ApiContext> {
        Arc::new(ApiContext {
            rack: Arc::new(api_impl::OxideRack::new()),
        })
    }

    /**
     * Retrieves our API-specific context out of the generic RequestContext
     * structure.  It should not be possible for this downcast to fail unless
     * the caller has passed us a RequestContext from a totally different
     * HttpServer created with a different type for its private data.  This
     * should not happen in practice.
     * TODO-cleanup: can we make this API statically type-safe?
     */
    fn from_request(rqctx: &Arc<RequestContext>) -> Arc<ApiContext> {
        let maybectx = Arc::clone(&rqctx.server.private);
        maybectx
            .downcast::<ApiContext>()
            .expect("ApiContext: wrong type for private data")
    }
}
