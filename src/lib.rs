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
mod api_http_entrypoints_internal;
pub mod api_model;
mod controller;
mod datastore;
mod http_client;
mod server_controller;
mod server_controller_client;
mod test_util;

pub use api_config::ApiServerConfig;
pub use api_model::ApiServerStartupInfo;
pub use controller::OxideController;
pub use controller::OxideControllerTestInterfaces;
pub use server_controller::run_server_controller_api_server;
pub use server_controller::sc_dropshot_api;
pub use server_controller::ConfigServerController;
pub use server_controller::ControllerClient;
pub use server_controller::ServerController;
pub use server_controller::SimMode;
pub use server_controller_client::ServerControllerTestInterfaces;

use api_model::ApiIdentityMetadataCreateParams;
use api_model::ApiName;
use api_model::ApiProjectCreateParams;
use dropshot::ApiDescription;
use dropshot::RequestContext;
use futures::join;
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;

#[macro_use]
extern crate slog;

/**
 * Returns a Dropshot `ApiDescription` for our external API.
 */
pub fn dropshot_api_external() -> ApiDescription {
    let mut api = ApiDescription::new();
    if let Err(err) = api_http_entrypoints::api_register_entrypoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/**
 * Returns a Dropshot `ApiDescription` for our internal API.
 */
pub fn dropshot_api_internal() -> ApiDescription {
    let mut api = ApiDescription::new();
    if let Err(err) =
        api_http_entrypoints_internal::api_register_entrypoints_internal(
            &mut api,
        )
    {
        panic!("failed to register internal entrypoints: {}", err);
    }
    api
}

/**
 * Run the OpenAPI generator for the external API, which emits the OpenAPI spec
 * to stdout.
 */
pub fn run_openapi_external() {
    dropshot_api_external().print_openapi();
}

/**
 * Run an instance of the API server.
 */
pub async fn run_server(config: &ApiServerConfig) -> Result<(), String> {
    let log = config
        .log
        .to_logger("oxide-controller")
        .map_err(|message| format!("initializing logger: {}", message))?;
    info!(log, "starting server");

    let api_log = log.new(o!("component" => "apictx"));
    let apictx = ApiContext::new(&Uuid::new_v4(), api_log);
    let c1 = Arc::clone(&apictx);

    populate_initial_data(&apictx).await;

    let mut http_server_external = dropshot::HttpServer::new(
        &config.dropshot_external,
        dropshot_api_external(),
        c1,
        &log.new(o!("component" => "dropshot_external")),
    )
    .map_err(|error| format!("initializing external server: {}", error))?;

    let mut http_server_internal = dropshot::HttpServer::new(
        &config.dropshot_internal,
        dropshot_api_internal(),
        apictx,
        &log.new(o!("component" => "dropshot_internal")),
    )
    .map_err(|error| format!("initializing internal server: {}", error))?;

    let join_handle_external = http_server_external.run();
    let join_handle_internal = http_server_internal.run();

    let (join_result_external, join_result_internal) =
        join!(join_handle_external, join_handle_internal);

    let (result_external, result_internal) =
        match (join_result_external, join_result_internal) {
            (Ok(rexternal), Ok(rinternal)) => (rexternal, rinternal),
            (Err(error_external), Err(error_internal)) => {
                return Err(format!(
                    "failed to join both external and internal servers \
                     (external: \"{}\", internal: \"{}\")",
                    error_external, error_internal
                ));
            }
            (Err(error_external), Ok(_)) => {
                return Err(format!(
                    "failed to join external server: {}",
                    error_external
                ));
            }
            (Ok(_), Err(error_internal)) => {
                return Err(format!(
                    "failed to join internal server: {}",
                    error_internal
                ));
            }
        };

    match (result_external, result_internal) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error_external), Err(error_internal)) => {
            return Err(format!(
                "external and internal HTTP servers both stopped (external: \
                 \"{}\", internal: \"{}\"",
                error_external, error_internal
            ));
        }
        (Err(error_external), Ok(())) => {
            return Err(format!("external server stopped: {}", error_external));
        }
        (Ok(()), Err(error_internal)) => {
            return Err(format!("internal server stopped: {}", error_internal));
        }
    }
}

/**
 * API-specific state that we'll associate with the server and make available to
 * API request handler functions.
 */
pub struct ApiContext {
    pub controller: Arc<OxideController>,
    pub log: slog::Logger,
}

impl ApiContext {
    pub fn new(rack_id: &Uuid, log: slog::Logger) -> Arc<ApiContext> {
        Arc::new(ApiContext {
            controller: Arc::new(OxideController::new_with_id(
                rack_id,
                log.new(o!("component" => "controller")),
            )),
            log: log,
        })
    }

    /**
     * Retrieves our API-specific context out of the generic RequestContext
     * structure
     */
    pub fn from_request(rqctx: &Arc<RequestContext>) -> Arc<ApiContext> {
        Self::from_private(Arc::clone(&rqctx.server.private))
    }

    /**
     * Retrieves our API-specific context out of the generic HttpServer
     * structure.
     */
    pub fn from_server(server: &dropshot::HttpServer) -> Arc<ApiContext> {
        Self::from_private(server.app_private())
    }

    /**
     * Retrieves our API-specific context from the generic one stored in
     * Dropshot.
     */
    fn from_private(
        ctx: Arc<dyn Any + Send + Sync + 'static>,
    ) -> Arc<ApiContext> {
        /*
         * It should not be possible for this downcast to fail unless the caller
         * has passed us a RequestContext from a totally different HttpServer
         * or a totally different HttpServer itself (in either case created with
         * a different type for its private data).  This seems quite unlikely in
         * practice.
         * TODO-cleanup: can we make this API statically type-safe?
         */
        ctx.downcast::<ApiContext>()
            .expect("ApiContext: wrong type for private data")
    }
}

/*
 * This is a one-off for prepopulating some useful data in a freshly-started
 * server.  This should be replaced with a config file or a data backend with a
 * demo initialization script or the like.
 */
pub async fn populate_initial_data(apictx: &Arc<ApiContext>) {
    let controller = &apictx.controller;
    let demo_projects: Vec<(&str, &str)> = vec![
        ("1eb2b543-b199-405f-b705-1739d01a197c", "simproject1"),
        ("4f57c123-3bda-4fae-94a2-46a9632d40b6", "simproject2"),
        ("4aac89b0-df9a-441d-b050-f953476ea290", "simproject3"),
    ];

    for (new_uuid, new_name) in demo_projects {
        let name_validated = ApiName::try_from(new_name).unwrap();
        controller
            .project_create_with_id(
                Uuid::parse_str(new_uuid).unwrap(),
                &ApiProjectCreateParams {
                    identity: ApiIdentityMetadataCreateParams {
                        name: name_validated,
                        description: "<auto-generated at server startup>"
                            .to_string(),
                    },
                },
            )
            .await
            .unwrap();
    }
}
