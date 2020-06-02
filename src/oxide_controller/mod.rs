/*!
 * Library interface to the Oxide Controller mechanisms.
 */

mod config;
mod context;
mod controller_client;
mod http_entrypoints_external;
mod http_entrypoints_internal;
mod oxide_controller;

pub use config::ConfigController;
pub use context::ControllerServerContext;
pub use controller_client::ControllerClient;
pub use oxide_controller::OxideController;
pub use oxide_controller::OxideControllerTestInterfaces;

use http_entrypoints_external::controller_external_api;
use http_entrypoints_internal::controller_internal_api;

use crate::api_model::ApiIdentityMetadataCreateParams;
use crate::api_model::ApiName;
use crate::api_model::ApiProjectCreateParams;
use futures::join;
use slog::Logger;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::task::JoinHandle;
use uuid::Uuid;

/**
 * Run the OpenAPI generator for the external API, which emits the OpenAPI spec
 * to stdout.
 */
pub fn controller_run_openapi_external() {
    controller_external_api().print_openapi();
}

pub struct OxideControllerServer {
    pub apictx: Arc<ControllerServerContext>,
    pub http_server_external: dropshot::HttpServer,
    pub http_server_internal: dropshot::HttpServer,

    join_handle_external: JoinHandle<Result<(), hyper::error::Error>>,
    join_handle_internal: JoinHandle<Result<(), hyper::error::Error>>,
}

impl OxideControllerServer {
    pub async fn start(
        config: &ConfigController,
        rack_id: &Uuid,
        log: &Logger,
    ) -> Result<OxideControllerServer, String> {
        info!(log, "setting up controller server");

        let ctxlog = log.new(o!("component" => "ControllerServerContext"));
        let apictx = ControllerServerContext::new(rack_id, ctxlog);
        populate_initial_data(&apictx).await;

        let c1 = Arc::clone(&apictx);
        let mut http_server_external = dropshot::HttpServer::new(
            &config.dropshot_external,
            controller_external_api(),
            c1,
            &log.new(o!("component" => "dropshot_external")),
        )
        .map_err(|error| format!("initializing external server: {}", error))?;

        let c2 = Arc::clone(&apictx);
        let mut http_server_internal = dropshot::HttpServer::new(
            &config.dropshot_internal,
            controller_internal_api(),
            c2,
            &log.new(o!("component" => "dropshot_internal")),
        )
        .map_err(|error| format!("initializing internal server: {}", error))?;

        let join_handle_external = http_server_external.run();
        let join_handle_internal = http_server_internal.run();

        Ok(OxideControllerServer {
            apictx,
            http_server_external,
            http_server_internal,
            join_handle_external,
            join_handle_internal,
        })
    }

    pub async fn wait_for_finish(self) -> Result<(), String> {
        let (join_result_external, join_result_internal) =
            join!(self.join_handle_external, self.join_handle_internal);

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
                    "external and internal HTTP servers both stopped \
                     (external: \"{}\", internal: \"{}\"",
                    error_external, error_internal
                ));
            }
            (Err(error_external), Ok(())) => {
                return Err(format!(
                    "external server stopped: {}",
                    error_external
                ));
            }
            (Ok(()), Err(error_internal)) => {
                return Err(format!(
                    "internal server stopped: {}",
                    error_internal
                ));
            }
        }
    }
}

/**
 * Run an instance of the API server.
 */
pub async fn controller_run_server(
    config: &ConfigController,
) -> Result<(), String> {
    let log = config
        .log
        .to_logger("oxide-controller")
        .map_err(|message| format!("initializing logger: {}", message))?;
    let rack_id = Uuid::new_v4();
    let server = OxideControllerServer::start(config, &rack_id, &log).await?;
    server.wait_for_finish().await
}

/*
 * This is a one-off for prepopulating some useful data in a freshly-started
 * server.  This should be replaced with a config file or a data backend with a
 * demo initialization script or the like.
 */
pub async fn populate_initial_data(apictx: &Arc<ControllerServerContext>) {
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
