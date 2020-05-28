/*
 * Library interface to the server controller mechanisms.
 */

mod controller_client;
mod server_controller;

use crate::api_http_entrypoints_internal::ApiServerStartupInfo;
use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiInstanceRuntimeState;
use crate::server_controller_client::DiskEnsureBody;
use crate::server_controller_client::InstanceEnsureBody;
use controller_client::ControllerClient;
use server_controller::ServerController;

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ExtractedParameter;
use dropshot::HttpError;
use dropshot::HttpResponseOkObject;
use dropshot::Json;
use dropshot::Path;
use dropshot::RequestContext;
use http::Method;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

pub use server_controller::ServerControllerTestInterfaces;

/**
 * How this `ServerController` simulates object states and transitions.
 */
#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum SimMode {
    /**
     * Indicates that asynchronous state transitions should be simulated
     * automatically using a timer to complete the transition a few seconds in
     * the future.
     */
    Auto,

    /**
     * Indicates that asynchronous state transitions should be simulated
     * explicitly, relying on calls through `ServerControllerTestInterfaces`.
     */
    Explicit,
}

/**
 * Configuration for a server controller.
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConfigServerController {
    pub id: Uuid,
    pub sim_mode: SimMode,
    pub controller_address: SocketAddr,
    pub dropshot: ConfigDropshot,
    pub log: ConfigLogging,
}

/* TODO-cleanup commonize with oxide-controller run_server() */
pub async fn run_server_controller_api_server(
    config: ConfigServerController,
) -> Result<(), String> {
    let log = config
        .log
        .to_logger("server-controller")
        .map_err(|message| format!("initializing logger: {}", message))?;
    info!(log, "starting server controller");

    let dropshot_log = log.new(o!("component" => "dropshot"));
    let sc_log = log.new(o!(
        "component" => "server_controller",
        "server" => config.id.clone().to_string()
    ));

    let client_log = log.new(o!("component" => "controller_client"));
    let controller_client = Arc::new(ControllerClient::new(
        config.controller_address.clone(),
        client_log,
    ));

    let sc = Arc::new(ServerController::new_simulated_with_id(
        &config.id,
        config.sim_mode,
        sc_log,
        Arc::clone(&controller_client),
    ));

    let my_address = config.dropshot.bind_address.clone();
    let mut http_server = dropshot::HttpServer::new(
        &config.dropshot,
        dropshot_api(),
        sc,
        &dropshot_log,
    )
    .map_err(|error| format!("initializing server: {}", error))?;

    let server_handle = http_server.run();

    /*
     * TODO this should happen continuously until it succeeds.
     */
    controller_client
        .notify_server_online(config.id.clone(), ApiServerStartupInfo {
            sc_address: my_address,
        })
        .await
        .unwrap();

    let join_handle = server_handle.await;
    let server_result = join_handle
        .map_err(|error| format!("waiting for server: {}", error))?;
    server_result.map_err(|error| format!("server stopped: {}", error))
}

/* TODO-cleanup commonize with ApiContext::from_private? */
fn rqctx_to_sc(rqctx: &Arc<RequestContext>) -> Arc<ServerController> {
    let ctx: Arc<dyn Any + Send + Sync + 'static> =
        Arc::clone(&rqctx.server.private);
    ctx.downcast::<ServerController>().expect("wrong type for private data")
}

/*
 * HTTP API functions
 */

fn dropshot_api() -> ApiDescription {
    let mut api = ApiDescription::new();
    api.register(scapi_instance_ensure).unwrap();
    api.register(scapi_disk_ensure).unwrap();
    api
}

#[derive(Deserialize, ExtractedParameter)]
struct InstancePathParam {
    instance_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}",
}]
async fn scapi_instance_ensure(
    rqctx: Arc<RequestContext>,
    path_params: Path<InstancePathParam>,
    body: Json<InstanceEnsureBody>,
) -> Result<HttpResponseOkObject<ApiInstanceRuntimeState>, HttpError> {
    let sc = rqctx_to_sc(&rqctx);
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOkObject(
        sc.instance_ensure(
            instance_id,
            body_args.initial_runtime.clone(),
            body_args.target.clone(),
        )
        .await?,
    ))
}

#[derive(Deserialize, ExtractedParameter)]
struct DiskPathParam {
    disk_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/disks/{disk_id}",
}]
async fn scapi_disk_ensure(
    rqctx: Arc<RequestContext>,
    path_params: Path<DiskPathParam>,
    body: Json<DiskEnsureBody>,
) -> Result<HttpResponseOkObject<ApiDiskRuntimeState>, HttpError> {
    let sc = rqctx_to_sc(&rqctx);
    let disk_id = path_params.into_inner().disk_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOkObject(
        sc.disk_ensure(
            disk_id,
            body_args.initial_runtime.clone(),
            body_args.target.clone(),
        )
        .await?,
    ))
}
