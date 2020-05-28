/*!
 * Handler functions (entrypoints) for HTTP APIs internal to the control plane
 */

use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiInstanceRuntimeState;
use crate::server_controller_client::ServerControllerClient;
use crate::ApiContext;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ExtractedParameter;
use dropshot::HttpError;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Json;
use dropshot::Path;
use dropshot::RequestContext;
use http::Method;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

pub fn api_register_entrypoints_internal(
    api: &mut ApiDescription,
) -> Result<(), String> {
    api.register(cpapi_servers_post)?;
    api.register(cpapi_instances_put)?;
    api.register(cpapi_disks_put)?;
    Ok(())
}

#[derive(Deserialize, ExtractedParameter)]
struct ServerPathParam {
    server_id: Uuid,
}

/* XXX commonize in an api_model_internal? */
#[derive(Serialize, Deserialize)]
pub struct ApiServerStartupInfo {
    pub sc_address: SocketAddr,
}

/**
 * Report that the server controller for the specified server has come online.
 */
#[endpoint {
     method = POST,
     path = "/servers/{server_id}",
 }]
async fn cpapi_servers_post(
    rqctx: Arc<RequestContext>,
    path_params: Path<ServerPathParam>,
    server_info: Json<ApiServerStartupInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let si = server_info.into_inner();
    let server_id = &path.server_id;
    let client_log = apictx
        .log
        .new(o!("server_controller" => server_id.clone().to_string()));
    let client = Arc::new(ServerControllerClient::new(
        &server_id,
        si.sc_address,
        client_log,
    ));
    /* XXX needs to do something saner when the client already exists */
    controller.add_server_controller(client).await;
    Ok(HttpResponseUpdatedNoContent())
}

/* XXX commonize in an api_model_internal? */
#[derive(Deserialize, ExtractedParameter)]
struct InstancePathParam {
    instance_id: Uuid,
}

/**
 * Report updated state for an instance.
 */
#[endpoint {
     method = PUT,
     path = "/instances/{instance_id}",
 }]
async fn cpapi_instances_put(
    rqctx: Arc<RequestContext>,
    path_params: Path<InstancePathParam>,
    new_runtime_state: Json<ApiInstanceRuntimeState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    controller
        .as_sc_api()
        .notify_instance_updated(&path.instance_id, &new_state)
        .await?;
    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, ExtractedParameter)]
struct DiskPathParam {
    disk_id: Uuid,
}

/**
 * Report updated state for a disk.
 */
#[endpoint {
     method = PUT,
     path = "/disks/{disk_id}",
 }]
async fn cpapi_disks_put(
    rqctx: Arc<RequestContext>,
    path_params: Path<DiskPathParam>,
    new_runtime_state: Json<ApiDiskRuntimeState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    controller
        .as_sc_api()
        .notify_disk_updated(&path.disk_id, &new_state)
        .await?;
    Ok(HttpResponseUpdatedNoContent())
}
