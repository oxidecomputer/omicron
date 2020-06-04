/*!
 * Handler functions (entrypoints) for HTTP APIs internal to the control plane
 */

use super::ControllerServerContext;

use crate::api_model::ApiDiskRuntimeState;
use crate::api_model::ApiInstanceRuntimeState;
use crate::api_model::ApiServerStartupInfo;
use crate::SledAgentClient;
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
use std::sync::Arc;
use uuid::Uuid;

pub fn controller_internal_api() -> ApiDescription {
    let mut api = ApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

fn register_endpoints(api: &mut ApiDescription) -> Result<(), String> {
    api.register(cpapi_servers_post)?;
    api.register(cpapi_instances_put)?;
    api.register(cpapi_disks_put)?;
    Ok(())
}

#[derive(Deserialize, ExtractedParameter)]
struct ServerPathParam {
    server_id: Uuid,
}

/**
 * Report that the sled agent for the specified server has come online.
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
    let apictx = ControllerServerContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let si = server_info.into_inner();
    let server_id = &path.server_id;
    let client_log =
        apictx.log.new(o!("SledAgent" => server_id.clone().to_string()));
    let client =
        Arc::new(SledAgentClient::new(&server_id, si.sa_address, client_log));
    controller.upsert_sled_agent(client).await;
    Ok(HttpResponseUpdatedNoContent())
}

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
    let apictx = ControllerServerContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    controller.notify_instance_updated(&path.instance_id, &new_state).await?;
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
    let apictx = ControllerServerContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    controller.notify_disk_updated(&path.disk_id, &new_state).await?;
    Ok(HttpResponseUpdatedNoContent())
}
