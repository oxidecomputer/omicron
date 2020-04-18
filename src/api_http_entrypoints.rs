/*!
 * Handler functions (entrypoints) for HTTP APIs
 */

use http::Method;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use crate::api_model::ApiInstance;
use crate::api_model::ApiInstanceCreateParams;
use crate::api_model::ApiInstanceView;
use crate::api_model::ApiName;
use crate::api_model::ApiObject;
use crate::api_model::ApiProject;
use crate::api_model::ApiProjectCreateParams;
use crate::api_model::ApiProjectUpdateParams;
use crate::api_model::ApiProjectView;
use crate::api_model::ApiRackView;
use crate::controller::to_view_list;
use crate::controller::PaginationParams;
use crate::ApiContext;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ExtractedParameter;
use dropshot::HttpError;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOkObject;
use dropshot::HttpResponseOkObjectList;
use dropshot::Json;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;

pub fn api_register_entrypoints(
    api: &mut ApiDescription,
) -> Result<(), String> {
    api.register(api_projects_get)?;
    api.register(api_projects_post)?;
    api.register(api_projects_get_project)?;
    api.register(api_projects_delete_project)?;
    api.register(api_projects_put_project)?;
    api.register(api_project_instances_get)?;
    api.register(api_project_instances_post)?;
    api.register(api_project_instances_get_instance)?;
    api.register(api_project_instances_delete_instance)?;
    api.register(api_hardware_racks_get)?;
    api.register(api_hardware_racks_get_rack)?;

    Ok(())
}

/*
 * API ENDPOINT FUNCTION NAMING CONVENTIONS
 *
 * Generally, HTTP resources are grouped within some collection.  For a
 * relatively simple example:
 *
 *   GET    /projects                 (list the projects in the collection)
 *   POST   /projects                 (create a project in the collection)
 *   GET    /projects/{project_name}  (look up a project in the collection)
 *   DELETE /projects/{project_name}  (delete a project in the collection)
 *   PUT    /projects/{project_name}  (update a project in the collection)
 *
 * There's a naming convention for the functions that implement these API entry
 * points.  When operating on the collection itself, we use:
 *
 *    api_{collection_path}_{verb}
 *
 * For examples:
 *
 *    GET  /projects                    -> api_projects_get()
 *    POST /projects                    -> api_projects_post()
 *
 * For operations on items within the collection, we use:
 *
 *    api_{collection_path}_{verb}_{object}
 *
 * For examples:
 *
 *    DELETE /projects/{project_name}   -> api_projects_delete_project()
 *    GET    /projects/{project_name}   -> api_projects_get_project()
 *    PUT    /projects/{project_name}   -> api_projects_put_project()
 */

/**
 * List all projects.
 */
#[endpoint {
     method = GET,
     path = "/projects",
 }]
async fn api_projects_get(
    rqctx: Arc<RequestContext>,
    query_params: Query<PaginationParams<ApiName>>,
) -> Result<HttpResponseOkObjectList<ApiProjectView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let query = query_params.into_inner();
    let project_stream = controller.projects_list(&query).await?;
    let view_list = to_view_list(project_stream).await;
    Ok(HttpResponseOkObjectList(view_list))
}

/**
 * Create a new project.
 */
#[endpoint {
    method = POST,
    path = "/projects"
}]
async fn api_projects_post(
    rqctx: Arc<RequestContext>,
    new_project: Json<ApiProjectCreateParams>,
) -> Result<HttpResponseCreated<ApiProjectView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let project = controller.project_create(&new_project.into_inner()).await?;
    Ok(HttpResponseCreated(project.to_view()))
}

#[derive(Deserialize, ExtractedParameter)]
struct ProjectPathParam {
    /// The project's unique ID.
    project_name: ApiName,
}

/**
 * Fetch a specific project
 */
#[endpoint {
    method = GET,
    path = "/projects/{project_name}",
}]
async fn api_projects_get_project(
    rqctx: Arc<RequestContext>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOkObject<ApiProjectView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let project: Arc<ApiProject> =
        controller.project_lookup(&project_name).await?;
    Ok(HttpResponseOkObject(project.to_view()))
}

/**
 * Delete a specific project.
 */
#[endpoint {
     method = DELETE,
     path = "/projects/{project_name}",
 }]
async fn api_projects_delete_project(
    rqctx: Arc<RequestContext>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let params = path_params.into_inner();
    let project_name = &params.project_name;
    controller.project_delete(&project_name).await?;
    Ok(HttpResponseDeleted())
}

/**
 * Update a specific project.
 *
 * TODO-correctness: Is it valid for PUT to accept application/json that's a
 * subset of what the resource actually represents?  If not, is that a problem?
 * (HTTP may require that this be idempotent.)  If so, can we get around that
 * having this be a slightly different content-type (e.g.,
 * "application/json-patch")?  We should see what other APIs do.
 */
#[endpoint {
     method = PUT,
     path = "/projects/{project_name}",
 }]
async fn api_projects_put_project(
    rqctx: Arc<RequestContext>,
    path_params: Path<ProjectPathParam>,
    updated_project: Json<ApiProjectUpdateParams>,
) -> Result<HttpResponseOkObject<ApiProjectView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let newproject = controller
        .project_update(&project_name, &updated_project.into_inner())
        .await?;
    Ok(HttpResponseOkObject(newproject.to_view()))
}

/*
 * Instances
 */

/**
 * List instances in a project.
 */
#[endpoint {
     method = GET,
     path = "/projects/{project_name}/instances",
 }]
async fn api_project_instances_get(
    rqctx: Arc<RequestContext>,
    query_params: Query<PaginationParams<ApiName>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOkObjectList<ApiInstanceView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_stream =
        controller.project_list_instances(&project_name, &query).await?;
    let view_list = to_view_list(instance_stream).await;
    Ok(HttpResponseOkObjectList(view_list))
}

/**
 * Create an instance in a project.
 *
 * TODO-correctness This is supposed to be async.  Is that right?  We can create
 * the instance immediately -- it's just not booted yet.  Maybe the boot
 * operation is what's a separate operation_id.  What about the response code
 * (201 Created vs 202 Accepted)?  Is that orthogonal?  Things can return a
 * useful response, including an operation id, with either response code.  Maybe
 * a "reboot" operation would return a 202 Accepted because there's no actual
 * resource created?
 */
#[endpoint {
     method = POST,
     path = "/projects/{project_name}/instances",
 }]
async fn api_project_instances_post(
    rqctx: Arc<RequestContext>,
    path_params: Path<ProjectPathParam>,
    new_instance: Json<ApiInstanceCreateParams>,
) -> Result<HttpResponseCreated<ApiInstanceView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let new_instance_params = &new_instance.into_inner();
    let instance = controller
        .project_create_instance(&project_name, &new_instance_params)
        .await?;
    Ok(HttpResponseCreated(instance.to_view()))
}

#[derive(Deserialize, ExtractedParameter)]
struct InstancePathParam {
    project_name: ApiName,
    instance_name: ApiName,
}

/**
 * Get an instance in a project.
 */
#[endpoint {
     method = GET,
     path = "/projects/{project_name}/instances/{instance_name}",
 }]
async fn api_project_instances_get_instance(
    rqctx: Arc<RequestContext>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOkObject<ApiInstanceView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let instance: Arc<ApiInstance> = controller
        .project_lookup_instance(&project_name, &instance_name)
        .await?;
    Ok(HttpResponseOkObject(instance.to_view()))
}

/**
 * Delete an instance from a project.
 */
#[endpoint {
     method = DELETE,
     path = "/projects/{project_name}/instances/{instance_name}",
 }]
async fn api_project_instances_delete_instance(
    rqctx: Arc<RequestContext>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    controller.project_delete_instance(&project_name, &instance_name).await?;
    Ok(HttpResponseDeleted())
}

/*
 * Racks
 */

/**
 * List racks in the system.
 */
#[endpoint {
     method = GET,
     path = "/hardware/racks",
 }]
async fn api_hardware_racks_get(
    rqctx: Arc<RequestContext>,
    params_raw: Query<PaginationParams<Uuid>>,
) -> Result<HttpResponseOkObjectList<ApiRackView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let params = params_raw.into_inner();
    let rack_stream = controller.racks_list(&params).await?;
    let view_list = to_view_list(rack_stream).await;
    Ok(HttpResponseOkObjectList(view_list))
}

#[derive(Deserialize, ExtractedParameter)]
struct RackPathParam {
    /** The rack's unique ID. */
    rack_id: Uuid,
}

/**
 * Fetch information about a particular rack.
 */
#[endpoint {
    method = GET,
    path = "/hardware/racks/{rack_id}",
}]
async fn api_hardware_racks_get_rack(
    rqctx: Arc<RequestContext>,
    path_params: Path<RackPathParam>,
) -> Result<HttpResponseOkObject<ApiRackView>, HttpError> {
    let apictx = ApiContext::from_request(&rqctx);
    let controller = &apictx.controller;
    let path = path_params.into_inner();
    let rack_info = controller.rack_lookup(&path.rack_id).await?;
    Ok(HttpResponseOkObject(rack_info.to_view()))
}
