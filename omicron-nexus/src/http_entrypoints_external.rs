/*!
 * Handler functions (entrypoints) for external HTTP APIs
 */

use super::ServerContext;

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseAccepted;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOk;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::TypedBody;
use omicron_common::http_pagination::data_page_params_for;
use omicron_common::http_pagination::data_page_params_nameid_id;
use omicron_common::http_pagination::data_page_params_nameid_name;
use omicron_common::http_pagination::pagination_field_for_scan_params;
use omicron_common::http_pagination::ApiPagField;
use omicron_common::http_pagination::ApiPaginatedById;
use omicron_common::http_pagination::ApiPaginatedByName;
use omicron_common::http_pagination::ApiPaginatedByNameOrId;
use omicron_common::http_pagination::ApiScanById;
use omicron_common::http_pagination::ApiScanByName;
use omicron_common::http_pagination::ApiScanByNameOrId;
use omicron_common::http_pagination::ScanParams;
use omicron_common::model::to_view_list;
use omicron_common::model::ApiDiskAttachment;
use omicron_common::model::ApiDiskCreateParams;
use omicron_common::model::ApiDiskView;
use omicron_common::model::ApiInstanceCreateParams;
use omicron_common::model::ApiInstanceView;
use omicron_common::model::ApiName;
use omicron_common::model::ApiObject;
use omicron_common::model::ApiProjectCreateParams;
use omicron_common::model::ApiProjectUpdateParams;
use omicron_common::model::ApiProjectView;
use omicron_common::model::ApiRackView;
use omicron_common::model::ApiSagaView;
use omicron_common::model::ApiSledView;
use omicron_common::model::DataPageParams;
use omicron_common::model::PaginationOrder;
use schemars::JsonSchema;
use serde::Deserialize;
use std::num::NonZeroU32;
use std::sync::Arc;
use uuid::Uuid;

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/**
 * Returns a description of the external nexus API
 */
pub fn external_api() -> NexusApiDescription {
    fn register_endpoints(api: &mut NexusApiDescription) -> Result<(), String> {
        api.register(api_projects_get)?;
        api.register(api_projects_post)?;
        api.register(api_projects_get_project)?;
        api.register(api_projects_delete_project)?;
        api.register(api_projects_put_project)?;

        api.register(api_project_disks_get)?;
        api.register(api_project_disks_post)?;
        api.register(api_project_disks_get_disk)?;
        api.register(api_project_disks_delete_disk)?;

        api.register(api_project_instances_get)?;
        api.register(api_project_instances_post)?;
        api.register(api_project_instances_get_instance)?;
        api.register(api_project_instances_delete_instance)?;
        api.register(api_project_instances_instance_reboot)?;
        api.register(api_project_instances_instance_start)?;
        api.register(api_project_instances_instance_stop)?;

        api.register(api_instance_disks_get)?;
        api.register(api_instance_disks_get_disk)?;
        api.register(api_instance_disks_put_disk)?;
        api.register(api_instance_disks_delete_disk)?;

        api.register(api_hardware_racks_get)?;
        api.register(api_hardware_racks_get_rack)?;
        api.register(api_hardware_sleds_get)?;
        api.register(api_hardware_sleds_get_sled)?;

        api.register(api_sagas_get)?;
        api.register(api_sagas_get_saga)?;

        Ok(())
    }

    let mut api = NexusApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<ApiPaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<ApiProjectView>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let params = ApiScanByNameOrId::from_query(&query)?;
    let field = pagination_field_for_scan_params(params);

    let project_stream = match field {
        ApiPagField::Id => {
            let page_selector = data_page_params_nameid_id(&rqctx, &query)?;
            nexus.projects_list_by_id(&page_selector).await?
        }

        ApiPagField::Name => {
            let page_selector = data_page_params_nameid_name(&rqctx, &query)?;
            nexus.projects_list_by_name(&page_selector).await?
        }
    };

    let view_list = to_view_list(project_stream).await;
    Ok(HttpResponseOk(ApiScanByNameOrId::results_page(&query, view_list)?))
}

/**
 * Create a new project.
 */
#[endpoint {
    method = POST,
    path = "/projects"
}]
async fn api_projects_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_project: TypedBody<ApiProjectCreateParams>,
) -> Result<HttpResponseCreated<ApiProjectView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let project = nexus.project_create(&new_project.into_inner()).await?;
    Ok(HttpResponseCreated(project.to_view()))
}

/**
 * Path parameters for Project requests
 */
#[derive(Deserialize, JsonSchema)]
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ApiProjectView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let project = nexus.project_fetch(&project_name).await?;
    Ok(HttpResponseOk(project.to_view()))
}

/**
 * Delete a specific project.
 */
#[endpoint {
     method = DELETE,
     path = "/projects/{project_name}",
 }]
async fn api_projects_delete_project(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let project_name = &params.project_name;
    nexus.project_delete(&project_name).await?;
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    updated_project: TypedBody<ApiProjectUpdateParams>,
) -> Result<HttpResponseOk<ApiProjectView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let newproject = nexus
        .project_update(&project_name, &updated_project.into_inner())
        .await?;
    Ok(HttpResponseOk(newproject.to_view()))
}

/*
 * Disks
 */

/**
 * List disks in a project.
 */
#[endpoint {
     method = GET,
     path = "/projects/{project_name}/disks",
 }]
async fn api_project_disks_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<ApiPaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<ApiDiskView>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let disk_stream = nexus
        .project_list_disks(
            project_name,
            &data_page_params_for(&rqctx, &query)?,
        )
        .await?;
    let view_list = to_view_list(disk_stream).await;
    Ok(HttpResponseOk(ApiScanByName::results_page(&query, view_list)?))
}

/**
 * Create a disk in a project.
 *
 * TODO-correctness See note about instance create.  This should be async.
 */
#[endpoint {
     method = POST,
     path = "/projects/{project_name}/disks",
 }]
async fn api_project_disks_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_disk: TypedBody<ApiDiskCreateParams>,
) -> Result<HttpResponseCreated<ApiDiskView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let new_disk_params = &new_disk.into_inner();
    let disk =
        nexus.project_create_disk(&project_name, &new_disk_params).await?;
    Ok(HttpResponseCreated(disk.to_view()))
}

/**
 * Path parameters for Disk requests
 */
#[derive(Deserialize, JsonSchema)]
struct DiskPathParam {
    project_name: ApiName,
    disk_name: ApiName,
}

/**
 * Fetch a single disk in a project.
 */
#[endpoint {
     method = GET,
     path = "/projects/{project_name}/disks/{disk_name}",
 }]
async fn api_project_disks_get_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseOk<ApiDiskView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let disk_name = &path.disk_name;
    let disk = nexus.project_lookup_disk(&project_name, &disk_name).await?;
    Ok(HttpResponseOk(disk.to_view()))
}

/**
 * Delete a disk from a project.
 */
#[endpoint {
     method = DELETE,
     path = "/projects/{project_name}/disks/{disk_name}",
 }]
async fn api_project_disks_delete_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let disk_name = &path.disk_name;
    nexus.project_delete_disk(&project_name, &disk_name).await?;
    Ok(HttpResponseDeleted())
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<ApiPaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<ApiInstanceView>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_stream = nexus
        .project_list_instances(
            &project_name,
            &data_page_params_for(&rqctx, &query)?,
        )
        .await?;
    let view_list = to_view_list(instance_stream).await;
    Ok(HttpResponseOk(ApiScanByName::results_page(&query, view_list)?))
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_instance: TypedBody<ApiInstanceCreateParams>,
) -> Result<HttpResponseCreated<ApiInstanceView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let new_instance_params = &new_instance.into_inner();
    let instance = nexus
        .project_create_instance(&project_name, &new_instance_params)
        .await?;
    Ok(HttpResponseCreated(instance.to_view()))
}

/**
 * Path parameters for Instance requests
 */
#[derive(Deserialize, JsonSchema)]
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<ApiInstanceView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let instance =
        nexus.project_lookup_instance(&project_name, &instance_name).await?;
    Ok(HttpResponseOk(instance.to_view()))
}

/**
 * Delete an instance from a project.
 */
#[endpoint {
     method = DELETE,
     path = "/projects/{project_name}/instances/{instance_name}",
 }]
async fn api_project_instances_delete_instance(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    nexus.project_destroy_instance(&project_name, &instance_name).await?;
    Ok(HttpResponseDeleted())
}

/**
 * Reboot an instance.
 */
#[endpoint {
    method = POST,
    path = "/projects/{project_name}/instances/{instance_name}/reboot",
}]
async fn api_project_instances_instance_reboot(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<ApiInstanceView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let instance = nexus.instance_reboot(&project_name, &instance_name).await?;
    Ok(HttpResponseAccepted(instance.to_view()))
}

/**
 * Boot an instance.
 */
#[endpoint {
    method = POST,
    path = "/projects/{project_name}/instances/{instance_name}/start",
}]
async fn api_project_instances_instance_start(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<ApiInstanceView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let instance = nexus.instance_start(&project_name, &instance_name).await?;
    Ok(HttpResponseAccepted(instance.to_view()))
}

/**
 * Halt an instance.
 */
#[endpoint {
    method = POST,
    path = "/projects/{project_name}/instances/{instance_name}/stop",
}]
/* Our naming convention kind of falls apart here. */
async fn api_project_instances_instance_stop(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<ApiInstanceView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let instance = nexus.instance_stop(&project_name, &instance_name).await?;
    Ok(HttpResponseAccepted(instance.to_view()))
}

/**
 * List disks attached to this instance.
 */
/* TODO-scalability needs to be paginated */
#[endpoint {
    method = GET,
    path = "/projects/{project_name}/instances/{instance_name}/disks"
}]
async fn api_instance_disks_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<Vec<ApiDiskAttachment>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let fake_query = DataPageParams {
        marker: None,
        direction: PaginationOrder::Ascending,
        limit: NonZeroU32::new(std::u32::MAX).unwrap(),
    };
    let disk_list = nexus
        .instance_list_disks(&project_name, &instance_name, &fake_query)
        .await?;
    let view_list = to_view_list(disk_list).await;
    Ok(HttpResponseOk(view_list))
}

/**
 * Path parameters for requests that access Disks attached to an Instance
 */
#[derive(Deserialize, JsonSchema)]
struct InstanceDiskPathParam {
    project_name: ApiName,
    instance_name: ApiName,
    disk_name: ApiName,
}

/**
 * Fetch a description of the attachment of this disk to this instance.
 */
#[endpoint {
    method = GET,
    path = "/projects/{project_name}/instances/{instance_name}/disks/{disk_name}"
}]
async fn api_instance_disks_get_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstanceDiskPathParam>,
) -> Result<HttpResponseOk<ApiDiskAttachment>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let disk_name = &path.disk_name;
    let attachment = nexus
        .instance_get_disk(&project_name, &instance_name, &disk_name)
        .await?;
    Ok(HttpResponseOk(attachment.to_view()))
}

/**
 * Attach a disk to this instance.
 */
#[endpoint {
    method = PUT,
    path = "/projects/{project_name}/instances/{instance_name}/disks/{disk_name}"
}]
async fn api_instance_disks_put_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstanceDiskPathParam>,
) -> Result<HttpResponseCreated<ApiDiskAttachment>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let disk_name = &path.disk_name;
    let attachment = nexus
        .instance_attach_disk(&project_name, &instance_name, &disk_name)
        .await?;
    Ok(HttpResponseCreated(attachment.to_view()))
}

/**
 * Detach a disk from this instance.
 */
#[endpoint {
    method = DELETE,
    path = "/projects/{project_name}/instances/{instance_name}/disks/{disk_name}"
}]
async fn api_instance_disks_delete_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstanceDiskPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let disk_name = &path.disk_name;
    nexus
        .instance_detach_disk(&project_name, &instance_name, &disk_name)
        .await?;
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<ApiPaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<ApiRackView>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let rack_stream =
        nexus.racks_list(&data_page_params_for(&rqctx, &query)?).await?;
    let view_list = to_view_list(rack_stream).await;
    Ok(HttpResponseOk(ApiScanById::results_page(&query, view_list)?))
}

/**
 * Path parameters for Rack requests
 */
#[derive(Deserialize, JsonSchema)]
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RackPathParam>,
) -> Result<HttpResponseOk<ApiRackView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let rack_info = nexus.rack_lookup(&path.rack_id).await?;
    Ok(HttpResponseOk(rack_info.to_view()))
}

/*
 * Sleds
 */

/**
 * List sleds in the system.
 */
#[endpoint {
     method = GET,
     path = "/hardware/sleds",
 }]
async fn api_hardware_sleds_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<ApiPaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<ApiSledView>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let sled_stream =
        nexus.sleds_list(&data_page_params_for(&rqctx, &query)?).await?;
    let view_list = to_view_list(sled_stream).await;
    Ok(HttpResponseOk(ApiScanById::results_page(&query, view_list)?))
}

/**
 * Path parameters for Sled requests
 */
#[derive(Deserialize, JsonSchema)]
struct SledPathParam {
    /** The sled's unique ID. */
    sled_id: Uuid,
}

/**
 * Fetch information about a sled in the system.
 */
#[endpoint {
     method = GET,
     path = "/hardware/sleds/{sled_id}",
 }]
async fn api_hardware_sleds_get_sled(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SledPathParam>,
) -> Result<HttpResponseOk<ApiSledView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let sled_info = nexus.sled_lookup(&path.sled_id).await?;
    Ok(HttpResponseOk(sled_info.to_view()))
}

/*
 * Sagas
 */

/**
 * List all sagas (for debugging)
 */
#[endpoint {
     method = GET,
     path = "/sagas",
 }]
async fn api_sagas_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<ApiPaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<ApiSagaView>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let saga_stream = nexus.sagas_list(&pagparams).await?;
    let view_list = to_view_list(saga_stream).await;
    Ok(HttpResponseOk(ApiScanById::results_page(&query, view_list)?))
}

/**
 * Path parameters for Saga requests
 */
#[derive(Deserialize, JsonSchema)]
struct SagaPathParam {
    saga_id: Uuid,
}

/**
 * Fetch information about a single saga (for debugging)
 */
#[endpoint {
     method = GET,
     path = "/sagas/{saga_id}",
 }]
async fn api_sagas_get_saga(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SagaPathParam>,
) -> Result<HttpResponseOk<ApiSagaView>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let saga = nexus.saga_get(path.saga_id).await?;
    Ok(HttpResponseOk(saga))
}
