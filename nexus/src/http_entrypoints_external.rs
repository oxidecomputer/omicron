/*!
 * Handler functions (entrypoints) for external HTTP APIs
 */

use super::ServerContext;
use crate::db;
use crate::db::model::Name;

use crate::context::OpContext;
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
use omicron_common::api::external::http_pagination::data_page_params_for;
use omicron_common::api::external::http_pagination::data_page_params_nameid_id;
use omicron_common::api::external::http_pagination::data_page_params_nameid_name;
use omicron_common::api::external::http_pagination::pagination_field_for_scan_params;
use omicron_common::api::external::http_pagination::PagField;
use omicron_common::api::external::http_pagination::PaginatedById;
use omicron_common::api::external::http_pagination::PaginatedByName;
use omicron_common::api::external::http_pagination::PaginatedByNameOrId;
use omicron_common::api::external::http_pagination::ScanById;
use omicron_common::api::external::http_pagination::ScanByName;
use omicron_common::api::external::http_pagination::ScanByNameOrId;
use omicron_common::api::external::http_pagination::ScanParams;
use omicron_common::api::external::to_list;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Disk;
use omicron_common::api::external::DiskAttachment;
use omicron_common::api::external::DiskCreateParams;
use omicron_common::api::external::Instance;
use omicron_common::api::external::InstanceCreateParams;
use omicron_common::api::external::Organization;
use omicron_common::api::external::OrganizationCreateParams;
use omicron_common::api::external::OrganizationUpdateParams;
use omicron_common::api::external::PaginationOrder;
use omicron_common::api::external::Project;
use omicron_common::api::external::ProjectCreateParams;
use omicron_common::api::external::ProjectUpdateParams;
use omicron_common::api::external::Rack;
use omicron_common::api::external::Saga;
use omicron_common::api::external::Sled;
use omicron_common::api::external::Vpc;
use omicron_common::api::external::VpcCreateParams;
use omicron_common::api::external::VpcRouter;
use omicron_common::api::external::VpcRouterCreateParams;
use omicron_common::api::external::VpcRouterUpdateParams;
use omicron_common::api::external::VpcSubnet;
use omicron_common::api::external::VpcSubnetCreateParams;
use omicron_common::api::external::VpcSubnetUpdateParams;
use omicron_common::api::external::VpcUpdateParams;
use ref_cast::RefCast;
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
        api.register(organizations_get)?;
        api.register(organizations_post)?;
        api.register(organizations_get_organization)?;
        api.register(organizations_delete_organization)?;
        api.register(organizations_put_organization)?;

        api.register(organization_projects_get)?;
        api.register(organization_projects_post)?;
        api.register(organization_projects_get_project)?;
        api.register(organization_projects_delete_project)?;
        api.register(organization_projects_put_project)?;

        api.register(project_disks_get)?;
        api.register(project_disks_post)?;
        api.register(project_disks_get_disk)?;
        api.register(project_disks_delete_disk)?;

        api.register(project_instances_get)?;
        api.register(project_instances_post)?;
        api.register(project_instances_get_instance)?;
        api.register(project_instances_delete_instance)?;
        api.register(project_instances_instance_reboot)?;
        api.register(project_instances_instance_start)?;
        api.register(project_instances_instance_stop)?;

        api.register(instance_disks_get)?;
        api.register(instance_disks_get_disk)?;
        api.register(instance_disks_put_disk)?;
        api.register(instance_disks_delete_disk)?;

        api.register(project_vpcs_get)?;
        api.register(project_vpcs_post)?;
        api.register(project_vpcs_get_vpc)?;
        api.register(project_vpcs_put_vpc)?;
        api.register(project_vpcs_delete_vpc)?;

        api.register(vpc_subnets_get)?;
        api.register(vpc_subnets_get_subnet)?;
        api.register(vpc_subnets_post)?;
        api.register(vpc_subnets_delete_subnet)?;
        api.register(vpc_subnets_put_subnet)?;

        api.register(vpc_routers_get)?;
        api.register(vpc_routers_get_router)?;
        api.register(vpc_routers_post)?;
        api.register(vpc_routers_delete_router)?;
        api.register(vpc_routers_put_router)?;

        api.register(hardware_racks_get)?;
        api.register(hardware_racks_get_rack)?;
        api.register(hardware_sleds_get)?;
        api.register(hardware_sleds_get_sled)?;

        api.register(sagas_get)?;
        api.register(sagas_get_saga)?;

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
 *   GET    /organizations            (list the organizations in the collection)
 *   POST   /organizations            (create a organization in the collection)
 *   GET    /organizations/{org_name} (look up a organization in the collection)
 *   DELETE /organizations/{org_name} (delete a organization in the collection)
 *   PUT    /organizations/{org_name} (update a organization in the collection)
 *
 * There's a naming convention for the functions that implement these API entry
 * points.  When operating on the collection itself, we use:
 *
 *    {collection_path}_{verb}
 *
 * For examples:
 *
 *    GET  /organizations                    -> organizations_get()
 *    POST /organizations                    -> organizations_post()
 *
 * For operations on items within the collection, we use:
 *
 *    {collection_path}_{verb}_{object}
 *
 * For examples:
 *
 *    DELETE /organizations/{org_name}   -> organizations_delete_organization()
 *    GET    /organizations/{org_name}   -> organizations_get_organization()
 *    PUT    /organizations/{org_name}   -> organizations_put_organization()
 *
 * Note that these function names end up in generated OpenAPI spec as the
 * operationId for each endpoint, and therefore represent a contract with
 * clients. Client generators use operationId to name API methods, so changing
 * a function name is a breaking change from a client perspective.
 */

/**
 * List all organizations.
 */
#[endpoint {
     method = GET,
     path = "/organizations",
 }]
async fn organizations_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Organization>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let query = query_params.into_inner();
        let params = ScanByNameOrId::from_query(&query)?;
        let field = pagination_field_for_scan_params(params);

        let organizations = match field {
            PagField::Id => {
                let page_selector = data_page_params_nameid_id(&rqctx, &query)?;
                nexus.organizations_list_by_id(&page_selector).await?
            }

            PagField::Name => {
                let page_selector =
                    data_page_params_nameid_name(&rqctx, &query)?
                        .map_name(|n| Name::ref_cast(n));
                nexus.organizations_list_by_name(&page_selector).await?
            }
        }
        .into_iter()
        .map(|p| p.into())
        .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(&query, organizations)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Create a new organization.
 */
#[endpoint {
    method = POST,
    path = "/organizations"
}]
async fn organizations_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_organization: TypedBody<OrganizationCreateParams>,
) -> Result<HttpResponseCreated<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization = nexus
            .organization_create(&opctx, &new_organization.into_inner())
            .await?;
        Ok(HttpResponseCreated(organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for Organization requests
 */
#[derive(Deserialize, JsonSchema)]
struct OrganizationPathParam {
    /// The organization's unique name.
    organization_name: Name,
}

/**
 * Fetch a specific organization
 */
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}",
}]
async fn organizations_get_organization(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let handler = async {
        let organization = nexus.organization_fetch(&organization_name).await?;
        Ok(HttpResponseOk(organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Delete a specific organization.
 */
#[endpoint {
     method = DELETE,
     path = "/organizations/{organization_name}",
 }]
async fn organizations_delete_organization(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let organization_name = &params.organization_name;
    let handler = async {
        nexus.organization_delete(&organization_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Update a specific organization.
 *
 * TODO-correctness: Is it valid for PUT to accept application/json that's a
 * subset of what the resource actually represents?  If not, is that a problem?
 * (HTTP may require that this be idempotent.)  If so, can we get around that
 * having this be a slightly different content-type (e.g.,
 * "application/json-patch")?  We should see what other APIs do.
 */
#[endpoint {
     method = PUT,
     path = "/organizations/{organization_name}",
 }]
async fn organizations_put_organization(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
    updated_organization: TypedBody<OrganizationUpdateParams>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let handler = async {
        let new_organization = nexus
            .organization_update(
                &organization_name,
                &updated_organization.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(new_organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * List all projects.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects",
 }]
async fn organization_projects_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByNameOrId>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Project>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;

    let handler = async {
        let params = ScanByNameOrId::from_query(&query)?;
        let field = pagination_field_for_scan_params(params);
        let projects = match field {
            PagField::Id => {
                let page_selector = data_page_params_nameid_id(&rqctx, &query)?;
                nexus
                    .projects_list_by_id(&organization_name, &page_selector)
                    .await?
            }

            PagField::Name => {
                let page_selector =
                    data_page_params_nameid_name(&rqctx, &query)?
                        .map_name(|n| Name::ref_cast(n));
                nexus
                    .projects_list_by_name(&organization_name, &page_selector)
                    .await?
            }
        }
        .into_iter()
        .map(|p| p.into())
        .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(&query, projects)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Create a new project.
 */
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects"
}]
async fn organization_projects_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
    new_project: TypedBody<ProjectCreateParams>,
) -> Result<HttpResponseCreated<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let organization_name = &params.organization_name;
    let handler = async {
        let project = nexus
            .project_create(&organization_name, &new_project.into_inner())
            .await?;
        Ok(HttpResponseCreated(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for Project requests
 */
#[derive(Deserialize, JsonSchema)]
struct ProjectPathParam {
    /// The organization's unique name.
    organization_name: Name,
    /// The project's unique name within the organization.
    project_name: Name,
}

/**
 * Fetch a specific project
 */
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}",
}]
async fn organization_projects_get_project(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let project =
            nexus.project_fetch(&organization_name, &project_name).await?;
        Ok(HttpResponseOk(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Delete a specific project.
 */
#[endpoint {
     method = DELETE,
     path = "/organizations/{organization_name}/projects/{project_name}",
 }]
async fn organization_projects_delete_project(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let organization_name = &params.organization_name;
    let project_name = &params.project_name;
    let handler = async {
        nexus.project_delete(&organization_name, &project_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
     path = "/organizations/{organization_name}/projects/{project_name}",
 }]
async fn organization_projects_put_project(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    updated_project: TypedBody<ProjectUpdateParams>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let newproject = nexus
            .project_update(
                &organization_name,
                &project_name,
                &updated_project.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(newproject.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/*
 * Disks
 */

/**
 * List disks in a project.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/disks",
 }]
async fn project_disks_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let disks = nexus
            .project_list_disks(
                organization_name,
                project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(&query, disks)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Create a disk in a project.
 *
 * TODO-correctness See note about instance create.  This should be async.
 */
#[endpoint {
     method = POST,
     path = "/organizations/{organization_name}/projects/{project_name}/disks",
 }]
async fn project_disks_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_disk: TypedBody<DiskCreateParams>,
) -> Result<HttpResponseCreated<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_disk_params = &new_disk.into_inner();
    let handler = async {
        let disk = nexus
            .project_create_disk(
                &organization_name,
                &project_name,
                &new_disk_params,
            )
            .await?;
        Ok(HttpResponseCreated(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for Disk requests
 */
#[derive(Deserialize, JsonSchema)]
struct DiskPathParam {
    organization_name: Name,
    project_name: Name,
    disk_name: Name,
}

/**
 * Fetch a single disk in a project.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
 }]
async fn project_disks_get_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseOk<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let disk_name = &path.disk_name;
    let handler = async {
        let disk = nexus
            .project_lookup_disk(&organization_name, &project_name, &disk_name)
            .await?;
        Ok(HttpResponseOk(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Delete a disk from a project.
 */
#[endpoint {
     method = DELETE,
     path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
 }]
async fn project_disks_delete_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let disk_name = &path.disk_name;
    let handler = async {
        nexus
            .project_delete_disk(&organization_name, &project_name, &disk_name)
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/*
 * Instances
 */

/**
 * List instances in a project.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/instances",
 }]
async fn project_instances_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Instance>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let instances = nexus
            .project_list_instances(
                &organization_name,
                &project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(&query, instances)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
     path = "/organizations/{organization_name}/projects/{project_name}/instances",
 }]
async fn project_instances_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_instance: TypedBody<InstanceCreateParams>,
) -> Result<HttpResponseCreated<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_instance_params = &new_instance.into_inner();
    let handler = async {
        let instance = nexus
            .project_create_instance(
                &organization_name,
                &project_name,
                &new_instance_params,
            )
            .await?;
        Ok(HttpResponseCreated(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for Instance requests
 */
#[derive(Deserialize, JsonSchema)]
struct InstancePathParam {
    organization_name: Name,
    project_name: Name,
    instance_name: Name,
}

/**
 * Get an instance in a project.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
 }]
async fn project_instances_get_instance(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let instance = nexus
            .project_lookup_instance(
                &organization_name,
                &project_name,
                &instance_name,
            )
            .await?;
        Ok(HttpResponseOk(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Delete an instance from a project.
 */
#[endpoint {
     method = DELETE,
     path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
 }]
async fn project_instances_delete_instance(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        nexus
            .project_destroy_instance(
                &organization_name,
                &project_name,
                &instance_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Reboot an instance.
 */
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/reboot",
}]
async fn project_instances_instance_reboot(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let instance = nexus
            .instance_reboot(&organization_name, &project_name, &instance_name)
            .await;
        let instance = instance?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Boot an instance.
 */
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/start",
}]
async fn project_instances_instance_start(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let instance = nexus
            .instance_start(&organization_name, &project_name, &instance_name)
            .await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Halt an instance.
 */
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/stop",
}]
/* Our naming convention kind of falls apart here. */
async fn project_instances_instance_stop(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let instance = nexus
            .instance_stop(&organization_name, &project_name, &instance_name)
            .await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * List disks attached to this instance.
 */
/* TODO-scalability needs to be paginated */
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks"
}]
async fn instance_disks_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<Vec<DiskAttachment>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let fake_query = DataPageParams {
        marker: None,
        direction: PaginationOrder::Ascending,
        limit: NonZeroU32::new(std::u32::MAX).unwrap(),
    };
    let handler = async {
        let disks = nexus
            .instance_list_disks(
                &organization_name,
                &project_name,
                &instance_name,
                &fake_query,
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(disks))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for requests that access Disks attached to an Instance
 */
#[derive(Deserialize, JsonSchema)]
struct InstanceDiskPathParam {
    organization_name: Name,
    project_name: Name,
    instance_name: Name,
    disk_name: Name,
}

/**
 * Fetch a description of the attachment of this disk to this instance.
 */
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/{disk_name}"
}]
async fn instance_disks_get_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstanceDiskPathParam>,
) -> Result<HttpResponseOk<DiskAttachment>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let disk_name = &path.disk_name;
    let handler = async {
        let attachment = nexus
            .instance_get_disk(
                &organization_name,
                &project_name,
                &instance_name,
                &disk_name,
            )
            .await?;
        Ok(HttpResponseOk(attachment))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Attach a disk to this instance.
 */
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/{disk_name}"
}]
async fn instance_disks_put_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstanceDiskPathParam>,
) -> Result<HttpResponseCreated<DiskAttachment>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let disk_name = &path.disk_name;
    let handler = async {
        let attachment = nexus
            .instance_attach_disk(
                &organization_name,
                &project_name,
                &instance_name,
                &disk_name,
            )
            .await?;
        Ok(HttpResponseCreated(attachment))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Detach a disk from this instance.
 */
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/{disk_name}"
}]
async fn instance_disks_delete_disk(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstanceDiskPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let disk_name = &path.disk_name;
    let handler = async {
        nexus
            .instance_detach_disk(
                &organization_name,
                &project_name,
                &instance_name,
                &disk_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/*
 * VPCs
 */

/**
 * List VPCs in a project.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs",
 }]
async fn project_vpcs_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Vpc>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let vpcs = nexus
            .project_list_vpcs(
                &organization_name,
                &project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?;
        Ok(HttpResponseOk(ScanByName::results_page(&query, vpcs)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for VPC requests
 */
#[derive(Deserialize, JsonSchema)]
struct VpcPathParam {
    organization_name: Name,
    project_name: Name,
    vpc_name: Name,
}

/**
 * Get a VPC in a project.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
 }]
async fn project_vpcs_get_vpc(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
) -> Result<HttpResponseOk<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let vpc_name = &path.vpc_name;
    let handler = async {
        let vpc = nexus
            .project_lookup_vpc(&organization_name, &project_name, &vpc_name)
            .await?;
        Ok(HttpResponseOk(vpc))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Create a VPC in a project.
 */
#[endpoint {
     method = POST,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs",
 }]
async fn project_vpcs_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_vpc: TypedBody<VpcCreateParams>,
) -> Result<HttpResponseCreated<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_vpc_params = &new_vpc.into_inner();
    let handler = async {
        let vpc = nexus
            .project_create_vpc(
                &organization_name,
                &project_name,
                &new_vpc_params,
            )
            .await?;
        Ok(HttpResponseCreated(vpc))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Update a VPC.
 */
#[endpoint {
     method = PUT,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
 }]
async fn project_vpcs_put_vpc(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
    updated_vpc: TypedBody<VpcUpdateParams>,
) -> Result<HttpResponseOk<()>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        nexus
            .project_update_vpc(
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &updated_vpc.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Delete a vpc from a project.
 */
#[endpoint {
     method = DELETE,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
 }]
async fn project_vpcs_delete_vpc(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let vpc_name = &path.vpc_name;
    let handler = async {
        nexus
            .project_delete_vpc(&organization_name, &project_name, &vpc_name)
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * List subnets in a VPC.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets",
 }]
async fn vpc_subnets_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<VpcPathParam>,
) -> Result<HttpResponseOk<ResultsPage<VpcSubnet>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let handler = async {
        let vpcs = nexus
            .vpc_list_subnets(
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?;
        Ok(HttpResponseOk(ScanByName::results_page(&query, vpcs)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for VPC Subnet requests
 */
#[derive(Deserialize, JsonSchema)]
struct VpcSubnetPathParam {
    organization_name: Name,
    project_name: Name,
    vpc_name: Name,
    subnet_name: Name,
}

/**
 * Get subnet in a VPC.
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
 }]
async fn vpc_subnets_get_subnet(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcSubnetPathParam>,
) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let subnet = nexus
            .vpc_lookup_subnet(
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.subnet_name,
            )
            .await?;
        Ok(HttpResponseOk(subnet))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Create a subnet in a VPC.
 */
#[endpoint {
     method = POST,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets",
 }]
async fn vpc_subnets_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
    create_params: TypedBody<VpcSubnetCreateParams>,
) -> Result<HttpResponseCreated<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let subnet = nexus
            .vpc_create_subnet(
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &create_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(subnet))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Delete a subnet from a VPC.
 */
#[endpoint {
     method = DELETE,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
 }]
async fn vpc_subnets_delete_subnet(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcSubnetPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        nexus
            .vpc_delete_subnet(
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.subnet_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Update a VPC Subnet.
 */
#[endpoint {
     method = PUT,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
 }]
async fn vpc_subnets_put_subnet(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcSubnetPathParam>,
    subnet_params: TypedBody<VpcSubnetUpdateParams>,
) -> Result<HttpResponseOk<()>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        nexus
            .vpc_update_subnet(
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.subnet_name,
                &subnet_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/*
 * VPC Routers
 */

/**
 * List VPC Custom and System Routers
 */
#[endpoint {
     method = GET,
     path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers",
 }]
async fn vpc_routers_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<VpcPathParam>,
) -> Result<HttpResponseOk<ResultsPage<VpcRouter>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let routers = nexus
        .vpc_list_routers(
            &path.organization_name,
            &path.project_name,
            &path.vpc_name,
            &data_page_params_for(&rqctx, &query)?
                .map_name(|n| Name::ref_cast(n)),
        )
        .await?;
    Ok(HttpResponseOk(ScanByName::results_page(&query, routers)?))
}

/**
 * Path parameters for VPC Router requests
 */
#[derive(Deserialize, JsonSchema)]
struct VpcRouterPathParam {
    organization_name: Name,
    project_name: Name,
    vpc_name: Name,
    router_name: Name,
}

/**
 * Get a VPC Router
 */
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}"
}]
async fn vpc_routers_get_router(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcRouterPathParam>,
) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let vpc_router = nexus
        .vpc_lookup_router(
            &path.organization_name,
            &path.project_name,
            &path.vpc_name,
            &path.router_name,
        )
        .await?;
    Ok(HttpResponseOk(vpc_router))
}

/**
 * Create a VPC Router
 */
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers",
}]
async fn vpc_routers_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
    create_params: TypedBody<VpcRouterCreateParams>,
) -> Result<HttpResponseCreated<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let router = nexus
        .vpc_create_router(
            &path.organization_name,
            &path.project_name,
            &path.vpc_name,
            &create_params.into_inner(),
        )
        .await?;
    Ok(HttpResponseCreated(router))
}

/**
 * Delete a router from its VPC
 */
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
}]
async fn vpc_routers_delete_router(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcRouterPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    nexus
        .vpc_delete_router(
            &path.organization_name,
            &path.project_name,
            &path.vpc_name,
            &path.router_name,
        )
        .await?;
    Ok(HttpResponseDeleted())
}

/**
 * Update a VPC Router
 */
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
}]
async fn vpc_routers_put_router(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcRouterPathParam>,
    router_params: TypedBody<VpcRouterUpdateParams>,
) -> Result<HttpResponseOk<()>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    nexus
        .vpc_update_router(
            &path.organization_name,
            &path.project_name,
            &path.vpc_name,
            &path.router_name,
            &router_params.into_inner(),
        )
        .await?;
    Ok(HttpResponseOk(()))
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
async fn hardware_racks_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Rack>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let rack_stream =
            nexus.racks_list(&data_page_params_for(&rqctx, &query)?).await?;
        let view_list = to_list::<db::model::Rack, Rack>(rack_stream).await;
        Ok(HttpResponseOk(ScanById::results_page(&query, view_list)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
async fn hardware_racks_get_rack(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RackPathParam>,
) -> Result<HttpResponseOk<Rack>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let rack_info = nexus.rack_lookup(&path.rack_id).await?;
        Ok(HttpResponseOk(rack_info.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
async fn hardware_sleds_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Sled>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let sleds = nexus
            .sleds_list(&data_page_params_for(&rqctx, &query)?)
            .await?
            .into_iter()
            .map(|s| s.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(&query, sleds)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
async fn hardware_sleds_get_sled(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SledPathParam>,
) -> Result<HttpResponseOk<Sled>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let sled_info = nexus.sled_lookup(&path.sled_id).await?;
        Ok(HttpResponseOk(sled_info.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
async fn sagas_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Saga>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let handler = async {
        let saga_stream = nexus.sagas_list(&pagparams).await?;
        let view_list = to_list(saga_stream).await;
        Ok(HttpResponseOk(ScanById::results_page(&query, view_list)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
async fn sagas_get_saga(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SagaPathParam>,
) -> Result<HttpResponseOk<Saga>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let saga = nexus.saga_get(path.saga_id).await?;
        Ok(HttpResponseOk(saga))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}
