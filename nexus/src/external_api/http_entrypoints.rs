// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for external HTTP APIs

use super::views::IpPool;
use super::views::IpPoolRange;
use super::{
    console_api, params, views,
    views::{
        GlobalImage, IdentityProvider, Image, Organization, Project, Rack,
        Role, Silo, Sled, Snapshot, SshKey, User, UserBuiltin, Vpc, VpcRouter,
        VpcSubnet,
    },
};
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::model::Name;
use crate::external_api::shared;
use crate::ServerContext;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::EmptyScanParams;
use dropshot::HttpError;
use dropshot::HttpResponseAccepted;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::PaginationOrder;
use dropshot::PaginationParams;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::TypedBody;
use dropshot::WhichPage;
use ipnetwork::IpNetwork;
use omicron_common::api::external::http_pagination::data_page_params_nameid_id;
use omicron_common::api::external::http_pagination::data_page_params_nameid_name;
use omicron_common::api::external::http_pagination::marker_for_name;
use omicron_common::api::external::http_pagination::marker_for_name_or_id;
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
use omicron_common::api::external::Error;
use omicron_common::api::external::Instance;
use omicron_common::api::external::NetworkInterface;
use omicron_common::api::external::RouterRoute;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::RouterRouteUpdateParams;
use omicron_common::api::external::Saga;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_common::api::external::VpcFirewallRules;
use omicron_common::{
    api::external::http_pagination::data_page_params_for, bail_unless,
};
use ref_cast::RefCast;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/// Returns a description of the external nexus API
pub fn external_api() -> NexusApiDescription {
    fn register_endpoints(api: &mut NexusApiDescription) -> Result<(), String> {
        api.register(policy_get)?;
        api.register(policy_put)?;

        api.register(silos_get)?;
        api.register(silos_post)?;
        api.register(silos_get_silo)?;
        api.register(silos_delete_silo)?;
        api.register(silos_get_identity_providers)?;
        api.register(silos_get_silo_policy)?;
        api.register(silos_put_silo_policy)?;

        api.register(silo_saml_idp_create)?;
        api.register(silo_saml_idp_fetch)?;

        api.register(organizations_get)?;
        api.register(organizations_post)?;
        api.register(organizations_get_organization)?;
        api.register(organizations_delete_organization)?;
        api.register(organizations_put_organization)?;
        api.register(organization_get_policy)?;
        api.register(organization_put_policy)?;

        api.register(organization_projects_get)?;
        api.register(organization_projects_post)?;
        api.register(organization_projects_get_project)?;
        api.register(organization_projects_delete_project)?;
        api.register(organization_projects_put_project)?;
        api.register(organization_projects_get_project_policy)?;
        api.register(organization_projects_put_project_policy)?;

        api.register(ip_pools_get)?;
        api.register(ip_pools_post)?;
        api.register(ip_pools_get_ip_pool)?;
        api.register(ip_pools_delete_ip_pool)?;
        api.register(ip_pools_put_ip_pool)?;

        api.register(ip_pool_ranges_get)?;
        api.register(ip_pool_ranges_add)?;
        api.register(ip_pool_ranges_delete)?;

        api.register(project_disks_get)?;
        api.register(project_disks_post)?;
        api.register(project_disks_get_disk)?;
        api.register(project_disks_delete_disk)?;

        api.register(project_instances_get)?;
        api.register(project_instances_post)?;
        api.register(project_instances_get_instance)?;
        api.register(project_instances_delete_instance)?;
        api.register(project_instances_migrate_instance)?;
        api.register(project_instances_instance_reboot)?;
        api.register(project_instances_instance_start)?;
        api.register(project_instances_instance_stop)?;
        api.register(project_instances_instance_serial_get)?;

        // Globally-scoped Images API
        api.register(images_get)?;
        api.register(images_post)?;
        api.register(images_get_image)?;
        api.register(images_delete_image)?;

        // Project-scoped images API
        api.register(project_images_get)?;
        api.register(project_images_post)?;
        api.register(project_images_get_image)?;
        api.register(project_images_delete_image)?;

        api.register(instance_disks_get)?;
        api.register(instance_disks_attach)?;
        api.register(instance_disks_detach)?;

        api.register(project_snapshots_get)?;
        api.register(project_snapshots_post)?;
        api.register(project_snapshots_get_snapshot)?;
        api.register(project_snapshots_delete_snapshot)?;

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

        api.register(subnet_network_interfaces_get)?;

        api.register(instance_network_interfaces_post)?;
        api.register(instance_network_interfaces_get)?;
        api.register(instance_network_interfaces_get_interface)?;
        api.register(instance_network_interfaces_put_interface)?;
        api.register(instance_network_interfaces_delete_interface)?;

        api.register(vpc_routers_get)?;
        api.register(vpc_routers_get_router)?;
        api.register(vpc_routers_post)?;
        api.register(vpc_routers_delete_router)?;
        api.register(vpc_routers_put_router)?;

        api.register(vpc_firewall_rules_get)?;
        api.register(vpc_firewall_rules_put)?;

        api.register(routers_routes_get)?;
        api.register(routers_routes_get_route)?;
        api.register(routers_routes_post)?;
        api.register(routers_routes_delete_route)?;
        api.register(routers_routes_put_route)?;

        api.register(hardware_racks_get)?;
        api.register(hardware_racks_get_rack)?;
        api.register(hardware_sleds_get)?;
        api.register(hardware_sleds_get_sled)?;

        api.register(updates_refresh)?;

        api.register(sagas_get)?;
        api.register(sagas_get_saga)?;

        api.register(silo_users_get)?;

        api.register(builtin_users_get)?;
        api.register(builtin_users_get_user)?;

        api.register(timeseries_schema_get)?;

        api.register(roles_get)?;
        api.register(roles_get_role)?;

        api.register(sshkeys_get)?;
        api.register(sshkeys_get_key)?;
        api.register(sshkeys_post)?;
        api.register(sshkeys_delete_key)?;

        api.register(console_api::spoof_login)?;
        api.register(console_api::spoof_login_form)?;
        api.register(console_api::login_redirect)?;
        api.register(console_api::session_me)?;
        api.register(console_api::logout)?;
        api.register(console_api::console_page)?;
        api.register(console_api::asset)?;

        api.register(console_api::login)?;
        api.register(console_api::consume_credentials)?;

        Ok(())
    }

    let conf = serde_json::from_str(include_str!("./tag-config.json")).unwrap();
    let mut api = NexusApiDescription::new().tag_config(conf);

    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

// API ENDPOINT FUNCTION NAMING CONVENTIONS
//
// Generally, HTTP resources are grouped within some collection.  For a
// relatively simple example:
//
//   GET    /organizations            (list the organizations in the collection)
//   POST   /organizations            (create a organization in the collection)
//   GET    /organizations/{org_name} (look up a organization in the collection)
//   DELETE /organizations/{org_name} (delete a organization in the collection)
//   PUT    /organizations/{org_name} (update a organization in the collection)
//
// There's a naming convention for the functions that implement these API entry
// points.  When operating on the collection itself, we use:
//
//    {collection_path}_{verb}
//
// For examples:
//
//    GET  /organizations                    -> organizations_get()
//    POST /organizations                    -> organizations_post()
//
// For operations on items within the collection, we use:
//
//    {collection_path}_{verb}_{object}
//
// For examples:
//
//    DELETE /organizations/{org_name}   -> organizations_delete_organization()
//    GET    /organizations/{org_name}   -> organizations_get_organization()
//    PUT    /organizations/{org_name}   -> organizations_put_organization()
//
// Note that these function names end up in generated OpenAPI spec as the
// operationId for each endpoint, and therefore represent a contract with
// clients. Client generators use operationId to name API methods, so changing
// a function name is a breaking change from a client perspective.

/// Fetch the top-level IAM policy
#[endpoint {
    method = GET,
    path = "/policy",
    tags = ["policy"],
}]
async fn policy_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<HttpResponseOk<shared::Policy<authz::FleetRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy = nexus.fleet_fetch_policy(&opctx).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update the top-level IAM policy
#[endpoint {
    method = PUT,
    path = "/policy",
    tags = ["policy"],
}]
async fn policy_put(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_policy: TypedBody<shared::Policy<authz::FleetRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::FleetRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let new_policy = new_policy.into_inner();

    let handler = async {
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy = nexus.fleet_update_policy(&opctx, &new_policy).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// List all silos (that are discoverable).
#[endpoint {
    method = GET,
    path = "/silos",
    tags = ["silos"],
}]
async fn silos_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Silo>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let query = query_params.into_inner();
        let params = ScanByNameOrId::from_query(&query)?;
        let field = pagination_field_for_scan_params(params);

        let silos = match field {
            PagField::Id => {
                let page_selector = data_page_params_nameid_id(&rqctx, &query)?;
                nexus.silos_list_by_id(&opctx, &page_selector).await?
            }

            PagField::Name => {
                let page_selector =
                    data_page_params_nameid_name(&rqctx, &query)?
                        .map_name(|n| Name::ref_cast(n));
                nexus.silos_list_by_name(&opctx, &page_selector).await?
            }
        }
        .into_iter()
        .map(|p| p.into())
        .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            silos,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a new silo.
#[endpoint {
    method = POST,
    path = "/silos",
    tags = ["silos"],
}]
async fn silos_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_silo_params: TypedBody<params::SiloCreate>,
) -> Result<HttpResponseCreated<Silo>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let silo =
            nexus.silo_create(&opctx, new_silo_params.into_inner()).await?;
        Ok(HttpResponseCreated(silo.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Silo requests
#[derive(Deserialize, JsonSchema)]
struct SiloPathParam {
    /// The silo's unique name.
    silo_name: Name,
}

/// Fetch a specific silo
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}",
    tags = ["silos"],
}]
async fn silos_get_silo(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SiloPathParam>,
) -> Result<HttpResponseOk<Silo>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let silo_name = &path.silo_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let silo = nexus.silo_fetch(&opctx, &silo_name).await?;
        Ok(HttpResponseOk(silo.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a specific silo.
#[endpoint {
    method = DELETE,
    path = "/silos/{silo_name}",
    tags = ["silos"],
}]
async fn silos_delete_silo(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SiloPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let silo_name = &params.silo_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.silo_delete(&opctx, &silo_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the IAM policy for this Silo
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}/policy",
    tags = ["silos"],
}]
async fn silos_get_silo_policy(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SiloPathParam>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let silo_name = &path.silo_name;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy = nexus.silo_fetch_policy(&opctx, silo_name).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update the IAM policy for this Silo
#[endpoint {
    method = PUT,
    path = "/silos/{silo_name}/policy",
    tags = ["silos"],
}]
async fn silos_put_silo_policy(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SiloPathParam>,
    new_policy: TypedBody<shared::Policy<authz::SiloRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_policy = new_policy.into_inner();
    let silo_name = &path.silo_name;

    let handler = async {
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy =
            nexus.silo_update_policy(&opctx, silo_name, &new_policy).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo identity providers

/// List Silo identity providers
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}/identity_providers",
    tags = ["silos"],
}]
async fn silos_get_identity_providers(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SiloPathParam>,
    query_params: Query<PaginatedByName>,
) -> Result<HttpResponseOk<ResultsPage<IdentityProvider>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let silo_name = &path.silo_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let query = query_params.into_inner();
        let pagination_params = data_page_params_for(&rqctx, &query)?
            .map_name(|n| Name::ref_cast(n));
        let identity_providers = nexus
            .identity_provider_list(&opctx, &silo_name, &pagination_params)
            .await?
            .into_iter()
            .map(|x| x.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            identity_providers,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo SAML identity providers

/// Create a new SAML identity provider for a silo.
#[endpoint {
    method = POST,
    path = "/silos/{silo_name}/saml_identity_providers",
    tags = ["silos"],
}]
async fn silo_saml_idp_create(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SiloPathParam>,
    new_provider: TypedBody<params::SamlIdentityProviderCreate>,
) -> Result<HttpResponseCreated<views::SamlIdentityProvider>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let provider = nexus
            .saml_identity_provider_create(
                &opctx,
                &path_params.into_inner().silo_name,
                new_provider.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(provider.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Silo SAML identity provider requests
#[derive(Deserialize, JsonSchema)]
struct SiloSamlPathParam {
    /// The silo's unique name.
    silo_name: Name,
    /// The SAML identity provider's name
    provider_name: Name,
}

/// GET a silo's SAML identity provider
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}/saml_identity_providers/{provider_name}",
    tags = ["silos"],
}]
async fn silo_saml_idp_fetch(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SiloSamlPathParam>,
) -> Result<HttpResponseOk<views::SamlIdentityProvider>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;

    let path_params = path_params.into_inner();

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let provider = nexus
            .saml_identity_provider_fetch(
                &opctx,
                &path_params.silo_name,
                &path_params.provider_name,
            )
            .await?;

        Ok(HttpResponseOk(provider.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List all organizations.
#[endpoint {
    method = GET,
    path = "/organizations",
    tags = ["organizations"],
}]
async fn organizations_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Organization>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let query = query_params.into_inner();
        let params = ScanByNameOrId::from_query(&query)?;
        let field = pagination_field_for_scan_params(params);

        let organizations = match field {
            PagField::Id => {
                let page_selector = data_page_params_nameid_id(&rqctx, &query)?;
                nexus.organizations_list_by_id(&opctx, &page_selector).await?
            }

            PagField::Name => {
                let page_selector =
                    data_page_params_nameid_name(&rqctx, &query)?
                        .map_name(|n| Name::ref_cast(n));
                nexus.organizations_list_by_name(&opctx, &page_selector).await?
            }
        }
        .into_iter()
        .map(|p| p.into())
        .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            organizations,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a new organization.
#[endpoint {
    method = POST,
    path = "/organizations",
    tags = ["organizations"],
}]
async fn organizations_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_organization: TypedBody<params::OrganizationCreate>,
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

/// Path parameters for Organization requests
#[derive(Deserialize, JsonSchema)]
struct OrganizationPathParam {
    /// The organization's unique name.
    organization_name: Name,
}

/// Fetch a specific organization
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization =
            nexus.organization_fetch(&opctx, &organization_name).await?;
        Ok(HttpResponseOk(organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a specific organization.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.organization_delete(&opctx, &organization_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a specific organization.
// TODO-correctness: Is it valid for PUT to accept application/json that's a
// subset of what the resource actually represents?  If not, is that a problem?
// (HTTP may require that this be idempotent.)  If so, can we get around that
// having this be a slightly different content-type (e.g.,
// "application/json-patch")?  We should see what other APIs do.
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
}]
async fn organizations_put_organization(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
    updated_organization: TypedBody<params::OrganizationUpdate>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let new_organization = nexus
            .organization_update(
                &opctx,
                &organization_name,
                &updated_organization.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(new_organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the IAM policy for this Organization
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/policy",
    tags = ["organizations"],
}]
async fn organization_get_policy(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseOk<shared::Policy<authz::OrganizationRole>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy =
            nexus.organization_fetch_policy(&opctx, organization_name).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update the IAM policy for this Organization
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/policy",
    tags = ["organizations"],
}]
async fn organization_put_policy(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
    new_policy: TypedBody<shared::Policy<authz::OrganizationRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::OrganizationRole>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_policy = new_policy.into_inner();
    let organization_name = &path.organization_name;

    let handler = async {
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy = nexus
            .organization_update_policy(&opctx, organization_name, &new_policy)
            .await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List all projects.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects",
    tags = ["projects"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let params = ScanByNameOrId::from_query(&query)?;
        let field = pagination_field_for_scan_params(params);
        let projects = match field {
            PagField::Id => {
                let page_selector = data_page_params_nameid_id(&rqctx, &query)?;
                nexus
                    .projects_list_by_id(
                        &opctx,
                        &organization_name,
                        &page_selector,
                    )
                    .await?
            }

            PagField::Name => {
                let page_selector =
                    data_page_params_nameid_name(&rqctx, &query)?
                        .map_name(|n| Name::ref_cast(n));
                nexus
                    .projects_list_by_name(
                        &opctx,
                        &organization_name,
                        &page_selector,
                    )
                    .await?
            }
        }
        .into_iter()
        .map(|p| p.into())
        .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            projects,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a new project.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects",
    tags = ["projects"],
}]
async fn organization_projects_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<OrganizationPathParam>,
    new_project: TypedBody<params::ProjectCreate>,
) -> Result<HttpResponseCreated<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let organization_name = &params.organization_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project = nexus
            .project_create(
                &opctx,
                &organization_name,
                &new_project.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Project requests
#[derive(Deserialize, JsonSchema)]
struct ProjectPathParam {
    /// The organization's unique name.
    organization_name: Name,
    /// The project's unique name within the organization.
    project_name: Name,
}

/// Fetch a specific project
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project = nexus
            .project_fetch(&opctx, &organization_name, &project_name)
            .await?;
        Ok(HttpResponseOk(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a specific project.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.project_delete(&opctx, &organization_name, &project_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a specific project.
// TODO-correctness: Is it valid for PUT to accept application/json that's a
// subset of what the resource actually represents?  If not, is that a problem?
// (HTTP may require that this be idempotent.)  If so, can we get around that
// having this be a slightly different content-type (e.g.,
// "application/json-patch")?  We should see what other APIs do.
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
}]
async fn organization_projects_put_project(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    updated_project: TypedBody<params::ProjectUpdate>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let newproject = nexus
            .project_update(
                &opctx,
                &organization_name,
                &project_name,
                &updated_project.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(newproject.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the IAM policy for this Project
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/policy",
    tags = ["projects"],
}]
async fn organization_projects_get_project_policy(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy = nexus
            .project_fetch_policy(&opctx, organization_name, project_name)
            .await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update the IAM policy for this Project
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/policy",
    tags = ["projects"],
}]
async fn organization_projects_put_project_policy(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_policy: TypedBody<shared::Policy<authz::ProjectRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_policy = new_policy.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;

    let handler = async {
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let policy = nexus
            .project_update_policy(
                &opctx,
                organization_name,
                project_name,
                &new_policy,
            )
            .await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// IP Pools

#[derive(Deserialize, JsonSchema)]
pub struct IpPoolPathParam {
    pub pool_name: Name,
}

/// List IP Pools.
#[endpoint {
    method = GET,
    path = "/ip-pools",
    tags = ["ip-pools"],
}]
async fn ip_pools_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<IpPool>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let params = ScanByNameOrId::from_query(&query)?;
        let field = pagination_field_for_scan_params(params);
        let pools = match field {
            PagField::Id => {
                let page_selector = data_page_params_nameid_id(&rqctx, &query)?;
                nexus.ip_pools_list_by_id(&opctx, &page_selector).await?
            }
            PagField::Name => {
                let page_selector =
                    data_page_params_nameid_name(&rqctx, &query)?
                        .map_name(|n| Name::ref_cast(n));
                nexus.ip_pools_list_by_name(&opctx, &page_selector).await?
            }
        }
        .into_iter()
        .map(IpPool::from)
        .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(&query, pools)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a new IP Pool.
#[endpoint {
    method = POST,
    path = "/ip-pools",
    tags = ["ip-pools"],
}]
async fn ip_pools_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    pool_params: TypedBody<params::IpPoolCreate>,
) -> Result<HttpResponseCreated<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let pool_params = pool_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let pool = nexus.ip_pool_create(&opctx, &pool_params).await?;
        Ok(HttpResponseCreated(IpPool::from(pool)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a single IP Pool.
#[endpoint {
    method = GET,
    path = "/ip-pools/{pool_name}",
    tags = ["ip-pools"],
}]
async fn ip_pools_get_ip_pool(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolPathParam>,
) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let pool_name = &path.pool_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let pool = nexus.ip_pool_fetch(&opctx, pool_name).await?;
        Ok(HttpResponseOk(IpPool::from(pool)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an IP Pool.
#[endpoint {
    method = DELETE,
    path = "/ip-pools/{pool_name}",
    tags = ["ip-pools"],
}]
async fn ip_pools_delete_ip_pool(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let pool_name = &path.pool_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.ip_pool_delete(&opctx, pool_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update an IP Pool.
#[endpoint {
    method = PUT,
    path = "/ip-pools/{pool_name}",
    tags = ["ip-pools"],
}]
async fn ip_pools_put_ip_pool(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolPathParam>,
    updates: TypedBody<params::IpPoolUpdate>,
) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let pool_name = &path.pool_name;
    let updates = updates.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let pool = nexus.ip_pool_update(&opctx, pool_name, &updates).await?;
        Ok(HttpResponseOk(pool.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

type IpPoolRangePaginationParams = PaginationParams<EmptyScanParams, IpNetwork>;

/// List the ranges of IP addresses within an existing IP Pool.
///
/// Note that ranges are listed sorted by their first address.
#[endpoint {
    method = GET,
    path = "/ip-pools/{pool_name}/ranges",
    tags = ["ip-pools"],
}]
async fn ip_pool_ranges_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolPathParam>,
    query_params: Query<IpPoolRangePaginationParams>,
) -> Result<HttpResponseOk<ResultsPage<IpPoolRange>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let pool_name = &path.pool_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let marker = match query.page {
            WhichPage::First(_) => None,
            WhichPage::Next(ref addr) => Some(addr),
        };
        let pag_params = DataPageParams {
            limit: rqctx.page_limit(&query)?,
            direction: PaginationOrder::Ascending,
            marker,
        };
        let ranges = nexus
            .ip_pool_list_ranges(&opctx, pool_name, &pag_params)
            .await?
            .into_iter()
            .map(|range| range.into())
            .collect();
        Ok(HttpResponseOk(ResultsPage::new(
            ranges,
            &EmptyScanParams {},
            |range: &IpPoolRange, _| {
                IpNetwork::from(range.range.first_address())
            },
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Add a new range to an existing IP Pool.
#[endpoint {
    method = POST,
    path = "/ip-pools/{pool_name}/ranges/add",
    tags = ["ip-pools"],
}]
async fn ip_pool_ranges_add(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolPathParam>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
    let apictx = &rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let pool_name = &path.pool_name;
    let range = range_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let out = nexus.ip_pool_add_range(&opctx, pool_name, &range).await?;
        Ok(HttpResponseCreated(out.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Remove a range from an existing IP Pool.
#[endpoint {
    method = POST,
    path = "/ip-pools/{pool_name}/ranges/delete",
    tags = ["ip-pools"],
}]
async fn ip_pool_ranges_delete(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolPathParam>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let pool_name = &path.pool_name;
    let range = range_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.ip_pool_delete_range(&opctx, pool_name, &range).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Disks

/// List disks in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/disks",
    tags = ["disks"]
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disks = nexus
            .project_list_disks(
                &opctx,
                organization_name,
                project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            disks,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a disk in a project.
// TODO-correctness See note about instance create.  This should be async.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/disks",
    tags = ["disks"]
}]
async fn project_disks_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_disk: TypedBody<params::DiskCreate>,
) -> Result<HttpResponseCreated<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_disk_params = &new_disk.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disk = nexus
            .project_create_disk(
                &opctx,
                &organization_name,
                &project_name,
                &new_disk_params,
            )
            .await?;
        Ok(HttpResponseCreated(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Disk requests
#[derive(Deserialize, JsonSchema)]
struct DiskPathParam {
    organization_name: Name,
    project_name: Name,
    disk_name: Name,
}

/// Fetch a single disk in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
    tags = ["disks"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disk = nexus
            .disk_fetch(&opctx, &organization_name, &project_name, &disk_name)
            .await?;
        Ok(HttpResponseOk(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a disk from a project.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
    tags = ["disks"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .project_delete_disk(
                &opctx,
                &organization_name,
                &project_name,
                &disk_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Instances

/// List instances in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances",
    tags = ["instances"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instances = nexus
            .project_list_instances(
                &opctx,
                &organization_name,
                &project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            instances,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create an instance in a project.
// TODO-correctness This is supposed to be async.  Is that right?  We can create
// the instance immediately -- it's just not booted yet.  Maybe the boot
// operation is what's a separate operation_id.  What about the response code
// (201 Created vs 202 Accepted)?  Is that orthogonal?  Things can return a
// useful response, including an operation id, with either response code.  Maybe
// a "reboot" operation would return a 202 Accepted because there's no actual
// resource created?
#[endpoint {
    method = POST,
     path = "/organizations/{organization_name}/projects/{project_name}/instances",
    tags = ["instances"],
}]
async fn project_instances_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_instance: TypedBody<params::InstanceCreate>,
) -> Result<HttpResponseCreated<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_instance_params = &new_instance.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance = nexus
            .project_create_instance(
                &opctx,
                &organization_name,
                &project_name,
                &new_instance_params,
            )
            .await?;
        Ok(HttpResponseCreated(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Instance requests
#[derive(Deserialize, JsonSchema)]
struct InstancePathParam {
    organization_name: Name,
    project_name: Name,
    instance_name: Name,
}

/// Get an instance in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
    tags = ["instances"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance = nexus
            .instance_fetch(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
            )
            .await?;
        Ok(HttpResponseOk(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an instance from a project.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
    tags = ["instances"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .project_destroy_instance(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Migrate an instance to a different propolis-server, possibly on a different sled.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/migrate",
    tags = ["instances"],
}]
async fn project_instances_migrate_instance(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
    migrate_params: TypedBody<params::InstanceMigrate>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let migrate_instance_params = migrate_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance = nexus
            .project_instance_migrate(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
                migrate_instance_params,
            )
            .await?;
        Ok(HttpResponseOk(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Reboot an instance.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/reboot",
    tags = ["instances"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance = nexus
            .instance_reboot(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
            )
            .await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Boot an instance.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/start",
    tags = ["instances"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance = nexus
            .instance_start(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
            )
            .await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Halt an instance.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/stop",
    tags = ["instances"],
}]
// Our naming convention kind of falls apart here.
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance = nexus
            .instance_stop(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
            )
            .await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Get contents of an instance's serial console.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/serial",
    tags = ["instances"],
}]
async fn project_instances_instance_serial_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
    query_params: Query<params::InstanceSerialConsoleRequest>,
) -> Result<HttpResponseOk<params::InstanceSerialConsoleData>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let data = nexus
            .instance_serial_console_data(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
                &query_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(data))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List disks attached to this instance.
// TODO-scalability needs to be paginated
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks",
    tags = ["instances"],
}]
async fn instance_disks_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disks = nexus
            .instance_list_disks(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            disks,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/attach",
    tags = ["instances"],
}]
async fn instance_disks_attach(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
    disk_to_attach: TypedBody<params::DiskIdentifier>,
) -> Result<HttpResponseAccepted<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disk = nexus
            .instance_attach_disk(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
                &disk_to_attach.into_inner().name.into(),
            )
            .await?;
        Ok(HttpResponseAccepted(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/detach",
    tags = ["instances"],
}]
async fn instance_disks_detach(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
    disk_to_detach: TypedBody<params::DiskIdentifier>,
) -> Result<HttpResponseAccepted<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disk = nexus
            .instance_detach_disk(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
                &disk_to_detach.into_inner().name.into(),
            )
            .await?;
        Ok(HttpResponseAccepted(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Images

/// List global images.
///
/// Returns a list of all the global images. Global images are returned sorted
/// by creation date, with the most recent images appearing first.
#[endpoint {
    method = GET,
    path = "/images",
    tags = ["images:global"],
}]
async fn images_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
) -> Result<HttpResponseOk<ResultsPage<GlobalImage>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let images = nexus
            .global_images_list(
                &opctx,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            images,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a global image.
///
/// Create a new global image. This image can then be used by any user as a base
/// for instances.
#[endpoint {
    method = POST,
    path = "/images",
    tags = ["images:global"]
}]
async fn images_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_image: TypedBody<params::GlobalImageCreate>,
) -> Result<HttpResponseCreated<GlobalImage>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let new_image_params = new_image.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let image = nexus.global_image_create(&opctx, new_image_params).await?;
        Ok(HttpResponseCreated(image.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Image requests
#[derive(Deserialize, JsonSchema)]
struct GlobalImagePathParam {
    image_name: Name,
}

/// Get a global image.
///
/// Returns the details of a specific global image.
#[endpoint {
    method = GET,
    path = "/images/{image_name}",
    tags = ["images:global"],
}]
async fn images_get_image(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<GlobalImagePathParam>,
) -> Result<HttpResponseOk<GlobalImage>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let image_name = &path.image_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let image = nexus.global_image_fetch(&opctx, &image_name).await?;
        Ok(HttpResponseOk(image.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a global image.
///
/// Permanently delete a global image. This operation cannot be undone. Any
/// instances using the global image will continue to run, however new instances
/// can not be created with this image.
#[endpoint {
    method = DELETE,
    path = "/images/{image_name}",
    tags = ["images:global"],
}]
async fn images_delete_image(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<GlobalImagePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let image_name = &path.image_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.global_image_delete(&opctx, &image_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List images
///
/// List images in a project. The images are returned sorted by creation date,
/// with the most recent images appearing first.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/images",
    tags = ["images"],
}]
async fn project_images_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Image>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let images = nexus
            .project_list_images(
                &opctx,
                organization_name,
                project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            images,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create an image
///
/// Create a new image in a project.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/images",
    tags = ["images"]
}]
async fn project_images_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_image: TypedBody<params::ImageCreate>,
) -> Result<HttpResponseCreated<Image>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_image_params = &new_image.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let image = nexus
            .project_create_image(
                &opctx,
                &organization_name,
                &project_name,
                &new_image_params,
            )
            .await?;
        Ok(HttpResponseCreated(image.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Image requests
#[derive(Deserialize, JsonSchema)]
struct ImagePathParam {
    organization_name: Name,
    project_name: Name,
    image_name: Name,
}

/// Get an image
///
/// Get the details of a specific image in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/images/{image_name}",
    tags = ["images"],
}]
async fn project_images_get_image(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ImagePathParam>,
) -> Result<HttpResponseOk<Image>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let image_name = &path.image_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let image = nexus
            .project_image_fetch(
                &opctx,
                &organization_name,
                &project_name,
                &image_name,
            )
            .await?;
        Ok(HttpResponseOk(image.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an image
///
/// Permanently delete an image from a project. This operation cannot be undone.
/// Any instances in the project using the image will continue to run, however
/// new instances can not be created with this image.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/images/{image_name}",
    tags = ["images"],
}]
async fn project_images_delete_image(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ImagePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let image_name = &path.image_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .project_delete_image(
                &opctx,
                &organization_name,
                &project_name,
                &image_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/*
 * VPCs
 */

/// List network interfaces attached to this instance.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces",
    tags = ["instances"],
}]
async fn instance_network_interfaces_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<ResultsPage<NetworkInterface>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let interfaces = nexus
            .instance_list_network_interfaces(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            interfaces,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a network interface for an instance.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces",
    tags = ["instances"],
}]
async fn instance_network_interfaces_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
    interface_params: TypedBody<params::NetworkInterfaceCreate>,
) -> Result<HttpResponseCreated<NetworkInterface>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let iface = nexus
            .instance_create_network_interface(
                &opctx,
                &organization_name,
                &project_name,
                &instance_name,
                &interface_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(iface.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct NetworkInterfacePathParam {
    pub organization_name: Name,
    pub project_name: Name,
    pub instance_name: Name,
    pub interface_name: Name,
}

/// Detach a network interface from an instance.
///
/// Note that the primary interface for an instance cannot be deleted if there
/// are any secondary interfaces. A new primary interface must be designated
/// first. The primary interface can be deleted if there are no secondary
/// interfaces.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces/{interface_name}",
    tags = ["instances"],
}]
async fn instance_network_interfaces_delete_interface(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<NetworkInterfacePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let interface_name = &path.interface_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .instance_delete_network_interface(
                &opctx,
                organization_name,
                project_name,
                instance_name,
                interface_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Get an interface attached to an instance.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces/{interface_name}",
    tags = ["instances"],
}]
async fn instance_network_interfaces_get_interface(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<NetworkInterfacePathParam>,
) -> Result<HttpResponseOk<NetworkInterface>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let interface_name = &path.interface_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let interface = nexus
            .network_interface_fetch(
                &opctx,
                organization_name,
                project_name,
                instance_name,
                interface_name,
            )
            .await?;
        Ok(HttpResponseOk(NetworkInterface::from(interface)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update information about an instance's network interface
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces/{interface_name}",
    tags = ["instances"],
}]
async fn instance_network_interfaces_put_interface(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<NetworkInterfacePathParam>,
    updated_iface: TypedBody<params::NetworkInterfaceUpdate>,
) -> Result<HttpResponseOk<NetworkInterface>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let interface_name = &path.interface_name;
    let updated_iface = updated_iface.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let interface = nexus
            .network_interface_update(
                &opctx,
                organization_name,
                project_name,
                instance_name,
                interface_name,
                updated_iface,
            )
            .await?;
        Ok(HttpResponseOk(NetworkInterface::from(interface)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Snapshots

/// List snapshots in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots",
    tags = ["snapshots"],
}]
async fn project_snapshots_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Snapshot>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let snapshots = nexus
            .project_list_snapshots(
                &opctx,
                organization_name,
                project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            snapshots,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a snapshot of a disk.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots",
    tags = ["snapshots"],
}]
async fn project_snapshots_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_snapshot: TypedBody<params::SnapshotCreate>,
) -> Result<HttpResponseCreated<Snapshot>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_snapshot_params = &new_snapshot.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let snapshot = nexus
            .project_create_snapshot(
                &opctx,
                &organization_name,
                &project_name,
                &new_snapshot_params,
            )
            .await?;
        Ok(HttpResponseCreated(snapshot.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Snapshot requests
#[derive(Deserialize, JsonSchema)]
struct SnapshotPathParam {
    organization_name: Name,
    project_name: Name,
    snapshot_name: Name,
}

/// Get a snapshot in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots/{snapshot_name}",
    tags = ["snapshots"],
}]
async fn project_snapshots_get_snapshot(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SnapshotPathParam>,
) -> Result<HttpResponseOk<Snapshot>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let snapshot_name = &path.snapshot_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let snapshot = nexus
            .snapshot_fetch(
                &opctx,
                &organization_name,
                &project_name,
                &snapshot_name,
            )
            .await?;
        Ok(HttpResponseOk(snapshot.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a snapshot from a project.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots/{snapshot_name}",
    tags = ["snapshots"],
}]
async fn project_snapshots_delete_snapshot(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SnapshotPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let snapshot_name = &path.snapshot_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .project_delete_snapshot(
                &opctx,
                &organization_name,
                &project_name,
                &snapshot_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// VPCs

/// List VPCs in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs",
    tags = ["vpcs"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let vpcs = nexus
            .project_list_vpcs(
                &opctx,
                &organization_name,
                &project_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|p| p.into())
            .collect();

        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            vpcs,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for VPC requests
#[derive(Deserialize, JsonSchema)]
struct VpcPathParam {
    organization_name: Name,
    project_name: Name,
    vpc_name: Name,
}

/// Get a VPC in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
    tags = ["vpcs"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let vpc = nexus
            .vpc_fetch(&opctx, &organization_name, &project_name, &vpc_name)
            .await?;
        Ok(HttpResponseOk(vpc.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a VPC in a project.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs",
    tags = ["vpcs"],
}]
async fn project_vpcs_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProjectPathParam>,
    new_vpc: TypedBody<params::VpcCreate>,
) -> Result<HttpResponseCreated<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let new_vpc_params = &new_vpc.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let vpc = nexus
            .project_create_vpc(
                &opctx,
                &organization_name,
                &project_name,
                &new_vpc_params,
            )
            .await?;
        Ok(HttpResponseCreated(vpc.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a VPC.
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
    tags = ["vpcs"],
}]
async fn project_vpcs_put_vpc(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
    updated_vpc: TypedBody<params::VpcUpdate>,
) -> Result<HttpResponseOk<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let newvpc = nexus
            .project_update_vpc(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &updated_vpc.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(newvpc.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a vpc from a project.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
    tags = ["vpcs"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .project_delete_vpc(
                &opctx,
                &organization_name,
                &project_name,
                &vpc_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List subnets in a VPC.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets",
    tags = ["subnets"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let vpcs = nexus
            .vpc_list_subnets(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|vpc| vpc.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            vpcs,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for VPC Subnet requests
#[derive(Deserialize, JsonSchema)]
struct VpcSubnetPathParam {
    organization_name: Name,
    project_name: Name,
    vpc_name: Name,
    subnet_name: Name,
}

/// Get subnet in a VPC.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
    tags = ["subnets"],
}]
async fn vpc_subnets_get_subnet(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcSubnetPathParam>,
) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let subnet = nexus
            .vpc_subnet_fetch(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.subnet_name,
            )
            .await?;
        Ok(HttpResponseOk(subnet.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a subnet in a VPC.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets",
    tags = ["subnets"],
}]
async fn vpc_subnets_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
    create_params: TypedBody<params::VpcSubnetCreate>,
) -> Result<HttpResponseCreated<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let subnet = nexus
            .vpc_create_subnet(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &create_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(subnet.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a subnet from a VPC.
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
    tags = ["subnets"],
}]
async fn vpc_subnets_delete_subnet(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcSubnetPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .vpc_delete_subnet(
                &opctx,
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

/// Update a VPC Subnet.
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
    tags = ["subnets"],
}]
async fn vpc_subnets_put_subnet(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcSubnetPathParam>,
    subnet_params: TypedBody<params::VpcSubnetUpdate>,
) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let subnet = nexus
            .vpc_update_subnet(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.subnet_name,
                &subnet_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(subnet.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List network interfaces in a VPC subnet.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}/network-interfaces",
    tags = ["subnets"],
}]
async fn subnet_network_interfaces_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<VpcSubnetPathParam>,
) -> Result<HttpResponseOk<ResultsPage<NetworkInterface>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let interfaces = nexus
            .subnet_list_network_interfaces(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.subnet_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|interfaces| interfaces.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            interfaces,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// VPC Firewalls

/// List firewall rules for a VPC.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/firewall/rules",
    tags = ["firewall"],
}]
async fn vpc_firewall_rules_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
    // TODO: Check If-Match and fail if the ETag doesn't match anymore.
    // Without this check, if firewall rules change while someone is listing
    // the rules, they will see a mix of the old and new rules.
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let rules = nexus
            .vpc_list_firewall_rules(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
            )
            .await?;
        Ok(HttpResponseOk(VpcFirewallRules {
            rules: rules.into_iter().map(|rule| rule.into()).collect(),
        }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Replace the firewall rules for a VPC
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/firewall/rules",
    tags = ["firewall"],
}]
async fn vpc_firewall_rules_put(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
    router_params: TypedBody<VpcFirewallRuleUpdateParams>,
) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
    // TODO: Check If-Match and fail if the ETag doesn't match anymore.
    // TODO: limit size of the ruleset because the GET endpoint is not paginated
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let rules = nexus
            .vpc_update_firewall_rules(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &router_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(VpcFirewallRules {
            rules: rules.into_iter().map(|rule| rule.into()).collect(),
        }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// VPC Routers

/// List VPC Custom and System Routers
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers",
    tags = ["routers"],
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
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let routers = nexus
            .vpc_list_routers(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|s| s.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            routers,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for VPC Router requests
#[derive(Deserialize, JsonSchema)]
struct VpcRouterPathParam {
    organization_name: Name,
    project_name: Name,
    vpc_name: Name,
    router_name: Name,
}

/// Get a VPC Router
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
    tags = ["routers"],
}]
async fn vpc_routers_get_router(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcRouterPathParam>,
) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let vpc_router = nexus
            .vpc_router_fetch(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
            )
            .await?;
        Ok(HttpResponseOk(vpc_router.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a VPC Router
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers",
    tags = ["routers"],
}]
async fn vpc_routers_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcPathParam>,
    create_params: TypedBody<params::VpcRouterCreate>,
) -> Result<HttpResponseCreated<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let router = nexus
            .vpc_create_router(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &db::model::VpcRouterKind::Custom,
                &create_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(router.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a router from its VPC
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
    tags = ["routers"],
}]
async fn vpc_routers_delete_router(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcRouterPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .vpc_delete_router(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a VPC Router
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
    tags = ["routers"],
}]
async fn vpc_routers_put_router(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcRouterPathParam>,
    router_params: TypedBody<params::VpcRouterUpdate>,
) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let router = nexus
            .vpc_update_router(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
                &router_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(router.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Vpc Router Routes

/// List a Router's routes
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes",
    tags = ["routes"],
}]
async fn routers_routes_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<VpcRouterPathParam>,
) -> Result<HttpResponseOk<ResultsPage<RouterRoute>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let routes = nexus
            .router_list_routes(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|route| route.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            routes,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Router Route requests
#[derive(Deserialize, JsonSchema)]
struct RouterRoutePathParam {
    organization_name: Name,
    project_name: Name,
    vpc_name: Name,
    router_name: Name,
    route_name: Name,
}

/// Get a VPC Router route
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes/{route_name}",
    tags = ["routes"],
}]
async fn routers_routes_get_route(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RouterRoutePathParam>,
) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let route = nexus
            .route_fetch(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
                &path.route_name,
            )
            .await?;
        Ok(HttpResponseOk(route.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a VPC Router
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes",
    tags = ["routes"],
}]
async fn routers_routes_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<VpcRouterPathParam>,
    create_params: TypedBody<RouterRouteCreateParams>,
) -> Result<HttpResponseCreated<RouterRoute>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let route = nexus
            .router_create_route(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
                &RouterRouteKind::Custom,
                &create_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(route.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a route from its router
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes/{route_name}",
    tags = ["routes"],
}]
async fn routers_routes_delete_route(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RouterRoutePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .router_delete_route(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
                &path.route_name,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a Router route
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes/{route_name}",
    tags = ["routes"],
}]
async fn routers_routes_put_route(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RouterRoutePathParam>,
    router_params: TypedBody<RouterRouteUpdateParams>,
) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let router_route = nexus
            .router_update_route(
                &opctx,
                &path.organization_name,
                &path.project_name,
                &path.vpc_name,
                &path.router_name,
                &path.route_name,
                &router_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(router_route.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Racks

/// List racks in the system.
#[endpoint {
    method = GET,
    path = "/hardware/racks",
    tags = ["racks"],
}]
async fn hardware_racks_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Rack>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let rack_stream = nexus
            .racks_list(&opctx, &data_page_params_for(&rqctx, &query)?)
            .await?;
        let view_list = to_list::<db::model::Rack, Rack>(rack_stream).await;
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            view_list,
            &|_, rack: &Rack| rack.identity.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Rack requests
#[derive(Deserialize, JsonSchema)]
struct RackPathParam {
    /// The rack's unique ID.
    rack_id: Uuid,
}

/// Fetch information about a particular rack.
#[endpoint {
    method = GET,
    path = "/hardware/racks/{rack_id}",
    tags = ["racks"],
}]
async fn hardware_racks_get_rack(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RackPathParam>,
) -> Result<HttpResponseOk<Rack>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let rack_info = nexus.rack_lookup(&opctx, &path.rack_id).await?;
        Ok(HttpResponseOk(rack_info.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Sleds

/// List sleds in the system.
#[endpoint {
    method = GET,
    path = "/hardware/sleds",
    tags = ["sleds"],
}]
async fn hardware_sleds_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Sled>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let sleds = nexus
            .sleds_list(&opctx, &data_page_params_for(&rqctx, &query)?)
            .await?
            .into_iter()
            .map(|s| s.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            sleds,
            &|_, sled: &Sled| sled.identity.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Sled requests
#[derive(Deserialize, JsonSchema)]
struct SledPathParam {
    /// The sled's unique ID.
    sled_id: Uuid,
}

/// Fetch information about a sled in the system.
#[endpoint {
    method = GET,
    path = "/hardware/sleds/{sled_id}",
    tags = ["sleds"],
}]
async fn hardware_sleds_get_sled(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SledPathParam>,
) -> Result<HttpResponseOk<Sled>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let sled_info = nexus.sled_lookup(&opctx, &path.sled_id).await?;
        Ok(HttpResponseOk(sled_info.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Updates

/// Refresh update metadata
#[endpoint {
     method = POST,
     path = "/updates/refresh",
     tags = ["updates"],
}]
async fn updates_refresh(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.updates_refresh_metadata(&opctx).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Sagas

/// List all sagas (for debugging)
#[endpoint {
    method = GET,
    path = "/sagas",
    tags = ["sagas"],
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
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let saga_stream = nexus.sagas_list(&opctx, &pagparams).await?;
        let view_list = to_list(saga_stream).await;
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            view_list,
            &|_, saga: &Saga| saga.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Saga requests
#[derive(Deserialize, JsonSchema)]
struct SagaPathParam {
    saga_id: Uuid,
}

/// Fetch information about a single saga (for debugging)
#[endpoint {
    method = GET,
    path = "/sagas/{saga_id}",
    tags = ["sagas"],
}]
async fn sagas_get_saga(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SagaPathParam>,
) -> Result<HttpResponseOk<Saga>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let saga = nexus.saga_get(&opctx, path.saga_id).await?;
        Ok(HttpResponseOk(saga))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo users

/// List users
#[endpoint {
    method = GET,
    path = "/users",
    tags = ["silos"],
}]
async fn silo_users_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<User>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let users = nexus
            .silo_users_list(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(&query, users)?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Built-in (system) users

/// List the built-in system users
#[endpoint {
    method = GET,
    path = "/users_builtin",
    tags = ["system"],
}]
async fn builtin_users_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
) -> Result<HttpResponseOk<ResultsPage<UserBuiltin>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams =
        data_page_params_for(&rqctx, &query)?.map_name(|n| Name::ref_cast(n));
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let users = nexus
            .users_builtin_list(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            users,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for global (system) user requests
#[derive(Deserialize, JsonSchema)]
struct UserPathParam {
    /// The built-in user's unique name.
    user_name: Name,
}

/// Fetch a specific built-in system user
#[endpoint {
    method = GET,
    path = "/users_builtin/{user_name}",
    tags = ["system"],
}]
async fn builtin_users_get_user(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<UserPathParam>,
) -> Result<HttpResponseOk<UserBuiltin>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let user_name = &path.user_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let user = nexus.user_builtin_fetch(&opctx, &user_name).await?;
        Ok(HttpResponseOk(user.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List all timeseries schema
#[endpoint {
    method = GET,
    path = "/timeseries/schema",
    tags = ["metrics"],
}]
async fn timeseries_schema_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<oximeter_db::TimeseriesSchemaPaginationParams>,
) -> Result<HttpResponseOk<ResultsPage<oximeter_db::TimeseriesSchema>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let limit = rqctx.page_limit(&query)?;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let list = nexus.timeseries_schema_list(&opctx, &query, limit).await?;
        Ok(HttpResponseOk(list))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Built-in roles

// Roles have their own pagination scheme because they do not use the usual "id"
// or "name" types.  For more, see the comment in dbinit.sql.
#[derive(Deserialize, JsonSchema, Serialize)]
struct RolePage {
    last_seen: String,
}

/// List the built-in roles
#[endpoint {
    method = GET,
    path = "/roles",
    tags = ["roles"],
}]
async fn roles_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginationParams<EmptyScanParams, RolePage>>,
) -> Result<HttpResponseOk<ResultsPage<Role>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let marker = match &query.page {
            WhichPage::First(..) => None,
            WhichPage::Next(RolePage { last_seen }) => {
                Some(last_seen.split_once('.').ok_or_else(|| {
                    Error::InvalidValue {
                        label: last_seen.clone(),
                        message: String::from("bad page token"),
                    }
                })?)
                .map(|(s1, s2)| (s1.to_string(), s2.to_string()))
            }
        };
        let pagparams = DataPageParams {
            limit: rqctx.page_limit(&query)?,
            direction: PaginationOrder::Ascending,
            marker: marker.as_ref(),
        };
        let roles = nexus
            .roles_builtin_list(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(dropshot::ResultsPage::new(
            roles,
            &EmptyScanParams {},
            |role: &Role, _| RolePage { last_seen: role.name.to_string() },
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for global (system) role requests
#[derive(Deserialize, JsonSchema)]
struct RolePathParam {
    /// The built-in role's unique name.
    role_name: String,
}

/// Fetch a specific built-in role
#[endpoint {
    method = GET,
    path = "/roles/{role_name}",
    tags = ["roles"],
}]
async fn roles_get_role(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<RolePathParam>,
) -> Result<HttpResponseOk<Role>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let role_name = &path.role_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let role = nexus.role_builtin_fetch(&opctx, &role_name).await?;
        Ok(HttpResponseOk(role.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Per-user SSH public keys

/// List the current user's SSH public keys
#[endpoint {
    method = GET,
    path = "/session/me/sshkeys",
    tags = ["sshkeys"],
}]
async fn sshkeys_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
) -> Result<HttpResponseOk<ResultsPage<SshKey>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx.authn.actor_required()?;
        let page_params =
            data_page_params_for(&rqctx, &query)?.map_name(Name::ref_cast);
        let ssh_keys = nexus
            .ssh_keys_list(&opctx, actor.actor_id(), &page_params)
            .await?
            .into_iter()
            .map(SshKey::from)
            .collect::<Vec<SshKey>>();
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            ssh_keys,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a new SSH public key for the current user
#[endpoint {
    method = POST,
    path = "/session/me/sshkeys",
    tags = ["sshkeys"],
}]
async fn sshkeys_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_key: TypedBody<params::SshKeyCreate>,
) -> Result<HttpResponseCreated<SshKey>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx.authn.actor_required()?;
        let ssh_key = nexus
            .ssh_key_create(&opctx, actor.actor_id(), new_key.into_inner())
            .await?;
        Ok(HttpResponseCreated(ssh_key.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for SSH key requests by name
#[derive(Deserialize, JsonSchema)]
struct SshKeyPathParams {
    ssh_key_name: Name,
}

/// Get (by name) an SSH public key belonging to the current user
#[endpoint {
    method = GET,
    path = "/session/me/sshkeys/{ssh_key_name}",
    tags = ["sshkeys"],
}]
async fn sshkeys_get_key(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SshKeyPathParams>,
) -> Result<HttpResponseOk<SshKey>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let ssh_key_name = &path.ssh_key_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx.authn.actor_required()?;
        let ssh_key =
            nexus.ssh_key_fetch(&opctx, actor.actor_id(), ssh_key_name).await?;
        Ok(HttpResponseOk(ssh_key.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete (by name) an SSH public key belonging to the current user
#[endpoint {
    method = DELETE,
    path = "/session/me/sshkeys/{ssh_key_name}",
    tags = ["sshkeys"],
}]
async fn sshkeys_delete_key(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SshKeyPathParams>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let ssh_key_name = &path.ssh_key_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx.authn.actor_required()?;
        nexus.ssh_key_delete(&opctx, actor.actor_id(), ssh_key_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[cfg(test)]
mod test {
    use super::external_api;

    #[test]
    fn test_nexus_tag_policy() {
        // This will fail if any of the endpoints don't match the policy in
        // ./tag-config.json
        let _ = external_api();
    }
}
