// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for external HTTP APIs

use super::views::IpPool;
use super::views::IpPoolRange;
use super::{
    console_api, device_auth, params, views,
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
use omicron_common::api::external::http_pagination::data_page_params_for;
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
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::NetworkInterface;
use omicron_common::api::external::RouterRoute;
use omicron_common::api::external::RouterRouteCreateParams;
use omicron_common::api::external::RouterRouteKind;
use omicron_common::api::external::RouterRouteUpdateParams;
use omicron_common::api::external::Saga;
use omicron_common::api::external::VpcFirewallRuleUpdateParams;
use omicron_common::api::external::VpcFirewallRules;
use omicron_common::bail_unless;
use parse_display::Display;
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
        api.register(policy_view)?;
        api.register(policy_update)?;

        api.register(organization_list)?;
        api.register(organization_create)?;
        api.register(organization_view)?;
        api.register(organization_view_by_id)?;
        api.register(organization_delete)?;
        api.register(organization_update)?;
        api.register(organization_policy_view)?;
        api.register(organization_policy_update)?;

        api.register(project_list)?;
        api.register(project_create)?;
        api.register(project_view)?;
        api.register(project_view_by_id)?;
        api.register(project_delete)?;
        api.register(project_update)?;
        api.register(project_policy_view)?;
        api.register(project_policy_update)?;

        // Customer-Accessible IP Pools API
        api.register(ip_pool_list)?;
        api.register(ip_pool_create)?;
        api.register(ip_pool_view)?;
        api.register(ip_pool_delete)?;
        api.register(ip_pool_update)?;

        // Customer-Accessible IP Pool Range API (used by instances)
        api.register(ip_pool_range_list)?;
        api.register(ip_pool_range_add)?;
        api.register(ip_pool_range_remove)?;

        // Operator-Accessible IP Pool Range API (used by Oxide services)
        api.register(ip_pool_service_range_list)?;
        api.register(ip_pool_service_range_add)?;
        api.register(ip_pool_service_range_remove)?;

        api.register(disk_list)?;
        api.register(disk_list)?;
        api.register(disk_create)?;
        api.register(disk_view)?;
        api.register(disk_view_by_id)?;
        api.register(disk_delete)?;
        api.register(disk_metrics_list)?;

        api.register(instance_list)?;
        api.register(instance_create)?;
        api.register(instance_view)?;
        api.register(instance_view_by_id)?;
        api.register(instance_delete)?;
        api.register(instance_migrate)?;
        api.register(instance_reboot)?;
        api.register(instance_start)?;
        api.register(instance_stop)?;
        api.register(instance_serial_console)?;

        // Project-scoped images API
        api.register(image_list)?;
        api.register(image_create)?;
        api.register(image_view)?;
        api.register(image_view_by_id)?;
        api.register(image_delete)?;

        api.register(instance_disk_list)?;
        api.register(instance_disk_attach)?;
        api.register(instance_disk_detach)?;

        api.register(snapshot_list)?;
        api.register(snapshot_create)?;
        api.register(snapshot_view)?;
        api.register(snapshot_view_by_id)?;
        api.register(snapshot_delete)?;

        api.register(vpc_list)?;
        api.register(vpc_create)?;
        api.register(vpc_view)?;
        api.register(vpc_view_by_id)?;
        api.register(vpc_update)?;
        api.register(vpc_delete)?;

        api.register(vpc_subnet_list)?;
        api.register(vpc_subnet_view)?;
        api.register(vpc_subnet_view_by_id)?;
        api.register(vpc_subnet_create)?;
        api.register(vpc_subnet_delete)?;
        api.register(vpc_subnet_update)?;
        api.register(vpc_subnet_list_network_interfaces)?;

        api.register(instance_network_interface_create)?;
        api.register(instance_network_interface_list)?;
        api.register(instance_network_interface_view)?;
        api.register(instance_network_interface_view_by_id)?;
        api.register(instance_network_interface_update)?;
        api.register(instance_network_interface_delete)?;

        api.register(instance_external_ip_list)?;

        api.register(vpc_router_list)?;
        api.register(vpc_router_view)?;
        api.register(vpc_router_view_by_id)?;
        api.register(vpc_router_create)?;
        api.register(vpc_router_delete)?;
        api.register(vpc_router_update)?;

        api.register(vpc_router_route_list)?;
        api.register(vpc_router_route_view)?;
        api.register(vpc_router_route_view_by_id)?;
        api.register(vpc_router_route_create)?;
        api.register(vpc_router_route_delete)?;
        api.register(vpc_router_route_update)?;

        api.register(vpc_firewall_rules_view)?;
        api.register(vpc_firewall_rules_update)?;

        api.register(rack_list)?;
        api.register(rack_view)?;
        api.register(sled_list)?;
        api.register(sled_view)?;

        api.register(saga_list)?;
        api.register(saga_view)?;

        api.register(system_user_list)?;
        api.register(system_user_view)?;

        api.register(timeseries_schema_get)?;

        api.register(role_list)?;
        api.register(role_view)?;

        api.register(session_sshkey_list)?;
        api.register(session_sshkey_view)?;
        api.register(session_sshkey_create)?;
        api.register(session_sshkey_delete)?;

        // Fleet-wide API operations
        api.register(silo_list)?;
        api.register(silo_create)?;
        api.register(silo_view)?;
        api.register(silo_delete)?;
        api.register(silo_identity_provider_list)?;
        api.register(silo_policy_view)?;
        api.register(silo_policy_update)?;

        api.register(silo_identity_provider_create)?;
        api.register(silo_identity_provider_view)?;

        api.register(image_global_list)?;
        api.register(image_global_create)?;
        api.register(image_global_view)?;
        api.register(image_global_view_by_id)?;
        api.register(image_global_delete)?;

        api.register(updates_refresh)?;
        api.register(user_list)?;

        // Console API operations
        api.register(console_api::spoof_login)?;
        api.register(console_api::spoof_login_form)?;
        api.register(console_api::login_redirect)?;
        api.register(console_api::session_me)?;
        api.register(console_api::logout)?;
        api.register(console_api::console_page)?;
        api.register(console_api::console_root)?;
        api.register(console_api::console_settings_page)?;
        api.register(console_api::asset)?;

        api.register(console_api::login)?;
        api.register(console_api::consume_credentials)?;

        api.register(device_auth::device_auth_request)?;
        api.register(device_auth::device_auth_verify)?;
        api.register(device_auth::device_auth_success)?;
        api.register(device_auth::device_auth_confirm)?;
        api.register(device_auth::device_access_token)?;

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
// An exception to this are id lookup operations which have a different top-level route
// but will still be grouped with the collection.  For example:
//
//  GET    /by-id/organizations/{id} (look up a organization in the collection by id)
//
// We pick a name for the function that implements a given API entrypoint
// based on how we expect it to appear in the CLI subcommand hierarchy. For
// example:
//
//   GET    /organizations                    -> organization_list()
//   POST   /organizations                    -> organization_create()
//   GET    /organizations/{org_name}         -> organization_view()
//   DELETE /organizations/{org_name}         -> organization_delete()
//   PUT    /organizations/{org_name}         -> organization_update()
//
// Note that the path typically uses the entity's plural form while the
// function name uses its singular.
//
// Operations beyond list, create, view, delete, and update should use a
// descriptive noun or verb, again bearing in mind that this will be
// transcribed into the CLI and SDKs:
//
//   POST   -> instance_reboot
//   POST   -> instance_stop
//   GET    -> instance_serial_console
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
async fn policy_view(
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

/// Path parameters for `/by-id/` endpoints
#[derive(Deserialize, JsonSchema)]
struct ByIdPathParams {
    id: Uuid,
}

/// Update the top-level IAM policy
#[endpoint {
    method = PUT,
    path = "/policy",
    tags = ["policy"],
}]
async fn policy_update(
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

/// List silos
///
/// Lists silos that are discoverable based on the current permissions.
#[endpoint {
    method = GET,
    path = "/silos",
    tags = ["silos"],
}]
async fn silo_list(
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

/// Create a silo
#[endpoint {
    method = POST,
    path = "/silos",
    tags = ["silos"],
}]
async fn silo_create(
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

/// Fetch a silo
///
/// Fetch a silo by name.
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}",
    tags = ["silos"],
}]
async fn silo_view(
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

/// Delete a silo
///
/// Delete a silo by name.
#[endpoint {
    method = DELETE,
    path = "/silos/{silo_name}",
    tags = ["silos"],
}]
async fn silo_delete(
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

/// Fetch a silo's IAM policy
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}/policy",
    tags = ["silos"],
}]
async fn silo_policy_view(
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

/// Update a silo's IAM policy
#[endpoint {
    method = PUT,
    path = "/silos/{silo_name}/policy",
    tags = ["silos"],
}]
async fn silo_policy_update(
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

/// List a silo's IDPs
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}/identity-providers",
    tags = ["silos"],
}]
async fn silo_identity_provider_list(
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

/// Create a SAML IDP
#[endpoint {
    method = POST,
    path = "/silos/{silo_name}/saml-identity-providers",
    tags = ["silos"],
}]
async fn silo_identity_provider_create(
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

/// Fetch a SAML IDP
#[endpoint {
    method = GET,
    path = "/silos/{silo_name}/saml-identity-providers/{provider_name}",
    tags = ["silos"],
}]
async fn silo_identity_provider_view(
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

// TODO: no DELETE for identity providers?

/// List organizations
#[endpoint {
    method = GET,
    path = "/organizations",
    tags = ["organizations"],
}]
async fn organization_list(
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

/// Create an organization
#[endpoint {
    method = POST,
    path = "/organizations",
    tags = ["organizations"],
}]
async fn organization_create(
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

/// Fetch an organization
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
}]
async fn organization_view(
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

/// Fetch an organization by id
#[endpoint {
    method = GET,
    path = "/by-id/organizations/{id}",
    tags = ["organizations"],
}]
async fn organization_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization = nexus.organization_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an organization
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
}]
async fn organization_delete(
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

/// Update an organization
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
async fn organization_update(
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

/// Fetch an organization's IAM policy
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/policy",
    tags = ["organizations"],
}]
async fn organization_policy_view(
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

/// Update an organization's IAM policy
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/policy",
    tags = ["organizations"],
}]
async fn organization_policy_update(
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

/// List projects
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects",
    tags = ["projects"],
}]
async fn project_list(
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

/// Create a project
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects",
    tags = ["projects"],
}]
async fn project_create(
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

/// Fetch a project
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
}]
async fn project_view(
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

/// Fetch a project by id
#[endpoint {
    method = GET,
    path = "/by-id/projects/{id}",
    tags = ["projects"],
}]
async fn project_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project = nexus.project_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a project
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
}]
async fn project_delete(
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

/// Update a project
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
async fn project_update(
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

/// Fetch a project's IAM policy
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/policy",
    tags = ["projects"],
}]
async fn project_policy_view(
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

/// Update a project's IAM policy
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/policy",
    tags = ["projects"],
}]
async fn project_policy_update(
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

/// List IP pools
#[endpoint {
    method = GET,
    path = "/ip-pools",
    tags = ["ip-pools"],
}]
async fn ip_pool_list(
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
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            pools,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create an IP pool
#[endpoint {
    method = POST,
    path = "/ip-pools",
    tags = ["ip-pools"],
}]
async fn ip_pool_create(
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

/// Fetch an IP pool
#[endpoint {
    method = GET,
    path = "/ip-pools/{pool_name}",
    tags = ["ip-pools"],
}]
async fn ip_pool_view(
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

/// Delete an IP Pool
#[endpoint {
    method = DELETE,
    path = "/ip-pools/{pool_name}",
    tags = ["ip-pools"],
}]
async fn ip_pool_delete(
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

/// Update an IP Pool
#[endpoint {
    method = PUT,
    path = "/ip-pools/{pool_name}",
    tags = ["ip-pools"],
}]
async fn ip_pool_update(
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

/// List ranges for an IP pool
///
/// Ranges are ordered by their first address.
#[endpoint {
    method = GET,
    path = "/ip-pools/{pool_name}/ranges",
    tags = ["ip-pools"],
}]
async fn ip_pool_range_list(
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

/// Add a range to an IP pool
#[endpoint {
    method = POST,
    path = "/ip-pools/{pool_name}/ranges/add",
    tags = ["ip-pools"],
}]
async fn ip_pool_range_add(
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

/// Remove a range from an IP pool
#[endpoint {
    method = POST,
    path = "/ip-pools/{pool_name}/ranges/remove",
    tags = ["ip-pools"],
}]
async fn ip_pool_range_remove(
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

#[derive(Deserialize, JsonSchema)]
pub struct IpPoolServicePathParam {
    pub rack_id: Uuid,
}

/// List ranges for an IP pool used for Oxide services.
///
/// Ranges are ordered by their first address.
#[endpoint {
    method = GET,
    path = "/ip-pools-service/{rack_id}/ranges",
    tags = ["ip-pools"],
}]
async fn ip_pool_service_range_list(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolServicePathParam>,
    query_params: Query<IpPoolRangePaginationParams>,
) -> Result<HttpResponseOk<ResultsPage<IpPoolRange>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let path = path_params.into_inner();
    let rack_id = path.rack_id;
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
            .ip_pool_service_list_ranges(&opctx, rack_id, &pag_params)
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

/// Add a range to an IP pool used for Oxide services.
#[endpoint {
    method = POST,
    path = "/ip-pools-service/{rack_id}/ranges/add",
    tags = ["ip-pools"],
}]
async fn ip_pool_service_range_add(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolServicePathParam>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
    let apictx = &rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let rack_id = path.rack_id;
    let range = range_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let out =
            nexus.ip_pool_service_add_range(&opctx, rack_id, &range).await?;
        Ok(HttpResponseCreated(out.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Remove a range from an IP pool used for Oxide services.
#[endpoint {
    method = POST,
    path = "/ip-pools-service/{rack_id}/ranges/remove",
    tags = ["ip-pools"],
}]
async fn ip_pool_service_range_remove(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<IpPoolServicePathParam>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let rack_id = path.rack_id;
    let range = range_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.ip_pool_service_delete_range(&opctx, rack_id, &range).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Disks

/// List disks
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/disks",
    tags = ["disks"]
}]
async fn disk_list(
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

/// Create a disk
// TODO-correctness See note about instance create.  This should be async.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/disks",
    tags = ["disks"]
}]
async fn disk_create(
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

/// Fetch a disk
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
    tags = ["disks"],
}]
async fn disk_view(
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

/// Fetch a disk by id
#[endpoint {
    method = GET,
    path = "/by-id/disks/{id}",
    tags = ["disks"],
}]
async fn disk_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disk = nexus.disk_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a disk
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
    tags = ["disks"],
}]
async fn disk_delete(
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

#[derive(Display, Deserialize, JsonSchema)]
#[display(style = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum DiskMetricName {
    Activated,
    Flush,
    Read,
    ReadBytes,
    Write,
    WriteBytes,
}

/// Fetch disk metrics
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}/metrics/{metric_name}",
    tags = ["disks"],
}]
async fn disk_metrics_list(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<MetricsPathParam<DiskPathParam, DiskMetricName>>,
    query_params: Query<
        PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
    >,
) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;

    let path = path_params.into_inner();
    let organization_name = &path.inner.organization_name;
    let project_name = &path.inner.project_name;
    let disk_name = &path.inner.disk_name;
    let metric_name = path.metric_name;

    let query = query_params.into_inner();
    let limit = rqctx.page_limit(&query)?;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;

        // This ensures the user is authorized on Action::Read for this disk
        let disk = nexus
            .disk_fetch(&opctx, organization_name, project_name, disk_name)
            .await?;
        let upstairs_uuid = disk.id();
        let result = nexus
            .select_timeseries(
                &format!("crucible_upstairs:{}", metric_name),
                &[&format!("upstairs_uuid=={}", upstairs_uuid)],
                query,
                limit,
            )
            .await?;

        Ok(HttpResponseOk(result))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Instances

/// List instances
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances",
    tags = ["instances"],
}]
async fn instance_list(
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

/// Create an instance
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
async fn instance_create(
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

/// Fetch an instance
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
    tags = ["instances"],
}]
async fn instance_view(
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

/// Fetch an instance by id
#[endpoint {
    method = GET,
    path = "/by-id/instances/{id}",
    tags = ["instances"],
}]
async fn instance_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance = nexus.instance_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an instance
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
    tags = ["instances"],
}]
async fn instance_delete(
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

// TODO should this be in the public API?
/// Migrate an instance
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/migrate",
    tags = ["instances"],
}]
async fn instance_migrate(
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

/// Reboot an instance
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/reboot",
    tags = ["instances"],
}]
async fn instance_reboot(
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

/// Boot an instance
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/start",
    tags = ["instances"],
}]
async fn instance_start(
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

/// Halt an instance
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/stop",
    tags = ["instances"],
}]
async fn instance_stop(
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

/// Fetch an instance's serial console
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/serial-console",
    tags = ["instances"],
}]
async fn instance_serial_console(
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

/// List an instance's disks
// TODO-scalability needs to be paginated
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks",
    tags = ["instances"],
}]
async fn instance_disk_list(
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

/// Attach a disk to an instance
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/attach",
    tags = ["instances"],
}]
async fn instance_disk_attach(
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

/// Detach a disk from an instance
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/detach",
    tags = ["instances"],
}]
async fn instance_disk_detach(
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

/// List global images
///
/// Returns a list of all the global images. Global images are returned sorted
/// by creation date, with the most recent images appearing first.
#[endpoint {
    method = GET,
    path = "/images",
    tags = ["images:global"],
}]
async fn image_global_list(
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

/// Create a global image
///
/// Create a new global image. This image can then be used by any user as a
/// base for instances.
#[endpoint {
    method = POST,
    path = "/images",
    tags = ["images:global"]
}]
async fn image_global_create(
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

/// Fetch a global image
///
/// Returns the details of a specific global image.
#[endpoint {
    method = GET,
    path = "/images/{image_name}",
    tags = ["images:global"],
}]
async fn image_global_view(
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

/// Fetch a global image by id
#[endpoint {
    method = GET,
    path = "/by-id/global-images/{id}",
    tags = ["images:global"],
}]
async fn image_global_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<GlobalImage>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let image = nexus.global_image_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(image.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a global image
///
/// Permanently delete a global image. This operation cannot be undone. Any
/// instances using the global image will continue to run, however new instances
/// can not be created with this image.
#[endpoint {
    method = DELETE,
    path = "/images/{image_name}",
    tags = ["images:global"],
}]
async fn image_global_delete(
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
async fn image_list(
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
async fn image_create(
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

/// Fetch an image
///
/// Fetch the details for a specific image in a project.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/images/{image_name}",
    tags = ["images"],
}]
async fn image_view(
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

/// Fetch an image by id
#[endpoint {
    method = GET,
    path = "/by-id/images/{id}",
    tags = ["images"],
}]
async fn image_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Image>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let image = nexus.project_image_fetch_by_id(&opctx, id).await?;
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
async fn image_delete(
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

/// List network interfaces
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces",
    tags = ["instances"],
}]
async fn instance_network_interface_list(
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

/// Create a network interface
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces",
    tags = ["instances"],
}]
async fn instance_network_interface_create(
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

/// Delete a network interface
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
async fn instance_network_interface_delete(
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

/// Fetch a network interface
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces/{interface_name}",
    tags = ["instances"],
}]
async fn instance_network_interface_view(
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

/// Fetch a network interface by id
#[endpoint {
    method = GET,
    path = "/by-id/network-interfaces/{id}",
    tags = ["instances"],
}]
async fn instance_network_interface_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<NetworkInterface>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let network_interface =
            nexus.network_interface_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(network_interface.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a network interface
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/network-interfaces/{interface_name}",
    tags = ["instances"],
}]
async fn instance_network_interface_update(
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

// External IP addresses for instances

/// List external IP addresses
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/external-ips",
    tags = ["instances"],
}]
async fn instance_external_ip_list(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<ResultsPage<views::ExternalIp>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let organization_name = &path.organization_name;
    let project_name = &path.project_name;
    let instance_name = &path.instance_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let ips = nexus
            .instance_list_external_ips(
                &opctx,
                organization_name,
                project_name,
                instance_name,
            )
            .await?;
        Ok(HttpResponseOk(ResultsPage { items: ips, next_page: None }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Snapshots

/// List snapshots
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots",
    tags = ["snapshots"],
}]
async fn snapshot_list(
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

/// Create a snapshot
///
/// Creates a point-in-time snapshot from a disk.
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots",
    tags = ["snapshots"],
}]
async fn snapshot_create(
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

/// Fetch a snapshot
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots/{snapshot_name}",
    tags = ["snapshots"],
}]
async fn snapshot_view(
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

/// Fetch a snapshot by id
#[endpoint {
    method = GET,
    path = "/by-id/snapshots/{id}",
    tags = ["snapshots"],
}]
async fn snapshot_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Snapshot>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let snapshot = nexus.snapshot_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(snapshot.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a snapshot
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/snapshots/{snapshot_name}",
    tags = ["snapshots"],
}]
async fn snapshot_delete(
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

/// List VPCs
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs",
    tags = ["vpcs"],
}]
async fn vpc_list(
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

/// Fetch a VPC
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
    tags = ["vpcs"],
}]
async fn vpc_view(
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

/// Fetch a VPC
#[endpoint {
    method = GET,
    path = "/by-id/vpcs/{id}",
    tags = ["vpcs"],
}]
async fn vpc_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let vpc = nexus.vpc_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(vpc.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a VPC
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs",
    tags = ["vpcs"],
}]
async fn vpc_create(
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

/// Update a VPC
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
    tags = ["vpcs"],
}]
async fn vpc_update(
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

/// Delete a VPC
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}",
    tags = ["vpcs"],
}]
async fn vpc_delete(
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

/// List subnets
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets",
    tags = ["vpcs"],
}]
async fn vpc_subnet_list(
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

/// Fetch a subnet
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
    tags = ["vpcs"],
}]
async fn vpc_subnet_view(
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

/// Fetch a subnet by id
#[endpoint {
    method = GET,
    path = "/by-id/vpc-subnets/{id}",
    tags = ["vpcs"],
}]
async fn vpc_subnet_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let subnet = nexus.vpc_subnet_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(subnet.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a subnet
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets",
    tags = ["vpcs"],
}]
async fn vpc_subnet_create(
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

/// Delete a subnet
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
    tags = ["vpcs"],
}]
async fn vpc_subnet_delete(
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

/// Update a subnet
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}",
    tags = ["vpcs"],
}]
async fn vpc_subnet_update(
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

/// List network interfaces
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/subnets/{subnet_name}/network-interfaces",
    tags = ["vpcs"],
}]
async fn vpc_subnet_list_network_interfaces(
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

// TODO Is the number of firewall rules bounded?
/// List firewall rules
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/firewall/rules",
    tags = ["vpcs"],
}]
async fn vpc_firewall_rules_view(
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

/// Replace firewall rules
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/firewall/rules",
    tags = ["vpcs"],
}]
async fn vpc_firewall_rules_update(
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

/// List routers
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers",
    tags = ["vpcs"],
}]
async fn vpc_router_list(
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

/// Get a router
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
    tags = ["vpcs"],
}]
async fn vpc_router_view(
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

/// Get a router by id
#[endpoint {
    method = GET,
    path = "/by-id/vpc-routers/{id}",
    tags = ["vpcs"],
}]
async fn vpc_router_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let router = nexus.vpc_router_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(router.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a router
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers",
    tags = ["vpcs"],
}]
async fn vpc_router_create(
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

/// Delete a router
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
    tags = ["vpcs"],
}]
async fn vpc_router_delete(
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

/// Update a router
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}",
    tags = ["vpcs"],
}]
async fn vpc_router_update(
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

/// List routes
///
/// List the routes associated with a router in a particular VPC.
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes",
    tags = ["vpcs"],
}]
async fn vpc_router_route_list(
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

/// Fetch a route
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes/{route_name}",
    tags = ["vpcs"],
}]
async fn vpc_router_route_view(
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

/// Fetch a route by id
#[endpoint {
    method = GET,
    path = "/by-id/vpc-router-routes/{id}",
    tags = ["vpcs"]
}]
async fn vpc_router_route_view_by_id(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let route = nexus.route_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(route.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a router
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes",
    tags = ["vpcs"],
}]
async fn vpc_router_route_create(
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

/// Delete a route
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes/{route_name}",
    tags = ["vpcs"],
}]
async fn vpc_router_route_delete(
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

/// Update a route
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}/vpcs/{vpc_name}/routers/{router_name}/routes/{route_name}",
    tags = ["vpcs"],
}]
async fn vpc_router_route_update(
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

/// List racks
#[endpoint {
    method = GET,
    path = "/hardware/racks",
    tags = ["hardware"],
}]
async fn rack_list(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Rack>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let racks = nexus
            .racks_list(&opctx, &data_page_params_for(&rqctx, &query)?)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            racks,
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

/// Fetch a rack
#[endpoint {
    method = GET,
    path = "/hardware/racks/{rack_id}",
    tags = ["hardware"],
}]
async fn rack_view(
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

/// List sleds
#[endpoint {
    method = GET,
    path = "/hardware/sleds",
    tags = ["hardware"],
}]
async fn sled_list(
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

/// Fetch a sled
#[endpoint {
    method = GET,
    path = "/hardware/sleds/{sled_id}",
    tags = ["hardware"],
}]
async fn sled_view(
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

/// Refresh update data
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

/// List sagas
#[endpoint {
    method = GET,
    path = "/sagas",
    tags = ["sagas"],
}]
async fn saga_list(
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

/// Fetch a saga
#[endpoint {
    method = GET,
    path = "/sagas/{saga_id}",
    tags = ["sagas"],
}]
async fn saga_view(
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
async fn user_list(
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
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            users,
            &|_, user: &User| user.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Built-in (system) users

/// List built-in users
#[endpoint {
    method = GET,
    path = "/system/user",
    tags = ["system"],
}]
async fn system_user_list(
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

/// Fetch a built-in user
#[endpoint {
    method = GET,
    path = "/system/user/{user_name}",
    tags = ["system"],
}]
async fn system_user_view(
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

/// List timeseries schema
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

/// List built-in roles
#[endpoint {
    method = GET,
    path = "/roles",
    tags = ["roles"],
}]
async fn role_list(
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

/// Fetch a built-in role
#[endpoint {
    method = GET,
    path = "/roles/{role_name}",
    tags = ["roles"],
}]
async fn role_view(
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

/// List SSH public keys
///
/// Lists SSH public keys for the currently authenticated user.
#[endpoint {
    method = GET,
    path = "/session/me/sshkeys",
    tags = ["session"],
}]
async fn session_sshkey_list(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query_params: Query<PaginatedByName>,
) -> Result<HttpResponseOk<ResultsPage<SshKey>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("listing current user's ssh keys")?;
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

/// Create an SSH public key
///
/// Create an SSH public key for the currently authenticated user.
#[endpoint {
    method = POST,
    path = "/session/me/sshkeys",
    tags = ["session"],
}]
async fn session_sshkey_create(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    new_key: TypedBody<params::SshKeyCreate>,
) -> Result<HttpResponseCreated<SshKey>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("creating ssh key for current user")?;
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

/// Fetch an SSH public key
///
/// Fetch an SSH public key associated with the currently authenticated user.
#[endpoint {
    method = GET,
    path = "/session/me/sshkeys/{ssh_key_name}",
    tags = ["session"],
}]
async fn session_sshkey_view(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SshKeyPathParams>,
) -> Result<HttpResponseOk<SshKey>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let ssh_key_name = &path.ssh_key_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("fetching one of current user's ssh keys")?;
        let ssh_key =
            nexus.ssh_key_fetch(&opctx, actor.actor_id(), ssh_key_name).await?;
        Ok(HttpResponseOk(ssh_key.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an SSH public key
///
/// Delete an SSH public key associated with the currently authenticated user.
#[endpoint {
    method = DELETE,
    path = "/session/me/sshkeys/{ssh_key_name}",
    tags = ["session"],
}]
async fn session_sshkey_delete(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SshKeyPathParams>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let ssh_key_name = &path.ssh_key_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("deleting one of current user's ssh keys")?;
        nexus.ssh_key_delete(&opctx, actor.actor_id(), ssh_key_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for metrics requests where `/metrics/{metric_name}` is
/// appended to an existing path parameter type
#[derive(Deserialize, JsonSchema)]
struct MetricsPathParam<T, M> {
    #[serde(flatten)]
    inner: T,
    metric_name: M,
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
