// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for external HTTP APIs

use super::views::IpPool;
use super::views::IpPoolRange;
use super::{
    console_api, device_auth, params, views,
    views::{
        Certificate, GlobalImage, Group, IdentityProvider, Image, Organization,
        Project, Rack, Role, Silo, Sled, Snapshot, SshKey, User, UserBuiltin,
        Vpc, VpcRouter, VpcSubnet,
    },
};
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::model::Name;
use crate::external_api::shared;
use crate::ServerContext;
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
use dropshot::{
    channel, endpoint, WebsocketChannelResult, WebsocketConnection,
};
use ipnetwork::IpNetwork;
use omicron_common::api::external::http_pagination::data_page_params_for;
use omicron_common::api::external::http_pagination::marker_for_name;
use omicron_common::api::external::http_pagination::marker_for_name_or_id;
use omicron_common::api::external::http_pagination::name_or_id_pagination;
use omicron_common::api::external::http_pagination::PaginatedBy;
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
use omicron_common::api::external::NameOrId;
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
        api.register(system_policy_view)?;
        api.register(system_policy_update)?;

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

        api.register(organization_list_v1)?;
        api.register(organization_create_v1)?;
        api.register(organization_view_v1)?;
        api.register(organization_delete_v1)?;
        api.register(organization_update_v1)?;
        api.register(organization_policy_view_v1)?;
        api.register(organization_policy_update_v1)?;

        api.register(project_list)?;
        api.register(project_create)?;
        api.register(project_view)?;
        api.register(project_view_by_id)?;
        api.register(project_delete)?;
        api.register(project_update)?;
        api.register(project_policy_view)?;
        api.register(project_policy_update)?;

        api.register(project_list_v1)?;
        api.register(project_create_v1)?;
        api.register(project_view_v1)?;
        api.register(project_delete_v1)?;
        api.register(project_update_v1)?;
        api.register(project_policy_view_v1)?;
        api.register(project_policy_update_v1)?;

        // Operator-Accessible IP Pools API
        api.register(ip_pool_list)?;
        api.register(ip_pool_create)?;
        api.register(ip_pool_view)?;
        api.register(ip_pool_view_by_id)?;
        api.register(ip_pool_delete)?;
        api.register(ip_pool_update)?;
        // Variants for internal services
        api.register(ip_pool_service_view)?;

        // Operator-Accessible IP Pool Range API
        api.register(ip_pool_range_list)?;
        api.register(ip_pool_range_add)?;
        api.register(ip_pool_range_remove)?;
        // Variants for internal services
        api.register(ip_pool_service_range_list)?;
        api.register(ip_pool_service_range_add)?;
        api.register(ip_pool_service_range_remove)?;

        api.register(disk_list)?;
        api.register(disk_create)?;
        api.register(disk_view)?;
        api.register(disk_view_by_id)?;
        api.register(disk_delete)?;
        api.register(disk_metrics_list)?;

        api.register(disk_list_v1)?;
        api.register(disk_create_v1)?;
        api.register(disk_view_v1)?;
        api.register(disk_delete_v1)?;

        api.register(instance_list)?;
        api.register(instance_create)?;
        api.register(instance_view)?;
        api.register(instance_view_by_id)?;
        api.register(instance_delete)?;
        api.register(instance_migrate)?;
        api.register(instance_reboot)?;
        api.register(instance_start)?;
        api.register(instance_stop)?;
        api.register(instance_disk_list)?;
        api.register(instance_disk_attach)?;
        api.register(instance_disk_detach)?;
        api.register(instance_serial_console)?;
        api.register(instance_serial_console_stream)?;

        api.register(instance_list_v1)?;
        api.register(instance_view_v1)?;
        api.register(instance_create_v1)?;
        api.register(instance_delete_v1)?;
        api.register(instance_migrate_v1)?;
        api.register(instance_reboot_v1)?;
        api.register(instance_start_v1)?;
        api.register(instance_stop_v1)?;
        api.register(instance_disk_list_v1)?;
        api.register(instance_disk_attach_v1)?;
        api.register(instance_disk_detach_v1)?;
        api.register(instance_serial_console_v1)?;
        api.register(instance_serial_console_stream_v1)?;

        // Project-scoped images API
        api.register(image_list)?;
        api.register(image_create)?;
        api.register(image_view)?;
        api.register(image_view_by_id)?;
        api.register(image_delete)?;

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
        api.register(silo_view_by_id)?;
        api.register(silo_delete)?;
        api.register(silo_identity_provider_list)?;
        api.register(silo_policy_view)?;
        api.register(silo_policy_update)?;

        api.register(saml_identity_provider_create)?;
        api.register(saml_identity_provider_view)?;

        api.register(local_idp_user_create)?;
        api.register(local_idp_user_delete)?;
        api.register(local_idp_user_set_password)?;

        api.register(certificate_list)?;
        api.register(certificate_create)?;
        api.register(certificate_view)?;
        api.register(certificate_delete)?;

        api.register(system_image_list)?;
        api.register(system_image_create)?;
        api.register(system_image_view)?;
        api.register(system_image_view_by_id)?;
        api.register(system_image_delete)?;

        api.register(system_metric)?;
        api.register(updates_refresh)?;

        api.register(user_list)?;
        api.register(silo_users_list)?;
        api.register(silo_user_view)?;
        api.register(group_list)?;

        // Console API operations
        api.register(console_api::login_begin)?;
        api.register(console_api::login_local)?;
        api.register(console_api::login_spoof_begin)?;
        api.register(console_api::login_spoof)?;
        api.register(console_api::login_saml_begin)?;
        api.register(console_api::login_saml)?;
        api.register(console_api::logout)?;

        api.register(console_api::session_me)?;
        api.register(console_api::session_me_groups)?;
        api.register(console_api::console_page)?;
        api.register(console_api::console_root)?;
        api.register(console_api::console_settings_page)?;
        api.register(console_api::console_system_page)?;
        api.register(console_api::asset)?;

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
    path = "/system/policy",
    tags = ["policy"],
}]
async fn system_policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/policy",
    tags = ["policy"],
}]
async fn system_policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Fetch the current silo's IAM policy
#[endpoint {
    method = GET,
    path = "/policy",
    tags = ["silos"],
 }]
pub async fn policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("loading current silo")?;

        let lookup = nexus.db_lookup(&opctx).silo_id(authz_silo.id());
        let policy = nexus.silo_fetch_policy(&opctx, lookup).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update the current silo's IAM policy
#[endpoint {
    method = PUT,
    path = "/policy",
    tags = ["silos"],
}]
async fn policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_policy: TypedBody<shared::Policy<authz::SiloRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let new_policy = new_policy.into_inner();

    let handler = async {
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("loading current silo")?;
        let lookup = nexus.db_lookup(&opctx).silo_id(authz_silo.id());
        let policy =
            nexus.silo_update_policy(&opctx, lookup, &new_policy).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List silos
///
/// Lists silos that are discoverable based on the current permissions.
#[endpoint {
    method = GET,
    path = "/system/silos",
    tags = ["system"],
}]
async fn silo_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Silo>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let silos = nexus
            .silos_list(&opctx, &paginated_by)
            .await?
            .into_iter()
            .map(|p| p.try_into())
            .collect::<Result<Vec<_>, Error>>()?;
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
    path = "/system/silos",
    tags = ["system"],
}]
async fn silo_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_silo_params: TypedBody<params::SiloCreate>,
) -> Result<HttpResponseCreated<Silo>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let silo =
            nexus.silo_create(&opctx, new_silo_params.into_inner()).await?;
        Ok(HttpResponseCreated(silo.try_into()?))
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
    path = "/system/silos/{silo_name}",
    tags = ["system"],
}]
async fn silo_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SiloPathParam>,
) -> Result<HttpResponseOk<Silo>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let silo_name = &path.silo_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let silo = nexus.silo_fetch(&opctx, &silo_name).await?;
        Ok(HttpResponseOk(silo.try_into()?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a silo by id
#[endpoint {
    method = GET,
    path = "/system/by-id/silos/{id}",
    tags = ["system"]
}]
async fn silo_view_by_id(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Silo>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let silo = nexus.silo_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(silo.try_into()?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a silo
///
/// Delete a silo by name.
#[endpoint {
    method = DELETE,
    path = "/system/silos/{silo_name}",
    tags = ["system"],
}]
async fn silo_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/silos/{silo_name}/policy",
    tags = ["system"],
}]
async fn silo_policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SiloPathParam>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let silo_name = &path.silo_name;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let lookup = nexus.db_lookup(&opctx).silo_name(silo_name);
        let policy = nexus.silo_fetch_policy(&opctx, lookup).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a silo's IAM policy
#[endpoint {
    method = PUT,
    path = "/system/silos/{silo_name}/policy",
    tags = ["system"],
}]
async fn silo_policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
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
        let lookup = nexus.db_lookup(&opctx).silo_name(silo_name);
        let policy =
            nexus.silo_update_policy(&opctx, lookup, &new_policy).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo-specific user endpoints

/// List users in a silo
#[endpoint {
    method = GET,
    path = "/system/silos/{silo_name}/users/all",
    tags = ["system"],
}]
async fn silo_users_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SiloPathParam>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<User>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let silo_name = path_params.into_inner().silo_name;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&rqctx, &query)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let users = nexus
            .silo_list_users(&opctx, &silo_name, &pagparams)
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

/// Path parameters for Silo User requests
#[derive(Deserialize, JsonSchema)]
struct UserPathParam {
    /// The silo's unique name.
    silo_name: Name,
    /// The user's internal id
    user_id: Uuid,
}

/// Fetch a user
#[endpoint {
    method = GET,
    path = "/system/silos/{silo_name}/users/id/{user_id}",
    tags = ["system"],
}]
async fn silo_user_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<UserPathParam>,
) -> Result<HttpResponseOk<User>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path_params = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let user = nexus
            .silo_user_fetch(
                &opctx,
                &path_params.silo_name,
                path_params.user_id,
            )
            .await?;
        Ok(HttpResponseOk(user.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo identity providers

/// List a silo's IDPs
#[endpoint {
    method = GET,
    path = "/system/silos/{silo_name}/identity-providers",
    tags = ["system"],
}]
async fn silo_identity_provider_list(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/silos/{silo_name}/identity-providers/saml",
    tags = ["system"],
}]
async fn saml_identity_provider_create(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/silos/{silo_name}/identity-providers/saml/{provider_name}",
    tags = ["system"],
}]
async fn saml_identity_provider_view(
    rqctx: RequestContext<Arc<ServerContext>>,
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

// "Local" Identity Provider

/// Create a user
///
/// Users can only be created in Silos with `provision_type` == `Fixed`.
/// Otherwise, Silo users are just-in-time (JIT) provisioned when a user first
/// logs in using an external Identity Provider.
#[endpoint {
    method = POST,
    path = "/system/silos/{silo_name}/identity-providers/local/users",
    tags = ["system"],
}]
async fn local_idp_user_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SiloPathParam>,
    new_user_params: TypedBody<params::UserCreate>,
) -> Result<HttpResponseCreated<User>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let silo_name = path_params.into_inner().silo_name;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let user = nexus
            .local_idp_create_user(
                &opctx,
                &silo_name,
                new_user_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(user.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a user
#[endpoint {
    method = DELETE,
    path = "/system/silos/{silo_name}/identity-providers/local/users/{user_id}",
    tags = ["system"],
}]
async fn local_idp_user_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<UserPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path_params = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .local_idp_delete_user(
                &opctx,
                &path_params.silo_name,
                path_params.user_id,
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Set or invalidate a user's password
///
/// Passwords can only be updated for users in Silos with identity mode
/// `LocalOnly`.
#[endpoint {
    method = POST,
    path = "/system/silos/{silo_name}/identity-providers/local/users/{user_id}/set-password",
    tags = ["system"],
}]
async fn local_idp_user_set_password(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<UserPathParam>,
    update: TypedBody<params::UserPassword>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path_params = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .local_idp_user_set_password(
                &opctx,
                &path_params.silo_name,
                path_params.user_id,
                update.into_inner(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List organizations
#[endpoint {
    method = GET,
    path = "/v1/organizations",
    tags = ["organizations"]
}]
async fn organization_list_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Organization>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organizations = nexus
            .organizations_list(&opctx, &paginated_by)
            .await?
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

/// List organizations
/// Use `GET /v1/organizations` instead
#[endpoint {
    method = GET,
    path = "/organizations",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Organization>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organizations = nexus
            .organizations_list(&opctx, &paginated_by)
            .await?
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
    path = "/v1/organizations",
    tags = ["organizations"],
}]
async fn organization_create_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Create an organization
/// Use `POST /v1/organizations` instead
#[endpoint {
    method = POST,
    path = "/organizations",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_create(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Fetch an organization
#[endpoint {
    method = GET,
    path = "/v1/organizations/{organization}",
    tags = ["organizations"],
}]
async fn organization_view_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::OrganizationPath>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., organization) = nexus
            .organization_lookup(
                &opctx,
                &params::OrganizationSelector {
                    organization: path.organization,
                },
            )?
            .fetch()
            .await?;
        Ok(HttpResponseOk(organization.into()))
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
/// Use `GET /v1/organizations/{organization}` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., organization) = nexus
            .organization_lookup(
                &opctx,
                &params::OrganizationSelector {
                    organization: path.organization_name.into(),
                },
            )?
            .fetch()
            .await?;
        Ok(HttpResponseOk(organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch an organization by id
/// Use `GET /v1/organizations/{organization}` instead
#[endpoint {
    method = GET,
    path = "/by-id/organizations/{id}",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_view_by_id(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., organization) = nexus
            .organization_lookup(
                &opctx,
                &params::OrganizationSelector { organization: path.id.into() },
            )?
            .fetch()
            .await?;
        Ok(HttpResponseOk(organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an organization
#[endpoint {
    method = DELETE,
    path = "/v1/organizations/{organization}",
    tags = ["organizations"],
}]
async fn organization_delete_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::OrganizationPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector =
            &params::OrganizationSelector { organization: params.organization };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        nexus.organization_delete(&opctx, &organization_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an organization
/// Use `DELETE /v1/organizations/{organization}` instead
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector = &params::OrganizationSelector {
            organization: params.organization_name.into(),
        };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        nexus.organization_delete(&opctx, &organization_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update an organization
#[endpoint {
    method = PUT,
    path = "/v1/organizations/{organization}",
    tags = ["organizations"],
}]
async fn organization_update_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::OrganizationPath>,
    updated_organization: TypedBody<params::OrganizationUpdate>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector =
            &params::OrganizationSelector { organization: params.organization };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let new_organization = nexus
            .organization_update(
                &opctx,
                &organization_lookup,
                &updated_organization.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(new_organization.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update an organization
// TODO-correctness: Is it valid for PUT to accept application/json that's a
// subset of what the resource actually represents?  If not, is that a problem?
// (HTTP may require that this be idempotent.)  If so, can we get around that
// having this be a slightly different content-type (e.g.,
// "application/json-patch")?  We should see what other APIs do.
/// Use `PUT /v1/organizations/{organization}` instead
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<OrganizationPathParam>,
    updated_organization: TypedBody<params::OrganizationUpdate>,
) -> Result<HttpResponseOk<Organization>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector = &params::OrganizationSelector {
            organization: path.organization_name.into(),
        };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let new_organization = nexus
            .organization_update(
                &opctx,
                &organization_lookup,
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
    path = "/v1/organizations/{organization}/policy",
    tags = ["organizations"],
}]
async fn organization_policy_view_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::OrganizationPath>,
) -> Result<HttpResponseOk<shared::Policy<authz::OrganizationRole>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector =
            &params::OrganizationSelector { organization: params.organization };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let policy = nexus
            .organization_fetch_policy(&opctx, &organization_lookup)
            .await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch an organization's IAM policy
/// Use `GET /v1/organizations/{organization}/policy` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/policy",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseOk<shared::Policy<authz::OrganizationRole>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector = &params::OrganizationSelector {
            organization: path.organization_name.into(),
        };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let policy = nexus
            .organization_fetch_policy(&opctx, &organization_lookup)
            .await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update an organization's IAM policy
#[endpoint {
    method = PUT,
    path = "/v1/organizations/{organization}/policy",
    tags = ["organizations"],
}]
async fn organization_policy_update_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::OrganizationPath>,
    new_policy: TypedBody<shared::Policy<authz::OrganizationRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::OrganizationRole>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let params = path_params.into_inner();
    let new_policy = new_policy.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector =
            &params::OrganizationSelector { organization: params.organization };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let policy = nexus
            .organization_update_policy(
                &opctx,
                &organization_lookup,
                &new_policy,
            )
            .await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update an organization's IAM policy
/// Use `PUT /v1/organizations/{organization}/policy` instead
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/policy",
    tags = ["organizations"],
    deprecated = true
}]
async fn organization_policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<OrganizationPathParam>,
    new_policy: TypedBody<shared::Policy<authz::OrganizationRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::OrganizationRole>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_policy = new_policy.into_inner();
    let handler = async {
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector = &params::OrganizationSelector {
            organization: path.organization_name.into(),
        };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let policy = nexus
            .organization_update_policy(
                &opctx,
                &organization_lookup,
                &new_policy,
            )
            .await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List projects
#[endpoint {
    method = GET,
    path = "/v1/projects",
    tags = ["projects"],
}]
async fn project_list_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::OrganizationSelector>>,
) -> Result<HttpResponseOk<ResultsPage<Project>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_lookup =
            nexus.organization_lookup(&opctx, &scan_params.selector)?;
        let projects = nexus
            .project_list(&opctx, &organization_lookup, &paginated_by)
            .await?
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

/// List projects
/// Use `GET /v1/projects` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects",
    tags = ["projects"],
    deprecated = true,
}]
async fn project_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
    path_params: Path<OrganizationPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Project>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let path = path_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let organization_selector = &params::OrganizationSelector {
            organization: path.organization_name.into(),
        };
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let projects = nexus
            .project_list(&opctx, &organization_lookup, &paginated_by)
            .await?
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
    path = "/v1/projects",
    tags = ["projects"],
}]
async fn project_create_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OrganizationSelector>,
    new_project: TypedBody<params::ProjectCreate>,
) -> Result<HttpResponseCreated<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector =
            params::OrganizationSelector { organization: query.organization };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let project = nexus
            .project_create(
                &opctx,
                &organization_lookup,
                &new_project.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a project
/// Use `POST /v1/projects` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects",
    tags = ["projects"],
    deprecated = true
}]
async fn project_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<OrganizationPathParam>,
    new_project: TypedBody<params::ProjectCreate>,
) -> Result<HttpResponseCreated<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let organization_selector = &params::OrganizationSelector {
            organization: path.organization_name.into(),
        };
        let organization_lookup =
            nexus.organization_lookup(&opctx, &organization_selector)?;
        let project = nexus
            .project_create(
                &opctx,
                &organization_lookup,
                &new_project.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a project
#[endpoint {
    method = GET,
    path = "/v1/projects/{project}",
    tags = ["projects"],
}]
async fn project_view_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
    query_params: Query<params::OptionalOrganizationSelector>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector {
            organization_selector: query.organization_selector,
            project: path.project,
        };
        let (.., project) =
            nexus.project_lookup(&opctx, &project_selector)?.fetch().await?;
        Ok(HttpResponseOk(project.into()))
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
/// Use `GET /v1/projects/{project}` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
    deprecated = true
}]
async fn project_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let (.., project) =
            nexus.project_lookup(&opctx, &project_selector)?.fetch().await?;
        Ok(HttpResponseOk(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a project by id
/// Use `GET /v1/projects/{project}` instead
#[endpoint {
    method = GET,
    path = "/by-id/projects/{id}",
    tags = ["projects"],
    deprecated = true
}]
async fn project_view_by_id(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector =
            params::ProjectSelector::new(None, path.id.into());
        let (.., project) =
            nexus.project_lookup(&opctx, &project_selector)?.fetch().await?;
        Ok(HttpResponseOk(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a project
#[endpoint {
    method = DELETE,
    path = "/v1/projects/{project}",
    tags = ["projects"],
}]
async fn project_delete_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
    query_params: Query<params::OptionalOrganizationSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector {
            organization_selector: query.organization_selector,
            project: path.project,
        };
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        nexus.project_delete(&opctx, &project_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a project
/// Use `DELETE /v1/projects/{project}` instead
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
    deprecated = true
}]
async fn project_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        nexus.project_delete(&opctx, &project_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a project
#[endpoint {
    method = PUT,
    path = "/v1/projects/{project}",
    tags = ["projects"],
}]
async fn project_update_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
    query_params: Query<params::OptionalOrganizationSelector>,
    updated_project: TypedBody<params::ProjectUpdate>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let updated_project = updated_project.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector {
            organization_selector: query.organization_selector,
            project: path.project,
        };
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let project = nexus
            .project_update(&opctx, &project_lookup, &updated_project)
            .await?;
        Ok(HttpResponseOk(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a project
// TODO-correctness: Is it valid for PUT to accept application/json that's a
// subset of what the resource actually represents?  If not, is that a problem?
// (HTTP may require that this be idempotent.)  If so, can we get around that
// having this be a slightly different content-type (e.g.,
// "application/json-patch")?  We should see what other APIs do.
/// Use `PUT /v1/projects/{project}` instead
#[endpoint {
    method = PUT,
    path = "/organizations/{organization_name}/projects/{project_name}",
    tags = ["projects"],
    deprecated = true
}]
async fn project_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
    updated_project: TypedBody<params::ProjectUpdate>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let new_project = nexus
            .project_update(
                &opctx,
                &project_lookup,
                &updated_project.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(new_project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a project's IAM policy
#[endpoint {
    method = GET,
    path = "/v1/projects/{project}/policy",
    tags = ["projects"],
}]
async fn project_policy_view_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
    query_params: Query<params::OptionalOrganizationSelector>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector {
            organization_selector: query.organization_selector,
            project: path.project,
        };
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let policy =
            nexus.project_fetch_policy(&opctx, &project_lookup).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a project's IAM policy
/// Use `GET /v1/projects/{project}/policy` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/policy",
    tags = ["projects"],
    deprecated = true
}]
async fn project_policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let policy =
            nexus.project_fetch_policy(&opctx, &project_lookup).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a project's IAM policy
#[endpoint {
    method = PUT,
    path = "/v1/projects/{project}/policy",
    tags = ["projects"],
}]
async fn project_policy_update_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
    query_params: Query<params::OptionalOrganizationSelector>,
    new_policy: TypedBody<shared::Policy<authz::ProjectRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let new_policy = new_policy.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector {
            organization_selector: query.organization_selector,
            project: path.project,
        };
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        nexus
            .project_update_policy(&opctx, &project_lookup, &new_policy)
            .await?;
        Ok(HttpResponseOk(new_policy))
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
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
    new_policy: TypedBody<shared::Policy<authz::ProjectRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_policy = new_policy.into_inner();
    let handler = async {
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let policy = nexus
            .project_update_policy(&opctx, &project_lookup, &new_policy)
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
    path = "/system/ip-pools",
    tags = ["system"],
}]
async fn ip_pool_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<IpPool>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let pools = nexus
            .ip_pools_list(&opctx, &paginated_by)
            .await?
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
    path = "/system/ip-pools",
    tags = ["system"],
}]
async fn ip_pool_create(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/ip-pools/{pool_name}",
    tags = ["system"],
}]
async fn ip_pool_view(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Fetch an IP pool by id
#[endpoint {
    method = GET,
    path = "/system/by-id/ip-pools/{id}",
    tags = ["system"],
}]
async fn ip_pool_view_by_id(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let pool = nexus.ip_pool_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(IpPool::from(pool)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an IP Pool
#[endpoint {
    method = DELETE,
    path = "/system/ip-pools/{pool_name}",
    tags = ["system"],
}]
async fn ip_pool_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/ip-pools/{pool_name}",
    tags = ["system"],
}]
async fn ip_pool_update(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Fetch the IP pool used for Oxide services.
#[endpoint {
    method = GET,
    path = "/system/ip-pools-service",
    tags = ["system"],
}]
async fn ip_pool_service_view(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let pool = nexus.ip_pool_service_fetch(&opctx).await?;
        Ok(HttpResponseOk(IpPool::from(pool)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

type IpPoolRangePaginationParams = PaginationParams<EmptyScanParams, IpNetwork>;

/// List ranges for an IP pool
///
/// Ranges are ordered by their first address.
#[endpoint {
    method = GET,
    path = "/system/ip-pools/{pool_name}/ranges",
    tags = ["system"],
}]
async fn ip_pool_range_list(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/ip-pools/{pool_name}/ranges/add",
    tags = ["system"],
}]
async fn ip_pool_range_add(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/ip-pools/{pool_name}/ranges/remove",
    tags = ["system"],
}]
async fn ip_pool_range_remove(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// List ranges for the IP pool used for Oxide services.
///
/// Ranges are ordered by their first address.
#[endpoint {
    method = GET,
    path = "/system/ip-pools-service/ranges",
    tags = ["system"],
}]
async fn ip_pool_service_range_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<IpPoolRangePaginationParams>,
) -> Result<HttpResponseOk<ResultsPage<IpPoolRange>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
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
            .ip_pool_service_list_ranges(&opctx, &pag_params)
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
    path = "/system/ip-pools-service/ranges/add",
    tags = ["system"],
}]
async fn ip_pool_service_range_add(
    rqctx: RequestContext<Arc<ServerContext>>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
    let apictx = &rqctx.context();
    let nexus = &apictx.nexus;
    let range = range_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let out = nexus.ip_pool_service_add_range(&opctx, &range).await?;
        Ok(HttpResponseCreated(out.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Remove a range from an IP pool used for Oxide services.
#[endpoint {
    method = POST,
    path = "/system/ip-pools-service/ranges/remove",
    tags = ["system"],
}]
async fn ip_pool_service_range_remove(
    rqctx: RequestContext<Arc<ServerContext>>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context();
    let nexus = &apictx.nexus;
    let range = range_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus.ip_pool_service_delete_range(&opctx, &range).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Disks

/// List disks
#[endpoint {
    method = GET,
    path = "/v1/disks",
    tags = ["disks"],
}]
async fn disk_list_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_lookup =
            nexus.project_lookup(&opctx, &scan_params.selector)?;
        let disks = nexus
            .disk_list(&opctx, &project_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|disk| disk.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            disks,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List disks
/// Use `GET /v1/disks` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/disks",
    tags = ["disks"],
    deprecated = true
}]
async fn disk_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let path = path_params.into_inner();
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let authz_project = nexus.project_lookup(&opctx, &project_selector)?;
        let disks = nexus
            .disk_list(
                &opctx,
                &authz_project,
                &PaginatedBy::Name(data_page_params_for(&rqctx, &query)?),
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
#[endpoint {
    method = POST,
    path = "/v1/disks",
    tags = ["disks"]
}]
async fn disk_create_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::ProjectSelector>,
    new_disk: TypedBody<params::DiskCreate>,
) -> Result<HttpResponseCreated<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let params = new_disk.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_lookup = nexus.project_lookup(&opctx, &query)?;
        let disk =
            nexus.project_create_disk(&opctx, &project_lookup, &params).await?;
        Ok(HttpResponseCreated(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// TODO-correctness See note about instance create.  This should be async.
/// Use `POST /v1/disks` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/disks",
    tags = ["disks"],
    deprecated = true
}]
async fn disk_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
    new_disk: TypedBody<params::DiskCreate>,
) -> Result<HttpResponseCreated<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_disk_params = &new_disk.into_inner();
    let project_selector = params::ProjectSelector::new(
        Some(path.organization_name.into()),
        path.project_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let disk = nexus
            .project_create_disk(&opctx, &project_lookup, &new_disk_params)
            .await?;
        Ok(HttpResponseCreated(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a disk
#[endpoint {
    method = GET,
    path = "/v1/disks/{disk}",
    tags = ["disks"]
}]
async fn disk_view_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseOk<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let disk_selector = params::DiskSelector {
        disk: path.disk,
        project_selector: query.project_selector,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., disk) =
            nexus.disk_lookup(&opctx, &disk_selector)?.fetch().await?;
        Ok(HttpResponseOk(disk.into()))
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
/// Use `GET /v1/disks/{disk}` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
    tags = ["disks"],
    deprecated = true
}]
async fn disk_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseOk<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let disk_selector = params::DiskSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.disk_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., disk) =
            nexus.disk_lookup(&opctx, &disk_selector)?.fetch().await?;
        Ok(HttpResponseOk(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a disk by id
/// Use `GET /v1/disks/{disk}` instead
#[endpoint {
    method = GET,
    path = "/by-id/disks/{id}",
    tags = ["disks"],
    deprecated = true
}]
async fn disk_view_by_id(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let disk_selector = params::DiskSelector::new(None, None, path.id.into());
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., disk) =
            nexus.disk_lookup(&opctx, &disk_selector)?.fetch().await?;
        Ok(HttpResponseOk(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a disk
#[endpoint {
    method = DELETE,
    path = "/v1/disks/{disk}",
    tags = ["disks"],
}]
async fn disk_delete_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let disk_selector = params::DiskSelector {
        disk: path.disk,
        project_selector: query.project_selector,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disk_lookup = nexus.disk_lookup(&opctx, &disk_selector)?;
        nexus.project_delete_disk(&opctx, &disk_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Use `DELETE /v1/disks/{disk}` instead
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/disks/{disk_name}",
    tags = ["disks"],
    deprecated = true
}]
async fn disk_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let disk_selector = params::DiskSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.disk_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let disk_lookup = nexus.disk_lookup(&opctx, &disk_selector)?;
        nexus.project_delete_disk(&opctx, &disk_lookup).await?;
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
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<MetricsPathParam<DiskPathParam, DiskMetricName>>,
    query_params: Query<
        PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
    >,
) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let limit = rqctx.page_limit(&query)?;
        let disk_selector = params::DiskSelector::new(
            Some(path.inner.organization_name.into()),
            Some(path.inner.project_name.into()),
            path.inner.disk_name.into(),
        );
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., authz_disk) = nexus
            .disk_lookup(&opctx, &disk_selector)?
            .lookup_for(authz::Action::Read)
            .await?;

        let result = nexus
            .select_timeseries(
                &format!("crucible_upstairs:{}", path.metric_name),
                &[&format!("upstairs_uuid=={}", authz_disk.id())],
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
    path = "/v1/instances",
    tags = ["instances"],
}]
async fn instance_list_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
) -> Result<HttpResponseOk<ResultsPage<Instance>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_lookup =
            nexus.project_lookup(&opctx, &scan_params.selector)?;
        let instances = nexus
            .instance_list(&opctx, &project_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            instances,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List instances
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances",
    tags = ["instances"],
}]
async fn instance_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<ProjectPathParam>,
) -> Result<HttpResponseOk<ResultsPage<Instance>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let paginated_by = PaginatedBy::Name(pag_params);
        let path = path_params.into_inner();
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let instances = nexus
            .instance_list(&opctx, &project_lookup, &paginated_by)
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
#[endpoint {
    method = POST,
    path = "/v1/instances",
    tags = ["instances"],
}]
async fn instance_create_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::ProjectSelector>,
    new_instance: TypedBody<params::InstanceCreate>,
) -> Result<HttpResponseCreated<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let project_selector = query_params.into_inner();
    let new_instance_params = &new_instance.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let instance = nexus
            .project_create_instance(
                &opctx,
                &project_lookup,
                &new_instance_params,
            )
            .await?;
        Ok(HttpResponseCreated(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create an instance
/// Use `POST /v1/instances` instead
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
    deprecated = true,
}]
async fn instance_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
    new_instance: TypedBody<params::InstanceCreate>,
) -> Result<HttpResponseCreated<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_instance_params = &new_instance.into_inner();
    let project_selector = params::ProjectSelector::new(
        Some(path.organization_name.into()),
        path.project_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let instance = nexus
            .project_create_instance(
                &opctx,
                &project_lookup,
                &new_instance_params,
            )
            .await?;
        Ok(HttpResponseCreated(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch an instance
#[endpoint {
    method = GET,
    path = "/v1/instances/{instance}",
    tags = ["instances"],
}]
async fn instance_view_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_selector = params::InstanceSelector {
            project_selector: query.project_selector,
            instance: path.instance,
        };
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let (.., instance) = instance_lookup.fetch().await?;
        Ok(HttpResponseOk(instance.into()))
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
/// Use `GET /v1/instances/{instance}` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let (.., instance) = instance_lookup.fetch().await?;
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
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., instance) = nexus
            .instance_lookup(
                &opctx,
                &params::InstanceSelector::new(None, None, path.id.into()),
            )?
            .fetch()
            .await?;
        Ok(HttpResponseOk(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an instance
#[endpoint {
    method = DELETE,
    path = "/v1/instances/{instance}",
    tags = ["instances"],
}]
async fn instance_delete_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector,
        instance: path.instance,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        nexus.project_destroy_instance(&opctx, &instance_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an instance
#[endpoint {
    method = DELETE,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        nexus.project_destroy_instance(&opctx, &instance_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// TODO should this be in the public API?
/// Migrate an instance
#[endpoint {
    method = POST,
    path = "/v1/instances/{instance}/migrate",
    tags = ["instances"],
}]
async fn instance_migrate_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
    migrate_params: TypedBody<params::InstanceMigrate>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let migrate_instance_params = migrate_params.into_inner();
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector,
        instance: path.instance,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus
            .project_instance_migrate(
                &opctx,
                &instance_lookup,
                migrate_instance_params,
            )
            .await?;
        Ok(HttpResponseOk(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// TODO should this be in the public API?
/// Migrate an instance
/// Use `POST /v1/instances/{instance}/migrate` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/migrate",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_migrate(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
    migrate_params: TypedBody<params::InstanceMigrate>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let migrate_instance_params = migrate_params.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus
            .project_instance_migrate(
                &opctx,
                &instance_lookup,
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
    path = "/v1/instances/{instance}/reboot",
    tags = ["instances"],
}]
async fn instance_reboot_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector,
        instance: path.instance,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus.instance_reboot(&opctx, &instance_lookup).await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Reboot an instance
/// Use `POST /v1/instances/{instance}/reboot` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/reboot",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_reboot(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus.instance_reboot(&opctx, &instance_lookup).await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Boot an instance
#[endpoint {
    method = POST,
    path = "/v1/instances/{instance}/start",
    tags = ["instances"],
}]
async fn instance_start_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector,
        instance: path.instance,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus.instance_start(&opctx, &instance_lookup).await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Boot an instance
/// Use `POST /v1/instances/{instance}/start` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/start",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_start(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus.instance_start(&opctx, &instance_lookup).await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Stop an instance
#[endpoint {
    method = POST,
    path = "/v1/instances/{instance}/stop",
    tags = ["instances"],
}]
async fn instance_stop_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector,
        instance: path.instance,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus.instance_stop(&opctx, &instance_lookup).await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Halt an instance
/// Use `POST /v1/instances/{instance}/stop` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/stop",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_stop(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseAccepted<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let instance = nexus.instance_stop(&opctx, &instance_lookup).await?;
        Ok(HttpResponseAccepted(instance.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch an instance's serial console
#[endpoint {
    method = GET,
    path = "/v1/instances/{instance}/serial-console",
    tags = ["instances"],
}]
async fn instance_serial_console_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::InstancePath>,
    query_params: Query<params::InstanceSerialConsoleRequest>,
    selector_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseOk<params::InstanceSerialConsoleData>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let selector = selector_params.into_inner();
    let instance_selector = params::InstanceSelector {
        project_selector: selector.project_selector,
        instance: path.instance,
    };
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let data = nexus
            .instance_serial_console_data(&instance_lookup, &query)
            .await?;
        Ok(HttpResponseOk(data))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch an instance's serial console
/// Use `GET /v1/instances/{instance}/serial-console` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/serial-console",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_serial_console(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
    query_params: Query<params::InstanceSerialConsoleRequest>,
) -> Result<HttpResponseOk<params::InstanceSerialConsoleData>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let data = nexus
            .instance_serial_console_data(
                &instance_lookup,
                &query_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseOk(data))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Stream an instance's serial console
#[channel {
    protocol = WEBSOCKETS,
    path = "/v1/instances/{instance}/serial-console/stream",
    tags = ["instances"],
}]
async fn instance_serial_console_stream_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::InstancePath>,
    query_params: Query<params::OptionalProjectSelector>,
    conn: WebsocketConnection,
) -> WebsocketChannelResult {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let opctx = OpContext::for_external_api(&rqctx).await?;
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector,
        instance: path.instance,
    };
    let instance_lookup = nexus.instance_lookup(&opctx, &instance_selector)?;
    nexus.instance_serial_console_stream(conn, &instance_lookup).await?;
    Ok(())
}

/// Connect to an instance's serial console
/// Use `GET /v1/instances/{instance}/serial-console/stream` instead
#[channel {
    protocol = WEBSOCKETS,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/serial-console/stream",
    tags = ["instances"],
    deprecated = true,
}]
async fn instance_serial_console_stream(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
    conn: WebsocketConnection,
) -> WebsocketChannelResult {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let opctx = OpContext::for_external_api(&rqctx).await?;
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.into()),
        Some(path.project_name.into()),
        path.instance_name.into(),
    );
    let instance_lookup = nexus.instance_lookup(&opctx, &instance_selector)?;
    nexus.instance_serial_console_stream(conn, &instance_lookup).await?;
    Ok(())
}

#[endpoint {
    method = GET,
    path = "/v1/instances/{instance}/disks",
    tags = ["instances"],
}]
async fn instance_disk_list_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::OptionalProjectSelector>>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_selector = params::InstanceSelector {
            project_selector: scan_params.selector.project_selector.clone(),
            instance: path.instance,
        };
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let disks = nexus
            .instance_list_disks(&opctx, &instance_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            disks,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List an instance's disks
/// Use `GET /v1/instances/{instance}/disks` instead
#[endpoint {
    method = GET,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks",
    tags = ["instances"],
    deprecated = true
}]
async fn instance_disk_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByName>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let path = path_params.into_inner();
        let instance_selector = params::InstanceSelector::new(
            Some(path.organization_name.into()),
            Some(path.project_name.into()),
            path.instance_name.into(),
        );
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let disks = nexus
            .instance_list_disks(
                &opctx,
                &instance_lookup,
                &PaginatedBy::Name(pag_params),
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
    path = "/v1/instances/{instance}/disks/attach",
    tags = ["instances"],
}]
async fn instance_disk_attach_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::InstancePath>,
    query_params: Query<params::OptionalProjectSelector>,
    disk_to_attach: TypedBody<params::DiskPath>,
) -> Result<HttpResponseAccepted<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let disk = disk_to_attach.into_inner().disk;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_selector = params::InstanceSelector {
            project_selector: query.project_selector,
            instance: path.instance,
        };
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let disk =
            nexus.instance_attach_disk(&opctx, &instance_lookup, disk).await?;
        Ok(HttpResponseAccepted(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Attach a disk to an instance
/// Use `POST /v1/instances/{instance}/disks/attach` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/attach",
    tags = ["instances"],
    deprecated = true
}]
async fn instance_disk_attach(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
    disk_to_attach: TypedBody<params::DiskIdentifier>,
) -> Result<HttpResponseAccepted<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let disk = disk_to_attach.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.clone().into()),
        Some(path.project_name.clone().into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let disk = nexus
            .instance_attach_disk(&opctx, &instance_lookup, disk.name.into())
            .await?;
        Ok(HttpResponseAccepted(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[endpoint {
    method = POST,
    path = "/v1/instances/{instance}/disks/detach",
    tags = ["instances"],
}]
async fn instance_disk_detach_v1(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::InstancePath>,
    query_params: Query<params::OptionalProjectSelector>,
    disk_to_detach: TypedBody<params::DiskPath>,
) -> Result<HttpResponseAccepted<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let disk = disk_to_detach.into_inner().disk;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_selector = params::InstanceSelector {
            project_selector: query.project_selector,
            instance: path.instance,
        };
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let disk =
            nexus.instance_detach_disk(&opctx, &instance_lookup, disk).await?;
        Ok(HttpResponseAccepted(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Detach a disk from an instance
/// Use `POST /v1/disks/{disk}/detach` instead
#[endpoint {
    method = POST,
    path = "/organizations/{organization_name}/projects/{project_name}/instances/{instance_name}/disks/detach",
    tags = ["instances"],
    deprecated = true
}]
async fn instance_disk_detach(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<InstancePathParam>,
    disk_to_detach: TypedBody<params::DiskIdentifier>,
) -> Result<HttpResponseAccepted<Disk>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let disk = disk_to_detach.into_inner();
    let instance_selector = params::InstanceSelector::new(
        Some(path.organization_name.clone().into()),
        Some(path.project_name.clone().into()),
        path.instance_name.into(),
    );
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let disk = nexus
            .instance_detach_disk(&opctx, &instance_lookup, disk.name.into())
            .await?;
        Ok(HttpResponseAccepted(disk.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Certificates

/// List system-wide certificates
///
/// Returns a list of all the system-wide certificates. System-wide certificates
/// are returned sorted by creation date, with the most recent certificates
/// appearing first.
#[endpoint {
    method = GET,
    path = "/system/certificates",
    tags = ["system"],
}]
async fn certificate_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByName>,
) -> Result<HttpResponseOk<ResultsPage<Certificate>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let certs = nexus
            .certificates_list(
                &opctx,
                &data_page_params_for(&rqctx, &query)?
                    .map_name(|n| Name::ref_cast(n)),
            )
            .await?
            .into_iter()
            .map(|d| d.try_into())
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(HttpResponseOk(ScanByName::results_page(
            &query,
            certs,
            &marker_for_name,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a new system-wide x.509 certificate.
///
/// This certificate is automatically used by the Oxide Control plane to serve
/// external connections.
#[endpoint {
    method = POST,
    path = "/system/certificates",
    tags = ["system"]
}]
async fn certificate_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_cert: TypedBody<params::CertificateCreate>,
) -> Result<HttpResponseCreated<Certificate>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let new_cert_params = new_cert.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let cert = nexus.certificate_create(&opctx, new_cert_params).await?;
        Ok(HttpResponseCreated(cert.try_into()?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Certificate requests
#[derive(Deserialize, JsonSchema)]
struct CertificatePathParam {
    certificate: NameOrId,
}

/// Fetch a certificate
///
/// Returns the details of a specific certificate
#[endpoint {
    method = GET,
    path = "/system/certificates/{certificate}",
    tags = ["system"],
}]
async fn certificate_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<CertificatePathParam>,
) -> Result<HttpResponseOk<Certificate>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let (.., cert) =
            nexus.certificate_lookup(&opctx, &path.certificate).fetch().await?;
        Ok(HttpResponseOk(cert.try_into()?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a certificate
///
/// Permanently delete a certificate. This operation cannot be undone.
#[endpoint {
    method = DELETE,
    path = "/system/certificates/{certificate}",
    tags = ["system"],
}]
async fn certificate_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<CertificatePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        nexus
            .certificate_delete(
                &opctx,
                nexus.certificate_lookup(&opctx, &path.certificate),
            )
            .await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Images

/// List system-wide images
///
/// Returns a list of all the system-wide images. System-wide images are returned sorted
/// by creation date, with the most recent images appearing first.
#[endpoint {
    method = GET,
    path = "/system/images",
    tags = ["system"],
}]
async fn system_image_list(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Create a system-wide image
///
/// Create a new system-wide image. This image can then be used by any user in any silo as a
/// base for instances.
#[endpoint {
    method = POST,
    path = "/system/images",
    tags = ["system"]
}]
async fn system_image_create(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Fetch a system-wide image
///
/// Returns the details of a specific system-wide image.
#[endpoint {
    method = GET,
    path = "/system/images/{image_name}",
    tags = ["system"],
}]
async fn system_image_view(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Fetch a system-wide image by id
#[endpoint {
    method = GET,
    path = "/system/by-id/images/{id}",
    tags = ["system"],
}]
async fn system_image_view_by_id(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Delete a system-wide image
///
/// Permanently delete a system-wide image. This operation cannot be undone. Any
/// instances using the system-wide image will continue to run, however new instances
/// can not be created with this image.
#[endpoint {
    method = DELETE,
    path = "/system/images/{image_name}",
    tags = ["system"],
}]
async fn system_image_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ProjectPathParam>,
    new_vpc: TypedBody<params::VpcCreate>,
) -> Result<HttpResponseCreated<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_vpc_params = &new_vpc.into_inner();
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let project_selector = params::ProjectSelector::new(
            Some(path.organization_name.into()),
            path.project_name.into(),
        );
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let vpc = nexus
            .project_create_vpc(&opctx, &project_lookup, &new_vpc_params)
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/hardware/racks",
    tags = ["system"],
}]
async fn rack_list(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/hardware/racks/{rack_id}",
    tags = ["system"],
}]
async fn rack_view(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/hardware/sleds",
    tags = ["system"],
}]
async fn sled_list(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/hardware/sleds/{sled_id}",
    tags = ["system"],
}]
async fn sled_view(
    rqctx: RequestContext<Arc<ServerContext>>,
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
     path = "/system/updates/refresh",
     tags = ["system"],
}]
async fn updates_refresh(
    rqctx: RequestContext<Arc<ServerContext>>,
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

// Metrics

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SystemMetricParams {
    #[serde(flatten)]
    pub pagination: dropshot::PaginationParams<
        params::ResourceMetrics,
        params::ResourceMetrics,
    >,

    /// The UUID of the container being queried
    // TODO: I might want to force the caller to specify type here?
    pub id: Uuid,
}

#[derive(Display, Deserialize, JsonSchema)]
#[display(style = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum SystemMetricName {
    VirtualDiskSpaceProvisioned,
    CpusProvisioned,
    RamProvisioned,
}

#[derive(Deserialize, JsonSchema)]
struct SystemMetricsPathParam {
    metric_name: SystemMetricName,
}

/// Access metrics data
#[endpoint {
     method = GET,
     path = "/system/metrics/{metric_name}",
     tags = ["system"],
}]
async fn system_metric(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SystemMetricsPathParam>,
    query_params: Query<SystemMetricParams>,
) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let metric_name = path_params.into_inner().metric_name;

    let query = query_params.into_inner();
    let limit = rqctx.page_limit(&query.pagination)?;

    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let result = nexus
            .system_metric_lookup(&opctx, metric_name, query, limit)
            .await?;

        Ok(HttpResponseOk(result))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Sagas

/// List sagas
#[endpoint {
    method = GET,
    path = "/system/sagas",
    tags = ["system"],
}]
async fn saga_list(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    path = "/system/sagas/{saga_id}",
    tags = ["system"],
}]
async fn saga_view(
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<User>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let users = nexus
            .silo_users_list_current(&opctx, &pagparams)
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

// Silo groups

/// List groups
#[endpoint {
    method = GET,
    path = "/groups",
    tags = ["silos"],
}]
async fn group_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Group>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let handler = async {
        let opctx = OpContext::for_external_api(&rqctx).await?;
        let groups = nexus
            .silo_groups_list(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            groups,
            &|_, group: &Group| group.id,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
struct BuiltinUserPathParam {
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
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<BuiltinUserPathParam>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
    rqctx: RequestContext<Arc<ServerContext>>,
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
