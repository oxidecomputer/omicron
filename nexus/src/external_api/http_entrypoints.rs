// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for external HTTP APIs

use super::{
    console_api, device_auth, params,
    views::{
        self, Certificate, GlobalImage, Group, IdentityProvider, Image, IpPool,
        IpPoolRange, PhysicalDisk, Project, Rack, Role, Silo, Sled, Snapshot,
        SshKey, User, UserBuiltin, Vpc, VpcRouter, VpcSubnet,
    },
};
use crate::authz;
use crate::db;
use crate::db::identity::Resource;
use crate::db::model::Name;
use crate::external_api::shared;
use crate::ServerContext;
use chrono::Utc;
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
use nexus_types::identity::AssetIdentityMetadata;
use omicron_common::api::external::http_pagination::data_page_params_for;
use omicron_common::api::external::http_pagination::marker_for_name;
use omicron_common::api::external::http_pagination::marker_for_name_or_id;
use omicron_common::api::external::http_pagination::name_or_id_pagination;
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
use omicron_common::api::external::RouterRouteKind;
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

        api.register(project_list)?;
        api.register(project_create)?;
        api.register(project_view)?;
        api.register(project_delete)?;
        api.register(project_update)?;
        api.register(project_policy_view)?;
        api.register(project_policy_update)?;

        // Operator-Accessible IP Pools API
        api.register(ip_pool_list)?;
        api.register(ip_pool_create)?;
        api.register(ip_pool_view)?;
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
        api.register(disk_delete)?;
        api.register(disk_metrics_list)?;

        api.register(disk_bulk_write_import_start)?;
        api.register(disk_bulk_write_import)?;
        api.register(disk_bulk_write_import_stop)?;
        api.register(disk_import_blocks_from_url)?;
        api.register(disk_finalize_import)?;

        api.register(instance_list)?;
        api.register(instance_view)?;
        api.register(instance_create)?;
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

        api.register(image_list)?;
        api.register(image_create)?;
        api.register(image_view)?;
        api.register(image_delete)?;

        api.register(snapshot_list)?;
        api.register(snapshot_create)?;
        api.register(snapshot_view)?;
        api.register(snapshot_delete)?;

        api.register(vpc_list)?;
        api.register(vpc_create)?;
        api.register(vpc_view)?;
        api.register(vpc_update)?;
        api.register(vpc_delete)?;

        api.register(vpc_subnet_list)?;
        api.register(vpc_subnet_view)?;
        api.register(vpc_subnet_create)?;
        api.register(vpc_subnet_delete)?;
        api.register(vpc_subnet_update)?;
        api.register(vpc_subnet_list_network_interfaces)?;

        api.register(instance_network_interface_create)?;
        api.register(instance_network_interface_list)?;
        api.register(instance_network_interface_view)?;
        api.register(instance_network_interface_update)?;
        api.register(instance_network_interface_delete)?;

        api.register(instance_external_ip_list)?;

        api.register(vpc_router_list)?;
        api.register(vpc_router_view)?;
        api.register(vpc_router_create)?;
        api.register(vpc_router_delete)?;
        api.register(vpc_router_update)?;

        api.register(vpc_router_route_list)?;
        api.register(vpc_router_route_view)?;
        api.register(vpc_router_route_create)?;
        api.register(vpc_router_route_delete)?;
        api.register(vpc_router_route_update)?;

        api.register(vpc_firewall_rules_view)?;
        api.register(vpc_firewall_rules_update)?;

        api.register(rack_list)?;
        api.register(rack_view)?;
        api.register(sled_list)?;
        api.register(sled_view)?;
        api.register(sled_physical_disk_list)?;
        api.register(physical_disk_list)?;

        api.register(saga_list)?;
        api.register(saga_view)?;

        api.register(user_builtin_list)?;
        api.register(user_builtin_view)?;

        api.register(role_list)?;
        api.register(role_view)?;

        api.register(current_user_view)?;
        api.register(current_user_groups)?;
        api.register(current_user_ssh_key_list)?;
        api.register(current_user_ssh_key_view)?;
        api.register(current_user_ssh_key_create)?;
        api.register(current_user_ssh_key_delete)?;

        // Fleet-wide API operations
        api.register(silo_list)?;
        api.register(silo_create)?;
        api.register(silo_view)?;
        api.register(silo_delete)?;
        api.register(silo_policy_view)?;
        api.register(silo_policy_update)?;

        api.register(silo_identity_provider_list)?;

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

        api.register(system_update_refresh)?;
        api.register(system_version)?;
        api.register(system_component_version_list)?;
        api.register(system_update_list)?;
        api.register(system_update_view)?;
        api.register(system_update_start)?;
        api.register(system_update_stop)?;
        api.register(system_update_components_list)?;
        api.register(update_deployments_list)?;
        api.register(update_deployment_view)?;

        api.register(user_list)?;
        api.register(silo_user_list)?;
        api.register(silo_user_view)?;
        api.register(group_list)?;
        api.register(group_view)?;

        // Console API operations
        api.register(console_api::login_begin)?;
        api.register(console_api::login_local)?;
        api.register(console_api::login_spoof_begin)?;
        api.register(console_api::login_spoof)?;
        api.register(console_api::login_saml_begin)?;
        api.register(console_api::login_saml)?;
        api.register(console_api::logout)?;

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
// Generally, HTTP resources are grouped within some collection. For a
// relatively simple example:
//
//   GET    v1/projects                (list the projects in the collection)
//   POST   v1/projects                (create a project in the collection)
//   GET    v1/projects/{project}      (look up a project in the collection)
//   DELETE v1/projects/{project}      (delete a project in the collection)
//   PUT    v1/projects/{project}      (update a project in the collection)
//
// We pick a name for the function that implements a given API entrypoint
// based on how we expect it to appear in the CLI subcommand hierarchy. For
// example:
//
//   GET    v1/projects                 -> project_list()
//   POST   v1/projects                 -> project_create()
//   GET    v1/projects/{project}       -> project_view()
//   DELETE v1/projects/{project}       -> project_delete()
//   PUT    v1/projects/{project}       -> project_update()
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
    path = "/v1/system/policy",
    tags = ["policy"],
}]
async fn system_policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<shared::Policy<authz::FleetRole>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/policy",
    tags = ["policy"],
}]
async fn system_policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_policy: TypedBody<shared::Policy<authz::FleetRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::FleetRole>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let new_policy = new_policy.into_inner();
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let policy = nexus.fleet_update_policy(&opctx, &new_policy).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the current silo's IAM policy
#[endpoint {
    method = GET,
    path = "/v1/policy",
    tags = ["silos"],
 }]
pub async fn policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let silo: NameOrId = opctx
            .authn
            .silo_required()
            .internal_context("loading current silo")?
            .id()
            .into();

        let silo_lookup = nexus.silo_lookup(&opctx, &silo)?;
        let policy = nexus.silo_fetch_policy(&opctx, &silo_lookup).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update the current silo's IAM policy
#[endpoint {
    method = PUT,
    path = "/v1/policy",
    tags = ["silos"],
}]
async fn policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_policy: TypedBody<shared::Policy<authz::SiloRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let new_policy = new_policy.into_inner();
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let silo: NameOrId = opctx
            .authn
            .silo_required()
            .internal_context("loading current silo")?
            .id()
            .into();
        let silo_lookup = nexus.silo_lookup(&opctx, &silo)?;
        let policy =
            nexus.silo_update_policy(&opctx, &silo_lookup, &new_policy).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List silos
///
/// Lists silos that are discoverable based on the current permissions.
#[endpoint {
    method = GET,
    path = "/v1/system/silos",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/silos",
    tags = ["system"],
}]
async fn silo_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_silo_params: TypedBody<params::SiloCreate>,
) -> Result<HttpResponseCreated<Silo>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let silo =
            nexus.silo_create(&opctx, new_silo_params.into_inner()).await?;
        Ok(HttpResponseCreated(silo.try_into()?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a silo
///
/// Fetch a silo by name.
#[endpoint {
    method = GET,
    path = "/v1/system/silos/{silo}",
    tags = ["system"],
}]
async fn silo_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SiloPath>,
) -> Result<HttpResponseOk<Silo>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &path.silo)?;
        let (.., silo) = silo_lookup.fetch().await?;
        Ok(HttpResponseOk(silo.try_into()?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a silo
///
/// Delete a silo by name.
#[endpoint {
    method = DELETE,
    path = "/v1/system/silos/{silo}",
    tags = ["system"],
}]
async fn silo_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SiloPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let params = path_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &params.silo)?;
        nexus.silo_delete(&opctx, &silo_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a silo's IAM policy
#[endpoint {
    method = GET,
    path = "/v1/system/silos/{silo}/policy",
    tags = ["system"],
}]
async fn silo_policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SiloPath>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &path.silo)?;
        let policy = nexus.silo_fetch_policy(&opctx, &silo_lookup).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a silo's IAM policy
#[endpoint {
    method = PUT,
    path = "/v1/system/silos/{silo}/policy",
    tags = ["system"],
}]
async fn silo_policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SiloPath>,
    new_policy: TypedBody<shared::Policy<authz::SiloRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::SiloRole>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let new_policy = new_policy.into_inner();
        let nasgns = new_policy.role_assignments.len();
        // This should have been validated during parsing.
        bail_unless!(nasgns <= shared::MAX_ROLE_ASSIGNMENTS_PER_RESOURCE);
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &path.silo)?;
        let policy =
            nexus.silo_update_policy(&opctx, &silo_lookup, &new_policy).await?;
        Ok(HttpResponseOk(policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo-specific user endpoints

/// List users in a silo
#[endpoint {
    method = GET,
    path = "/v1/system/users",
    tags = ["system"],
}]
async fn silo_user_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById<params::SiloSelector>>,
) -> Result<HttpResponseOk<ResultsPage<User>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanById::from_query(&query)?;
        let silo_lookup =
            nexus.silo_lookup(&opctx, &scan_params.selector.silo)?;
        let users = nexus
            .silo_list_users(&opctx, &silo_lookup, &pag_params)
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
struct UserParam {
    /// The user's internal id
    user_id: Uuid,
}

/// Fetch a user
#[endpoint {
    method = GET,
    path = "/v1/system/users/{user_id}",
    tags = ["system"],
}]
async fn silo_user_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<UserParam>,
    query_params: Query<params::SiloSelector>,
) -> Result<HttpResponseOk<User>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &query.silo)?;
        let user =
            nexus.silo_user_fetch(&opctx, &silo_lookup, path.user_id).await?;
        Ok(HttpResponseOk(user.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo identity providers

/// List a silo's IDPs_name
#[endpoint {
    method = GET,
    path = "/v1/system/identity-providers",
    tags = ["system"],
}]
async fn silo_identity_provider_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::SiloSelector>>,
) -> Result<HttpResponseOk<ResultsPage<IdentityProvider>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let silo_lookup =
            nexus.silo_lookup(&opctx, &scan_params.selector.silo)?;
        let identity_providers = nexus
            .identity_provider_list(&opctx, &silo_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|x| x.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            identity_providers,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo SAML identity providers

/// Create a SAML IDP
#[endpoint {
    method = POST,
    path = "/v1/system/identity-providers/saml",
    tags = ["system"],
}]
async fn saml_identity_provider_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::SiloSelector>,
    new_provider: TypedBody<params::SamlIdentityProviderCreate>,
) -> Result<HttpResponseCreated<views::SamlIdentityProvider>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &query.silo)?;
        let provider = nexus
            .saml_identity_provider_create(
                &opctx,
                &silo_lookup,
                new_provider.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(provider.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a SAML IDP
#[endpoint {
    method = GET,
    path = "/v1/system/identity-providers/saml/{provider}",
    tags = ["system"],
}]
async fn saml_identity_provider_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProviderPath>,
    query_params: Query<params::SiloSelector>,
) -> Result<HttpResponseOk<views::SamlIdentityProvider>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let saml_identity_provider_selector =
            params::SamlIdentityProviderSelector {
                silo_selector: Some(query),
                saml_identity_provider: path.provider,
            };
        let (.., provider) = nexus
            .saml_identity_provider_lookup(
                &opctx,
                &saml_identity_provider_selector,
            )?
            .fetch()
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
    path = "/v1/system/identity-providers/local/users",
    tags = ["system"],
}]
async fn local_idp_user_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::SiloSelector>,
    new_user_params: TypedBody<params::UserCreate>,
) -> Result<HttpResponseCreated<User>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &query.silo)?;
        let user = nexus
            .local_idp_create_user(
                &opctx,
                &silo_lookup,
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
    path = "/v1/system/identity-providers/local/users/{user_id}",
    tags = ["system"],
}]
async fn local_idp_user_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<UserParam>,
    query_params: Query<params::SiloSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &query.silo)?;
        nexus.local_idp_delete_user(&opctx, &silo_lookup, path.user_id).await?;
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
    path = "/v1/system/identity-providers/local/users/{user_id}/set-password",
    tags = ["system"],
}]
async fn local_idp_user_set_password(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<UserParam>,
    query_params: Query<params::SiloPath>,
    update: TypedBody<params::UserPassword>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let silo_lookup = nexus.silo_lookup(&opctx, &query.silo)?;
        nexus
            .local_idp_user_set_password(
                &opctx,
                &silo_lookup,
                path.user_id,
                update.into_inner(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List projects
#[endpoint {
    method = GET,
    path = "/v1/projects",
    tags = ["projects"],
}]
async fn project_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Project>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let projects = nexus
            .project_list(&opctx, &paginated_by)
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
async fn project_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_project: TypedBody<params::ProjectCreate>,
) -> Result<HttpResponseCreated<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project =
            nexus.project_create(&opctx, &new_project.into_inner()).await?;
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
async fn project_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project_selector =
            params::ProjectSelector { project: path.project };
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
async fn project_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project_selector =
            params::ProjectSelector { project: path.project };
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        nexus.project_delete(&opctx, &project_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// TODO-correctness: Is it valid for PUT to accept application/json that's a
// subset of what the resource actually represents?  If not, is that a problem?
// (HTTP may require that this be idempotent.)  If so, can we get around that
// having this be a slightly different content-type (e.g.,
// "application/json-patch")?  We should see what other APIs do.
/// Update a project
#[endpoint {
    method = PUT,
    path = "/v1/projects/{project}",
    tags = ["projects"],
}]
async fn project_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
    updated_project: TypedBody<params::ProjectUpdate>,
) -> Result<HttpResponseOk<Project>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let updated_project = updated_project.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project_selector =
            params::ProjectSelector { project: path.project };
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        let project = nexus
            .project_update(&opctx, &project_lookup, &updated_project)
            .await?;
        Ok(HttpResponseOk(project.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a project's IAM policy
#[endpoint {
    method = GET,
    path = "/v1/projects/{project}/policy",
    tags = ["projects"],
}]
async fn project_policy_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project_selector =
            params::ProjectSelector { project: path.project };
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
async fn project_policy_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ProjectPath>,
    new_policy: TypedBody<shared::Policy<authz::ProjectRole>>,
) -> Result<HttpResponseOk<shared::Policy<authz::ProjectRole>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_policy = new_policy.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project_selector =
            params::ProjectSelector { project: path.project };
        let project_lookup = nexus.project_lookup(&opctx, &project_selector)?;
        nexus
            .project_update_policy(&opctx, &project_lookup, &new_policy)
            .await?;
        Ok(HttpResponseOk(new_policy))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// IP Pools

/// List IP pools
#[endpoint {
    method = GET,
    path = "/v1/system/ip-pools",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

#[derive(Deserialize, JsonSchema)]
pub struct IpPoolPathParam {
    pub pool_name: Name,
}

/// Create an IP pool
#[endpoint {
    method = POST,
    path = "/v1/system/ip-pools",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let pool = nexus.ip_pool_create(&opctx, &pool_params).await?;
        Ok(HttpResponseCreated(IpPool::from(pool)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch an IP pool
#[endpoint {
    method = GET,
    path = "/v1/system/ip-pools/{pool}",
    tags = ["system"],
}]
async fn ip_pool_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::IpPoolPath>,
) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let pool_selector = path_params.into_inner().pool;
        let (.., pool) =
            nexus.ip_pool_lookup(&opctx, &pool_selector)?.fetch().await?;
        Ok(HttpResponseOk(IpPool::from(pool)))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an IP Pool
#[endpoint {
    method = DELETE,
    path = "/v1/system/ip-pools/{pool}",
    tags = ["system"],
}]
async fn ip_pool_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::IpPoolPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
        nexus.ip_pool_delete(&opctx, &pool_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update an IP Pool
#[endpoint {
    method = PUT,
    path = "/v1/system/ip-pools/{pool}",
    tags = ["system"],
}]
async fn ip_pool_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::IpPoolPath>,
    updates: TypedBody<params::IpPoolUpdate>,
) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let updates = updates.into_inner();
        let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
        let pool = nexus.ip_pool_update(&opctx, &pool_lookup, &updates).await?;
        Ok(HttpResponseOk(pool.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the IP pool used for Oxide services.
#[endpoint {
    method = GET,
    path = "/v1/system/ip-pools-service",
    tags = ["system"],
}]
async fn ip_pool_service_view(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<views::IpPool>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/ip-pools/{pool}/ranges",
    tags = ["system"],
}]
async fn ip_pool_range_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::IpPoolPath>,
    query_params: Query<IpPoolRangePaginationParams>,
) -> Result<HttpResponseOk<ResultsPage<IpPoolRange>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let path = path_params.into_inner();
        let marker = match query.page {
            WhichPage::First(_) => None,
            WhichPage::Next(ref addr) => Some(addr),
        };
        let pag_params = DataPageParams {
            limit: rqctx.page_limit(&query)?,
            direction: PaginationOrder::Ascending,
            marker,
        };
        let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
        let ranges = nexus
            .ip_pool_list_ranges(&opctx, &pool_lookup, &pag_params)
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
    path = "/v1/system/ip-pools/{pool}/ranges/add",
    tags = ["system"],
}]
async fn ip_pool_range_add(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::IpPoolPath>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
    let apictx = &rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let range = range_params.into_inner();
        let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
        let out = nexus.ip_pool_add_range(&opctx, &pool_lookup, &range).await?;
        Ok(HttpResponseCreated(out.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Remove a range from an IP pool
#[endpoint {
    method = POST,
    path = "/v1/system/ip-pools/{pool}/ranges/remove",
    tags = ["system"],
}]
async fn ip_pool_range_remove(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::IpPoolPath>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let range = range_params.into_inner();
        let pool_lookup = nexus.ip_pool_lookup(&opctx, &path.pool)?;
        nexus.ip_pool_delete_range(&opctx, &pool_lookup, &range).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List ranges for the IP pool used for Oxide services.
///
/// Ranges are ordered by their first address.
#[endpoint {
    method = GET,
    path = "/v1/system/ip-pools-service/ranges",
    tags = ["system"],
}]
async fn ip_pool_service_range_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<IpPoolRangePaginationParams>,
) -> Result<HttpResponseOk<ResultsPage<IpPoolRange>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
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
    path = "/v1/system/ip-pools-service/ranges/add",
    tags = ["system"],
}]
async fn ip_pool_service_range_add(
    rqctx: RequestContext<Arc<ServerContext>>,
    range_params: TypedBody<shared::IpRange>,
) -> Result<HttpResponseCreated<IpPoolRange>, HttpError> {
    let apictx = &rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let range = range_params.into_inner();
        let out = nexus.ip_pool_service_add_range(&opctx, &range).await?;
        Ok(HttpResponseCreated(out.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Remove a range from an IP pool used for Oxide services.
#[endpoint {
    method = POST,
    path = "/v1/system/ip-pools-service/ranges/remove",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
async fn disk_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
) -> Result<HttpResponseOk<ResultsPage<Disk>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
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

// TODO-correctness See note about instance create.  This should be async.
/// Create a disk
#[endpoint {
    method = POST,
    path = "/v1/disks",
    tags = ["disks"]
}]
async fn disk_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::ProjectSelector>,
    new_disk: TypedBody<params::DiskCreate>,
) -> Result<HttpResponseCreated<Disk>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let params = new_disk.into_inner();
        let project_lookup = nexus.project_lookup(&opctx, &query)?;
        let disk =
            nexus.project_create_disk(&opctx, &project_lookup, &params).await?;
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
async fn disk_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseOk<Disk>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let disk_selector = params::DiskSelector {
            disk: path.disk,
            project_selector: query.project_selector,
        };
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
async fn disk_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let disk_selector = params::DiskSelector {
            disk: path.disk,
            project_selector: query.project_selector,
        };
        let disk_lookup = nexus.disk_lookup(&opctx, &disk_selector)?;
        nexus.project_delete_disk(&opctx, &disk_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Display, Serialize, Deserialize, JsonSchema)]
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

#[derive(Serialize, Deserialize, JsonSchema)]
struct DiskMetricsPath {
    disk: NameOrId,
    metric: DiskMetricName,
}

/// Fetch disk metrics
#[endpoint {
    method = GET,
    path = "/v1/disks/{disk}/metrics/{metric}",
    tags = ["disks"],
}]
async fn disk_metrics_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<DiskMetricsPath>,
    query_params: Query<
        PaginationParams<params::ResourceMetrics, params::ResourceMetrics>,
    >,
    selector_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseOk<ResultsPage<oximeter_db::Measurement>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();

        let selector = selector_params.into_inner();
        let limit = rqctx.page_limit(&query)?;
        let disk_selector = params::DiskSelector {
            disk: path.disk,
            project_selector: selector.project_selector.clone(),
        };
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let (.., authz_disk) = nexus
            .disk_lookup(&opctx, &disk_selector)?
            .lookup_for(authz::Action::Read)
            .await?;

        let result = nexus
            .select_timeseries(
                &format!("crucible_upstairs:{}", path.metric),
                &[&format!("upstairs_uuid=={}", authz_disk.id())],
                query,
                limit,
            )
            .await?;

        Ok(HttpResponseOk(result))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Start the process of importing blocks into a disk
#[endpoint {
    method = POST,
    path = "/v1/disks/{disk}/bulk-write-start",
    tags = ["disks"],
}]
async fn disk_bulk_write_import_start(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();

        let disk_selector = params::DiskSelector {
            disk: path.disk,
            project_selector: query.project_selector,
        };

        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;

        nexus.disk_manual_import_start(&opctx, &disk_selector).await?;

        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Import blocks into a disk
#[endpoint {
    method = POST,
    path = "/v1/disks/{disk}/bulk-write",
    tags = ["disks"],
}]
async fn disk_bulk_write_import(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
    import_params: TypedBody<params::ImportBlocksBulkWrite>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let params = import_params.into_inner();

        let disk_selector = params::DiskSelector {
            disk: path.disk,
            project_selector: query.project_selector,
        };

        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;

        nexus.disk_manual_import(&opctx, &disk_selector, params).await?;

        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Stop the process of importing blocks into a disk
#[endpoint {
    method = POST,
    path = "/v1/disks/{disk}/bulk-write-stop",
    tags = ["disks"],
}]
async fn disk_bulk_write_import_stop(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();

        let disk_selector = params::DiskSelector {
            disk: path.disk,
            project_selector: query.project_selector,
        };

        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;

        nexus.disk_manual_import_stop(&opctx, &disk_selector).await?;

        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Send request to import blocks from URL
#[endpoint {
    method = POST,
    path = "/v1/disks/{disk}/import",
    tags = ["disks"],
}]
async fn disk_import_blocks_from_url(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::OptionalProjectSelector>,
    import_params: TypedBody<params::ImportBlocksFromUrl>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let params = import_params.into_inner();

        let disk_selector = params::DiskSelector {
            disk: path.disk,
            project_selector: query.project_selector,
        };

        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;

        nexus
            .import_blocks_from_url_for_disk(&opctx, &disk_selector, params)
            .await?;

        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Finalize disk when imports are done
#[endpoint {
    method = POST,
    path = "/v1/disks/{disk}/finalize",
    tags = ["disks"],
}]
async fn disk_finalize_import(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::DiskPath>,
    query_params: Query<params::FinalizeDisk>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();

        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;

        nexus.disk_finalize_import(&opctx, path.disk, query).await?;

        Ok(HttpResponseUpdatedNoContent())
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
async fn instance_list(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

/// Create an instance
#[endpoint {
    method = POST,
    path = "/v1/instances",
    tags = ["instances"],
}]
async fn instance_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::ProjectSelector>,
    new_instance: TypedBody<params::InstanceCreate>,
) -> Result<HttpResponseCreated<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let project_selector = query_params.into_inner();
    let new_instance_params = &new_instance.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
async fn instance_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseOk<Instance>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

/// Delete an instance
#[endpoint {
    method = DELETE,
    path = "/v1/instances/{instance}",
    tags = ["instances"],
}]
async fn instance_delete(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
async fn instance_migrate(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
async fn instance_reboot(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
async fn instance_start(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
async fn instance_stop(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
async fn instance_serial_console(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::InstancePath>,
    query_params: Query<params::InstanceSerialConsoleRequest>,
) -> Result<HttpResponseOk<params::InstanceSerialConsoleData>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector.clone(),
        instance: path.instance,
    };
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let data = nexus
            .instance_serial_console_data(&instance_lookup, &query)
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
async fn instance_serial_console_stream(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::InstancePath>,
    query_params: Query<params::InstanceSerialConsoleRequest>,
    conn: WebsocketConnection,
) -> WebsocketChannelResult {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let query = query_params.into_inner();
    let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
    let instance_selector = params::InstanceSelector {
        project_selector: query.project_selector.clone(),
        instance: path.instance,
    };
    let instance_lookup = nexus.instance_lookup(&opctx, &instance_selector)?;
    nexus
        .instance_serial_console_stream(conn, &instance_lookup, &query)
        .await?;
    Ok(())
}

/// List an instance's disks
#[endpoint {
    method = GET,
    path = "/v1/instances/{instance}/disks",
    tags = ["instances"],
}]
async fn instance_disk_list(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

/// Attach a disk to an instance
#[endpoint {
    method = POST,
    path = "/v1/instances/{instance}/disks/attach",
    tags = ["instances"],
}]
async fn instance_disk_attach(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

/// Detach a disk from an instance
#[endpoint {
    method = POST,
    path = "/v1/instances/{instance}/disks/detach",
    tags = ["instances"],
}]
async fn instance_disk_detach(
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

// Certificates

/// List system-wide certificates
///
/// Returns a list of all the system-wide certificates. System-wide certificates
/// are returned sorted by creation date, with the most recent certificates
/// appearing first.
#[endpoint {
    method = GET,
    path = "/v1/system/certificates",
    tags = ["system"],
}]
async fn certificate_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<Certificate>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let certs = nexus
            .certificates_list(&opctx, &paginated_by)
            .await?
            .into_iter()
            .map(|d| d.try_into())
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            certs,
            &marker_for_name_or_id,
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
    path = "/v1/system/certificates",
    tags = ["system"]
}]
async fn certificate_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_cert: TypedBody<params::CertificateCreate>,
) -> Result<HttpResponseCreated<Certificate>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let new_cert_params = new_cert.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/certificates/{certificate}",
    tags = ["system"],
}]
async fn certificate_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<CertificatePathParam>,
) -> Result<HttpResponseOk<Certificate>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/certificates/{certificate}",
    tags = ["system"],
}]
async fn certificate_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<CertificatePathParam>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    tags = ["system"],
    deprecated = true,
}]
async fn system_image_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_image: TypedBody<params::GlobalImageCreate>,
) -> Result<HttpResponseCreated<GlobalImage>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let new_image_params = new_image.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    deprecated = true,
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    deprecated = true,
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    deprecated = true,
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        nexus.global_image_delete(&opctx, &image_name).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List images
///
/// List images which are global or scoped to the specified project. The images
/// are returned sorted by creation date, with the most recent images appearing first.
#[endpoint {
    method = GET,
    path = "/v1/images",
    tags = ["images"],
}]
async fn image_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
) -> Result<HttpResponseOk<ResultsPage<Image>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let project_lookup =
            nexus.project_lookup(&opctx, &scan_params.selector)?;
        let images = nexus
            .image_list(&opctx, &project_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            images,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create an image
///
/// Create a new image in a project.
#[endpoint {
    method = POST,
    path = "/v1/images",
    tags = ["images"]
}]
async fn image_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::ProjectSelector>,
    new_image: TypedBody<params::ImageCreate>,
) -> Result<HttpResponseCreated<Image>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let params = &new_image.into_inner();
        let project_lookup = nexus.project_lookup(&opctx, &query)?;
        let image =
            nexus.image_create(&opctx, &project_lookup, &params).await?;
        Ok(HttpResponseCreated(image.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch an image
///
/// Fetch the details for a specific image in a project.
#[endpoint {
    method = GET,
    path = "/v1/images/{image}",
    tags = ["images"],
}]
async fn image_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ImagePath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseOk<Image>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let image_selector = params::ImageSelector {
            image: path.image,
            project_selector: query.project_selector,
        };
        let (.., image) =
            nexus.image_lookup(&opctx, &image_selector)?.fetch().await?;
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
    path = "/v1/images/{image}",
    tags = ["images"],
}]
async fn image_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::ImagePath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let image_selector = params::ImageSelector {
            image: path.image,
            project_selector: query.project_selector,
        };
        let image_lookup = nexus.image_lookup(&opctx, &image_selector)?;
        nexus.image_delete(&opctx, &image_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List network interfaces
#[endpoint {
    method = GET,
    path = "/v1/network-interfaces",
    tags = ["instances"],
}]
async fn instance_network_interface_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::InstanceSelector>>,
) -> Result<HttpResponseOk<ResultsPage<NetworkInterface>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let instance_lookup =
            nexus.instance_lookup(&opctx, &scan_params.selector)?;
        let interfaces = nexus
            .network_interface_list(&opctx, &instance_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            interfaces,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a network interface
#[endpoint {
    method = POST,
    path = "/v1/network-interfaces",
    tags = ["instances"],
}]
async fn instance_network_interface_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::InstanceSelector>,
    interface_params: TypedBody<params::NetworkInterfaceCreate>,
) -> Result<HttpResponseCreated<NetworkInterface>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let instance_lookup = nexus.instance_lookup(&opctx, &query)?;
        let iface = nexus
            .network_interface_create(
                &opctx,
                &instance_lookup,
                &interface_params.into_inner(),
            )
            .await?;
        Ok(HttpResponseCreated(iface.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a network interface
///
/// Note that the primary interface for an instance cannot be deleted if there
/// are any secondary interfaces. A new primary interface must be designated
/// first. The primary interface can be deleted if there are no secondary
/// interfaces.
#[endpoint {
    method = DELETE,
    path = "/v1/network-interfaces/{interface}",
    tags = ["instances"],
}]
async fn instance_network_interface_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::NetworkInterfacePath>,
    query_params: Query<params::OptionalInstanceSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let interface_selector = params::NetworkInterfaceSelector {
            instance_selector: query.instance_selector,
            network_interface: path.interface,
        };
        let interface_lookup =
            nexus.network_interface_lookup(&opctx, &interface_selector)?;
        nexus.network_interface_delete(&opctx, &interface_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a network interface
#[endpoint {
    method = GET,
    path = "/v1/network-interfaces/{interface}",
    tags = ["instances"],
}]
async fn instance_network_interface_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::NetworkInterfacePath>,
    query_params: Query<params::OptionalInstanceSelector>,
) -> Result<HttpResponseOk<NetworkInterface>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let interface_selector = params::NetworkInterfaceSelector {
            instance_selector: query.instance_selector,
            network_interface: path.interface,
        };
        let (.., interface) = nexus
            .network_interface_lookup(&opctx, &interface_selector)?
            .fetch()
            .await?;
        Ok(HttpResponseOk(interface.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a network interface
#[endpoint {
    method = PUT,
    path = "/v1/network-interfaces/{interface}",
    tags = ["instances"],
}]
async fn instance_network_interface_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::NetworkInterfacePath>,
    query_params: Query<params::OptionalInstanceSelector>,
    updated_iface: TypedBody<params::NetworkInterfaceUpdate>,
) -> Result<HttpResponseOk<NetworkInterface>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let updated_iface = updated_iface.into_inner();
        let network_interface_selector = params::NetworkInterfaceSelector {
            instance_selector: query.instance_selector,
            network_interface: path.interface,
        };
        let network_interface_lookup = nexus
            .network_interface_lookup(&opctx, &network_interface_selector)?;
        let interface = nexus
            .network_interface_update(
                &opctx,
                &network_interface_lookup,
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
    path = "/v1/instances/{instance}/external-ips",
    tags = ["instances"],
}]
async fn instance_external_ip_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::OptionalProjectSelector>,
    path_params: Path<params::InstancePath>,
) -> Result<HttpResponseOk<ResultsPage<views::ExternalIp>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let instance_selector = params::InstanceSelector {
            project_selector: query.project_selector,
            instance: path.instance,
        };
        let instance_lookup =
            nexus.instance_lookup(&opctx, &instance_selector)?;
        let ips =
            nexus.instance_list_external_ips(&opctx, &instance_lookup).await?;
        Ok(HttpResponseOk(ResultsPage { items: ips, next_page: None }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Snapshots

/// List snapshots
#[endpoint {
    method = GET,
    path = "/v1/snapshots",
    tags = ["snapshots"],
}]
async fn snapshot_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
) -> Result<HttpResponseOk<ResultsPage<Snapshot>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let project_lookup =
            nexus.project_lookup(&opctx, &scan_params.selector)?;
        let snapshots = nexus
            .snapshot_list(&opctx, &project_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            snapshots,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a snapshot
///
/// Creates a point-in-time snapshot from a disk.
#[endpoint {
    method = POST,
    path = "/v1/snapshots",
    tags = ["snapshots"],
}]
async fn snapshot_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::ProjectSelector>,
    new_snapshot: TypedBody<params::SnapshotCreate>,
) -> Result<HttpResponseCreated<Snapshot>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let new_snapshot_params = &new_snapshot.into_inner();
        let project_lookup = nexus.project_lookup(&opctx, &query)?;
        let snapshot = nexus
            .snapshot_create(&opctx, project_lookup, &new_snapshot_params)
            .await?;
        Ok(HttpResponseCreated(snapshot.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a snapshot
#[endpoint {
    method = GET,
    path = "/v1/snapshots/{snapshot}",
    tags = ["snapshots"],
}]
async fn snapshot_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SnapshotPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseOk<Snapshot>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let snapshot_selector = params::SnapshotSelector {
            project_selector: query.project_selector,
            snapshot: path.snapshot,
        };
        let (.., snapshot) =
            nexus.snapshot_lookup(&opctx, &snapshot_selector)?.fetch().await?;
        Ok(HttpResponseOk(snapshot.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a snapshot
#[endpoint {
    method = DELETE,
    path = "/v1/snapshots/{snapshot}",
    tags = ["snapshots"],
}]
async fn snapshot_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SnapshotPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let snapshot_selector = params::SnapshotSelector {
            project_selector: query.project_selector,
            snapshot: path.snapshot,
        };
        let snapshot_lookup =
            nexus.snapshot_lookup(&opctx, &snapshot_selector)?;
        nexus.snapshot_delete(&opctx, &snapshot_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// VPCs

/// List VPCs
#[endpoint {
    method = GET,
    path = "/v1/vpcs",
    tags = ["vpcs"],
}]
async fn vpc_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::ProjectSelector>>,
) -> Result<HttpResponseOk<ResultsPage<Vpc>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project_lookup =
            nexus.project_lookup(&opctx, &scan_params.selector)?;
        let vpcs = nexus
            .vpc_list(&opctx, &project_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|p| p.into())
            .collect();

        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            vpcs,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a VPC
#[endpoint {
    method = POST,
    path = "/v1/vpcs",
    tags = ["vpcs"],
}]
async fn vpc_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::ProjectSelector>,
    body: TypedBody<params::VpcCreate>,
) -> Result<HttpResponseCreated<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let new_vpc_params = body.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let project_lookup = nexus.project_lookup(&opctx, &query)?;
        let vpc = nexus
            .project_create_vpc(&opctx, &project_lookup, &new_vpc_params)
            .await?;
        Ok(HttpResponseCreated(vpc.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a VPC
#[endpoint {
    method = GET,
    path = "/v1/vpcs/{vpc}",
    tags = ["vpcs"],
}]
async fn vpc_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::VpcPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseOk<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let vpc_selector = params::VpcSelector {
            project_selector: query.project_selector,
            vpc: path.vpc,
        };
        let (.., vpc) =
            nexus.vpc_lookup(&opctx, &vpc_selector)?.fetch().await?;
        Ok(HttpResponseOk(vpc.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a VPC
#[endpoint {
    method = PUT,
    path = "/v1/vpcs/{vpc}",
    tags = ["vpcs"],
}]
async fn vpc_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::VpcPath>,
    query_params: Query<params::OptionalProjectSelector>,
    updated_vpc: TypedBody<params::VpcUpdate>,
) -> Result<HttpResponseOk<Vpc>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let updated_vpc_params = &updated_vpc.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let vpc_selector = params::VpcSelector {
            project_selector: query.project_selector,
            vpc: path.vpc,
        };
        let vpc_lookup = nexus.vpc_lookup(&opctx, &vpc_selector)?;
        let vpc = nexus
            .project_update_vpc(&opctx, &vpc_lookup, &updated_vpc_params)
            .await?;
        Ok(HttpResponseOk(vpc.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a VPC
#[endpoint {
    method = DELETE,
    path = "/v1/vpcs/{vpc}",
    tags = ["vpcs"],
}]
async fn vpc_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::VpcPath>,
    query_params: Query<params::OptionalProjectSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let vpc_selector = params::VpcSelector {
            project_selector: query.project_selector,
            vpc: path.vpc,
        };
        let vpc_lookup = nexus.vpc_lookup(&opctx, &vpc_selector)?;
        nexus.project_delete_vpc(&opctx, &vpc_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a subnet
#[endpoint {
    method = GET,
    path = "/v1/vpc-subnets",
    tags = ["vpcs"],
}]
async fn vpc_subnet_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
) -> Result<HttpResponseOk<ResultsPage<VpcSubnet>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let vpc_lookup = nexus.vpc_lookup(&opctx, &scan_params.selector)?;
        let subnets = nexus
            .vpc_subnet_list(&opctx, &vpc_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|vpc| vpc.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            subnets,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a subnet
#[endpoint {
    method = POST,
    path = "/v1/vpc-subnets",
    tags = ["vpcs"],
}]
async fn vpc_subnet_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::VpcSelector>,
    create_params: TypedBody<params::VpcSubnetCreate>,
) -> Result<HttpResponseCreated<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let create = create_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let vpc_lookup = nexus.vpc_lookup(&opctx, &query)?;
        let subnet =
            nexus.vpc_create_subnet(&opctx, &vpc_lookup, &create).await?;
        Ok(HttpResponseCreated(subnet.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a subnet
#[endpoint {
    method = GET,
    path = "/v1/vpc-subnets/{subnet}",
    tags = ["vpcs"],
}]
async fn vpc_subnet_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SubnetPath>,
    query_params: Query<params::OptionalVpcSelector>,
) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let subnet_selector = params::SubnetSelector {
            vpc_selector: query.vpc_selector,
            subnet: path.subnet,
        };
        let (.., subnet) =
            nexus.vpc_subnet_lookup(&opctx, &subnet_selector)?.fetch().await?;
        Ok(HttpResponseOk(subnet.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a subnet
#[endpoint {
    method = DELETE,
    path = "/v1/vpc-subnets/{subnet}",
    tags = ["vpcs"],
}]
async fn vpc_subnet_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SubnetPath>,
    query_params: Query<params::OptionalVpcSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let subnet_selector = params::SubnetSelector {
            vpc_selector: query.vpc_selector,
            subnet: path.subnet,
        };
        let subnet_lookup =
            nexus.vpc_subnet_lookup(&opctx, &subnet_selector)?;
        nexus.vpc_delete_subnet(&opctx, &subnet_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a subnet
#[endpoint {
    method = PUT,
    path = "/v1/vpc-subnets/{subnet}",
    tags = ["vpcs"],
}]
async fn vpc_subnet_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SubnetPath>,
    query_params: Query<params::OptionalVpcSelector>,
    subnet_params: TypedBody<params::VpcSubnetUpdate>,
) -> Result<HttpResponseOk<VpcSubnet>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let subnet_params = subnet_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let subnet_selector = params::SubnetSelector {
            vpc_selector: query.vpc_selector,
            subnet: path.subnet,
        };
        let subnet_lookup =
            nexus.vpc_subnet_lookup(&opctx, &subnet_selector)?;
        let subnet = nexus
            .vpc_update_subnet(&opctx, &subnet_lookup, &subnet_params)
            .await?;
        Ok(HttpResponseOk(subnet.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// This endpoint is likely temporary. We would rather list all IPs allocated in
// a subnet whether they come from NICs or something else. See
// https://github.com/oxidecomputer/omicron/issues/2476

/// List network interfaces
#[endpoint {
    method = GET,
    path = "/v1/vpc-subnets/{subnet}/network-interfaces",
    tags = ["vpcs"],
}]
async fn vpc_subnet_list_network_interfaces(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SubnetPath>,
    query_params: Query<PaginatedByNameOrId<params::OptionalVpcSelector>>,
) -> Result<HttpResponseOk<ResultsPage<NetworkInterface>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let path = path_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let subnet_selector = params::SubnetSelector {
            vpc_selector: scan_params.selector.vpc_selector.clone(),
            subnet: path.subnet,
        };
        let subnet_lookup =
            nexus.vpc_subnet_lookup(&opctx, &subnet_selector)?;
        let interfaces = nexus
            .subnet_list_network_interfaces(
                &opctx,
                &subnet_lookup,
                &paginated_by,
            )
            .await?
            .into_iter()
            .map(|interfaces| interfaces.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            interfaces,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// VPC Firewalls

// TODO Is the number of firewall rules bounded?
/// List firewall rules
#[endpoint {
    method = GET,
    path = "/v1/vpc-firewall-rules",
    tags = ["vpcs"],
}]
async fn vpc_firewall_rules_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::VpcSelector>,
) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
    // TODO: Check If-Match and fail if the ETag doesn't match anymore.
    // Without this check, if firewall rules change while someone is listing
    // the rules, they will see a mix of the old and new rules.
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let vpc_lookup = nexus.vpc_lookup(&opctx, &query)?;
        let rules = nexus.vpc_list_firewall_rules(&opctx, &vpc_lookup).await?;
        Ok(HttpResponseOk(VpcFirewallRules {
            rules: rules.into_iter().map(|rule| rule.into()).collect(),
        }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Replace firewall rules
#[endpoint {
    method = PUT,
    path = "/v1/vpc-firewall-rules",
    tags = ["vpcs"],
}]
async fn vpc_firewall_rules_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::VpcSelector>,
    router_params: TypedBody<VpcFirewallRuleUpdateParams>,
) -> Result<HttpResponseOk<VpcFirewallRules>, HttpError> {
    // TODO: Check If-Match and fail if the ETag doesn't match anymore.
    // TODO: limit size of the ruleset because the GET endpoint is not paginated
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let router_params = router_params.into_inner();
        let vpc_lookup = nexus.vpc_lookup(&opctx, &query)?;
        let rules = nexus
            .vpc_update_firewall_rules(&opctx, &vpc_lookup, &router_params)
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
    path = "/v1/vpc-routers",
    tags = ["vpcs"],
}]
async fn vpc_router_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::VpcSelector>>,
) -> Result<HttpResponseOk<ResultsPage<VpcRouter>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let vpc_lookup = nexus.vpc_lookup(&opctx, &scan_params.selector)?;
        let routers = nexus
            .vpc_router_list(&opctx, &vpc_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|s| s.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            routers,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Get a router
#[endpoint {
    method = GET,
    path = "/v1/vpc-routers/{router}",
    tags = ["vpcs"],
}]
async fn vpc_router_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::RouterPath>,
    query_params: Query<params::OptionalVpcSelector>,
) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let router_selector = params::RouterSelector {
            vpc_selector: query.vpc_selector,
            router: path.router,
        };
        let (.., vpc_router) =
            nexus.vpc_router_lookup(&opctx, &router_selector)?.fetch().await?;
        Ok(HttpResponseOk(vpc_router.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a VPC router
#[endpoint {
    method = POST,
    path = "/v1/vpc-routers",
    tags = ["vpcs"],
}]
async fn vpc_router_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::VpcSelector>,
    create_params: TypedBody<params::VpcRouterCreate>,
) -> Result<HttpResponseCreated<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let create = create_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let vpc_lookup = nexus.vpc_lookup(&opctx, &query)?;
        let router = nexus
            .vpc_create_router(
                &opctx,
                &vpc_lookup,
                &db::model::VpcRouterKind::Custom,
                &create,
            )
            .await?;
        Ok(HttpResponseCreated(router.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a router
#[endpoint {
    method = DELETE,
    path = "/v1/vpc-routers/{router}",
    tags = ["vpcs"],
}]
async fn vpc_router_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::RouterPath>,
    query_params: Query<params::OptionalVpcSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let router_selector = params::RouterSelector {
            vpc_selector: query.vpc_selector,
            router: path.router,
        };
        let router_lookup =
            nexus.vpc_router_lookup(&opctx, &router_selector)?;
        nexus.vpc_delete_router(&opctx, &router_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a router
#[endpoint {
    method = PUT,
    path = "/v1/vpc-routers/{router}",
    tags = ["vpcs"],
}]
async fn vpc_router_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::RouterPath>,
    query_params: Query<params::OptionalVpcSelector>,
    router_params: TypedBody<params::VpcRouterUpdate>,
) -> Result<HttpResponseOk<VpcRouter>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let router_params = router_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let router_selector = params::RouterSelector {
            vpc_selector: query.vpc_selector,
            router: path.router,
        };
        let router_lookup =
            nexus.vpc_router_lookup(&opctx, &router_selector)?;
        let router = nexus
            .vpc_update_router(&opctx, &router_lookup, &router_params)
            .await?;
        Ok(HttpResponseOk(router.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List routes
///
/// List the routes associated with a router in a particular VPC.
#[endpoint {
    method = GET,
    path = "/v1/vpc-router-routes",
    tags = ["vpcs"],
}]
async fn vpc_router_route_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId<params::RouterSelector>>,
) -> Result<HttpResponseOk<ResultsPage<RouterRoute>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let router_lookup =
            nexus.vpc_router_lookup(&opctx, &scan_params.selector)?;
        let routes = nexus
            .vpc_router_route_list(&opctx, &router_lookup, &paginated_by)
            .await?
            .into_iter()
            .map(|route| route.into())
            .collect();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            routes,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Vpc Router Routes

/// Fetch a route
#[endpoint {
    method = GET,
    path = "/v1/vpc-router-routes/{route}",
    tags = ["vpcs"],
}]
async fn vpc_router_route_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::RoutePath>,
    query_params: Query<params::RouterSelector>,
) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let route_selector = params::RouteSelector {
            router_selector: Some(query),
            route: path.route,
        };
        let (.., route) = nexus
            .vpc_router_route_lookup(&opctx, &route_selector)?
            .fetch()
            .await?;
        Ok(HttpResponseOk(route.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create a router
#[endpoint {
    method = POST,
    path = "/v1/vpc-router-routes",
    tags = ["vpcs"],
}]
async fn vpc_router_route_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<params::RouterSelector>,
    create_params: TypedBody<params::RouterRouteCreate>,
) -> Result<HttpResponseCreated<RouterRoute>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let create = create_params.into_inner();
        let router_lookup = nexus.vpc_router_lookup(&opctx, &query)?;
        let route = nexus
            .router_create_route(
                &opctx,
                &router_lookup,
                &RouterRouteKind::Custom,
                &create,
            )
            .await?;
        Ok(HttpResponseCreated(route.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete a route
#[endpoint {
    method = DELETE,
    path = "/v1/vpc-router-routes/{route}",
    tags = ["vpcs"],
}]
async fn vpc_router_route_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::RoutePath>,
    query_params: Query<params::OptionalRouterSelector>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let route_selector = params::RouteSelector {
            router_selector: query.router_selector,
            route: path.route,
        };
        let route_lookup =
            nexus.vpc_router_route_lookup(&opctx, &route_selector)?;
        nexus.router_delete_route(&opctx, &route_lookup).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Update a route
#[endpoint {
    method = PUT,
    path = "/v1/vpc-router-routes/{route}",
    tags = ["vpcs"],
}]
async fn vpc_router_route_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::RoutePath>,
    query_params: Query<params::OptionalRouterSelector>,
    router_params: TypedBody<params::RouterRouteUpdate>,
) -> Result<HttpResponseOk<RouterRoute>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let router_params = router_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let route_selector = params::RouteSelector {
            router_selector: query.router_selector,
            route: path.route,
        };
        let route_lookup =
            nexus.vpc_router_route_lookup(&opctx, &route_selector)?;
        let route = nexus
            .router_update_route(&opctx, &route_lookup, &router_params)
            .await?;
        Ok(HttpResponseOk(route.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Racks

/// List racks
#[endpoint {
    method = GET,
    path = "/v1/system/hardware/racks",
    tags = ["system"],
}]
async fn rack_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Rack>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/hardware/racks/{rack_id}",
    tags = ["system"],
}]
async fn rack_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<RackPathParam>,
) -> Result<HttpResponseOk<Rack>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let rack_info = nexus.rack_lookup(&opctx, &path.rack_id).await?;
        Ok(HttpResponseOk(rack_info.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Sleds

/// List sleds
#[endpoint {
    method = GET,
    path = "/v1/system/hardware/sleds",
    tags = ["system"],
}]
async fn sled_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Sled>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/hardware/sleds/{sled_id}",
    tags = ["system"],
}]
async fn sled_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SledPathParam>,
) -> Result<HttpResponseOk<Sled>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let sled_info = nexus.sled_lookup(&opctx, &path.sled_id).await?;
        Ok(HttpResponseOk(sled_info.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Physical disks

/// List physical disks
#[endpoint {
    method = GET,
    path = "/v1/system/hardware/disks",
    tags = ["system"],
}]
async fn physical_disk_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<PhysicalDisk>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let disks = nexus
            .physical_disk_list(&opctx, &data_page_params_for(&rqctx, &query)?)
            .await?
            .into_iter()
            .map(|s| s.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            disks,
            &|_, disk: &PhysicalDisk| disk.identity.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List physical disks attached to sleds
#[endpoint {
    method = GET,
    path = "/v1/system/hardware/sleds/{sled_id}/disks",
    tags = ["system"],
}]
async fn sled_physical_disk_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SledPathParam>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<PhysicalDisk>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let disks = nexus
            .sled_list_physical_disks(
                &opctx,
                path.sled_id,
                &data_page_params_for(&rqctx, &query)?,
            )
            .await?
            .into_iter()
            .map(|s| s.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            disks,
            &|_, disk: &PhysicalDisk| disk.identity.id,
        )?))
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
     path = "/v1/system/metrics/{metric_name}",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let result = nexus
            .system_metric_lookup(&opctx, metric_name, query, limit)
            .await?;

        Ok(HttpResponseOk(result))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Updates

/// Refresh update data
#[endpoint {
     method = POST,
     path = "/v1/system/update/refresh",
     tags = ["system"],
}]
async fn system_update_refresh(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        nexus.updates_refresh_metadata(&opctx).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// View system version and update status
#[endpoint {
     method = GET,
     path = "/v1/system/update/version",
     tags = ["system"],
}]
async fn system_version(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<views::SystemVersion>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        // The only way we have no latest deployment is if the rack was just set
        // up and no system updates have ever been run. In this case there is no
        // update running, so we can fall back to steady.
        let status = nexus
            .latest_update_deployment(&opctx)
            .await
            .map_or(views::UpdateStatus::Steady, |d| d.status.into());

        // Updateable components, however, are populated at rack setup before
        // the external API is even started, so if we get here and there are no
        // components, that's a real issue and the 500 we throw is appropriate.
        let low = nexus.lowest_component_system_version(&opctx).await?.into();
        let high = nexus.highest_component_system_version(&opctx).await?.into();

        Ok(HttpResponseOk(views::SystemVersion {
            version_range: views::VersionRange { low, high },
            status,
        }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// View version and update status of component tree
#[endpoint {
     method = GET,
     path = "/v1/system/update/components",
     tags = ["system"],
}]
async fn system_component_version_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<views::UpdateableComponent>>, HttpError>
{
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let components = nexus
            .updateable_components_list_by_id(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|u| u.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            components,
            &|_, u: &views::UpdateableComponent| u.identity.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List all updates
#[endpoint {
     method = GET,
     path = "/v1/system/update/updates",
     tags = ["system"],
}]
async fn system_update_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<views::SystemUpdate>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let updates = nexus
            .system_updates_list_by_id(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|u| u.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            updates,
            &|_, u: &views::SystemUpdate| u.identity.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// View system update
#[endpoint {
     method = GET,
     path = "/v1/system/update/updates/{version}",
     tags = ["system"],
}]
async fn system_update_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SystemUpdatePath>,
) -> Result<HttpResponseOk<views::SystemUpdate>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let system_update =
            nexus.system_update_fetch_by_version(&opctx, &path.version).await?;
        Ok(HttpResponseOk(system_update.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// View system update component tree
#[endpoint {
    method = GET,
    path = "/v1/system/update/updates/{version}/components",
    tags = ["system"],
}]
async fn system_update_components_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SystemUpdatePath>,
) -> Result<HttpResponseOk<ResultsPage<views::ComponentUpdate>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let components = nexus
            .system_update_list_components(&opctx, &path.version)
            .await?
            .into_iter()
            .map(|i| i.into())
            .collect();
        Ok(HttpResponseOk(ResultsPage { items: components, next_page: None }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Start system update
#[endpoint {
    method = POST,
    path = "/v1/system/update/start",
    tags = ["system"],
}]
async fn system_update_start(
    rqctx: RequestContext<Arc<ServerContext>>,
    // The use of the request body here instead of a path param is deliberate.
    // Unlike instance start (which uses a path param), update start is about
    // modifying the state of the system rather than the state of the resource
    // (instance there, system update here) identified by the param. This
    // approach also gives us symmetry with the /stop endpoint.
    update: TypedBody<params::SystemUpdateStart>,
) -> Result<HttpResponseAccepted<views::UpdateDeployment>, HttpError> {
    let apictx = rqctx.context();
    let _nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // inverse situation to stop: we only want to actually start an update
        // if there isn't one already in progress.

        // 1. check that there is no update in progress
        //   a. if there is one, this should probably 409
        // 2. kick off the update start saga, which
        //   a. tells the update system to get going
        //   b. creates an update deployment

        // similar question for stop: do we return the deployment directly, or a
        // special StartUpdateResult that includes a deployment ID iff an update
        // was actually started

        Ok(HttpResponseAccepted(views::UpdateDeployment {
            identity: AssetIdentityMetadata {
                id: Uuid::new_v4(),
                time_created: Utc::now(),
                time_modified: Utc::now(),
            },
            version: update.into_inner().version,
            status: views::UpdateStatus::Updating,
        }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Stop system update
///
/// If there is no update in progress, do nothing.
#[endpoint {
    method = POST,
    path = "/v1/system/update/stop",
    tags = ["system"],
}]
async fn system_update_stop(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let _nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        // TODO: Implement stopping an update. Should probably be a saga.

        // Ask update subsystem if it's doing anything. If so, tell it to stop.
        // This could be done in a single call to the updater if the latter can
        // respond to a stop command differently depending on whether it did
        // anything or not.

        // If we did in fact stop a running update, update the status on the
        // latest update deployment in the DB to `stopped` and respond with that
        // deployment. If we do nothing, what should we return? Maybe instead of
        // responding with the deployment, this endpoint gets its own
        // `StopUpdateResult` response view that says whether it was a noop, and
        // if it wasn't, includes the ID of the stopped deployment, which allows
        // the client to fetch it if it actually wants it.

        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List all update deployments
#[endpoint {
     method = GET,
     path = "/v1/system/update/deployments",
     tags = ["system"],
}]
async fn update_deployments_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<views::UpdateDeployment>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams = data_page_params_for(&rqctx, &query)?;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let updates = nexus
            .update_deployments_list_by_id(&opctx, &pagparams)
            .await?
            .into_iter()
            .map(|u| u.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            updates,
            &|_, u: &views::UpdateDeployment| u.identity.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch a system update deployment
#[endpoint {
     method = GET,
     path = "/v1/system/update/deployments/{id}",
     tags = ["system"],
}]
async fn update_deployment_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ByIdPathParams>,
) -> Result<HttpResponseOk<views::UpdateDeployment>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let id = &path.id;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let deployment =
            nexus.update_deployment_fetch_by_id(&opctx, id).await?;
        Ok(HttpResponseOk(deployment.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Sagas

/// List sagas
#[endpoint {
    method = GET,
    path = "/v1/system/sagas",
    tags = ["system"],
}]
async fn saga_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Saga>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&rqctx, &query)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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
    path = "/v1/system/sagas/{saga_id}",
    tags = ["system"],
}]
async fn saga_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SagaPathParam>,
) -> Result<HttpResponseOk<Saga>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let saga = nexus.saga_get(&opctx, path.saga_id).await?;
        Ok(HttpResponseOk(saga))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo users

/// List users
#[endpoint {
    method = GET,
    path = "/v1/users",
    tags = ["silos"],
}]
async fn user_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById<params::OptionalGroupSelector>>,
) -> Result<HttpResponseOk<ResultsPage<User>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&rqctx, &query)?;
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let scan_params = ScanById::from_query(&query)?;

        // TODO: a valid UUID gets parsed here and will 404 if it doesn't exist
        // (as expected) but a non-UUID string just gets let through as None
        // (i.e., ignored) instead of 400ing

        let users = if let Some(group_id) = scan_params.selector.group {
            nexus
                .current_silo_group_users_list(&opctx, &pagparams, &group_id)
                .await?
        } else {
            nexus.silo_users_list_current(&opctx, &pagparams).await?
        };

        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            users.into_iter().map(|i| i.into()).collect(),
            &|_, user: &User| user.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Silo groups

/// List groups
#[endpoint {
    method = GET,
    path = "/v1/groups",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

/// Fetch group
#[endpoint {
    method = GET,
    path = "/v1/groups/{group}",
    tags = ["silos"],
}]
async fn group_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::GroupPath>,
) -> Result<HttpResponseOk<Group>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let (.., group) =
            nexus.silo_group_lookup(&opctx, &path.group).fetch().await?;
        Ok(HttpResponseOk(group.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Built-in (system) users

/// List built-in users
#[endpoint {
    method = GET,
    path = "/v1/system/users-builtin",
    tags = ["system"],
}]
async fn user_builtin_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByName>,
) -> Result<HttpResponseOk<ResultsPage<UserBuiltin>>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let query = query_params.into_inner();
    let pagparams =
        data_page_params_for(&rqctx, &query)?.map_name(|n| Name::ref_cast(n));
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

/// Fetch a built-in user
#[endpoint {
    method = GET,
    path = "/v1/system/users-builtin/{user}",
    tags = ["system"],
}]
async fn user_builtin_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::UserBuiltinSelector>,
) -> Result<HttpResponseOk<UserBuiltin>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let nexus = &apictx.nexus;
        let user_selector = path_params.into_inner();
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let (.., user) =
            nexus.user_builtin_lookup(&opctx, &user_selector)?.fetch().await?;
        Ok(HttpResponseOk(user.into()))
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

/// Path parameters for global (system) role requests
#[derive(Deserialize, JsonSchema)]
struct RolePathParam {
    /// The built-in role's unique name.
    role_name: String,
}

/// List built-in roles
#[endpoint {
    method = GET,
    path = "/v1/system/roles",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
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

/// Fetch a built-in role
#[endpoint {
    method = GET,
    path = "/v1/system/roles/{role_name}",
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
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let role = nexus.role_builtin_fetch(&opctx, &role_name).await?;
        Ok(HttpResponseOk(role.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Current user

/// Fetch the user associated with the current session
#[endpoint {
   method = GET,
   path = "/v1/me",
   tags = ["session"],
}]
pub async fn current_user_view(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<views::CurrentUser>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let user = nexus.silo_user_fetch_self(&opctx).await?;
        let silo = nexus.silo_user_fetch_silo(&opctx).await?;
        Ok(HttpResponseOk(views::CurrentUser {
            user: user.into(),
            silo_name: silo.name().clone(),
        }))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetch the silo groups the current user belongs to
#[endpoint {
    method = GET,
    path = "/v1/me/groups",
    tags = ["session"],
 }]
pub async fn current_user_groups(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<views::Group>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let groups = nexus
            .silo_user_fetch_groups_for_self(
                &opctx,
                &data_page_params_for(&rqctx, &query)?,
            )
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect();
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            groups,
            &|_, group: &views::Group| group.id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Per-user SSH public keys

/// List SSH public keys
///
/// Lists SSH public keys for the currently authenticated user.
#[endpoint {
    method = GET,
    path = "/v1/me/ssh-keys",
    tags = ["session"],
}]
async fn current_user_ssh_key_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    query_params: Query<PaginatedByNameOrId>,
) -> Result<HttpResponseOk<ResultsPage<SshKey>>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pag_params = data_page_params_for(&rqctx, &query)?;
        let scan_params = ScanByNameOrId::from_query(&query)?;
        let paginated_by = name_or_id_pagination(&pag_params, scan_params)?;
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("listing current user's ssh keys")?;
        let ssh_keys = nexus
            .ssh_keys_list(&opctx, actor.actor_id(), &paginated_by)
            .await?
            .into_iter()
            .map(SshKey::from)
            .collect::<Vec<SshKey>>();
        Ok(HttpResponseOk(ScanByNameOrId::results_page(
            &query,
            ssh_keys,
            &marker_for_name_or_id,
        )?))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Create an SSH public key
///
/// Create an SSH public key for the currently authenticated user.
#[endpoint {
    method = POST,
    path = "/v1/me/ssh-keys",
    tags = ["session"],
}]
async fn current_user_ssh_key_create(
    rqctx: RequestContext<Arc<ServerContext>>,
    new_key: TypedBody<params::SshKeyCreate>,
) -> Result<HttpResponseCreated<SshKey>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
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

/// Fetch an SSH public key
///
/// Fetch an SSH public key associated with the currently authenticated user.
#[endpoint {
    method = GET,
    path = "/v1/me/ssh-keys/{ssh_key}",
    tags = ["session"],
}]
async fn current_user_ssh_key_view(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SshKeyPath>,
) -> Result<HttpResponseOk<SshKey>, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("fetching one of current user's ssh keys")?;
        let ssh_key_selector = params::SshKeySelector {
            silo_user_id: actor.actor_id(),
            ssh_key: path.ssh_key,
        };
        let ssh_key_lookup = nexus.ssh_key_lookup(&opctx, &ssh_key_selector)?;
        let (.., silo_user, _, ssh_key) = ssh_key_lookup.fetch().await?;
        // Ensure the SSH key exists in the current silo
        assert_eq!(silo_user.id(), actor.actor_id());
        Ok(HttpResponseOk(ssh_key.into()))
    };
    apictx.external_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Delete an SSH public key
///
/// Delete an SSH public key associated with the currently authenticated user.
#[endpoint {
    method = DELETE,
    path = "/v1/me/ssh-keys/{ssh_key}",
    tags = ["session"],
}]
async fn current_user_ssh_key_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<params::SshKeyPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = rqctx.context();
    let handler = async {
        let opctx = crate::context::op_context_for_external_api(&rqctx).await?;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let &actor = opctx
            .authn
            .actor_required()
            .internal_context("deleting one of current user's ssh keys")?;
        let ssh_key_selector = params::SshKeySelector {
            silo_user_id: actor.actor_id(),
            ssh_key: path.ssh_key,
        };
        let ssh_key_lookup = nexus.ssh_key_lookup(&opctx, &ssh_key_selector)?;
        nexus.ssh_key_delete(&opctx, actor.actor_id(), &ssh_key_lookup).await?;
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
