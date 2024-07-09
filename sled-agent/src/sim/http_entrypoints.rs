// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use super::collection::PokeMode;
use crate::bootstrap::params::AddSledRequest;
use crate::params::{
    DiskEnsureBody, InstanceEnsureBody, InstanceExternalIpBody,
    InstancePutMigrationIdsBody, InstancePutStateBody,
    InstancePutStateResponse, InstanceUnregisterResponse,
    VpcFirewallRulesEnsureBody,
};
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::TypedBody;
use dropshot::{endpoint, ApiDescriptionRegisterError};
use illumos_utils::opte::params::VirtualNetworkInterfaceHost;
use nexus_sled_agent_shared::inventory::{Inventory, OmicronZonesConfig};
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::SledInstanceState;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use omicron_common::api::internal::shared::{
    ResolvedVpcRouteSet, ResolvedVpcRouteState, SwitchPorts,
};
use omicron_common::disk::OmicronPhysicalDisksConfig;
use omicron_uuid_kinds::{GenericUuid, InstanceUuid};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::EarlyNetworkConfig;
use sled_storage::resources::DisksManagementResult;
use std::sync::Arc;
use uuid::Uuid;

use super::sled_agent::SledAgent;

type SledApiDescription = ApiDescription<Arc<SledAgent>>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints(
        api: &mut SledApiDescription,
    ) -> Result<(), ApiDescriptionRegisterError> {
        api.register(instance_put_migration_ids)?;
        api.register(instance_put_state)?;
        api.register(instance_get_state)?;
        api.register(instance_register)?;
        api.register(instance_unregister)?;
        api.register(instance_put_external_ip)?;
        api.register(instance_delete_external_ip)?;
        api.register(instance_poke_post)?;
        api.register(instance_poke_single_step_post)?;
        api.register(instance_post_sim_migration_source)?;
        api.register(disk_put)?;
        api.register(disk_poke_post)?;
        api.register(update_artifact)?;
        api.register(instance_issue_disk_snapshot_request)?;
        api.register(vpc_firewall_rules_put)?;
        api.register(set_v2p)?;
        api.register(del_v2p)?;
        api.register(list_v2p)?;
        api.register(uplink_ensure)?;
        api.register(read_network_bootstore_config)?;
        api.register(write_network_bootstore_config)?;
        api.register(inventory)?;
        api.register(omicron_physical_disks_get)?;
        api.register(omicron_physical_disks_put)?;
        api.register(omicron_zones_get)?;
        api.register(omicron_zones_put)?;
        api.register(sled_add)?;
        api.register(list_vpc_routes)?;
        api.register(set_vpc_routes)?;

        Ok(())
    }

    let mut api = SledApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// Path parameters for Instance requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
struct InstancePathParam {
    instance_id: InstanceUuid,
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}",
}]
async fn instance_register(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceEnsureBody>,
) -> Result<HttpResponseOk<SledInstanceState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_register(
            instance_id,
            body_args.propolis_id,
            body_args.hardware,
            body_args.instance_runtime,
            body_args.vmm_runtime,
            body_args.metadata,
        )
        .await?,
    ))
}

#[endpoint {
    method = DELETE,
    path = "/instances/{instance_id}",
}]
async fn instance_unregister(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<InstanceUnregisterResponse>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    Ok(HttpResponseOk(sa.instance_unregister(instance_id).await?))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/state",
}]
async fn instance_put_state(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstancePutStateBody>,
) -> Result<HttpResponseOk<InstancePutStateResponse>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure_state(instance_id, body_args.state).await?,
    ))
}

#[endpoint {
    method = GET,
    path = "/instances/{instance_id}/state",
}]
async fn instance_get_state(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<SledInstanceState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    Ok(HttpResponseOk(sa.instance_get_state(instance_id).await?))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/migration-ids",
}]
async fn instance_put_migration_ids(
    _: RequestContext<Arc<SledAgent>>,
    _: Path<InstancePathParam>,
    _: TypedBody<InstancePutMigrationIdsBody>,
) -> Result<HttpResponseOk<SledInstanceState>, HttpError> {
    Err(HttpError::for_bad_request(
        None,
        "operation no longer supported".to_string(),
    ))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/external-ip",
}]
async fn instance_put_external_ip(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceExternalIpBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    sa.instance_put_external_ip(instance_id, &body_args).await?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = DELETE,
    path = "/instances/{instance_id}/external-ip",
}]
async fn instance_delete_external_ip(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceExternalIpBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    sa.instance_delete_external_ip(instance_id, &body_args).await?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/instances/{instance_id}/poke",
}]
async fn instance_poke_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    sa.instance_poke(instance_id, PokeMode::Drain).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/instances/{instance_id}/poke-single-step",
}]
async fn instance_poke_single_step_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    sa.instance_poke(instance_id, PokeMode::SingleStep).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/instances/{instance_id}/sim-migration-source",
}]
async fn instance_post_sim_migration_source(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<super::instance::SimulateMigrationSource>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    sa.instance_simulate_migration_source(instance_id, body.into_inner())
        .await?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Path parameters for Disk requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
struct DiskPathParam {
    disk_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/disks/{disk_id}",
}]
async fn disk_put(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<DiskPathParam>,
    body: TypedBody<DiskEnsureBody>,
) -> Result<HttpResponseOk<DiskRuntimeState>, HttpError> {
    let sa = rqctx.context();
    let disk_id = path_params.into_inner().disk_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.disk_ensure(
            disk_id,
            body_args.initial_runtime.clone(),
            body_args.target.clone(),
        )
        .await?,
    ))
}

#[endpoint {
    method = POST,
    path = "/disks/{disk_id}/poke",
}]
async fn disk_poke_post(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let disk_id = path_params.into_inner().disk_id;
    sa.disk_poke(disk_id).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = POST,
    path = "/update"
}]
async fn update_artifact(
    rqctx: RequestContext<Arc<SledAgent>>,
    artifact: TypedBody<UpdateArtifactId>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    sa.updates()
        .download_artifact(
            artifact.into_inner(),
            rqctx.context().nexus_client.as_ref(),
        )
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Deserialize, JsonSchema)]
pub struct InstanceIssueDiskSnapshotRequestPathParam {
    instance_id: Uuid,
    disk_id: Uuid,
}

#[derive(Deserialize, JsonSchema)]
pub struct InstanceIssueDiskSnapshotRequestBody {
    snapshot_id: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct InstanceIssueDiskSnapshotRequestResponse {
    snapshot_id: Uuid,
}

/// Take a snapshot of a disk that is attached to an instance
#[endpoint {
    method = POST,
    path = "/instances/{instance_id}/disks/{disk_id}/snapshot",
}]
async fn instance_issue_disk_snapshot_request(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstanceIssueDiskSnapshotRequestPathParam>,
    body: TypedBody<InstanceIssueDiskSnapshotRequestBody>,
) -> Result<HttpResponseOk<InstanceIssueDiskSnapshotRequestResponse>, HttpError>
{
    let sa = rqctx.context();
    let path_params = path_params.into_inner();
    let body = body.into_inner();

    sa.instance_issue_disk_snapshot_request(
        InstanceUuid::from_untyped_uuid(path_params.instance_id),
        path_params.disk_id,
        body.snapshot_id,
    )
    .await
    .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(InstanceIssueDiskSnapshotRequestResponse {
        snapshot_id: body.snapshot_id,
    }))
}

/// Path parameters for VPC requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
struct VpcPathParam {
    vpc_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/vpc/{vpc_id}/firewall/rules",
}]
async fn vpc_firewall_rules_put(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<VpcPathParam>,
    body: TypedBody<VpcFirewallRulesEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let _sa = rqctx.context();
    let _vpc_id = path_params.into_inner().vpc_id;
    let _body_args = body.into_inner();

    Ok(HttpResponseUpdatedNoContent())
}

/// Create a mapping from a virtual NIC to a physical host
#[endpoint {
    method = PUT,
    path = "/v2p/",
}]
async fn set_v2p(
    rqctx: RequestContext<Arc<SledAgent>>,
    body: TypedBody<VirtualNetworkInterfaceHost>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();

    sa.set_virtual_nic_host(&body_args)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Delete a mapping from a virtual NIC to a physical host
#[endpoint {
    method = DELETE,
    path = "/v2p/",
}]
async fn del_v2p(
    rqctx: RequestContext<Arc<SledAgent>>,
    body: TypedBody<VirtualNetworkInterfaceHost>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();

    sa.unset_virtual_nic_host(&body_args)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseUpdatedNoContent())
}

/// List v2p mappings present on sled
#[endpoint {
    method = GET,
    path = "/v2p/",
}]
async fn list_v2p(
    rqctx: RequestContext<Arc<SledAgent>>,
) -> Result<HttpResponseOk<Vec<VirtualNetworkInterfaceHost>>, HttpError> {
    let sa = rqctx.context();

    let vnics = sa.list_virtual_nics().await.map_err(HttpError::from)?;

    Ok(HttpResponseOk(vnics))
}

#[endpoint {
    method = POST,
    path = "/switch-ports",
}]
async fn uplink_ensure(
    _rqctx: RequestContext<Arc<SledAgent>>,
    _body: TypedBody<SwitchPorts>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/network-bootstore-config",
}]
async fn read_network_bootstore_config(
    rqctx: RequestContext<Arc<SledAgent>>,
) -> Result<HttpResponseOk<EarlyNetworkConfig>, HttpError> {
    let config = rqctx.context().bootstore_network_config.lock().await.clone();
    Ok(HttpResponseOk(config))
}

#[endpoint {
    method = PUT,
    path = "/network-bootstore-config",
}]
async fn write_network_bootstore_config(
    rqctx: RequestContext<Arc<SledAgent>>,
    body: TypedBody<EarlyNetworkConfig>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let mut config = rqctx.context().bootstore_network_config.lock().await;
    *config = body.into_inner();
    Ok(HttpResponseUpdatedNoContent())
}

/// Fetch basic information about this sled
#[endpoint {
    method = GET,
    path = "/inventory",
}]
async fn inventory(
    rqctx: RequestContext<Arc<SledAgent>>,
) -> Result<HttpResponseOk<Inventory>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(
        sa.inventory(rqctx.server.local_addr)
            .await
            .map_err(|e| HttpError::for_internal_error(format!("{:#}", e)))?,
    ))
}

#[endpoint {
    method = PUT,
    path = "/omicron-physical-disks",
}]
async fn omicron_physical_disks_put(
    rqctx: RequestContext<Arc<SledAgent>>,
    body: TypedBody<OmicronPhysicalDisksConfig>,
) -> Result<HttpResponseOk<DisksManagementResult>, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    let result = sa.omicron_physical_disks_ensure(body_args).await?;
    Ok(HttpResponseOk(result))
}

#[endpoint {
    method = GET,
    path = "/omicron-physical-disks",
}]
async fn omicron_physical_disks_get(
    rqctx: RequestContext<Arc<SledAgent>>,
) -> Result<HttpResponseOk<OmicronPhysicalDisksConfig>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.omicron_physical_disks_list().await?))
}

#[endpoint {
    method = GET,
    path = "/omicron-zones",
}]
async fn omicron_zones_get(
    rqctx: RequestContext<Arc<SledAgent>>,
) -> Result<HttpResponseOk<OmicronZonesConfig>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.omicron_zones_list().await))
}

#[endpoint {
    method = PUT,
    path = "/omicron-zones",
}]
async fn omicron_zones_put(
    rqctx: RequestContext<Arc<SledAgent>>,
    body: TypedBody<OmicronZonesConfig>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    sa.omicron_zones_ensure(body_args).await;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = PUT,
    path = "/sleds"
}]
async fn sled_add(
    _rqctx: RequestContext<Arc<SledAgent>>,
    _body: TypedBody<AddSledRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/vpc-routes",
}]
async fn list_vpc_routes(
    rqctx: RequestContext<Arc<SledAgent>>,
) -> Result<HttpResponseOk<Vec<ResolvedVpcRouteState>>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.list_vpc_routes().await))
}

#[endpoint {
    method = PUT,
    path = "/vpc-routes",
}]
async fn set_vpc_routes(
    rqctx: RequestContext<Arc<SledAgent>>,
    body: TypedBody<Vec<ResolvedVpcRouteSet>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    sa.set_vpc_routes(body.into_inner()).await;
    Ok(HttpResponseUpdatedNoContent())
}
