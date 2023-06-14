// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use crate::params::{
    DatasetEnsureBody, DiskEnsureBody, InstanceEnsureBody,
    InstancePutMigrationIdsBody, InstancePutStateBody,
    InstancePutStateResponse, InstanceUnregisterResponse, ServiceEnsureBody,
    SledRole, TimeSync, VpcFirewallRulesEnsureBody, Zpool,
};
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, RequestContext, TypedBody,
};
use illumos_utils::opte::params::SetVirtualNetworkInterfaceHost;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::sled_agent::SledAgent;

type SledApiDescription = ApiDescription<SledAgent>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints(api: &mut SledApiDescription) -> Result<(), String> {
        api.register(disk_put)?;
        api.register(filesystem_put)?;
        api.register(instance_issue_disk_snapshot_request)?;
        api.register(instance_put_migration_ids)?;
        api.register(instance_put_state)?;
        api.register(instance_register)?;
        api.register(instance_unregister)?;
        api.register(services_put)?;
        api.register(sled_role_get)?;
        api.register(set_v2p)?;
        api.register(del_v2p)?;
        api.register(timesync_get)?;
        api.register(update_artifact)?;
        api.register(vpc_firewall_rules_put)?;
        api.register(zpools_get)?;

        Ok(())
    }

    let mut api = SledApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[endpoint {
    method = PUT,
    path = "/services",
}]
async fn services_put(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<ServiceEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context().clone();
    let body_args = body.into_inner();

    // Spawn a separate task to run `services_ensure`: cancellation of this
    // endpoint's future (as might happen if the client abandons the request or
    // times out) could result in leaving zones partially configured and the
    // in-memory state of the service manager invalid. See:
    // oxidecomputer/omicron#3098.
    match tokio::spawn(async move { sa.services_ensure(body_args).await }).await
    {
        Ok(result) => result.map_err(|e| Error::from(e))?,

        Err(e) => {
            return Err(HttpError::for_internal_error(format!(
                "unexpected failure awaiting \"services_ensure\": {:#}",
                e
            )));
        }
    }

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/zpools",
}]
async fn zpools_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<Vec<Zpool>>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.zpools_get().await.map_err(|e| Error::from(e))?))
}

#[endpoint {
    method = GET,
    path = "/sled-role",
}]
async fn sled_role_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<SledRole>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.get_role()))
}

#[endpoint {
    method = PUT,
    path = "/filesystem",
}]
async fn filesystem_put(
    rqctx: RequestContext<SledAgent>,
    body: TypedBody<DatasetEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    sa.filesystem_ensure(
        body_args.id,
        body_args.zpool_id,
        body_args.dataset_kind,
        body_args.address,
    )
    .await
    .map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Path parameters for Instance requests (sled agent API)
#[derive(Deserialize, JsonSchema)]
struct InstancePathParam {
    instance_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}",
}]
async fn instance_register(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceEnsureBody>,
) -> Result<HttpResponseOk<InstanceRuntimeState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure_registered(instance_id, body_args.initial)
            .await
            .map_err(Error::from)?,
    ))
}

#[endpoint {
    method = DELETE,
    path = "/instances/{instance_id}",
}]
async fn instance_unregister(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
) -> Result<HttpResponseOk<InstanceUnregisterResponse>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    Ok(HttpResponseOk(
        sa.instance_ensure_unregistered(instance_id)
            .await
            .map_err(Error::from)?,
    ))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/state",
}]
async fn instance_put_state(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstancePutStateBody>,
) -> Result<HttpResponseOk<InstancePutStateResponse>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure_state(instance_id, body_args.state)
            .await
            .map_err(Error::from)?,
    ))
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}/migration-ids",
}]
async fn instance_put_migration_ids(
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstancePutMigrationIdsBody>,
) -> Result<HttpResponseOk<InstanceRuntimeState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_put_migration_ids(
            instance_id,
            &body_args.old_runtime,
            &body_args.migration_params,
        )
        .await
        .map_err(Error::from)?,
    ))
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
    rqctx: RequestContext<SledAgent>,
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
        .await
        .map_err(|e| Error::from(e))?,
    ))
}

#[endpoint {
    method = POST,
    path = "/update"
}]
async fn update_artifact(
    rqctx: RequestContext<SledAgent>,
    artifact: TypedBody<UpdateArtifactId>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    sa.update_artifact(artifact.into_inner()).await.map_err(Error::from)?;
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
    rqctx: RequestContext<SledAgent>,
    path_params: Path<InstanceIssueDiskSnapshotRequestPathParam>,
    body: TypedBody<InstanceIssueDiskSnapshotRequestBody>,
) -> Result<HttpResponseOk<InstanceIssueDiskSnapshotRequestResponse>, HttpError>
{
    let sa = rqctx.context();
    let path_params = path_params.into_inner();
    let body = body.into_inner();

    sa.instance_issue_disk_snapshot_request(
        path_params.instance_id,
        path_params.disk_id,
        body.snapshot_id,
    )
    .await?;

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
    rqctx: RequestContext<SledAgent>,
    path_params: Path<VpcPathParam>,
    body: TypedBody<VpcFirewallRulesEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let vpc_id = path_params.into_inner().vpc_id;
    let body_args = body.into_inner();

    sa.firewall_rules_ensure(vpc_id, &body_args.rules[..])
        .map_err(Error::from)?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Path parameters for V2P mapping related requests (sled agent API)
#[allow(dead_code)]
#[derive(Deserialize, JsonSchema)]
struct V2pPathParam {
    interface_id: Uuid,
}

/// Create a mapping from a virtual NIC to a physical host
// Keep interface_id to maintain parity with the simulated sled agent, which
// requires interface_id on the path.
#[endpoint {
    method = PUT,
    path = "/v2p/{interface_id}",
}]
async fn set_v2p(
    rqctx: RequestContext<SledAgent>,
    _path_params: Path<V2pPathParam>,
    body: TypedBody<SetVirtualNetworkInterfaceHost>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();

    sa.set_virtual_nic_host(&body_args).map_err(Error::from)?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Delete a mapping from a virtual NIC to a physical host
// Keep interface_id to maintain parity with the simulated sled agent, which
// requires interface_id on the path.
#[endpoint {
    method = DELETE,
    path = "/v2p/{interface_id}",
}]
async fn del_v2p(
    rqctx: RequestContext<SledAgent>,
    _path_params: Path<V2pPathParam>,
    body: TypedBody<SetVirtualNetworkInterfaceHost>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();

    sa.unset_virtual_nic_host(&body_args).map_err(Error::from)?;

    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/timesync",
}]
async fn timesync_get(
    rqctx: RequestContext<SledAgent>,
) -> Result<HttpResponseOk<TimeSync>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.timesync_get().await.map_err(|e| Error::from(e))?))
}
