// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use crate::params::{
    DiskEnsureBody, InstanceEnsureBody, InstanceSerialConsoleData,
    InstanceSerialConsoleRequest, VpcFirewallRulesEnsureBody,
};
use crate::serial::ByteOffset;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::TypedBody;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::UpdateArtifactId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use super::sled_agent::SledAgent;

use crucible_client_types::VolumeConstructionRequest;

type SledApiDescription = ApiDescription<Arc<SledAgent>>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints(api: &mut SledApiDescription) -> Result<(), String> {
        api.register(instance_put)?;
        api.register(instance_poke_post)?;
        api.register(disk_put)?;
        api.register(disk_poke_post)?;
        api.register(update_artifact)?;
        api.register(instance_serial_get)?;
        api.register(instance_issue_disk_snapshot_request)?;
        api.register(issue_disk_snapshot_request)?;
        api.register(vpc_firewall_rules_put)?;

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
    instance_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/instances/{instance_id}",
}]
async fn instance_put(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceEnsureBody>,
) -> Result<HttpResponseOk<InstanceRuntimeState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure(instance_id, body_args.initial, body_args.target)
            .await?,
    ))
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
    sa.instance_poke(instance_id).await;
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
    crate::updates::download_artifact(
        artifact.into_inner(),
        rqctx.context().nexus_client.as_ref(),
    )
    .await
    .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/instances/{instance_id}/serial",
}]
async fn instance_serial_get(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<InstancePathParam>,
    query: Query<InstanceSerialConsoleRequest>,
) -> Result<HttpResponseOk<InstanceSerialConsoleData>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let query_params = query.into_inner();

    let byte_offset = match query_params {
        InstanceSerialConsoleRequest {
            from_start: Some(offset),
            most_recent: None,
            ..
        } => ByteOffset::FromStart(offset as usize),
        InstanceSerialConsoleRequest {
            from_start: None,
            most_recent: Some(offset),
            ..
        } => ByteOffset::MostRecent(offset as usize),
        _ => return Err(HttpError::for_bad_request(
            None,
            "Exactly one of 'from_start' or 'most_recent' must be specified."
                .to_string(),
        )),
    };

    let data = sa
        .instance_serial_console_data(
            instance_id,
            byte_offset,
            query_params.max_bytes.map(|x| x as usize),
        )
        .await
        .map_err(HttpError::for_internal_error)?;

    Ok(HttpResponseOk(data))
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
///
/// For disks not attached to an instance, see [`issue_disk_snapshot_request`]
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
        path_params.instance_id,
        path_params.disk_id,
        body.snapshot_id,
    )
    .await
    .map_err(|e| HttpError::for_internal_error(e.to_string()))?;

    Ok(HttpResponseOk(InstanceIssueDiskSnapshotRequestResponse {
        snapshot_id: body.snapshot_id,
    }))
}

#[derive(Deserialize, JsonSchema)]
pub struct IssueDiskSnapshotRequestPathParam {
    disk_id: Uuid,
}

#[derive(Deserialize, JsonSchema)]
pub struct DiskSnapshotRequestBody {
    volume_construction_request: VolumeConstructionRequest,
    snapshot_id: Uuid,
}

#[derive(Serialize, JsonSchema)]
pub struct DiskSnapshotRequestResponse {
    snapshot_id: Uuid,
}

/// Take a snapshot of a disk that is not attached to an instance.
///
/// For disks attached to an instance, see
/// [`instance_issue_disk_snapshot_request`]
#[endpoint {
    method = POST,
    path = "/disks/{disk_id}/snapshot",
}]
async fn issue_disk_snapshot_request(
    rqctx: RequestContext<Arc<SledAgent>>,
    path_params: Path<IssueDiskSnapshotRequestPathParam>,
    body: TypedBody<DiskSnapshotRequestBody>,
) -> Result<HttpResponseOk<DiskSnapshotRequestResponse>, HttpError> {
    let sa = rqctx.context();
    let path_params = path_params.into_inner();
    let body = body.into_inner();

    sa.issue_disk_snapshot_request(
        path_params.disk_id,
        body.volume_construction_request,
        body.snapshot_id,
    )
    .await?;

    Ok(HttpResponseOk(DiskSnapshotRequestResponse {
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
