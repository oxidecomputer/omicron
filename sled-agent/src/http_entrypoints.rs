// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use crate::params::{
    DatasetEnsureBody, DiskEnsureBody, InstanceEnsureBody,
    InstanceSerialConsoleData, InstanceSerialConsoleRequest, ServiceEnsureBody,
    VpcFirewallRulesEnsureBody, Zpool,
};
use crate::serial::ByteOffset;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, Query, RequestContext, TypedBody,
};
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::UpdateArtifact;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use super::sled_agent::SledAgent;

type SledApiDescription = ApiDescription<SledAgent>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints(api: &mut SledApiDescription) -> Result<(), String> {
        api.register(services_put)?;
        api.register(zpools_get)?;
        api.register(filesystem_put)?;
        api.register(instance_put)?;
        api.register(disk_put)?;
        api.register(update_artifact)?;
        api.register(instance_serial_get)?;
        api.register(instance_issue_disk_snapshot_request)?;
        api.register(vpc_firewall_rules_put)?;

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
    rqctx: Arc<RequestContext<SledAgent>>,
    body: TypedBody<ServiceEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    sa.services_ensure(body_args).await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/zpools",
}]
async fn zpools_get(
    rqctx: Arc<RequestContext<SledAgent>>,
) -> Result<HttpResponseOk<Vec<Zpool>>, HttpError> {
    let sa = rqctx.context();
    Ok(HttpResponseOk(sa.zpools_get().await.map_err(|e| Error::from(e))?))
}

#[endpoint {
    method = PUT,
    path = "/filesystem",
}]
async fn filesystem_put(
    rqctx: Arc<RequestContext<SledAgent>>,
    body: TypedBody<DatasetEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    sa.filesystem_ensure(
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
async fn instance_put(
    rqctx: Arc<RequestContext<SledAgent>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceEnsureBody>,
) -> Result<HttpResponseOk<InstanceRuntimeState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure(
            instance_id,
            body_args.initial,
            body_args.target,
            body_args.migrate,
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
    rqctx: Arc<RequestContext<SledAgent>>,
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
    rqctx: Arc<RequestContext<SledAgent>>,
    artifact: TypedBody<UpdateArtifact>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    sa.update_artifact(artifact.into_inner()).await.map_err(Error::from)?;
    Ok(HttpResponseUpdatedNoContent())
}

#[endpoint {
    method = GET,
    path = "/instances/{instance_id}/serial",
}]
async fn instance_serial_get(
    rqctx: Arc<RequestContext<SledAgent>>,
    path_params: Path<InstancePathParam>,
    query: Query<InstanceSerialConsoleRequest>,
) -> Result<HttpResponseOk<InstanceSerialConsoleData>, HttpError> {
    // TODO: support websocket in dropshot, detect websocket upgrade header and proxy the data

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
        .map_err(Error::from)?;

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
#[endpoint {
    method = POST,
    path = "/instances/{instance_id}/disks/{disk_id}/snapshot",
}]
async fn instance_issue_disk_snapshot_request(
    rqctx: Arc<RequestContext<SledAgent>>,
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
    rqctx: Arc<RequestContext<SledAgent>>,
    path_params: Path<VpcPathParam>,
    body: TypedBody<VpcFirewallRulesEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let vpc_id = path_params.into_inner().vpc_id;
    let body_args = body.into_inner();

    sa.firewall_rules_ensure(vpc_id, &body_args.rules[..])
        .await
        .map_err(Error::from)?;

    Ok(HttpResponseUpdatedNoContent())
}
