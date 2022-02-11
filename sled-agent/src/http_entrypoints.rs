// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the sled agent's exposed API

use super::params::DiskEnsureBody;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::TypedBody;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::sled_agent::PartitionEnsureBody;
use omicron_common::api::internal::sled_agent::InstanceEnsureBody;
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use super::sled_agent::SledAgent;

type SledApiDescription = ApiDescription<SledAgent>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints(api: &mut SledApiDescription) -> Result<(), String> {
        api.register(filesystem_put)?;
        api.register(instance_put)?;
        api.register(disk_put)?;
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
    path = "/filesystem",
}]
async fn filesystem_put(
    rqctx: Arc<RequestContext<SledAgent>>,
    body: TypedBody<PartitionEnsureBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let sa = rqctx.context();
    let body_args = body.into_inner();
    sa.filesystem_ensure(
            body_args.zpool_uuid,
            body_args.partition_kind,
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
        .map_err(|e| Error::from(e))?,
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
