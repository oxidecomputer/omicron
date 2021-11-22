//! HTTP entrypoint functions for the sled agent's exposed API

use super::params::DiskEnsureBody;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk, Path, RequestContext,
    TypedBody,
};
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
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
        api.register(instance_put)?;
        api.register(disk_put)?;
        api.register(update_artifact)?;
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
    rqctx: Arc<RequestContext<SledAgent>>,
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
        .await?,
    ))
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub enum UpdateArtifactKind {
    Zone,
}

// TODO: De-duplicate this struct with the one in iliana's PR?
#[derive(Clone, Debug, Deserialize, JsonSchema)]
struct UpdateArtifact {
    pub name: String,
    pub version: i64,
    pub kind: UpdateArtifactKind,
}

#[endpoint {
    method = POST,
    path = "/update"
}]
async fn update_artifact(
    rqctx: Arc<RequestContext<SledAgent>>,
    artifact: TypedBody<UpdateArtifact>,
) -> Result<HttpResponseOk<()>, HttpError> {
    let sa = rqctx.context();

    // TODO: pass to `update_artifact`.
    let _artifact = artifact.into_inner();

    Ok(HttpResponseOk(sa.update_artifact().await?))
}
