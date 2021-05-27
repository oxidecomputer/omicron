//! HTTP entrypoint functions for the sled agent's exposed API

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::TypedBody;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::DiskEnsureBody;
use omicron_common::model::InstanceEnsureBody;
use omicron_common::model::ProducerId;
use oximeter::collect::ProducerResults;
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

use super::sled_agent::SledAgent;

type SledApiDescription = ApiDescription<Arc<SledAgent>>;

/// Returns a description of the sled agent API
pub fn api() -> SledApiDescription {
    fn register_endpoints(api: &mut SledApiDescription) -> Result<(), String> {
        api.register(instance_put)?;
        api.register(disk_put)?;
        api.register(metrics_collect)?;
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
    rqctx: Arc<RequestContext<Arc<SledAgent>>>,
    path_params: Path<InstancePathParam>,
    body: TypedBody<InstanceEnsureBody>,
) -> Result<HttpResponseOk<ApiInstanceRuntimeState>, HttpError> {
    let sa = rqctx.context();
    let instance_id = path_params.into_inner().instance_id;
    let body_args = body.into_inner();
    Ok(HttpResponseOk(
        sa.instance_ensure(
            instance_id,
            body_args.initial_runtime.clone(),
            body_args.target.clone(),
        )
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
    rqctx: Arc<RequestContext<Arc<SledAgent>>>,
    path_params: Path<DiskPathParam>,
    body: TypedBody<DiskEnsureBody>,
) -> Result<HttpResponseOk<ApiDiskRuntimeState>, HttpError> {
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
    method = GET,
    path = "/metrics/{producer_id}",
}]
async fn metrics_collect(
    rqctx: Arc<RequestContext<Arc<SledAgent>>>,
    path_params: Path<ProducerId>,
) -> Result<HttpResponseOk<ProducerResults>, HttpError> {
    let sled_agent = rqctx.context();
    let producer_id = path_params.into_inner();
    Ok(HttpResponseOk(sled_agent.collect_metrics(producer_id).await?))
}
