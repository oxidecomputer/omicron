/**
 * Handler functions (entrypoints) for HTTP APIs internal to the control plane
 */
use super::ServerContext;

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::TypedBody;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::OximeterInfo;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::SledAgentPoolInfo;
use omicron_common::api::internal::nexus::SledAgentStartupInfo;
use oximeter::types::ProducerResults;
use oximeter_producer::{collect, ProducerIdPathParams};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/**
 * Returns a description of the internal nexus API
 */
pub fn internal_api() -> NexusApiDescription {
    fn register_endpoints(api: &mut NexusApiDescription) -> Result<(), String> {
        api.register(cpapi_sled_agents_post)?;
        api.register(zpool_post)?;
        api.register(cpapi_instances_put)?;
        api.register(cpapi_disks_put)?;
        api.register(cpapi_producers_post)?;
        api.register(cpapi_collectors_post)?;
        api.register(cpapi_metrics_collect)?;
        Ok(())
    }

    let mut api = NexusApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/**
 * Path parameters for Sled Agent requests (internal API)
 */
#[derive(Deserialize, JsonSchema)]
struct SledAgentPathParam {
    sled_id: Uuid,
}

/**
 * Report that the sled agent for the specified sled has come online.
 */
#[endpoint {
     method = POST,
     path = "/sled_agents/{sled_id}",
 }]
async fn cpapi_sled_agents_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<SledAgentPathParam>,
    sled_info: TypedBody<SledAgentStartupInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let si = sled_info.into_inner();
    let sled_id = &path.sled_id;
    let handler = async {
        nexus.upsert_sled(*sled_id, si.sa_address).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for Sled Agent requests (internal API)
 */
#[derive(Deserialize, JsonSchema)]
struct ZpoolPathParam {
    sled_id: Uuid,
    zpool_id: Uuid,
}

/**
 * Report that a pool for a specified sled has come online.
 */
#[endpoint {
     method = POST,
     path = "/sled_agents/{sled_id}/zpools/{zpool_id}",
 }]
async fn zpool_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ZpoolPathParam>,
    pool_info: TypedBody<SledAgentPoolInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let pi = pool_info.into_inner();
    nexus.upsert_zpool(path.zpool_id, path.sled_id, pi.address).await?;
    Ok(HttpResponseUpdatedNoContent())
}

/**
 * Path parameters for Instance requests (internal API)
 */
#[derive(Deserialize, JsonSchema)]
struct InstancePathParam {
    instance_id: Uuid,
}

/**
 * Report updated state for an instance.
 */
#[endpoint {
     method = PUT,
     path = "/instances/{instance_id}",
 }]
async fn cpapi_instances_put(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<InstancePathParam>,
    new_runtime_state: TypedBody<InstanceRuntimeState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    let handler = async {
        nexus.notify_instance_updated(&path.instance_id, &new_state).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Path parameters for Disk requests (internal API)
 */
#[derive(Deserialize, JsonSchema)]
struct DiskPathParam {
    disk_id: Uuid,
}

/**
 * Report updated state for a disk.
 */
#[endpoint {
     method = PUT,
     path = "/disks/{disk_id}",
 }]
async fn cpapi_disks_put(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<DiskPathParam>,
    new_runtime_state: TypedBody<DiskRuntimeState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    let handler = async {
        nexus.notify_disk_updated(&path.disk_id, &new_state).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/**
 * Accept a registration from a new metric producer
 */
#[endpoint {
     method = POST,
     path = "/metrics/producers",
 }]
async fn cpapi_producers_post(
    request_context: Arc<RequestContext<Arc<ServerContext>>>,
    producer_info: TypedBody<ProducerEndpoint>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let context = request_context.context();
    let nexus = &context.nexus;
    let producer_info = producer_info.into_inner();
    let handler = async {
        nexus.assign_producer(producer_info).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    context
        .internal_latencies
        .instrument_dropshot_handler(&request_context, handler)
        .await
}

/**
 * Accept a notification of a new oximeter collection server.
 */
#[endpoint {
     method = POST,
     path = "/metrics/collectors",
 }]
async fn cpapi_collectors_post(
    request_context: Arc<RequestContext<Arc<ServerContext>>>,
    oximeter_info: TypedBody<OximeterInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let context = request_context.context();
    let nexus = &context.nexus;
    let oximeter_info = oximeter_info.into_inner();
    let handler = async {
        nexus.upsert_oximeter_collector(&oximeter_info).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    context
        .internal_latencies
        .instrument_dropshot_handler(&request_context, handler)
        .await
}

/**
 * Endpoint for oximeter to collect nexus server metrics.
 */
#[endpoint {
    method = GET,
    path = "/metrics/collect/{producer_id}",
}]
async fn cpapi_metrics_collect(
    request_context: Arc<RequestContext<Arc<ServerContext>>>,
    path_params: Path<ProducerIdPathParams>,
) -> Result<HttpResponseOk<ProducerResults>, HttpError> {
    let context = request_context.context();
    let producer_id = path_params.into_inner().producer_id;
    let handler =
        async { collect(&context.producer_registry, producer_id).await };
    context
        .internal_latencies
        .instrument_dropshot_handler(&request_context, handler)
        .await
}
