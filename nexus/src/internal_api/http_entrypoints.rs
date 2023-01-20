// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Handler functions (entrypoints) for HTTP APIs internal to the control plane
use crate::context::OpContext;
use crate::ServerContext;

use super::params::{
    OximeterInfo, RackInitializationRequest, SledAgentStartupInfo,
    ZpoolPutRequest, ZpoolPutResponse,
};
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::FreeformBody;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::TypedBody;
use hyper::Body;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::UpdateArtifact;
use oximeter::types::ProducerResults;
use oximeter_producer::{collect, ProducerIdPathParams};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

type NexusApiDescription = ApiDescription<Arc<ServerContext>>;

/// Returns a description of the internal nexus API
pub fn internal_api() -> NexusApiDescription {
    fn register_endpoints(api: &mut NexusApiDescription) -> Result<(), String> {
        api.register(sled_agent_put)?;
        api.register(rack_initialization_complete)?;
        api.register(zpool_put)?;
        api.register(cpapi_instances_put)?;
        api.register(cpapi_disks_put)?;
        api.register(cpapi_volume_remove_read_only_parent)?;
        api.register(cpapi_disk_remove_read_only_parent)?;
        api.register(cpapi_producers_post)?;
        api.register(cpapi_collectors_post)?;
        api.register(cpapi_metrics_collect)?;
        api.register(cpapi_artifact_download)?;
        Ok(())
    }

    let mut api = NexusApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// Path parameters for Sled Agent requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct SledAgentPathParam {
    sled_id: Uuid,
}

/// Report that the sled agent for the specified sled has come online.
#[endpoint {
     method = POST,
     path = "/sled-agents/{sled_id}",
 }]
async fn sled_agent_put(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<SledAgentPathParam>,
    sled_info: TypedBody<SledAgentStartupInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let info = sled_info.into_inner();
    let sled_id = &path.sled_id;
    let handler = async {
        nexus.upsert_sled(*sled_id, info).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Rack requests.
#[derive(Deserialize, JsonSchema)]
struct RackPathParam {
    rack_id: Uuid,
}

/// Report that the Rack Setup Service initialization is complete
///
/// See RFD 278 for more details.
#[endpoint {
     method = PUT,
     path = "/racks/{rack_id}/initialization-complete",
 }]
async fn rack_initialization_complete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<RackPathParam>,
    info: TypedBody<RackInitializationRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let request = info.into_inner();
    let opctx = OpContext::for_internal_api(&rqctx).await;

    nexus.rack_initialize(&opctx, path.rack_id, request).await?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Path parameters for Sled Agent requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct ZpoolPathParam {
    sled_id: Uuid,
    zpool_id: Uuid,
}

/// Report that a pool for a specified sled has come online.
#[endpoint {
     method = PUT,
     path = "/sled-agents/{sled_id}/zpools/{zpool_id}",
 }]
async fn zpool_put(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<ZpoolPathParam>,
    pool_info: TypedBody<ZpoolPutRequest>,
) -> Result<HttpResponseOk<ZpoolPutResponse>, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let pi = pool_info.into_inner();
    nexus.upsert_zpool(path.zpool_id, path.sled_id, pi).await?;
    Ok(HttpResponseOk(ZpoolPutResponse {}))
}

/// Path parameters for Instance requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct InstancePathParam {
    instance_id: Uuid,
}

/// Report updated state for an instance.
#[endpoint {
     method = PUT,
     path = "/instances/{instance_id}",
 }]
async fn cpapi_instances_put(
    rqctx: RequestContext<Arc<ServerContext>>,
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

/// Path parameters for Disk requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct DiskPathParam {
    disk_id: Uuid,
}

/// Report updated state for a disk.
#[endpoint {
     method = PUT,
     path = "/disks/{disk_id}",
 }]
async fn cpapi_disks_put(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<DiskPathParam>,
    new_runtime_state: TypedBody<DiskRuntimeState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    let handler = async {
        let opctx = OpContext::for_internal_api(&rqctx).await;
        nexus.notify_disk_updated(&opctx, path.disk_id, &new_state).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Volume requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct VolumePathParam {
    volume_id: Uuid,
}

/// Request removal of a read_only_parent from a volume
/// A volume can be created with the source data for that volume being another
/// volume that attached as a "read_only_parent". In the background there
/// exists a scrubber that will copy the data from the read_only_parent
/// into the volume. When that scrubber has completed copying the data, this
/// endpoint can be called to update the database that the read_only_parent
/// is no longer needed for a volume and future attachments of this volume
/// should not include that read_only_parent.
#[endpoint {
     method = POST,
     path = "/volume/{volume_id}/remove-read-only-parent",
 }]
async fn cpapi_volume_remove_read_only_parent(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<VolumePathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = OpContext::for_internal_api(&rqctx).await;
        nexus.volume_remove_read_only_parent(&opctx, path.volume_id).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Request removal of a read_only_parent from a disk
/// This is a thin wrapper around the volume_remove_read_only_parent saga.
/// All we are doing here is, given a disk UUID, figure out what the
/// volume_id is for that disk, then use that to call the
/// volume_remove_read_only_parent saga on it.
#[endpoint {
     method = POST,
     path = "/disk/{disk_id}/remove-read-only-parent",
 }]
async fn cpapi_disk_remove_read_only_parent(
    rqctx: RequestContext<Arc<ServerContext>>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = OpContext::for_internal_api(&rqctx).await;
        nexus.disk_remove_read_only_parent(&opctx, path.disk_id).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Accept a registration from a new metric producer
#[endpoint {
     method = POST,
     path = "/metrics/producers",
 }]
async fn cpapi_producers_post(
    request_context: RequestContext<Arc<ServerContext>>,
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

/// Accept a notification of a new oximeter collection server.
#[endpoint {
     method = POST,
     path = "/metrics/collectors",
 }]
async fn cpapi_collectors_post(
    request_context: RequestContext<Arc<ServerContext>>,
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

/// Endpoint for oximeter to collect nexus server metrics.
#[endpoint {
    method = GET,
    path = "/metrics/collect/{producer_id}",
}]
async fn cpapi_metrics_collect(
    request_context: RequestContext<Arc<ServerContext>>,
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

/// Endpoint used by Sled Agents to download cached artifacts.
#[endpoint {
    method = GET,
    path = "/artifacts/{kind}/{name}/{version}",
}]
async fn cpapi_artifact_download(
    request_context: RequestContext<Arc<ServerContext>>,
    path_params: Path<UpdateArtifact>,
) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
    let context = request_context.context();
    let nexus = &context.nexus;
    let opctx = OpContext::for_internal_api(&request_context).await;
    // TODO: return 404 if the error we get here says that the record isn't found
    let body =
        nexus.download_artifact(&opctx, path_params.into_inner()).await?;

    Ok(HttpResponseOk(Body::from(body).into()))
}
