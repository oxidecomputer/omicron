// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for HTTP APIs internal to the control plane

use super::params::{OximeterInfo, RackInitializationRequest};
use crate::context::ApiContext;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ApiDescriptionRegisterError;
use dropshot::FreeformBody;
use dropshot::HttpError;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseDeleted;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::TypedBody;
use hyper::Body;
use nexus_db_model::Ipv4NatEntryView;
use nexus_db_queries::db::datastore::ProbeInfo;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::external_api::params::SledSelector;
use nexus_types::external_api::params::UninitializedSledId;
use nexus_types::external_api::shared::UninitializedSled;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::internal_api::params::SledAgentInfo;
use nexus_types::internal_api::params::SwitchPutRequest;
use nexus_types::internal_api::params::SwitchPutResponse;
use nexus_types::internal_api::views::to_list;
use nexus_types::internal_api::views::BackgroundTask;
use nexus_types::internal_api::views::Saga;
use omicron_common::api::external::http_pagination::data_page_params_for;
use omicron_common::api::external::http_pagination::PaginatedById;
use omicron_common::api::external::http_pagination::ScanById;
use omicron_common::api::external::http_pagination::ScanParams;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::DownstairsClientStopRequest;
use omicron_common::api::internal::nexus::DownstairsClientStopped;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerRegistrationResponse;
use omicron_common::api::internal::nexus::RepairFinishInfo;
use omicron_common::api::internal::nexus::RepairProgress;
use omicron_common::api::internal::nexus::RepairStartInfo;
use omicron_common::api::internal::nexus::SledInstanceState;
use omicron_common::update::ArtifactId;
use omicron_uuid_kinds::DownstairsKind;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsKind;
use omicron_uuid_kinds::UpstairsRepairKind;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use uuid::Uuid;

type NexusApiDescription = ApiDescription<ApiContext>;

/// Returns a description of the internal nexus API
pub(crate) fn internal_api() -> NexusApiDescription {
    fn register_endpoints(
        api: &mut NexusApiDescription,
    ) -> Result<(), ApiDescriptionRegisterError> {
        api.register(sled_agent_get)?;
        api.register(sled_agent_put)?;
        api.register(sled_firewall_rules_request)?;
        api.register(switch_put)?;
        api.register(rack_initialization_complete)?;
        api.register(cpapi_instances_put)?;
        api.register(cpapi_disks_put)?;
        api.register(cpapi_volume_remove_read_only_parent)?;
        api.register(cpapi_disk_remove_read_only_parent)?;
        api.register(cpapi_producers_post)?;
        api.register(cpapi_assigned_producers_list)?;
        api.register(cpapi_collectors_post)?;
        api.register(cpapi_artifact_download)?;

        api.register(cpapi_upstairs_repair_start)?;
        api.register(cpapi_upstairs_repair_finish)?;
        api.register(cpapi_upstairs_repair_progress)?;
        api.register(cpapi_downstairs_client_stop_request)?;
        api.register(cpapi_downstairs_client_stopped)?;

        api.register(saga_list)?;
        api.register(saga_view)?;

        api.register(ipv4_nat_changeset)?;

        api.register(bgtask_list)?;
        api.register(bgtask_view)?;
        api.register(bgtask_activate)?;

        api.register(blueprint_list)?;
        api.register(blueprint_view)?;
        api.register(blueprint_delete)?;
        api.register(blueprint_target_view)?;
        api.register(blueprint_target_set)?;
        api.register(blueprint_target_set_enabled)?;
        api.register(blueprint_regenerate)?;
        api.register(blueprint_import)?;

        api.register(sled_list_uninitialized)?;
        api.register(sled_add)?;
        api.register(sled_expunge)?;

        api.register(probes_get)?;

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

/// Return information about the given sled agent
#[endpoint {
     method = GET,
     path = "/sled-agents/{sled_id}",
 }]
async fn sled_agent_get(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<SledAgentPathParam>,
) -> Result<HttpResponseOk<SledAgentInfo>, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
    let path = path_params.into_inner();
    let sled_id = &path.sled_id;
    let handler = async {
        let (.., sled) = nexus.sled_lookup(&opctx, sled_id)?.fetch().await?;
        Ok(HttpResponseOk(sled.into()))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Report that the sled agent for the specified sled has come online.
#[endpoint {
     method = POST,
     path = "/sled-agents/{sled_id}",
 }]
async fn sled_agent_put(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<SledAgentPathParam>,
    sled_info: TypedBody<SledAgentInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
    let path = path_params.into_inner();
    let info = sled_info.into_inner();
    let sled_id = &path.sled_id;
    let handler = async {
        nexus.upsert_sled(&opctx, *sled_id, info).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Request a new set of firewall rules for a sled.
///
/// This causes Nexus to read the latest set of rules for the sled,
/// and call a Sled endpoint which applies the rules to all OPTE ports
/// that happen to exist.
#[endpoint {
     method = POST,
     path = "/sled-agents/{sled_id}/firewall-rules-update",
 }]
async fn sled_firewall_rules_request(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<SledAgentPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
    let path = path_params.into_inner();
    let sled_id = &path.sled_id;
    let handler = async {
        nexus.sled_request_firewall_rules(&opctx, *sled_id).await?;
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
    rqctx: RequestContext<ApiContext>,
    path_params: Path<RackPathParam>,
    info: TypedBody<RackInitializationRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let request = info.into_inner();
    let opctx = crate::context::op_context_for_internal_api(&rqctx).await;

    nexus.rack_initialize(&opctx, path.rack_id, request).await?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Path parameters for Switch requests.
#[derive(Deserialize, JsonSchema)]
struct SwitchPathParam {
    switch_id: Uuid,
}

#[endpoint {
    method = PUT,
    path = "/switch/{switch_id}",
}]
async fn switch_put(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<SwitchPathParam>,
    body: TypedBody<SwitchPutRequest>,
) -> Result<HttpResponseOk<SwitchPutResponse>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let switch = body.into_inner();
        nexus.switch_upsert(path.switch_id, switch).await?;
        Ok(HttpResponseOk(SwitchPutResponse {}))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
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
    rqctx: RequestContext<ApiContext>,
    path_params: Path<InstancePathParam>,
    new_runtime_state: TypedBody<SledInstanceState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
    let handler = async {
        nexus
            .notify_instance_updated(
                &opctx,
                &InstanceUuid::from_untyped_uuid(path.instance_id),
                &new_state,
            )
            .await?;
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
    rqctx: RequestContext<ApiContext>,
    path_params: Path<DiskPathParam>,
    new_runtime_state: TypedBody<DiskRuntimeState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();
    let new_state = new_runtime_state.into_inner();
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
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
    rqctx: RequestContext<ApiContext>,
    path_params: Path<VolumePathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
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
    rqctx: RequestContext<ApiContext>,
    path_params: Path<DiskPathParam>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
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
    request_context: RequestContext<ApiContext>,
    producer_info: TypedBody<ProducerEndpoint>,
) -> Result<HttpResponseCreated<ProducerRegistrationResponse>, HttpError> {
    let context = &request_context.context().context;
    let handler = async {
        let nexus = &context.nexus;
        let producer_info = producer_info.into_inner();
        let opctx =
            crate::context::op_context_for_internal_api(&request_context).await;
        nexus
            .assign_producer(&opctx, producer_info)
            .await
            .map_err(HttpError::from)
            .map(|_| {
                HttpResponseCreated(ProducerRegistrationResponse {
                    lease_duration:
                        crate::app::oximeter::PRODUCER_LEASE_DURATION,
                })
            })
    };
    context
        .internal_latencies
        .instrument_dropshot_handler(&request_context, handler)
        .await
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CollectorIdPathParams {
    /// The ID of the oximeter collector.
    pub collector_id: Uuid,
}

/// List all metric producers assigned to an oximeter collector.
#[endpoint {
     method = GET,
     path = "/metrics/collectors/{collector_id}/producers",
 }]
async fn cpapi_assigned_producers_list(
    request_context: RequestContext<ApiContext>,
    path_params: Path<CollectorIdPathParams>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<ProducerEndpoint>>, HttpError> {
    let context = &request_context.context().context;
    let handler = async {
        let nexus = &context.nexus;
        let collector_id = path_params.into_inner().collector_id;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&request_context, &query)?;
        let opctx =
            crate::context::op_context_for_internal_api(&request_context).await;
        let producers = nexus
            .list_assigned_producers(&opctx, collector_id, &pagparams)
            .await?;
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            producers,
            &|_, producer: &ProducerEndpoint| producer.id,
        )?))
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
    request_context: RequestContext<ApiContext>,
    oximeter_info: TypedBody<OximeterInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let context = &request_context.context().context;
    let handler = async {
        let nexus = &context.nexus;
        let oximeter_info = oximeter_info.into_inner();
        let opctx =
            crate::context::op_context_for_internal_api(&request_context).await;
        nexus.upsert_oximeter_collector(&opctx, &oximeter_info).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
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
    request_context: RequestContext<ApiContext>,
    path_params: Path<ArtifactId>,
) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
    let context = &request_context.context().context;
    let nexus = &context.nexus;
    let opctx =
        crate::context::op_context_for_internal_api(&request_context).await;
    // TODO: return 404 if the error we get here says that the record isn't found
    let body = nexus
        .updates_download_artifact(&opctx, path_params.into_inner())
        .await?;

    Ok(HttpResponseOk(Body::from(body).into()))
}

/// Path parameters for Upstairs requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct UpstairsPathParam {
    upstairs_id: TypedUuid<UpstairsKind>,
}

/// An Upstairs will notify this endpoint when a repair starts
#[endpoint {
     method = POST,
     path = "/crucible/0/upstairs/{upstairs_id}/repair-start",
 }]
async fn cpapi_upstairs_repair_start(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<UpstairsPathParam>,
    repair_start_info: TypedBody<RepairStartInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        nexus
            .upstairs_repair_start(
                &opctx,
                path.upstairs_id,
                repair_start_info.into_inner(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// An Upstairs will notify this endpoint when a repair finishes.
#[endpoint {
     method = POST,
     path = "/crucible/0/upstairs/{upstairs_id}/repair-finish",
 }]
async fn cpapi_upstairs_repair_finish(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<UpstairsPathParam>,
    repair_finish_info: TypedBody<RepairFinishInfo>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        nexus
            .upstairs_repair_finish(
                &opctx,
                path.upstairs_id,
                repair_finish_info.into_inner(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Upstairs requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct UpstairsRepairPathParam {
    upstairs_id: TypedUuid<UpstairsKind>,
    repair_id: TypedUuid<UpstairsRepairKind>,
}

/// An Upstairs will update this endpoint with the progress of a repair
#[endpoint {
     method = POST,
     path = "/crucible/0/upstairs/{upstairs_id}/repair/{repair_id}/progress",
 }]
async fn cpapi_upstairs_repair_progress(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<UpstairsRepairPathParam>,
    repair_progress: TypedBody<RepairProgress>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        nexus
            .upstairs_repair_progress(
                &opctx,
                path.upstairs_id,
                path.repair_id,
                repair_progress.into_inner(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Downstairs requests (internal API)
#[derive(Deserialize, JsonSchema)]
struct UpstairsDownstairsPathParam {
    upstairs_id: TypedUuid<UpstairsKind>,
    downstairs_id: TypedUuid<DownstairsKind>,
}

/// An Upstairs will update this endpoint if a Downstairs client task is
/// requested to stop
#[endpoint {
     method = POST,
     path = "/crucible/0/upstairs/{upstairs_id}/downstairs/{downstairs_id}/stop-request",
 }]
async fn cpapi_downstairs_client_stop_request(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<UpstairsDownstairsPathParam>,
    downstairs_client_stop_request: TypedBody<DownstairsClientStopRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        nexus
            .downstairs_client_stop_request_notification(
                &opctx,
                path.upstairs_id,
                path.downstairs_id,
                downstairs_client_stop_request.into_inner(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// An Upstairs will update this endpoint if a Downstairs client task stops for
/// any reason (not just after being requested to)
#[endpoint {
     method = POST,
     path = "/crucible/0/upstairs/{upstairs_id}/downstairs/{downstairs_id}/stopped",
 }]
async fn cpapi_downstairs_client_stopped(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<UpstairsDownstairsPathParam>,
    downstairs_client_stopped: TypedBody<DownstairsClientStopped>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let path = path_params.into_inner();

    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        nexus
            .downstairs_client_stopped_notification(
                &opctx,
                path.upstairs_id,
                path.downstairs_id,
                downstairs_client_stopped.into_inner(),
            )
            .await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Sagas

/// List sagas
#[endpoint {
    method = GET,
    path = "/sagas",
}]
async fn saga_list(
    rqctx: RequestContext<ApiContext>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<Saga>>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let pagparams = data_page_params_for(&rqctx, &query)?;
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let saga_stream = nexus.sagas_list(&opctx, &pagparams).await?;
        let view_list = to_list(saga_stream).await;
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            view_list,
            &|_, saga: &Saga| saga.id,
        )?))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Saga requests
#[derive(Deserialize, JsonSchema)]
struct SagaPathParam {
    saga_id: Uuid,
}

/// Fetch a saga
#[endpoint {
    method = GET,
    path = "/sagas/{saga_id}",
}]
async fn saga_view(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<SagaPathParam>,
) -> Result<HttpResponseOk<Saga>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let saga = nexus.saga_get(&opctx, path.saga_id).await?;
        Ok(HttpResponseOk(saga))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Background Tasks

/// List background tasks
///
/// This is a list of discrete background activities that Nexus carries out.
/// This is exposed for support and debugging.
#[endpoint {
    method = GET,
    path = "/bgtasks",
}]
async fn bgtask_list(
    rqctx: RequestContext<ApiContext>,
) -> Result<HttpResponseOk<BTreeMap<String, BackgroundTask>>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let nexus = &apictx.nexus;
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let bgtask_list = nexus.bgtasks_list(&opctx).await?;
        Ok(HttpResponseOk(bgtask_list))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for Background Task requests
#[derive(Deserialize, JsonSchema)]
struct BackgroundTaskPathParam {
    bgtask_name: String,
}

/// Query parameters for Background Task activation requests.
#[derive(Deserialize, JsonSchema)]
struct BackgroundTasksActivateRequest {
    bgtask_names: BTreeSet<String>,
}

/// Fetch status of one background task
///
/// This is exposed for support and debugging.
#[endpoint {
    method = GET,
    path = "/bgtasks/view/{bgtask_name}",
}]
async fn bgtask_view(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<BackgroundTaskPathParam>,
) -> Result<HttpResponseOk<BackgroundTask>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let bgtask = nexus.bgtask_status(&opctx, &path.bgtask_name).await?;
        Ok(HttpResponseOk(bgtask))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Activates one or more background tasks, causing them to be run immediately
/// if idle, or scheduled to run again as soon as possible if already running.
#[endpoint {
    method = POST,
    path = "/bgtasks/activate",
}]
async fn bgtask_activate(
    rqctx: RequestContext<ApiContext>,
    body: TypedBody<BackgroundTasksActivateRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let body = body.into_inner();
        nexus.bgtask_activate(&opctx, body.bgtask_names).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// NAT RPW internal APIs

/// Path parameters for NAT ChangeSet
#[derive(Deserialize, JsonSchema)]
struct RpwNatPathParam {
    /// which change number to start generating
    /// the change set from
    from_gen: i64,
}

/// Query parameters for NAT ChangeSet
#[derive(Deserialize, JsonSchema)]
struct RpwNatQueryParam {
    limit: u32,
}

/// Fetch NAT ChangeSet
///
/// Caller provides their generation as `from_gen`, along with a query
/// parameter for the page size (`limit`). Endpoint will return changes
/// that have occured since the caller's generation number up to the latest
/// change or until the `limit` is reached. If there are no changes, an
/// empty vec is returned.
#[endpoint {
    method = GET,
    path = "/nat/ipv4/changeset/{from_gen}"
}]
async fn ipv4_nat_changeset(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<RpwNatPathParam>,
    query_params: Query<RpwNatQueryParam>,
) -> Result<HttpResponseOk<Vec<Ipv4NatEntryView>>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let query = query_params.into_inner();
        let mut changeset = nexus
            .datastore()
            .ipv4_nat_changeset(&opctx, path.from_gen, query.limit)
            .await?;
        changeset.sort_by_key(|e| e.gen);
        Ok(HttpResponseOk(changeset))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// APIs for managing blueprints
//
// These are not (yet) intended for use by any other programs.  Eventually, we
// will want this functionality part of the public API.  But we don't want to
// commit to any of this yet.  These properly belong in an RFD 399-style
// "Service and Support API".  Absent that, we stick them here.

/// Lists blueprints
#[endpoint {
    method = GET,
    path = "/deployment/blueprints/all",
}]
async fn blueprint_list(
    rqctx: RequestContext<ApiContext>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<ResultsPage<BlueprintMetadata>>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let nexus = &apictx.nexus;
        let query = query_params.into_inner();
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let pagparams = data_page_params_for(&rqctx, &query)?;
        let blueprints = nexus.blueprint_list(&opctx, &pagparams).await?;
        Ok(HttpResponseOk(ScanById::results_page(
            &query,
            blueprints,
            &|_, blueprint: &BlueprintMetadata| blueprint.id,
        )?))
    };

    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Fetches one blueprint
#[endpoint {
    method = GET,
    path = "/deployment/blueprints/all/{blueprint_id}",
}]
async fn blueprint_view(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<nexus_types::external_api::params::BlueprintPath>,
) -> Result<HttpResponseOk<Blueprint>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let blueprint = nexus.blueprint_view(&opctx, path.blueprint_id).await?;
        Ok(HttpResponseOk(blueprint))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Deletes one blueprint
#[endpoint {
    method = DELETE,
    path = "/deployment/blueprints/all/{blueprint_id}",
}]
async fn blueprint_delete(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<nexus_types::external_api::params::BlueprintPath>,
) -> Result<HttpResponseDeleted, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        nexus.blueprint_delete(&opctx, path.blueprint_id).await?;
        Ok(HttpResponseDeleted())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Managing the current target blueprint

/// Fetches the current target blueprint, if any
#[endpoint {
    method = GET,
    path = "/deployment/blueprints/target",
}]
async fn blueprint_target_view(
    rqctx: RequestContext<ApiContext>,
) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let target = nexus.blueprint_target_view(&opctx).await?;
        Ok(HttpResponseOk(target))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Make the specified blueprint the new target
#[endpoint {
    method = POST,
    path = "/deployment/blueprints/target",
}]
async fn blueprint_target_set(
    rqctx: RequestContext<ApiContext>,
    target: TypedBody<BlueprintTargetSet>,
) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let target = target.into_inner();
        let target = nexus.blueprint_target_set(&opctx, target).await?;
        Ok(HttpResponseOk(target))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Set the `enabled` field of the current target blueprint
#[endpoint {
    method = PUT,
    path = "/deployment/blueprints/target/enabled",
}]
async fn blueprint_target_set_enabled(
    rqctx: RequestContext<ApiContext>,
    target: TypedBody<BlueprintTargetSet>,
) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let target = target.into_inner();
        let target = nexus.blueprint_target_set_enabled(&opctx, target).await?;
        Ok(HttpResponseOk(target))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

// Generating blueprints

/// Generates a new blueprint for the current system, re-evaluating anything
/// that's changed since the last one was generated
#[endpoint {
    method = POST,
    path = "/deployment/blueprints/regenerate",
}]
async fn blueprint_regenerate(
    rqctx: RequestContext<ApiContext>,
) -> Result<HttpResponseOk<Blueprint>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let result = nexus.blueprint_create_regenerate(&opctx).await?;
        Ok(HttpResponseOk(result))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Imports a client-provided blueprint
///
/// This is intended for development and support, not end users or operators.
#[endpoint {
    method = POST,
    path = "/deployment/blueprints/import",
}]
async fn blueprint_import(
    rqctx: RequestContext<ApiContext>,
    blueprint: TypedBody<Blueprint>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let nexus = &apictx.nexus;
        let blueprint = blueprint.into_inner();
        nexus.blueprint_import(&opctx, blueprint).await?;
        Ok(HttpResponseUpdatedNoContent())
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// List uninitialized sleds
#[endpoint {
    method = GET,
    path = "/sleds/uninitialized",
}]
async fn sled_list_uninitialized(
    rqctx: RequestContext<ApiContext>,
) -> Result<HttpResponseOk<ResultsPage<UninitializedSled>>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let nexus = &apictx.nexus;
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let sleds = nexus.sled_list_uninitialized(&opctx).await?;
        Ok(HttpResponseOk(ResultsPage { items: sleds, next_page: None }))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct SledId {
    pub id: SledUuid,
}

/// Add sled to initialized rack
//
// TODO: In the future this should really be a PUT request, once we resolve
// https://github.com/oxidecomputer/omicron/issues/4494. It should also
// explicitly be tied to a rack via a `rack_id` path param. For now we assume
// we are only operating on single rack systems.
#[endpoint {
    method = POST,
    path = "/sleds/add",
}]
async fn sled_add(
    rqctx: RequestContext<ApiContext>,
    sled: TypedBody<UninitializedSledId>,
) -> Result<HttpResponseCreated<SledId>, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let id = nexus.sled_add(&opctx, sled.into_inner()).await?;
        Ok(HttpResponseCreated(SledId { id }))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Mark a sled as expunged
///
/// This is an irreversible process! It should only be called after
/// sufficient warning to the operator.
///
/// This is idempotent, and it returns the old policy of the sled.
#[endpoint {
    method = POST,
    path = "/sleds/expunge",
}]
async fn sled_expunge(
    rqctx: RequestContext<ApiContext>,
    sled: TypedBody<SledSelector>,
) -> Result<HttpResponseOk<SledPolicy>, HttpError> {
    let apictx = &rqctx.context().context;
    let nexus = &apictx.nexus;
    let handler = async {
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let previous_policy =
            nexus.sled_expunge(&opctx, sled.into_inner().sled).await?;
        Ok(HttpResponseOk(previous_policy))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}

/// Path parameters for probes
#[derive(Deserialize, JsonSchema)]
struct ProbePathParam {
    sled: Uuid,
}

/// Get all the probes associated with a given sled.
#[endpoint {
    method = GET,
    path = "/probes/{sled}"
}]
async fn probes_get(
    rqctx: RequestContext<ApiContext>,
    path_params: Path<ProbePathParam>,
    query_params: Query<PaginatedById>,
) -> Result<HttpResponseOk<Vec<ProbeInfo>>, HttpError> {
    let apictx = &rqctx.context().context;
    let handler = async {
        let query = query_params.into_inner();
        let path = path_params.into_inner();
        let nexus = &apictx.nexus;
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let pagparams = data_page_params_for(&rqctx, &query)?;
        Ok(HttpResponseOk(
            nexus.probe_list_for_sled(&opctx, &pagparams, path.sled).await?,
        ))
    };
    apictx.internal_latencies.instrument_dropshot_handler(&rqctx, handler).await
}
