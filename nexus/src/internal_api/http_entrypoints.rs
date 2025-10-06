// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for HTTP APIs internal to the control plane

use super::params::OximeterInfo;
use crate::context::ApiContext;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseCreated;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::TypedBody;
use nexus_internal_api::*;
use nexus_types::external_api::shared::ProbeInfo;
use nexus_types::internal_api::params::SledAgentInfo;
use nexus_types::internal_api::params::SwitchPutRequest;
use nexus_types::internal_api::params::SwitchPutResponse;
use nexus_types::internal_api::views::NatEntryView;
use omicron_common::api::external::http_pagination::PaginatedById;
use omicron_common::api::external::http_pagination::ScanById;
use omicron_common::api::external::http_pagination::ScanParams;
use omicron_common::api::external::http_pagination::data_page_params_for;
use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::nexus::DownstairsClientStopRequest;
use omicron_common::api::internal::nexus::DownstairsClientStopped;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use omicron_common::api::internal::nexus::ProducerRegistrationResponse;
use omicron_common::api::internal::nexus::RepairFinishInfo;
use omicron_common::api::internal::nexus::RepairProgress;
use omicron_common::api::internal::nexus::RepairStartInfo;
use omicron_common::api::internal::nexus::SledVmmState;

type NexusApiDescription = ApiDescription<ApiContext>;

/// Returns a description of the internal nexus API
pub(crate) fn internal_api() -> NexusApiDescription {
    nexus_internal_api_mod::api_description::<NexusInternalApiImpl>()
        .expect("registered API endpoints successfully")
}

enum NexusInternalApiImpl {}

impl NexusInternalApi for NexusInternalApiImpl {
    type Context = ApiContext;

    async fn sled_agent_get(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SledAgentPathParam>,
    ) -> Result<HttpResponseOk<SledAgentInfo>, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let path = path_params.into_inner();
        let sled_id = &path.sled_id;
        let handler = async {
            let (.., sled) =
                nexus.sled_lookup(&opctx, &sled_id)?.fetch().await?;
            Ok(HttpResponseOk(sled.into()))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_agent_put(
        rqctx: RequestContext<Self::Context>,
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
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_firewall_rules_request(
        rqctx: RequestContext<Self::Context>,
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
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn switch_put(
        rqctx: RequestContext<Self::Context>,
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
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_instances_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VmmPathParam>,
        new_runtime_state: TypedBody<SledVmmState>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let new_state = new_runtime_state.into_inner();
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;
        let handler = async {
            nexus
                .notify_vmm_updated(&opctx, path.propolis_id, &new_state)
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_disks_put(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DiskPathParam>,
        new_runtime_state: TypedBody<DiskRuntimeState>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let new_state = new_runtime_state.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus.notify_disk_updated(&opctx, path.disk_id, &new_state).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_volume_remove_read_only_parent(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VolumePathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();

        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus
                .volume_remove_read_only_parent(&opctx, path.volume_id)
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_disk_remove_read_only_parent(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DiskPathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();

        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus.disk_remove_read_only_parent(&opctx, path.disk_id).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_producers_post(
        request_context: RequestContext<Self::Context>,
        producer_info: TypedBody<ProducerEndpoint>,
    ) -> Result<HttpResponseCreated<ProducerRegistrationResponse>, HttpError>
    {
        let context = &request_context.context().context;
        let handler = async {
            let nexus = &context.nexus;
            let producer_info = producer_info.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&request_context)
                    .await;
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

    async fn cpapi_assigned_producers_list(
        request_context: RequestContext<Self::Context>,
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
                crate::context::op_context_for_internal_api(&request_context)
                    .await;
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

    async fn cpapi_collectors_post(
        request_context: RequestContext<Self::Context>,
        oximeter_info: TypedBody<OximeterInfo>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let context = &request_context.context().context;
        let handler = async {
            let nexus = &context.nexus;
            let oximeter_info = oximeter_info.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&request_context)
                    .await;
            nexus.upsert_oximeter_collector(&opctx, &oximeter_info).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        context
            .internal_latencies
            .instrument_dropshot_handler(&request_context, handler)
            .await
    }

    async fn cpapi_upstairs_repair_start(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsPathParam>,
        repair_start_info: TypedBody<RepairStartInfo>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();

        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus
                .upstairs_repair_start(
                    &opctx,
                    path.upstairs_id,
                    repair_start_info.into_inner(),
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_upstairs_repair_finish(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsPathParam>,
        repair_finish_info: TypedBody<RepairFinishInfo>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();

        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus
                .upstairs_repair_finish(
                    &opctx,
                    path.upstairs_id,
                    repair_finish_info.into_inner(),
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_upstairs_repair_progress(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsRepairPathParam>,
        repair_progress: TypedBody<RepairProgress>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();

        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
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
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_downstairs_client_stop_request(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsDownstairsPathParam>,
        downstairs_client_stop_request: TypedBody<DownstairsClientStopRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();

        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
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
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn cpapi_downstairs_client_stopped(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<UpstairsDownstairsPathParam>,
        downstairs_client_stopped: TypedBody<DownstairsClientStopped>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();

        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
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
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn bgtask_activate(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<BackgroundTasksActivateRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let body = body.into_inner();
            nexus.bgtask_activate(&opctx, body.bgtask_names).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // NAT RPW internal APIs

    async fn ipv4_nat_changeset(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<RpwNatPathParam>,
        query_params: Query<RpwNatQueryParam>,
    ) -> Result<HttpResponseOk<Vec<NatEntryView>>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let path = path_params.into_inner();
            let query = query_params.into_inner();
            let mut changeset = nexus
                .datastore()
                .nat_changeset(&opctx, path.from_gen, query.limit)
                .await?;
            changeset.sort_by_key(|e| e.gen);
            Ok(HttpResponseOk(changeset))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn probes_get(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<ProbePathParam>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<Vec<ProbeInfo>>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let query = query_params.into_inner();
            let path = path_params.into_inner();
            let nexus = &apictx.nexus;
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let pagparams = data_page_params_for(&rqctx, &query)?;
            Ok(HttpResponseOk(
                nexus
                    .probe_list_for_sled(&opctx, &pagparams, path.sled)
                    .await?,
            ))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }
}
