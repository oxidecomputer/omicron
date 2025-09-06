// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for HTTP APIs internal to the control plane

use super::params::{OximeterInfo, RackInitializationRequest};
use crate::app::support_bundles::SupportBundleQueryType;
use crate::context::ApiContext;
use crate::external_api::shared;
use dropshot::ApiDescription;
use dropshot::Body;
use dropshot::Header;
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
use http::Response;
use nexus_internal_api::*;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintMetadata;
use nexus_types::deployment::BlueprintTarget;
use nexus_types::deployment::BlueprintTargetSet;
use nexus_types::deployment::ClickhousePolicy;
use nexus_types::deployment::OximeterReadPolicy;
use nexus_types::deployment::ReconfiguratorConfigParam;
use nexus_types::deployment::ReconfiguratorConfigView;
use nexus_types::external_api::headers::RangeRequest;
use nexus_types::external_api::params::PhysicalDiskPath;
use nexus_types::external_api::params::SledSelector;
use nexus_types::external_api::params::SupportBundleFilePath;
use nexus_types::external_api::params::SupportBundlePath;
use nexus_types::external_api::params::SupportBundleUpdate;
use nexus_types::external_api::params::UninitializedSledId;
use nexus_types::external_api::shared::ProbeInfo;
use nexus_types::external_api::shared::UninitializedSled;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::internal_api::params::InstanceMigrateRequest;
use nexus_types::internal_api::params::SledAgentInfo;
use nexus_types::internal_api::params::SwitchPutRequest;
use nexus_types::internal_api::params::SwitchPutResponse;
use nexus_types::internal_api::views::BackgroundTask;
use nexus_types::internal_api::views::DemoSaga;
use nexus_types::internal_api::views::MgsUpdateDriverStatus;
use nexus_types::internal_api::views::NatEntryView;
use nexus_types::internal_api::views::QuiesceStatus;
use nexus_types::internal_api::views::Saga;
use nexus_types::internal_api::views::UpdateStatus;
use nexus_types::internal_api::views::to_list;
use omicron_common::api::external::Instance;
use omicron_common::api::external::http_pagination::PaginatedById;
use omicron_common::api::external::http_pagination::PaginatedByTimeAndId;
use omicron_common::api::external::http_pagination::ScanById;
use omicron_common::api::external::http_pagination::ScanByTimeAndId;
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
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::InstanceUuid;
use omicron_uuid_kinds::SupportBundleUuid;
use range_requests::PotentialRange;
use std::collections::BTreeMap;

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
            let (.., sled) = nexus
                .sled_lookup(&opctx, &sled_id.into_untyped_uuid())?
                .fetch()
                .await?;
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
            nexus
                .upsert_sled(&opctx, sled_id.into_untyped_uuid(), info)
                .await?;
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
            nexus
                .sled_request_firewall_rules(
                    &opctx,
                    sled_id.into_untyped_uuid(),
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn rack_initialization_complete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<RackPathParam>,
        info: TypedBody<RackInitializationRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let request = info.into_inner();
        let opctx = crate::context::op_context_for_internal_api(&rqctx).await;

        nexus
            .rack_initialize(
                &opctx,
                path.rack_id,
                request,
                true, // blueprint_execution_enabled
            )
            .await?;

        Ok(HttpResponseUpdatedNoContent())
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

    async fn instance_migrate(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<InstancePathParam>,
        migrate_params: TypedBody<InstanceMigrateRequest>,
    ) -> Result<HttpResponseOk<Instance>, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let path = path_params.into_inner();
        let migrate = migrate_params.into_inner();
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let instance = nexus
                .instance_migrate(
                    &opctx,
                    InstanceUuid::from_untyped_uuid(path.instance_id),
                    migrate,
                )
                .await?;
            Ok(HttpResponseOk(instance.into()))
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

    // Debug interfaces for Sagas

    async fn saga_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<Saga>>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let saga_stream = nexus.sagas_list(&opctx, &pagparams).await?;
            let view_list = to_list(saga_stream).await;
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                view_list,
                &|_, saga: &Saga| saga.id,
            )?))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn saga_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SagaPathParam>,
    ) -> Result<HttpResponseOk<Saga>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let path = path_params.into_inner();
            let saga = nexus.saga_get(&opctx, path.saga_id).await?;
            Ok(HttpResponseOk(saga))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn saga_demo_create(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<DemoSaga>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let demo_saga = nexus.saga_demo_create().await?;
            Ok(HttpResponseOk(demo_saga))
        };

        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn saga_demo_complete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DemoSagaPathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let path = path_params.into_inner();
            nexus.saga_demo_complete(path.demo_saga_id)?;
            Ok(HttpResponseUpdatedNoContent())
        };

        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    // Debug interfaces for Background Tasks

    async fn bgtask_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BTreeMap<String, BackgroundTask>>, HttpError>
    {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let bgtask_list = nexus.bgtasks_list(&opctx).await?;
            Ok(HttpResponseOk(bgtask_list))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn bgtask_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BackgroundTaskPathParam>,
    ) -> Result<HttpResponseOk<BackgroundTask>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let path = path_params.into_inner();
            let bgtask = nexus.bgtask_status(&opctx, &path.bgtask_name).await?;
            Ok(HttpResponseOk(bgtask))
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

    // Debug interfaces for MGS updates

    async fn mgs_updates(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<MgsUpdateDriverStatus>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            Ok(HttpResponseOk(nexus.mgs_updates(&opctx).await?))
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

    // APIs for managing blueprints
    async fn blueprint_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<BlueprintMetadata>>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let query = query_params.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let pagparams = data_page_params_for(&rqctx, &query)?;
            let blueprints = nexus.blueprint_list(&opctx, &pagparams).await?;
            Ok(HttpResponseOk(ScanById::results_page(
                &query,
                blueprints,
                &|_, blueprint: &BlueprintMetadata| {
                    blueprint.id.into_untyped_uuid()
                },
            )?))
        };

        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Fetches one blueprint
    async fn blueprint_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<nexus_types::external_api::params::BlueprintPath>,
    ) -> Result<HttpResponseOk<Blueprint>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let path = path_params.into_inner();
            let blueprint =
                nexus.blueprint_view(&opctx, path.blueprint_id).await?;
            Ok(HttpResponseOk(blueprint))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    /// Deletes one blueprint
    async fn blueprint_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<nexus_types::external_api::params::BlueprintPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let path = path_params.into_inner();
            nexus.blueprint_delete(&opctx, path.blueprint_id).await?;
            Ok(HttpResponseDeleted())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn blueprint_target_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let target = nexus.blueprint_target_view(&opctx).await?;
            Ok(HttpResponseOk(target))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn blueprint_target_set(
        rqctx: RequestContext<Self::Context>,
        target: TypedBody<BlueprintTargetSet>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let target = target.into_inner();
            let target = nexus.blueprint_target_set(&opctx, target).await?;
            Ok(HttpResponseOk(target))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn blueprint_target_set_enabled(
        rqctx: RequestContext<Self::Context>,
        target: TypedBody<BlueprintTargetSet>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let target = target.into_inner();
            let target =
                nexus.blueprint_target_set_enabled(&opctx, target).await?;
            Ok(HttpResponseOk(target))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn blueprint_regenerate(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Blueprint>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let result = nexus.blueprint_create_regenerate(&opctx).await?;
            Ok(HttpResponseOk(result))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn blueprint_import(
        rqctx: RequestContext<Self::Context>,
        blueprint: TypedBody<Blueprint>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let blueprint = blueprint.into_inner();
            nexus.blueprint_import(&opctx, blueprint).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn reconfigurator_config_show_current(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ReconfiguratorConfigView>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let datastore = &apictx.nexus.datastore();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            match datastore.reconfigurator_config_get_latest(&opctx).await? {
                Some(switches) => Ok(HttpResponseOk(switches)),
                None => Err(HttpError::for_not_found(
                    None,
                    "No config in database".into(),
                )),
            }
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn reconfigurator_config_show(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VersionPathParam>,
    ) -> Result<HttpResponseOk<ReconfiguratorConfigView>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let datastore = &apictx.nexus.datastore();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let version = path_params.into_inner().version;
            match datastore.reconfigurator_config_get(&opctx, version).await? {
                Some(switches) => Ok(HttpResponseOk(switches)),
                None => Err(HttpError::for_not_found(
                    None,
                    format!("No config in database at version {version}"),
                )),
            }
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn reconfigurator_config_set(
        rqctx: RequestContext<Self::Context>,
        switches: TypedBody<ReconfiguratorConfigParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let datastore = &apictx.nexus.datastore();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            datastore
                .reconfigurator_config_insert_latest_version(
                    &opctx,
                    switches.into_inner(),
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn update_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<UpdateStatus>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let nexus = &apictx.nexus;
            let result = nexus.update_status(&opctx).await?;
            Ok(HttpResponseOk(result))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_list_uninitialized(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ResultsPage<UninitializedSled>>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let sleds = nexus.sled_list_uninitialized(&opctx).await?;
            Ok(HttpResponseOk(ResultsPage { items: sleds, next_page: None }))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_add(
        rqctx: RequestContext<Self::Context>,
        sled: TypedBody<UninitializedSledId>,
    ) -> Result<HttpResponseCreated<SledId>, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let id = nexus.sled_add(&opctx, sled.into_inner()).await?;
            Ok(HttpResponseCreated(SledId { id }))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn sled_expunge(
        rqctx: RequestContext<Self::Context>,
        sled: TypedBody<SledSelector>,
    ) -> Result<HttpResponseOk<SledPolicy>, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let previous_policy =
                nexus.sled_expunge(&opctx, sled.into_inner().sled).await?;
            Ok(HttpResponseOk(previous_policy))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn physical_disk_expunge(
        rqctx: RequestContext<Self::Context>,
        disk: TypedBody<PhysicalDiskPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus.physical_disk_expunge(&opctx, disk.into_inner()).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_list(
        rqctx: RequestContext<ApiContext>,
        query_params: Query<PaginatedByTimeAndId>,
    ) -> Result<HttpResponseOk<ResultsPage<shared::SupportBundleInfo>>, HttpError>
    {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;

            let query = query_params.into_inner();
            let pagparams = data_page_params_for(&rqctx, &query)?;

            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            let bundles = nexus
                .support_bundle_list(&opctx, &pagparams)
                .await?
                .into_iter()
                .map(|p| p.into())
                .collect();

            Ok(HttpResponseOk(ScanByTimeAndId::results_page(
                &query,
                bundles,
                &|_, bundle: &shared::SupportBundleInfo| {
                    (bundle.time_created, bundle.id.into_untyped_uuid())
                },
            )?))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePath>,
    ) -> Result<HttpResponseOk<shared::SupportBundleInfo>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();

            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            let bundle = nexus
                .support_bundle_view(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                )
                .await?;

            Ok(HttpResponseOk(bundle.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            let head = false;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    SupportBundleQueryType::Index,
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            let head = false;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    SupportBundleQueryType::Whole,
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let head = false;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle.bundle_id),
                    SupportBundleQueryType::Path { file_path: path.file },
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let head = true;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    SupportBundleQueryType::Whole,
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let head = true;
            let range = headers
                .into_inner()
                .range
                .map(|r| PotentialRange::new(r.as_bytes()));

            let body = nexus
                .support_bundle_download(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle.bundle_id),
                    SupportBundleQueryType::Path { file_path: path.file },
                    head,
                    range,
                )
                .await?;
            Ok(body)
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<nexus_types::external_api::params::SupportBundleCreate>,
    ) -> Result<HttpResponseCreated<shared::SupportBundleInfo>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let create_params = body.into_inner();

            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            let bundle = nexus
                .support_bundle_create(
                    &opctx,
                    "Created by internal API",
                    create_params.user_comment,
                )
                .await?;
            Ok(HttpResponseCreated(bundle.into()))
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();

            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            nexus
                .support_bundle_delete(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                )
                .await?;

            Ok(HttpResponseDeleted())
        };
        apictx
            .context
            .external_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn support_bundle_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SupportBundlePath>,
        body: TypedBody<SupportBundleUpdate>,
    ) -> Result<HttpResponseOk<shared::SupportBundleInfo>, HttpError> {
        let apictx = rqctx.context();
        let handler = async {
            let nexus = &apictx.context.nexus;
            let path = path_params.into_inner();
            let update = body.into_inner();

            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;

            let bundle = nexus
                .support_bundle_update_user_comment(
                    &opctx,
                    SupportBundleUuid::from_untyped_uuid(path.bundle_id),
                    update.user_comment,
                )
                .await?;

            Ok(HttpResponseOk(bundle.into()))
        };
        apictx
            .context
            .external_latencies
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

    async fn clickhouse_policy_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhousePolicy>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            match nexus.datastore().clickhouse_policy_get_latest(&opctx).await?
            {
                Some(policy) => Ok(HttpResponseOk(policy)),
                None => Err(HttpError::for_not_found(
                    None,
                    "No clickhouse policy in database".into(),
                )),
            }
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn clickhouse_policy_set(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<ClickhousePolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus
                .datastore()
                .clickhouse_policy_insert_latest_version(
                    &opctx,
                    &policy.into_inner(),
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn oximeter_read_policy_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<OximeterReadPolicy>, HttpError> {
        let apictx = &rqctx.context().context;
        let handler = async {
            let nexus = &apictx.nexus;
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            let policy = nexus
                .datastore()
                .oximeter_read_policy_get_latest(&opctx)
                .await?;
            Ok(HttpResponseOk(policy))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn oximeter_read_policy_set(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<OximeterReadPolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus
                .datastore()
                .oximeter_read_policy_insert_latest_version(
                    &opctx,
                    &policy.into_inner(),
                )
                .await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn quiesce_start(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            nexus.quiesce_start(&opctx).await?;
            Ok(HttpResponseUpdatedNoContent())
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }

    async fn quiesce_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<QuiesceStatus>, HttpError> {
        let apictx = &rqctx.context().context;
        let nexus = &apictx.nexus;
        let handler = async {
            let opctx =
                crate::context::op_context_for_internal_api(&rqctx).await;
            Ok(HttpResponseOk(nexus.quiesce_state(&opctx).await?))
        };
        apictx
            .internal_latencies
            .instrument_dropshot_handler(&rqctx, handler)
            .await
    }
}
