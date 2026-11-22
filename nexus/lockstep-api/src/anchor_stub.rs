// TODO-RAINCLAUDE: Generated stub impl of the API trait. It exists so that
// this crate, rather than omicron-nexus, instantiates the context-independent
// extractor and serde code for every endpoint.
#![allow(unused_variables)]
use super::*;

pub struct AnchorStub;

impl NexusLockstepApi for AnchorStub {
    type Context = ();

    async fn rack_initialization_complete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<RackPathParam>,
        info: TypedBody<RackInitializationRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn fetch_omdb(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn instance_migrate(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<InstancePathParam>,
        migrate_params: TypedBody<InstanceMigrateRequest>,
    ) -> Result<HttpResponseOk<Instance>, HttpError> {
        unimplemented!()
    }

    async fn saga_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<Saga>>, HttpError> {
        unimplemented!()
    }

    async fn saga_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<SagaPathParam>,
    ) -> Result<HttpResponseOk<Saga>, HttpError> {
        unimplemented!()
    }

    async fn saga_demo_create(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<DemoSaga>, HttpError> {
        unimplemented!()
    }

    async fn saga_demo_complete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<DemoSagaPathParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn bgtask_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BTreeMap<String, BackgroundTask>>, HttpError> {
        unimplemented!()
    }

    async fn bgtask_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BackgroundTaskPathParam>,
    ) -> Result<HttpResponseOk<BackgroundTask>, HttpError> {
        unimplemented!()
    }

    async fn bgtask_activate(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<BackgroundTasksActivateRequest>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn mgs_updates(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<MgsUpdateDriverStatus>, HttpError> {
        unimplemented!()
    }

    async fn blueprint_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedById>,
    ) -> Result<HttpResponseOk<ResultsPage<BlueprintMetadata>>, HttpError> {
        unimplemented!()
    }

    async fn blueprint_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BlueprintPath>,
    ) -> Result<HttpResponseOk<Blueprint>, HttpError> {
        unimplemented!()
    }

    async fn blueprint_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<BlueprintPath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn blueprint_target_view(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
        unimplemented!()
    }

    async fn blueprint_target_set(
        rqctx: RequestContext<Self::Context>,
        target: TypedBody<BlueprintTargetSet>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
        unimplemented!()
    }

    async fn blueprint_target_set_enabled(
        rqctx: RequestContext<Self::Context>,
        target: TypedBody<BlueprintTargetSet>,
    ) -> Result<HttpResponseOk<BlueprintTarget>, HttpError> {
        unimplemented!()
    }

    async fn blueprint_regenerate(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Blueprint>, HttpError> {
        unimplemented!()
    }

    async fn blueprint_import(
        rqctx: RequestContext<Self::Context>,
        blueprint: TypedBody<Blueprint>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn reconfigurator_config_show_current(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ReconfiguratorConfigView>, HttpError> {
        unimplemented!()
    }

    async fn reconfigurator_config_show(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VersionPathParam>,
    ) -> Result<HttpResponseOk<ReconfiguratorConfigView>, HttpError> {
        unimplemented!()
    }

    async fn reconfigurator_config_set(
        rqctx: RequestContext<Self::Context>,
        switches: TypedBody<ReconfiguratorConfigParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn update_status(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<UpdateStatus>, HttpError> {
        unimplemented!()
    }

    async fn sled_list_uninitialized(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ResultsPage<UninitializedSled>>, HttpError> {
        unimplemented!()
    }

    async fn sled_expunge(
        rqctx: RequestContext<Self::Context>,
        sled: TypedBody<SledSelector>,
    ) -> Result<HttpResponseOk<SledPolicy>, HttpError> {
        unimplemented!()
    }

    async fn physical_disk_expunge(
        rqctx: RequestContext<Self::Context>,
        disk: TypedBody<PhysicalDiskPath>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_list(
        rqctx: RequestContext<Self::Context>,
        query_params: Query<PaginatedByTimeAndId>,
    ) -> Result<HttpResponseOk<ResultsPage<SupportBundleInfo>>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_view(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<HttpResponseOk<SupportBundleInfo>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_index(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_download(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_download_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_head(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_head_file(
        rqctx: RequestContext<Self::Context>,
        headers: Header<RangeRequest>,
        path_params: Path<support_bundle::SupportBundleFilePath>,
    ) -> Result<Response<Body>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_create(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<support_bundle::SupportBundleCreate>,
    ) -> Result<HttpResponseCreated<SupportBundleInfo>, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_delete(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<support_bundle::SupportBundlePath>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        unimplemented!()
    }

    async fn support_bundle_update(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<support_bundle::SupportBundlePath>,
        body: TypedBody<support_bundle::SupportBundleUpdate>,
    ) -> Result<HttpResponseOk<SupportBundleInfo>, HttpError> {
        unimplemented!()
    }

    async fn clickhouse_policy_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhousePolicy>, HttpError> {
        unimplemented!()
    }

    async fn clickhouse_policy_set(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<ClickhousePolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn oximeter_read_policy_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<OximeterReadPolicy>, HttpError> {
        unimplemented!()
    }

    async fn oximeter_read_policy_set(
        rqctx: RequestContext<Self::Context>,
        policy: TypedBody<OximeterReadPolicy>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn quiesce_start(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn quiesce_get(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<QuiesceStatus>, HttpError> {
        unimplemented!()
    }

    async fn trust_quorum_get_config(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<
            nexus_types::external_api::rack::RackMembershipConfigPathParams,
        >,
        query_params: Query<TrustQuorumEpochQueryParam>,
    ) -> Result<HttpResponseOk<TrustQuorumConfig>, HttpError> {
        unimplemented!()
    }

    async fn trust_quorum_lrtq_upgrade(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Epoch>, HttpError> {
        unimplemented!()
    }

    async fn trust_quorum_remove_sled(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<
            nexus_types::external_api::rack::RackMembershipConfigPathParams,
        >,
        sled: TypedBody<BaseboardId>,
    ) -> Result<HttpResponseOk<Epoch>, HttpError> {
        unimplemented!()
    }

    async fn fm_known_ereport_classes_list(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<String>>, HttpError> {
        unimplemented!()
    }

    async fn fm_config_show_current(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<FmConfigView>, HttpError> {
        unimplemented!()
    }

    async fn fm_config_set(
        rqctx: RequestContext<Self::Context>,
        config: TypedBody<FmConfigParam>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        unimplemented!()
    }

    async fn fm_config_show_version(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<VersionPathParam>,
    ) -> Result<HttpResponseOk<FmConfigView>, HttpError> {
        unimplemented!()
    }

}

#[inline(never)]
pub fn anchor() {
    let _ = std::hint::black_box(nexus_lockstep_api_mod::api_description::<AnchorStub>());
}
