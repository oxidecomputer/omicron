// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Handler functions (entrypoints) for HTTP APIs internal to the control plane
//! whose callers are updated in lockstep with Nexus

use std::collections::BTreeMap;

use dropshot::ApiDescription;
use dropshot::Body;
use dropshot::ErrorStatusCode;
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
use http::StatusCode;
use nexus_lockstep_api::*;
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
use nexus_types::external_api::shared;
use nexus_types::external_api::shared::UninitializedSled;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::internal_api::params::InstanceMigrateRequest;
use nexus_types::internal_api::views::BackgroundTask;
use nexus_types::internal_api::views::DemoSaga;
use nexus_types::internal_api::views::MgsUpdateDriverStatus;
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
use omicron_uuid_kinds::*;
use range_requests::PotentialRange;
use slog_error_chain::InlineErrorChain;

use crate::app::support_bundles::SupportBundleQueryType;
use crate::context::ApiContext;

type NexusApiDescription = ApiDescription<ApiContext>;

/// Returns a description of the nexus lockstep API
pub(crate) fn lockstep_api() -> NexusApiDescription {
    nexus_lockstep_api_mod::api_description::<NexusLockstepApiImpl>()
        .expect("registered API endpoints successfully")
}

enum NexusLockstepApiImpl {}

impl NexusLockstepApi for NexusLockstepApiImpl {
    type Context = ApiContext;

    async fn fetch_omdb(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<Response<Body>, HttpError> {
        let apictx = &rqctx.context().context;
        let omdb_config = apictx.omdb_config.as_ref().ok_or_else(|| {
            let err =
                "Nexus not configured with a local omdb to fetch".to_string();
            HttpError {
                status_code: ErrorStatusCode::SERVICE_UNAVAILABLE,
                error_code: None,
                external_message: err.clone(),
                internal_message: err,
                headers: None,
            }
        })?;
        let path = &omdb_config.bin_path;
        let f = tokio::fs::File::open(path).await.map_err(|err| {
            let err = format!(
                "could not open {path}: {}",
                InlineErrorChain::new(&err)
            );
            HttpError {
                status_code: ErrorStatusCode::SERVICE_UNAVAILABLE,
                error_code: None,
                external_message: err.clone(),
                internal_message: err,
                headers: None,
            }
        })?;
        let f = hyper_staticfile::vfs::TokioFileAccess::new(f);
        let f = hyper_staticfile::util::FileBytesStream::new(f);
        let body = Body::wrap(hyper_staticfile::Body::Full(f));
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "application/octet-stream")
            .body(body)?)
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
                .instance_migrate(&opctx, path.instance_id, migrate)
                .await?;
            Ok(HttpResponseOk(instance.into()))
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
