// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::{KeeperServerContext, ServerContext, SingleServerContext};
use clickhouse_admin_api::*;
use clickhouse_admin_types::{
    ClickhouseKeeperClusterMembership, DistributedDdlQueue, KeeperConf,
    KeeperConfig, KeeperConfigurableSettings, Lgif, MetricInfoPath, RaftConfig,
    ReplicaConfig, ServerConfigurableSettings, SystemTimeSeries,
    SystemTimeSeriesSettings, TimeSeriesSettingsQuery,
};
use dropshot::{
    ApiDescription, HttpError, HttpResponseCreated, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, Query, RequestContext, TypedBody,
};
use http::StatusCode;
use illumos_utils::svcadm::Svcadm;
use omicron_common::api::external::Generation;
use oximeter_db::OXIMETER_VERSION;
use slog::info;
use std::sync::Arc;

pub fn clickhouse_admin_server_api() -> ApiDescription<Arc<ServerContext>> {
    clickhouse_admin_server_api_mod::api_description::<ClickhouseAdminServerImpl>()
        .expect("registered entrypoints")
}

pub fn clickhouse_admin_keeper_api() -> ApiDescription<Arc<KeeperServerContext>>
{
    clickhouse_admin_keeper_api_mod::api_description::<ClickhouseAdminKeeperImpl>()
        .expect("registered entrypoints")
}

pub fn clickhouse_admin_single_api() -> ApiDescription<Arc<SingleServerContext>>
{
    clickhouse_admin_single_api_mod::api_description::<ClickhouseAdminSingleImpl>()
        .expect("registered entrypoints")
}

enum ClickhouseAdminServerImpl {}

impl ClickhouseAdminServerApi for ClickhouseAdminServerImpl {
    type Context = Arc<ServerContext>;

    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerConfigurableSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError> {
        let ctx = rqctx.context();
        let replica_server = body.into_inner();
        let current_generation = ctx.generation().await;
        let incoming_generation = replica_server.generation();

        // If the incoming generation number is lower, then we have a problem.
        // We should return an error instead of silently skipping the configuration
        // file generation.
        if let Some(current) = current_generation {
            if current > incoming_generation {
                return Err(HttpError::for_internal_error(
                    "current generation is greater than incoming generation"
                        .to_string(),
                ));
            }
        };

        let output = ctx.clickward().generate_server_config(replica_server)?;

        // We want to update the generation number only if the config file has been
        // generated successfully.
        *ctx.generation.lock().unwrap() = Some(incoming_generation);

        // Once we have generated the client we can safely enable the clickhouse_server service
        let fmri = "svc:/oxide/clickhouse_server:default".to_string();
        Svcadm::enable_service(fmri)?;

        Ok(HttpResponseCreated(output))
    }

    async fn generation(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Generation>, HttpError> {
        let ctx = rqctx.context();
        let gen = match ctx.generation().await {
            Some(g) => g,
            None => {
                return Err(HttpError::for_client_error(
                    Some(String::from("ObjectNotFound")),
                    StatusCode::NOT_FOUND,
                    "no generation number found".to_string(),
                ))
            }
        };
        Ok(HttpResponseOk(gen))
    }

    async fn distributed_ddl_queue(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Vec<DistributedDdlQueue>>, HttpError> {
        let ctx = rqctx.context();
        let output = ctx.clickhouse_cli().distributed_ddl_queue().await?;
        Ok(HttpResponseOk(output))
    }

    async fn system_timeseries_avg(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<MetricInfoPath>,
        query_params: Query<TimeSeriesSettingsQuery>,
    ) -> Result<HttpResponseOk<Vec<SystemTimeSeries>>, HttpError> {
        let ctx = rqctx.context();
        let retrieval_settings = query_params.into_inner();
        let metric_info = path_params.into_inner();
        let settings =
            SystemTimeSeriesSettings { retrieval_settings, metric_info };
        let output =
            ctx.clickhouse_cli().system_timeseries_avg(settings).await?;
        Ok(HttpResponseOk(output))
    }

    async fn init_db(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let log = ctx.log();

        // Database initialization is idempotent, but not concurrency-safe.
        // Use a mutex to serialize requests.
        let lock = ctx.initialization_lock();
        let _guard = lock.lock().await;

        // Initialize the database only if it was not previously initialized.
        // TODO: Migrate schema to newer version without wiping data.
        let client = ctx.oximeter_client();
        let version = client.read_latest_version().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "can't read ClickHouse version: {e}",
            ))
        })?;
        if version == 0 {
            info!(
                log,
                "initializing replicated ClickHouse cluster to version {OXIMETER_VERSION}"
            );
            let replicated = true;
            ctx.oximeter_client()
                .initialize_db_with_version(replicated, OXIMETER_VERSION)
                .await
                .map_err(|e| {
                    HttpError::for_internal_error(format!(
                        "can't initialize replicated ClickHouse cluster \
                         to version {OXIMETER_VERSION}: {e}",
                    ))
                })?;
        } else {
            info!(
                log,
                "skipping initialization of replicated ClickHouse cluster at version {version}"
            );
        }

        Ok(HttpResponseUpdatedNoContent())
    }
}

enum ClickhouseAdminKeeperImpl {}

impl ClickhouseAdminKeeperApi for ClickhouseAdminKeeperImpl {
    type Context = Arc<KeeperServerContext>;

    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<KeeperConfigurableSettings>,
    ) -> Result<HttpResponseCreated<KeeperConfig>, HttpError> {
        let ctx = rqctx.context();
        let keeper = body.into_inner();
        let current_generation = ctx.generation();
        let incoming_generation = keeper.generation();

        // If the incoming generation number is lower, then we have a problem.
        // We should return an error instead of silently skipping the configuration
        // file generation.
        if let Some(current) = current_generation {
            if current > incoming_generation {
                return Err(HttpError::for_internal_error(
                    "current generation is greater than incoming generation"
                        .to_string(),
                ));
            }
        };

        let output = ctx.clickward().generate_keeper_config(keeper)?;

        // We want to update the generation number only if the config file has been
        // generated successfully.
        *ctx.generation.lock().unwrap() = Some(incoming_generation);

        // Once we have generated the client we can safely enable the clickhouse_keeper service
        let fmri = "svc:/oxide/clickhouse_keeper:default".to_string();
        Svcadm::enable_service(fmri)?;

        Ok(HttpResponseCreated(output))
    }

    async fn generation(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Generation>, HttpError> {
        let ctx = rqctx.context();
        let gen = match ctx.generation() {
            Some(g) => g,
            None => {
                return Err(HttpError::for_client_error(
                    Some(String::from("ObjectNotFound")),
                    StatusCode::NOT_FOUND,
                    "no generation number found".to_string(),
                ))
            }
        };
        Ok(HttpResponseOk(gen))
    }

    async fn lgif(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Lgif>, HttpError> {
        let ctx = rqctx.context();
        let output = ctx.clickhouse_cli().lgif().await?;
        Ok(HttpResponseOk(output))
    }

    async fn raft_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RaftConfig>, HttpError> {
        let ctx = rqctx.context();
        let output = ctx.clickhouse_cli().raft_config().await?;
        Ok(HttpResponseOk(output))
    }

    async fn keeper_conf(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<KeeperConf>, HttpError> {
        let ctx = rqctx.context();
        let output = ctx.clickhouse_cli().keeper_conf().await?;
        Ok(HttpResponseOk(output))
    }

    async fn keeper_cluster_membership(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhouseKeeperClusterMembership>, HttpError>
    {
        let ctx = rqctx.context();
        let output = ctx.clickhouse_cli().keeper_cluster_membership().await?;
        Ok(HttpResponseOk(output))
    }
}

enum ClickhouseAdminSingleImpl {}

impl ClickhouseAdminSingleApi for ClickhouseAdminSingleImpl {
    type Context = Arc<SingleServerContext>;

    async fn init_db(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let log = ctx.log();

        // Database initialization is idempotent, but not concurrency-safe.
        // Use a mutex to serialize requests.
        let lock = ctx.initialization_lock();
        let _guard = lock.lock().await;

        // Initialize the database only if it was not previously initialized.
        // TODO: Migrate schema to newer version without wiping data.
        let client = ctx.oximeter_client();
        let version = client.read_latest_version().await.map_err(|e| {
            HttpError::for_internal_error(format!(
                "can't read ClickHouse version: {e}",
            ))
        })?;
        if version == 0 {
            info!(
                log,
                "initializing single-node ClickHouse to version {OXIMETER_VERSION}"
            );
            ctx.oximeter_client()
                .initialize_db_with_version(false, OXIMETER_VERSION)
                .await
                .map_err(|e| {
                    HttpError::for_internal_error(format!(
                        "can't initialize single-node ClickHouse \
                         to version {OXIMETER_VERSION}: {e}",
                    ))
                })?;
        } else {
            info!(
                log,
                "skipping initialization of single-node ClickHouse at version {version}"
            );
        }

        Ok(HttpResponseUpdatedNoContent())
    }

    async fn system_timeseries_avg(
        rqctx: RequestContext<Self::Context>,
        path_params: Path<MetricInfoPath>,
        query_params: Query<TimeSeriesSettingsQuery>,
    ) -> Result<HttpResponseOk<Vec<SystemTimeSeries>>, HttpError> {
        let ctx = rqctx.context();
        let retrieval_settings = query_params.into_inner();
        let metric_info = path_params.into_inner();
        let settings =
            SystemTimeSeriesSettings { retrieval_settings, metric_info };
        let output =
            ctx.clickhouse_cli().system_timeseries_avg(settings).await?;
        Ok(HttpResponseOk(output))
    }
}
