// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::{KeeperServerContext, ServerContext};
use clickhouse_admin_api::*;
use clickhouse_admin_types::{
    ClickhouseKeeperClusterMembership, DistributedDdlQueue,
    GenerateConfigResult, KeeperConf, KeeperConfigurableSettings, Lgif,
    MetricInfoPath, RaftConfig, ServerConfigurableSettings, SystemTimeSeries,
    SystemTimeSeriesSettings, TimeSeriesSettingsQuery,
};
use dropshot::{
    ApiDescription, ClientErrorStatusCode, HttpError, HttpResponseCreated,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, Query, RequestContext,
    TypedBody,
};
use omicron_common::api::external::Generation;
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

pub fn clickhouse_admin_single_api() -> ApiDescription<Arc<ServerContext>> {
    clickhouse_admin_single_api_mod::api_description::<ClickhouseAdminSingleImpl>()
        .expect("registered entrypoints")
}

enum ClickhouseAdminServerImpl {}

impl ClickhouseAdminServerApi for ClickhouseAdminServerImpl {
    type Context = Arc<ServerContext>;

    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerConfigurableSettings>,
    ) -> Result<HttpResponseCreated<GenerateConfigResult>, HttpError> {
        let ctx = rqctx.context();
        let replica_settings = body.into_inner();
        let result =
            ctx.generate_config_and_enable_svc(replica_settings).await?;
        Ok(HttpResponseCreated(result))
    }

    async fn generation(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Generation>, HttpError> {
        let ctx = rqctx.context();
        let generation = match ctx.generation() {
            Some(g) => g,
            None => {
                return Err(HttpError::for_client_error(
                    Some(String::from("ObjectNotFound")),
                    ClientErrorStatusCode::NOT_FOUND,
                    "no generation number found".to_string(),
                ));
            }
        };
        Ok(HttpResponseOk(generation))
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
        let replicated = true;
        ctx.init_db(replicated).await?;
        Ok(HttpResponseUpdatedNoContent())
    }
}

enum ClickhouseAdminKeeperImpl {}

impl ClickhouseAdminKeeperApi for ClickhouseAdminKeeperImpl {
    type Context = Arc<KeeperServerContext>;

    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<KeeperConfigurableSettings>,
    ) -> Result<HttpResponseCreated<GenerateConfigResult>, HttpError> {
        let ctx = rqctx.context();
        let keeper_settings = body.into_inner();
        let result =
            ctx.generate_config_and_enable_svc(keeper_settings).await?;
        Ok(HttpResponseCreated(result))
    }

    async fn generation(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Generation>, HttpError> {
        let ctx = rqctx.context();
        let generation = match ctx.generation() {
            Some(g) => g,
            None => {
                return Err(HttpError::for_client_error(
                    Some(String::from("ObjectNotFound")),
                    ClientErrorStatusCode::NOT_FOUND,
                    "no generation number found".to_string(),
                ));
            }
        };
        Ok(HttpResponseOk(generation))
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
    type Context = Arc<ServerContext>;

    async fn init_db(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let ctx = rqctx.context();
        let replicated = false;
        ctx.init_db(replicated).await?;
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
