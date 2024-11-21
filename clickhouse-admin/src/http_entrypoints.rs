// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::{ServerContext, SingleServerContext};
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
use illumos_utils::svcadm::Svcadm;
use omicron_common::address::CLICKHOUSE_TCP_PORT;
use oximeter_db::{Client as OximeterClient, OXIMETER_VERSION};
use slog::debug;
use std::net::SocketAddrV6;
use std::sync::Arc;

pub fn clickhouse_admin_server_api() -> ApiDescription<Arc<ServerContext>> {
    clickhouse_admin_server_api_mod::api_description::<ClickhouseAdminServerImpl>()
        .expect("registered entrypoints")
}

pub fn clickhouse_admin_keeper_api() -> ApiDescription<Arc<ServerContext>> {
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
        let output =
            ctx.clickward().generate_server_config(replica_server.settings)?;

        // Once we have generated the client we can safely enable the clickhouse_server service
        let fmri = "svc:/oxide/clickhouse_server:default".to_string();
        Svcadm::enable_service(fmri)?;

        Ok(HttpResponseCreated(output))
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
}

enum ClickhouseAdminKeeperImpl {}

impl ClickhouseAdminKeeperApi for ClickhouseAdminKeeperImpl {
    type Context = Arc<ServerContext>;

    async fn generate_config_and_enable_svc(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<KeeperConfigurableSettings>,
    ) -> Result<HttpResponseCreated<KeeperConfig>, HttpError> {
        let ctx = rqctx.context();
        let keeper = body.into_inner();
        let output = ctx.clickward().generate_keeper_config(keeper.settings)?;

        // Once we have generated the client we can safely enable the clickhouse_keeper service
        let fmri = "svc:/oxide/clickhouse_keeper:default".to_string();
        Svcadm::enable_service(fmri)?;

        Ok(HttpResponseCreated(output))
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
        let log = &rqctx.log;
        let ctx = rqctx.context();
        let http_address = ctx.clickhouse_cli().listen_address;
        let native_address =
            SocketAddrV6::new(*http_address.ip(), CLICKHOUSE_TCP_PORT, 0, 0);
        let client = OximeterClient::new(
            http_address.into(),
            native_address.into(),
            log,
        );
        debug!(
            log,
            "initializing single-node ClickHouse \
             at {http_address} to version {OXIMETER_VERSION}"
        );

        // Database initialization is idempotent, but not concurrency-safe.
        // Use a mutex to serialize requests.
        let lock = ctx.initialization_lock();
        let _guard = lock.lock().await;
        client
            .initialize_db_with_version(false, OXIMETER_VERSION)
            .await
            .map_err(|e| {
                HttpError::for_internal_error(format!(
                    "can't initialize single-node ClickHouse \
                     at {http_address} to version {OXIMETER_VERSION}: {e}",
                ))
            })?;

        Ok(HttpResponseUpdatedNoContent())
    }
}
