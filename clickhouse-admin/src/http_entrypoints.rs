// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use clickhouse_admin_api::*;
use clickhouse_admin_types::{
    ClickhouseKeeperClusterMembership, KeeperConf, KeeperConfig,
    KeeperConfigurableSettings, Lgif, RaftConfig, ReplicaConfig,
    ServerConfigurableSettings,
};
use dropshot::{
    HttpError, HttpResponseCreated, HttpResponseOk, RequestContext, TypedBody,
};
use illumos_utils::svcadm::Svcadm;
use std::sync::Arc;

type ClickhouseApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn clickhouse_admin_server_api() -> ClickhouseApiDescription {
    clickhouse_admin_server_api_mod::api_description::<ClickhouseAdminServerImpl>()
        .expect("registered entrypoints")
}

pub fn clickhouse_admin_keeper_api() -> ClickhouseApiDescription {
    clickhouse_admin_keeper_api_mod::api_description::<ClickhouseAdminKeeperImpl>()
        .expect("registered entrypoints")
}

enum ClickhouseAdminServerImpl {}

impl ClickhouseAdminServerApi for ClickhouseAdminServerImpl {
    type Context = Arc<ServerContext>;

    async fn generate_config(
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
}

enum ClickhouseAdminKeeperImpl {}

impl ClickhouseAdminKeeperApi for ClickhouseAdminKeeperImpl {
    type Context = Arc<ServerContext>;

    async fn generate_config(
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
