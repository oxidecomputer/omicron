// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use clickhouse_admin_api::*;
use clickhouse_admin_types::config::{KeeperConfig, ReplicaConfig};
use clickhouse_admin_types::{
    ClickhouseKeeperClusterMembership, KeeperConf, KeeperId, Lgif, RaftConfig,
};
use dropshot::{
    HttpError, HttpResponseCreated, HttpResponseOk, RequestContext, TypedBody,
};
use std::collections::BTreeSet;
use std::sync::Arc;

type ClickhouseApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> ClickhouseApiDescription {
    clickhouse_admin_api_mod::api_description::<ClickhouseAdminImpl>()
        .expect("registered entrypoints")
}

enum ClickhouseAdminImpl {}

impl ClickhouseAdminApi for ClickhouseAdminImpl {
    type Context = Arc<ServerContext>;

    async fn generate_server_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerConfigurableSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError> {
        let ctx = rqctx.context();
        let replica_server = body.into_inner();
        // TODO(https://github.com/oxidecomputer/omicron/issues/5999): Do something
        // with the generation number `replica_server.generation`
        let output =
            ctx.clickward().generate_server_config(replica_server.settings)?;
        Ok(HttpResponseCreated(output))
    }

    async fn generate_keeper_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<KeeperConfigurableSettings>,
    ) -> Result<HttpResponseCreated<KeeperConfig>, HttpError> {
        let ctx = rqctx.context();
        let keeper = body.into_inner();
        // TODO(https://github.com/oxidecomputer/omicron/issues/5999): Do something
        // with the generation number `keeper.generation`
        let output = ctx.clickward().generate_keeper_config(keeper.settings)?;
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
        let lgif_output = ctx.clickhouse_cli().lgif().await?;
        let conf_output = ctx.clickhouse_cli().keeper_conf().await?;
        let raft_output = ctx.clickhouse_cli().raft_config().await?;
        let raft_config: BTreeSet<KeeperId> =
            raft_output.keeper_servers.iter().map(|s| s.server_id).collect();

        Ok(HttpResponseOk(ClickhouseKeeperClusterMembership {
            queried_keeper: conf_output.server_id,
            leader_committed_log_index: lgif_output.leader_committed_log_idx,
            raft_config,
        }))
    }
}
