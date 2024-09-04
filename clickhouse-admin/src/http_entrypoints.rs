// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use clickhouse_admin_api::*;
use clickhouse_admin_types::config::{KeeperConfig, ReplicaConfig};
use dropshot::{
    HttpError, HttpResponseCreated, RequestContext, TypedBody,
};
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
        let output = ctx.clickward().generate_server_config(replica_server.settings)?;
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
}
