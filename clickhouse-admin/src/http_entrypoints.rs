// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use clickhouse_admin_api::*;
use clickhouse_admin_types::config::{KeeperConfig, ReplicaConfig};
use dropshot::{HttpError, HttpResponseCreated, Path, RequestContext, TypedBody};
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
        path: Path<GenerationNum>,
        body: TypedBody<ServerSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError> {
        let ctx = rqctx.context();
        let server_settings = body.into_inner();
        let output = ctx.clickward().generate_server_config(server_settings)?;
        // TODO: Do something with the generation number
        println!("{path:?}");
        Ok(HttpResponseCreated(output))
    }

    async fn generate_keeper_config(
        rqctx: RequestContext<Self::Context>,
        path: Path<GenerationNum>,
        body: TypedBody<KeeperSettings>,
    ) -> Result<HttpResponseCreated<KeeperConfig>, HttpError> {
        let ctx = rqctx.context();
        let keeper_settings = body.into_inner();
        let output = ctx.clickward().generate_keeper_config(keeper_settings)?;
        // TODO: Do something with the generation number
        println!("{path:?}");
        Ok(HttpResponseCreated(output))
    }
}
