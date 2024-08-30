// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use clickhouse_admin_api::*;
use clickhouse_admin_types::config::ReplicaConfig;
use dropshot::HttpError;
use dropshot::{
    HttpResponseCreated, HttpResponseOk, RequestContext, TypedBody,
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

    async fn clickhouse_address(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhouseAddress>, HttpError> {
        let ctx = rqctx.context();
        let output = ctx.clickward().clickhouse_address()?;
        Ok(HttpResponseOk(output))
    }

    async fn generate_server_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<ServerSettings>,
    ) -> Result<HttpResponseCreated<ReplicaConfig>, HttpError> {
        let ctx = rqctx.context();
        let server_settings = body.into_inner();
        let output = ctx.clickward().generate_server_config(server_settings)?;
        Ok(HttpResponseCreated(output))
    }
}
