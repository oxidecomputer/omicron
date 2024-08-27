// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use clickhouse_admin_api::*;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
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
}
