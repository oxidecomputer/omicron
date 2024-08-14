// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseOk, RequestContext};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;

#[dropshot::api_description]
pub trait ClickhouseAdminApi {
    type Context;

    /// Retrieve the address the ClickHouse server or keeper node is listening on
    #[endpoint {
        method = GET,
        path = "/node/address",
    }]
    async fn clickhouse_address(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ClickhouseAddress>, HttpError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ClickhouseAddress {
    pub clickhouse_address: SocketAddrV6,
}
