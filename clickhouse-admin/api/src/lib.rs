// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseOk, RequestContext};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[dropshot::api_description]
pub trait ClickhouseAdminApi {
    type Context;

    /// Get the status of all nodes in the ClickHouse cluster.
    #[endpoint {
        method = GET,
        path = "/test",
    }]
    async fn ch_test(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ChTest>, HttpError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ChTest {
    pub result: String,
}
