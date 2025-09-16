// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseOk, RequestContext};
use nexus_types::external_api::views::{Ping, PingStatus};

#[dropshot::api_description]
pub trait NexusLockstepApi {
    type Context;

    /// Ping API
    ///
    /// Always responds with Ok if it responds at all.
    #[endpoint {
        method = GET,
        path = "/v1/ping",
    }]
    async fn ping(
        _rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<Ping>, HttpError> {
        Ok(HttpResponseOk(Ping { status: PingStatus::Ok }))
    }
}
