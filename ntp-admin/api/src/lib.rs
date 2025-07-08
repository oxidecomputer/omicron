// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseOk, RequestContext};

#[dropshot::api_description]
pub trait NtpAdminApi {
    type Context;

    /// Query for the state of time synchronization
    #[endpoint {
        method = GET,
        path = "/timesync",
    }]
    async fn timesync(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<ntp_admin_types::TimeSync>, HttpError>;
}
