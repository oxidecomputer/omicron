// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseOk, RequestContext};
use dropshot_api_manager_types::api_versions;
use ntp_admin_types_versions::latest;

api_versions!([
    // Do not create new versions of this client-side versioned API.
    // https://github.com/oxidecomputer/omicron/issues/9290
    (1, INITIAL),
]);

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
    ) -> Result<HttpResponseOk<latest::timesync::TimeSync>, HttpError>;
}
