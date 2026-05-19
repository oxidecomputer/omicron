// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{HttpError, HttpResponseOk, RequestContext};
use dropshot_api_manager_types::api_versions;
use ntp_admin_types_versions::{latest, v1};

api_versions!([
    // Do not create new versions of this client-side versioned API.
    // https://github.com/oxidecomputer/omicron/issues/9290
    //
    // NOTE: Version 2 of this API was added to address
    // //github.com/oxidecomputer/omicron/issues/7668, a bad failure mode in
    // which CRDB panics early during control plane startup because the clocks
    // are not synchronized well-enough. We're adding this as part of a
    // two-phase rollout to get around #9290 for now.
    (2, ADD_MAX_ERROR_AND_OFFSET),
    (1, INITIAL),
]);

#[dropshot::api_description]
pub trait NtpAdminApi {
    type Context;

    /// Query for the state of time synchronization
    #[endpoint {
        method = GET,
        path = "/timesync",
        versions = VERSION_ADD_MAX_ERROR_AND_OFFSET..
    }]
    async fn timesync(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::timesync::TimeSync>, HttpError>;

    /// Query for the state of time synchronization
    #[endpoint {
        method = GET,
        path = "/timesync",
        operation_id = "timesync",
        versions = VERSION_INITIAL..VERSION_ADD_MAX_ERROR_AND_OFFSET,
    }]
    async fn timesync_v1(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<v1::timesync::TimeSync>, HttpError> {
        Ok(Self::timesync(rqctx).await?.map(|ts| ts.into()))
    }
}
