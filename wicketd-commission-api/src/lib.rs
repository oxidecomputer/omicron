// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The commission API: a subset of the wicketd API used by external tools
//! (such as rkdeploy) to drive rack commissioning.

use bootstrap_agent_lockstep_client::types::RackOperationStatus;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use dropshot_api_manager_types::api_versions;
use omicron_uuid_kinds::RackInitUuid;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicketd_commission_types_versions::latest;

api_versions!([(1, INITIAL)]);

/// Full release repositories are currently (Dec 2024) 1.8 GiB and are likely to
/// continue growing.
const PUT_REPOSITORY_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;

#[dropshot::api_description]
pub trait WicketdCommissionApi {
    type Context;

    /// Get wicketd's current view of all sleds visible on the bootstrap network.
    #[endpoint {
        method = GET,
        path = "/bootstrap-sleds",
        versions = VERSION_INITIAL..,
    }]
    async fn get_bootstrap_sleds(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::bootstrap_sleds::BootstrapSledIps>,
        HttpError,
    >;

    /// Get the current status of the user-provided (or system-default-provided, in
    /// some cases) RSS configuration.
    #[endpoint {
        method = GET,
        path = "/rack-setup/config",
        versions = VERSION_INITIAL..,
    }]
    async fn get_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::rss_config::CurrentRssUserConfig>,
        HttpError,
    >;

    /// Update (a subset of) the current RSS configuration.
    ///
    /// Sensitive values (certificates and password hash) are not set through
    /// this endpoint.
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config",
        versions = VERSION_INITIAL..,
    }]
    async fn put_rss_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRssUserConfigInsensitive>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Add an external certificate.
    ///
    /// This must be paired with its private key. They may be posted in either
    /// order, but one cannot post two certs in a row (or two keys in a row).
    #[endpoint {
        method = POST,
        path = "/rack-setup/config/cert",
        versions = VERSION_INITIAL..,
    }]
    async fn post_rss_config_cert(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<
        HttpResponseOk<latest::rss_config::CertificateUploadResponse>,
        HttpError,
    >;

    /// Add the private key of an external certificate.
    ///
    /// This must be paired with its certificate. They may be posted in either
    /// order, but one cannot post two keys in a row (or two certs in a row).
    #[endpoint {
        method = POST,
        path = "/rack-setup/config/key",
        versions = VERSION_INITIAL..,
    }]
    async fn post_rss_config_key(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<
        HttpResponseOk<latest::rss_config::CertificateUploadResponse>,
        HttpError,
    >;

    /// Update the RSS config recovery silo user password hash.
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config/recovery-user-password-hash",
        versions = VERSION_INITIAL..,
    }]
    async fn put_rss_config_recovery_user_password_hash(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::rss_config::PutRssRecoveryUserPasswordHash>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Reset all RSS configuration to their default values.
    #[endpoint {
        method = DELETE,
        path = "/rack-setup/config",
        versions = VERSION_INITIAL..,
    }]
    async fn delete_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Query current state of rack setup.
    #[endpoint {
        method = GET,
        path = "/rack-setup",
        versions = VERSION_INITIAL..,
    }]
    async fn get_rack_setup_state(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackOperationStatus>, HttpError>;

    /// Run rack setup.
    ///
    /// Will return an error if not all of the rack setup configuration has
    /// been populated.
    #[endpoint {
        method = POST,
        path = "/rack-setup",
        versions = VERSION_INITIAL..,
    }]
    async fn post_run_rack_setup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError>;

    /// A status endpoint used to report high level information known to
    /// wicketd.
    ///
    /// This endpoint can be polled to see if there have been state changes in
    /// the system that are useful to report to wicket.
    ///
    /// Wicket, and possibly other callers, will retrieve the changed
    /// information, with follow up calls.
    #[endpoint {
        method = GET,
        path = "/inventory",
        versions = VERSION_INITIAL..,
    }]
    async fn get_inventory(
        rqctx: RequestContext<Self::Context>,
        body_params: TypedBody<latest::inventory::GetInventoryParams>,
    ) -> Result<
        HttpResponseOk<latest::inventory::GetInventoryResponse>,
        HttpError,
    >;

    /// Upload a TUF repository to the server.
    ///
    /// At any given time, wicketd will keep at most one TUF repository in
    /// memory. Any previously-uploaded repositories will be discarded.
    #[endpoint {
        method = PUT,
        path = "/repository",
        request_body_max_bytes = PUT_REPOSITORY_MAX_BYTES,
        versions = VERSION_INITIAL..,
    }]
    async fn put_repository(
        rqctx: RequestContext<Self::Context>,
        body: StreamingBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// An endpoint used to report all available artifacts and event reports.
    ///
    /// The order of the returned artifacts is unspecified, and may change between
    /// calls even if the total set of artifacts has not.
    #[endpoint {
        method = GET,
        path = "/artifacts-and-event-reports",
        versions = VERSION_INITIAL..,
    }]
    async fn get_artifacts_and_event_reports(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::artifacts::GetArtifactsAndEventReportsResponse>,
        HttpError,
    >;

    /// Report the identity of the sled and switch we're currently running on /
    /// connected to.
    #[endpoint {
        method = GET,
        path = "/location",
        versions = VERSION_INITIAL..,
    }]
    async fn get_location(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::location::GetLocationResponse>, HttpError>;

    /// An endpoint to start updating one or more sleds, switches and PSCs.
    #[endpoint {
        method = POST,
        path = "/update",
        versions = VERSION_INITIAL..,
    }]
    async fn post_start_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::update::StartUpdateParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Resets update state for a sled.
    ///
    /// Use this to clear update state after a failed update.
    #[endpoint {
        method = POST,
        path = "/clear-update-state",
        versions = VERSION_INITIAL..,
    }]
    async fn post_clear_update_state(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::update::ClearUpdateStateParams>,
    ) -> Result<
        HttpResponseOk<wicket_common::rack_update::ClearUpdateStateResponse>,
        HttpError,
    >;
}
