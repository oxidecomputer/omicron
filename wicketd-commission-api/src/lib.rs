// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The stable, versioned commissioning API served by wicketd.
//!
//! This is a small surface intended for the rack commissioning tool (rkdeploy).

use dropshot::{
    HttpError, HttpResponseOk, HttpResponseUpdatedNoContent, Path,
    RequestContext, StreamingBody, TypedBody,
};
use dropshot_api_manager_types::api_versions;
use iddqd::IdOrdMap;
use omicron_uuid_kinds::RackInitUuid;
use wicketd_commission_types_versions::latest;

api_versions!([(1, INITIAL),]);

/// Full release repositories are currently (2026) well over 1 GiB and continue
/// to grow.
const PUT_REPOSITORY_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;

#[dropshot::api_description]
pub trait WicketdCommissionApi {
    type Context;

    /// Get inventory of all service processors visible to wicketd
    ///
    /// If `force_refresh` is non-empty, the request will wait for the listed SPs
    /// to report fresh data, or return a 503 if they do not respond within the
    /// server-side timeout.
    ///
    /// Returns 400 if the SP is unknown.
    #[endpoint {
        method = POST,
        path = "/inventory/sps",
    }]
    async fn get_sp_inventory(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::inventory::SpInventoryParams>,
    ) -> Result<HttpResponseOk<latest::inventory::SpInventory>, HttpError>;

    /// Report the physical location (switch and sled) wicketd is running at
    ///
    /// Returns 503 if the location is not yet known (typically because
    /// wicketd hasn't been able to connect to MGS).
    #[endpoint {
        method = GET,
        path = "/location",
    }]
    async fn get_location(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::inventory::LocationInfo>, HttpError>;

    /// List all sleds known to wicketd and their bootstrap-network addresses
    #[endpoint {
        method = GET,
        path = "/bootstrap-sleds",
    }]
    async fn get_bootstrap_sleds(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<IdOrdMap<latest::inventory::BootstrapSled>>,
        HttpError,
    >;

    /// Describe the TUF repository currently held by wicketd
    #[endpoint {
        method = GET,
        path = "/repository",
    }]
    async fn get_repository(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<latest::update::RepositoryDescription>, HttpError>;

    /// Upload a TUF repository to wicketd
    ///
    /// At any given time, wicketd holds at most one TUF repository, extracted to
    /// ephemeral storage; any previously-uploaded repository is discarded. This
    /// request is rejected with a 400 while any service-processor update is in
    /// progress. A successful upload clears all update progress state, so a
    /// subsequent `GET /update-progress` returns an empty list.
    #[endpoint {
        method = PUT,
        path = "/repository",
        request_body_max_bytes = PUT_REPOSITORY_MAX_BYTES,
    }]
    async fn put_repository(
        rqctx: RequestContext<Self::Context>,
        body: StreamingBody,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Report the update progress of every service processor with update state
    #[endpoint {
        method = GET,
        path = "/update-progress",
    }]
    async fn get_update_progress(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<IdOrdMap<latest::update::SpUpdateProgress>>,
        HttpError,
    >;

    /// Start updating one or more service processors
    ///
    /// A target that already has update state (even from a successful update) is
    /// rejected until that state is cleared with `/clear-update-state` or a new
    /// repository is uploaded.
    #[endpoint {
        method = POST,
        path = "/update",
    }]
    async fn post_start_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::update::StartUpdateParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Clear update state for one or more service processors
    ///
    /// Use this to reset update state after a completed or failed update. This
    /// fails with a 400 if any of the targeted service processors are currently
    /// being updated. Otherwise, the response reports which targets had update
    /// state cleared and which had no update data to clear.
    #[endpoint {
        method = POST,
        path = "/clear-update-state",
    }]
    async fn post_clear_update_state(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<latest::update::ClearUpdateStateParams>,
    ) -> Result<
        HttpResponseOk<latest::update::ClearUpdateStateResponse>,
        HttpError,
    >;

    /// Query the current state of rack setup
    #[endpoint {
        method = GET,
        path = "/rack-setup",
    }]
    async fn get_rack_setup_state(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<
        HttpResponseOk<latest::rack_setup::RackOperationStatus>,
        HttpError,
    >;

    /// Update (a subset of) the current RSS configuration
    ///
    /// Sensitive values (certificates, the recovery password hash, and BGP
    /// authentication keys) are not set through this endpoint.
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config",
    }]
    async fn put_rss_config(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::rack_setup::PutRssUserConfigInsensitive>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Reset all RSS configuration to default values
    #[endpoint {
        method = DELETE,
        path = "/rack-setup/config",
    }]
    async fn delete_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Add an external certificate
    ///
    /// A certificate must be paired with its private key; the two may be posted
    /// in either order. Posting a certificate again before the pair completes
    /// replaces the pending certificate. Re-uploading a certificate and key that
    /// have already been accepted is detected as a duplicate: nothing is stored,
    /// and the response is `cert_key_duplicate_ignored`. If validation of a
    /// completed cert/key pair fails, both pending halves are discarded and must
    /// be re-posted.
    #[endpoint {
        method = POST,
        path = "/rack-setup/config/cert",
    }]
    async fn post_rss_config_cert(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<
        HttpResponseOk<latest::rack_setup::CertificateUploadResponse>,
        HttpError,
    >;

    /// Add the private key of an external certificate
    ///
    /// A private key must be paired with its certificate; the two may be posted
    /// in either order. Posting a key again before the pair completes replaces
    /// the pending key. Re-uploading a certificate and key that have already
    /// been accepted is detected as a duplicate: nothing is stored, and the
    /// response is `cert_key_duplicate_ignored`. If validation of a completed
    /// cert/key pair fails, both pending halves are discarded and must be
    /// re-posted.
    #[endpoint {
        method = POST,
        path = "/rack-setup/config/key",
    }]
    async fn post_rss_config_key(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<
        HttpResponseOk<latest::rack_setup::CertificateUploadResponse>,
        HttpError,
    >;

    /// Set the BGP authentication key for a key ID
    ///
    /// A BGP peer in the RSS configuration may reference an authentication key
    /// by ID; the key material itself is set here, and rack setup is rejected
    /// until every referenced key ID has been set. Keys are write-only: this
    /// API never returns key material.
    ///
    /// Returns 400 if the key ID is not referenced by the current RSS
    /// configuration.
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config/bgp-auth-key/{key_id}",
    }]
    async fn put_bgp_auth_key(
        rqctx: RequestContext<Self::Context>,
        path: Path<latest::rack_setup::BgpAuthKeyPath>,
        body: TypedBody<latest::rack_setup::BgpAuthKey>,
    ) -> Result<
        HttpResponseOk<latest::rack_setup::SetBgpAuthKeyStatus>,
        HttpError,
    >;

    /// Set the recovery-silo user password hash
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config/recovery-user-password-hash",
    }]
    async fn put_rss_config_recovery_user_password_hash(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<latest::rack_setup::PutRecoveryUserPasswordHash>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Run rack setup
    ///
    /// Returns an error if the rack setup configuration has not been fully
    /// populated.
    #[endpoint {
        method = POST,
        path = "/rack-setup",
    }]
    async fn post_run_rack_setup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError>;
}
