// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::helpers::sps_to_string;
use crate::helpers::SpIdentifierDisplay;
use crate::mgs::GetInventoryError;
use crate::mgs::GetInventoryResponse;
use crate::mgs::MgsHandle;
use crate::mgs::ShutdownInProgress;
use crate::preflight_check::UplinkEventReport;
use crate::RackV1Inventory;
use crate::SmfConfigValues;
use bootstrap_agent_client::types::RackInitId;
use bootstrap_agent_client::types::RackOperationStatus;
use bootstrap_agent_client::types::RackResetId;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use gateway_client::types::IgnitionCommand;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use http::StatusCode;
use internal_dns::resolver::Resolver;
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::shared::SwitchLocation;
use omicron_common::update::ArtifactHashId;
use omicron_common::update::ArtifactId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_hardware_types::Baseboard;
use slog::o;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use std::time::Duration;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::BgpAuthKeyId;
use wicket_common::rack_setup::CurrentRssUserConfigInsensitive;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::update_events::EventReport;
use wicket_common::WICKETD_TIMEOUT;

use crate::ServerContext;

type WicketdApiDescription = ApiDescription<ServerContext>;

/// Return a description of the wicketd api for use in generating an OpenAPI spec
pub fn api() -> WicketdApiDescription {
    fn register_endpoints(
        api: &mut WicketdApiDescription,
    ) -> Result<(), String> {
        api.register(get_bootstrap_sleds)?;
        api.register(get_rss_config)?;
        api.register(put_rss_config)?;
        api.register(put_rss_config_recovery_user_password_hash)?;
        api.register(post_rss_config_cert)?;
        api.register(post_rss_config_key)?;
        api.register(get_bgp_auth_key_info)?;
        api.register(put_bgp_auth_key)?;
        api.register(delete_rss_config)?;
        api.register(get_rack_setup_state)?;
        api.register(post_run_rack_setup)?;
        api.register(post_run_rack_reset)?;
        api.register(get_inventory)?;
        api.register(get_location)?;
        api.register(put_repository)?;
        api.register(get_artifacts_and_event_reports)?;
        api.register(get_baseboard)?;
        api.register(post_start_update)?;
        api.register(post_abort_update)?;
        api.register(post_clear_update_state)?;
        api.register(get_update_sp)?;
        api.register(post_ignition_command)?;
        api.register(post_start_preflight_uplink_check)?;
        api.register(get_preflight_uplink_report)?;
        api.register(post_reload_config)?;
        Ok(())
    }

    let mut api = WicketdApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct BootstrapSledIp {
    pub baseboard: Baseboard,
    pub ip: Ipv6Addr,
}

#[derive(
    Clone,
    Debug,
    Serialize,
    Deserialize,
    JsonSchema,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct BootstrapSledIps {
    pub sleds: Vec<BootstrapSledIp>,
}

/// Get wicketd's current view of all sleds visible on the bootstrap network.
#[endpoint {
    method = GET,
    path = "/bootstrap-sleds"
}]
async fn get_bootstrap_sleds(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<BootstrapSledIps>, HttpError> {
    let ctx = rqctx.context();

    let sleds = ctx
        .bootstrap_peers
        .sleds()
        .into_iter()
        .map(|(baseboard, ip)| BootstrapSledIp { baseboard, ip })
        .collect();

    Ok(HttpResponseOk(BootstrapSledIps { sleds }))
}

// This is a summary of the subset of `RackInitializeRequest` that is sensitive;
// we only report a summary instead of returning actual data.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CurrentRssUserConfigSensitive {
    pub num_external_certificates: usize,
    pub recovery_silo_password_set: bool,
    // We define GetBgpAuthKeyInfoResponse in wicket-common and use a
    // progenitor replace directive for it, because we don't want typify to
    // turn the BTreeMap into a HashMap. Use the same struct here to piggyback
    // on that.
    pub bgp_auth_keys: GetBgpAuthKeyInfoResponse,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CurrentRssUserConfig {
    pub sensitive: CurrentRssUserConfigSensitive,
    pub insensitive: CurrentRssUserConfigInsensitive,
}

// Get the current inventory or return a 503 Unavailable.
async fn inventory_or_unavail(
    mgs_handle: &MgsHandle,
) -> Result<RackV1Inventory, HttpError> {
    match mgs_handle.get_cached_inventory().await {
        Ok(GetInventoryResponse::Response { inventory, .. }) => Ok(inventory),
        Ok(GetInventoryResponse::Unavailable) => Err(HttpError::for_unavail(
            None,
            "Rack inventory not yet available".into(),
        )),
        Err(ShutdownInProgress) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
    }
}

/// Get the current status of the user-provided (or system-default-provided, in
/// some cases) RSS configuration.
#[endpoint {
    method = GET,
    path = "/rack-setup/config"
}]
async fn get_rss_config(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<CurrentRssUserConfig>, HttpError> {
    let ctx = rqctx.context();

    // We can't run RSS if we don't have an inventory from MGS yet; we always
    // need to fill in the bootstrap sleds first.
    let inventory = inventory_or_unavail(&ctx.mgs_handle).await?;

    let mut config = ctx.rss_config.lock().unwrap();
    config.update_with_inventory_and_bootstrap_peers(
        &inventory,
        &ctx.bootstrap_peers,
    );

    Ok(HttpResponseOk((&*config).into()))
}

/// Update (a subset of) the current RSS configuration.
///
/// Sensitive values (certificates and password hash) are not set through this
/// endpoint.
#[endpoint {
    method = PUT,
    path = "/rack-setup/config"
}]
async fn put_rss_config(
    rqctx: RequestContext<ServerContext>,
    body: TypedBody<PutRssUserConfigInsensitive>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ctx = rqctx.context();

    // We can't run RSS if we don't have an inventory from MGS yet; we always
    // need to fill in the bootstrap sleds first.
    let inventory = inventory_or_unavail(&ctx.mgs_handle).await?;

    let mut config = ctx.rss_config.lock().unwrap();
    config.update_with_inventory_and_bootstrap_peers(
        &inventory,
        &ctx.bootstrap_peers,
    );
    config
        .update(body.into_inner(), ctx.baseboard.as_ref())
        .map_err(|err| HttpError::for_bad_request(None, err))?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CertificateUploadResponse {
    /// The key has been uploaded, but we're waiting on its corresponding
    /// certificate chain.
    WaitingOnCert,
    /// The cert chain has been uploaded, but we're waiting on its corresponding
    /// private key.
    WaitingOnKey,
    /// A cert chain and its key have been accepted.
    CertKeyAccepted,
    /// A cert chain and its key are valid, but have already been uploaded.
    CertKeyDuplicateIgnored,
}

/// Add an external certificate.
///
/// This must be paired with its private key. They may be posted in either
/// order, but one cannot post two certs in a row (or two keys in a row).
#[endpoint {
    method = POST,
    path = "/rack-setup/config/cert"
}]
async fn post_rss_config_cert(
    rqctx: RequestContext<ServerContext>,
    body: TypedBody<String>,
) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
    let ctx = rqctx.context();

    let mut config = ctx.rss_config.lock().unwrap();
    let response = config
        .push_cert(body.into_inner())
        .map_err(|err| HttpError::for_bad_request(None, err))?;

    Ok(HttpResponseOk(response))
}

/// Add the private key of an external certificate.
///
/// This must be paired with its certificate. They may be posted in either
/// order, but one cannot post two keys in a row (or two certs in a row).
#[endpoint {
    method = POST,
    path = "/rack-setup/config/key"
}]
async fn post_rss_config_key(
    rqctx: RequestContext<ServerContext>,
    body: TypedBody<String>,
) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
    let ctx = rqctx.context();

    let mut config = ctx.rss_config.lock().unwrap();
    let response = config
        .push_key(body.into_inner())
        .map_err(|err| HttpError::for_bad_request(None, err))?;

    Ok(HttpResponseOk(response))
}

// -- BGP authentication key management

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq)]
pub(crate) struct GetBgpAuthKeyParams {
    /// Checks that these keys are valid.
    check_valid: BTreeSet<BgpAuthKeyId>,
}

/// Return information about BGP authentication keys, including checking
/// validity of keys.
///
/// Produces an error if the rack setup config wasn't set, or if any of the
/// requested key IDs weren't found.
#[endpoint(
    method = GET,
    path = "/rack-setup/config/bgp/auth-key"
)]
async fn get_bgp_auth_key_info(
    rqctx: RequestContext<ServerContext>,
    // A bit weird for a GET request to have a TypedBody, but there's no other
    // nice way to transmit this information as a batch.
    params: TypedBody<GetBgpAuthKeyParams>,
) -> Result<HttpResponseOk<GetBgpAuthKeyInfoResponse>, HttpError> {
    let ctx = rqctx.context();
    let params = params.into_inner();

    let config = ctx.rss_config.lock().unwrap();
    config
        .check_bgp_auth_keys_valid(&params.check_valid)
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;
    let data = config.get_bgp_auth_key_data();

    Ok(HttpResponseOk(GetBgpAuthKeyInfoResponse { data }))
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
struct PutBgpAuthKeyParams {
    key_id: BgpAuthKeyId,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq)]
struct PutBgpAuthKeyBody {
    key: BgpAuthKey,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
struct PutBgpAuthKeyResponse {
    status: SetBgpAuthKeyStatus,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SetBgpAuthKeyStatus {
    /// The key was accepted and replaced an old key.
    Replaced,

    /// The key was accepted, and is the same as the existing key.
    Unchanged,

    /// The key was accepted and is new.
    Added,
}

/// Set the BGP authentication key for a particular key ID.
#[endpoint {
    method = PUT,
    path = "/rack-setup/config/bgp/auth-key/{key_id}"
}]
async fn put_bgp_auth_key(
    rqctx: RequestContext<ServerContext>,
    params: Path<PutBgpAuthKeyParams>,
    body: TypedBody<PutBgpAuthKeyBody>,
) -> Result<HttpResponseOk<PutBgpAuthKeyResponse>, HttpError> {
    let ctx = rqctx.context();
    let params = params.into_inner();

    let mut config = ctx.rss_config.lock().unwrap();
    let status = config
        .set_bgp_auth_key(params.key_id, body.into_inner().key)
        .map_err(|err| HttpError::for_bad_request(None, err.to_string()))?;

    Ok(HttpResponseOk(PutBgpAuthKeyResponse { status }))
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PutRssRecoveryUserPasswordHash {
    pub hash: omicron_passwords::NewPasswordHash,
}

/// Update the RSS config recovery silo user password hash.
#[endpoint {
    method = PUT,
    path = "/rack-setup/config/recovery-user-password-hash"
}]
async fn put_rss_config_recovery_user_password_hash(
    rqctx: RequestContext<ServerContext>,
    body: TypedBody<PutRssRecoveryUserPasswordHash>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ctx = rqctx.context();

    let mut config = ctx.rss_config.lock().unwrap();
    config.set_recovery_user_password_hash(body.into_inner().hash);

    Ok(HttpResponseUpdatedNoContent())
}

/// Reset all RSS configuration to their default values.
#[endpoint {
    method = DELETE,
    path = "/rack-setup/config"
}]
async fn delete_rss_config(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ctx = rqctx.context();

    let mut config = ctx.rss_config.lock().unwrap();
    *config = Default::default();

    Ok(HttpResponseUpdatedNoContent())
}

/// Query current state of rack setup.
#[endpoint {
    method = GET,
    path = "/rack-setup"
}]
async fn get_rack_setup_state(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<RackOperationStatus>, HttpError> {
    let ctx = rqctx.context();

    let sled_agent_addr = ctx
        .bootstrap_agent_addr()
        .map_err(|err| HttpError::for_bad_request(None, format!("{err:#}")))?;

    let client = bootstrap_agent_client::Client::new(
        &format!("http://{}", sled_agent_addr),
        ctx.log.new(slog::o!("component" => "bootstrap client")),
    );

    let op_status = client
        .rack_initialization_status()
        .await
        .map_err(|err| {
            use bootstrap_agent_client::Error as BaError;
            match err {
                BaError::CommunicationError(err) => {
                    let message =
                        format!("Failed to send rack setup request: {err}");
                    HttpError {
                        status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                        error_code: None,
                        external_message: message.clone(),
                        internal_message: message,
                    }
                }
                other => HttpError::for_bad_request(
                    None,
                    format!("Rack setup request failed: {other}"),
                ),
            }
        })?
        .into_inner();

    Ok(HttpResponseOk(op_status))
}

/// Run rack setup.
///
/// Will return an error if not all of the rack setup configuration has been
/// populated.
#[endpoint {
    method = POST,
    path = "/rack-setup"
}]
async fn post_run_rack_setup(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<RackInitId>, HttpError> {
    let ctx = rqctx.context();
    let log = &rqctx.log;

    let sled_agent_addr = ctx
        .bootstrap_agent_addr()
        .map_err(|err| HttpError::for_bad_request(None, format!("{err:#}")))?;

    let request = {
        let mut config = ctx.rss_config.lock().unwrap();
        config.start_rss_request(&ctx.bootstrap_peers, log).map_err(|err| {
            HttpError::for_bad_request(None, format!("{err:#}"))
        })?
    };

    slog::info!(
        ctx.log,
        "Sending RSS initialize request to {}",
        sled_agent_addr
    );
    let client = bootstrap_agent_client::Client::new(
        &format!("http://{}", sled_agent_addr),
        ctx.log.new(slog::o!("component" => "bootstrap client")),
    );

    let init_id = client
        .rack_initialize(&request)
        .await
        .map_err(|err| {
            use bootstrap_agent_client::Error as BaError;
            match err {
                BaError::CommunicationError(err) => {
                    let message =
                        format!("Failed to send rack setup request: {err}");
                    HttpError {
                        status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                        error_code: None,
                        external_message: message.clone(),
                        internal_message: message,
                    }
                }
                other => HttpError::for_bad_request(
                    None,
                    format!("Rack setup request failed: {other}"),
                ),
            }
        })?
        .into_inner();

    Ok(HttpResponseOk(init_id))
}

/// Run rack reset.
#[endpoint {
    method = DELETE,
    path = "/rack-setup"
}]
async fn post_run_rack_reset(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<RackResetId>, HttpError> {
    let ctx = rqctx.context();

    let sled_agent_addr = ctx
        .bootstrap_agent_addr()
        .map_err(|err| HttpError::for_bad_request(None, format!("{err:#}")))?;

    slog::info!(ctx.log, "Sending RSS reset request to {}", sled_agent_addr);
    let client = bootstrap_agent_client::Client::new(
        &format!("http://{}", sled_agent_addr),
        ctx.log.new(slog::o!("component" => "bootstrap client")),
    );

    let reset_id = client
        .rack_reset()
        .await
        .map_err(|err| {
            use bootstrap_agent_client::Error as BaError;
            match err {
                BaError::CommunicationError(err) => {
                    let message =
                        format!("Failed to send rack reset request: {err}");
                    HttpError {
                        status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                        error_code: None,
                        external_message: message.clone(),
                        internal_message: message,
                    }
                }
                other => HttpError::for_bad_request(
                    None,
                    format!("Rack setup request failed: {other}"),
                ),
            }
        })?
        .into_inner();

    Ok(HttpResponseOk(reset_id))
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct GetInventoryParams {
    /// If true, refresh the state of these SPs from MGS prior to returning
    /// (instead of returning cached data).
    pub force_refresh: Vec<SpIdentifier>,
}

/// A status endpoint used to report high level information known to wicketd.
///
/// This endpoint can be polled to see if there have been state changes in the
/// system that are useful to report to wicket.
///
/// Wicket, and possibly other callers, will retrieve the changed information,
/// with follow up calls.
#[endpoint {
    method = GET,
    path = "/inventory"
}]
async fn get_inventory(
    rqctx: RequestContext<ServerContext>,
    body_params: TypedBody<GetInventoryParams>,
) -> Result<HttpResponseOk<GetInventoryResponse>, HttpError> {
    let GetInventoryParams { force_refresh } = body_params.into_inner();
    match rqctx
        .context()
        .mgs_handle
        .get_inventory_refreshing_sps(force_refresh)
        .await
    {
        Ok(response) => Ok(HttpResponseOk(response)),
        Err(GetInventoryError::InvalidSpIdentifier) => {
            Err(HttpError::for_unavail(
                None,
                "Invalid SP identifier in request".into(),
            ))
        }
        Err(GetInventoryError::ShutdownInProgress) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
    }
}

/// Upload a TUF repository to the server.
///
/// At any given time, wicketd will keep at most one TUF repository in memory.
/// Any previously-uploaded repositories will be discarded.
#[endpoint {
    method = PUT,
    path = "/repository",
}]
async fn put_repository(
    rqctx: RequestContext<ServerContext>,
    body: StreamingBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let rqctx = rqctx.context();

    rqctx.update_tracker.put_repository(body.into_stream()).await?;

    Ok(HttpResponseUpdatedNoContent())
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct InstallableArtifacts {
    pub artifact_id: ArtifactId,
    pub installable: Vec<ArtifactHashId>,
}

/// The response to a `get_artifacts` call: the system version, and the list of
/// all artifacts currently held by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetArtifactsAndEventReportsResponse {
    pub system_version: Option<SemverVersion>,

    /// Map of artifacts we ingested from the most-recently-uploaded TUF
    /// repository to a list of artifacts we're serving over the bootstrap
    /// network. In some cases the list of artifacts being served will have
    /// length 1 (when we're serving the artifact directly); in other cases the
    /// artifact in the TUF repo contains multiple nested artifacts inside it
    /// (e.g., RoT artifacts contain both A and B images), and we serve the list
    /// of extracted artifacts but not the original combination.
    ///
    /// Conceptually, this is a `BTreeMap<ArtifactId, Vec<ArtifactHashId>>`, but
    /// JSON requires string keys for maps, so we give back a vec of pairs
    /// instead.
    pub artifacts: Vec<InstallableArtifacts>,

    pub event_reports: BTreeMap<SpType, BTreeMap<u32, EventReport>>,
}

/// An endpoint used to report all available artifacts and event reports.
///
/// The order of the returned artifacts is unspecified, and may change between
/// calls even if the total set of artifacts has not.
#[endpoint {
    method = GET,
    path = "/artifacts-and-event-reports",
}]
async fn get_artifacts_and_event_reports(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<GetArtifactsAndEventReportsResponse>, HttpError> {
    let response =
        rqctx.context().update_tracker.artifacts_and_event_reports().await;
    Ok(HttpResponseOk(response))
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub(crate) struct StartUpdateParams {
    /// The SP identifiers to start the update with. Must be non-empty.
    pub(crate) targets: BTreeSet<SpIdentifier>,

    /// Options for the update.
    pub(crate) options: StartUpdateOptions,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub(crate) struct StartUpdateOptions {
    /// If passed in, fails the update with a simulated error.
    pub(crate) test_error: Option<UpdateTestError>,

    /// If passed in, creates a test step that lasts these many seconds long.
    ///
    /// This is used for testing.
    pub(crate) test_step_seconds: Option<u64>,

    /// If passed in, simulates a result for the RoT Bootloader update.
    ///
    /// This is used for testing.
    pub(crate) test_simulate_rot_bootloader_result:
        Option<UpdateSimulatedResult>,

    /// If passed in, simulates a result for the RoT update.
    ///
    /// This is used for testing.
    pub(crate) test_simulate_rot_result: Option<UpdateSimulatedResult>,

    /// If passed in, simulates a result for the SP update.
    ///
    /// This is used for testing.
    pub(crate) test_simulate_sp_result: Option<UpdateSimulatedResult>,

    /// If true, skip the check on the current RoT version and always update it
    /// regardless of whether the update appears to be neeeded.
    pub(crate) skip_rot_bootloader_version_check: bool,

    /// If true, skip the check on the current RoT version and always update it
    /// regardless of whether the update appears to be neeeded.
    pub(crate) skip_rot_version_check: bool,

    /// If true, skip the check on the current SP version and always update it
    /// regardless of whether the update appears to be neeeded.
    pub(crate) skip_sp_version_check: bool,
}

/// A simulated result for a component update.
///
/// Used by [`StartUpdateOptions`].
#[derive(Clone, Debug, JsonSchema, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UpdateSimulatedResult {
    Success,
    Warning,
    Skipped,
    Failure,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub(crate) struct ClearUpdateStateParams {
    /// The SP identifiers to clear the update state for. Must be non-empty.
    pub(crate) targets: BTreeSet<SpIdentifier>,

    /// Options for clearing update state
    pub(crate) options: ClearUpdateStateOptions,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub(crate) struct ClearUpdateStateOptions {
    /// If passed in, fails the clear update state operation with a simulated
    /// error.
    pub(crate) test_error: Option<UpdateTestError>,
}

#[derive(Clone, Debug, Default, JsonSchema, Serialize)]
pub(crate) struct ClearUpdateStateResponse {
    /// The SPs for which update data was cleared.
    pub(crate) cleared: BTreeSet<SpIdentifier>,

    /// The SPs that had no update state to clear.
    pub(crate) no_update_data: BTreeSet<SpIdentifier>,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub(crate) struct AbortUpdateOptions {
    /// The message to abort the update with.
    pub(crate) message: String,

    /// If passed in, fails the force cancel update operation with a simulated
    /// error.
    pub(crate) test_error: Option<UpdateTestError>,
}

#[derive(Copy, Clone, Debug, JsonSchema, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind", content = "content")]
pub(crate) enum UpdateTestError {
    /// Simulate an error where the operation fails to complete.
    Fail,

    /// Simulate an issue where the operation times out.
    Timeout {
        /// The number of seconds to time out after.
        secs: u64,
    },
}

impl UpdateTestError {
    pub(crate) async fn into_http_error(
        self,
        log: &slog::Logger,
        reason: &str,
    ) -> HttpError {
        let message = self.into_error_string(log, reason).await;
        HttpError::for_bad_request(None, message)
    }

    pub(crate) async fn into_error_string(
        self,
        log: &slog::Logger,
        reason: &str,
    ) -> String {
        match self {
            UpdateTestError::Fail => {
                format!("Simulated failure while {reason}")
            }
            UpdateTestError::Timeout { secs } => {
                slog::info!(log, "Simulating timeout while {reason}");
                // 15 seconds should be enough to cause a timeout.
                tokio::time::sleep(Duration::from_secs(secs)).await;
                "XXX request should time out before this is hit".into()
            }
        }
    }
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetBaseboardResponse {
    pub baseboard: Option<Baseboard>,
}

/// Report the configured baseboard details
#[endpoint {
    method = GET,
    path = "/baseboard",
}]
async fn get_baseboard(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<GetBaseboardResponse>, HttpError> {
    let rqctx = rqctx.context();
    Ok(HttpResponseOk(GetBaseboardResponse {
        baseboard: rqctx.baseboard.clone(),
    }))
}

/// All the fields of this response are optional, because it's possible we don't
/// know any of them (yet) if MGS has not yet finished discovering its location
/// or (ever) if we're running in a dev environment that doesn't support
/// MGS-location / baseboard mapping.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetLocationResponse {
    /// The identity of our sled (where wicketd is running).
    pub sled_id: Option<SpIdentifier>,
    /// The baseboard of our sled (where wicketd is running).
    pub sled_baseboard: Option<Baseboard>,
    /// The baseboard of the switch our sled is physically connected to.
    pub switch_baseboard: Option<Baseboard>,
    /// The identity of the switch our sled is physically connected to.
    pub switch_id: Option<SpIdentifier>,
}

/// Report the identity of the sled and switch we're currently running on /
/// connected to.
#[endpoint {
    method = GET,
    path = "/location",
}]
async fn get_location(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<GetLocationResponse>, HttpError> {
    let rqctx = rqctx.context();
    let inventory = inventory_or_unavail(&rqctx.mgs_handle).await?;

    let switch_id = rqctx.local_switch_id().await;
    let sled_baseboard = rqctx.baseboard.clone();

    let mut switch_baseboard = None;
    let mut sled_id = None;

    for sp in &inventory.sps {
        if Some(sp.id) == switch_id {
            switch_baseboard = sp.state.as_ref().map(|state| {
                // TODO-correctness `new_gimlet` isn't the right name: this is a
                // sidecar baseboard.
                Baseboard::new_gimlet(
                    state.serial_number.clone(),
                    state.model.clone(),
                    i64::from(state.revision),
                )
            });
        } else if let (Some(sled_baseboard), Some(state)) =
            (sled_baseboard.as_ref(), sp.state.as_ref())
        {
            if sled_baseboard.identifier() == state.serial_number
                && sled_baseboard.model() == state.model
                && sled_baseboard.revision() == i64::from(state.revision)
            {
                sled_id = Some(sp.id);
            }
        }
    }

    Ok(HttpResponseOk(GetLocationResponse {
        sled_id,
        sled_baseboard,
        switch_baseboard,
        switch_id,
    }))
}

/// An endpoint to start updating one or more sleds, switches and PSCs.
#[endpoint {
    method = POST,
    path = "/update",
}]
async fn post_start_update(
    rqctx: RequestContext<ServerContext>,
    params: TypedBody<StartUpdateParams>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let log = &rqctx.log;
    let rqctx = rqctx.context();
    let params = params.into_inner();

    if params.targets.is_empty() {
        return Err(HttpError::for_bad_request(
            None,
            "No update targets specified".into(),
        ));
    }

    // Can we update the target SPs? We refuse to update if, for any target SP:
    //
    // 1. We haven't pulled its state in our inventory (most likely cause: the
    //    cubby is empty; less likely cause: the SP is misbehaving, which will
    //    make updating it very unlikely to work anyway)
    // 2. We have pulled its state but our hardware manager says we can't
    //    update it (most likely cause: the target is the sled we're currently
    //    running on; less likely cause: our hardware manager failed to get our
    //    local identifying information, and it refuses to update this target
    //    out of an abundance of caution).
    //
    // First, get our most-recently-cached inventory view. (Only wait 80% of
    // WICKETD_TIMEOUT for this: if even a cached inventory isn't available,
    // it's because we've never established contact with MGS. In that case, we
    // should produce a useful error message rather than timing out on the
    // client.)
    let inventory = match tokio::time::timeout(
        WICKETD_TIMEOUT.mul_f32(0.8),
        rqctx.mgs_handle.get_cached_inventory(),
    )
    .await
    {
        Ok(Ok(inventory)) => inventory,
        Ok(Err(ShutdownInProgress)) => {
            return Err(HttpError::for_unavail(
                None,
                "Server is shutting down".into(),
            ));
        }
        Err(_) => {
            // Have to construct an HttpError manually because
            // HttpError::for_unavail doesn't accept an external message.
            let message =
                "Rack inventory not yet available (is MGS alive?)".to_owned();
            return Err(HttpError {
                status_code: http::StatusCode::SERVICE_UNAVAILABLE,
                error_code: None,
                external_message: message.clone(),
                internal_message: message,
            });
        }
    };

    // Error cases.
    let mut inventory_absent = BTreeSet::new();
    let mut self_update = None;
    let mut maybe_self_update = BTreeSet::new();

    // Next, do we have the states of the target SP?
    let sp_states = match inventory {
        GetInventoryResponse::Response { inventory, .. } => inventory
            .sps
            .into_iter()
            .filter_map(|sp| {
                if params.targets.contains(&sp.id) {
                    if let Some(sp_state) = sp.state {
                        Some((sp.id, sp_state))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect(),
        GetInventoryResponse::Unavailable => BTreeMap::new(),
    };

    for target in &params.targets {
        let sp_state = match sp_states.get(target) {
            Some(sp_state) => sp_state,
            None => {
                // The state isn't present, so add to inventory_absent.
                inventory_absent.insert(*target);
                continue;
            }
        };

        // If we have the state of the SP, are we allowed to update it? We
        // refuse to try to update our own sled.
        match &rqctx.baseboard {
            Some(baseboard) => {
                if baseboard.identifier() == sp_state.serial_number
                    && baseboard.model() == sp_state.model
                    && baseboard.revision() == i64::from(sp_state.revision)
                {
                    self_update = Some(*target);
                    continue;
                }
            }
            None => {
                // We don't know our own baseboard, which is a very questionable
                // state to be in! For now, we will hard-code the possibly
                // locations where we could be running: scrimlets can only be in
                // cubbies 14 or 16, so we refuse to update either of those.
                let target_is_scrimlet = matches!(
                    (target.type_, target.slot),
                    (SpType::Sled, 14 | 16)
                );
                if target_is_scrimlet {
                    maybe_self_update.insert(*target);
                    continue;
                }
            }
        }
    }

    // Do we have any errors?
    let mut errors = Vec::new();
    if !inventory_absent.is_empty() {
        errors.push(format!(
            "cannot update sleds (no inventory state present for {})",
            sps_to_string(&inventory_absent)
        ));
    }
    if let Some(self_update) = self_update {
        errors.push(format!(
            "cannot update sled where wicketd is running ({})",
            SpIdentifierDisplay(self_update)
        ));
    }
    if !maybe_self_update.is_empty() {
        errors.push(format!(
            "wicketd does not know its own baseboard details: \
             refusing to update either scrimlet ({})",
            sps_to_string(&inventory_absent)
        ));
    }

    if let Some(test_error) = &params.options.test_error {
        errors.push(test_error.into_error_string(log, "starting update").await);
    }

    let start_update_errors = if errors.is_empty() {
        // No errors: we can try and proceed with this update.
        match rqctx.update_tracker.start(params.targets, params.options).await {
            Ok(()) => return Ok(HttpResponseUpdatedNoContent {}),
            Err(errors) => errors,
        }
    } else {
        // We've already found errors, so all we want to do is to check whether
        // the update tracker thinks there are any errors as well.
        match rqctx.update_tracker.update_pre_checks(params.targets).await {
            Ok(()) => Vec::new(),
            Err(errors) => errors,
        }
    };

    errors.extend(start_update_errors.iter().map(|error| error.to_string()));

    // If we get here, we have errors to report.

    match errors.len() {
        0 => {
            unreachable!(
                "we already returned Ok(_) above if there were no errors"
            )
        }
        1 => {
            return Err(HttpError::for_bad_request(
                None,
                errors.pop().unwrap(),
            ));
        }
        _ => {
            return Err(HttpError::for_bad_request(
                None,
                format!(
                    "multiple errors encountered:\n - {}",
                    itertools::join(errors, "\n - ")
                ),
            ));
        }
    }
}

/// An endpoint to get the status of any update being performed or recently
/// completed on a single SP.
#[endpoint {
    method = GET,
    path = "/update/{type}/{slot}",
}]
async fn get_update_sp(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpIdentifier>,
) -> Result<HttpResponseOk<EventReport>, HttpError> {
    let event_report =
        rqctx.context().update_tracker.event_report(target.into_inner()).await;
    Ok(HttpResponseOk(event_report))
}

/// Forcibly cancels a running update.
///
/// This is a potentially dangerous operation, but one that is sometimes
/// required. A machine reset might be required after this operation completes.
#[endpoint {
    method = POST,
    path = "/abort-update/{type}/{slot}",
}]
async fn post_abort_update(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpIdentifier>,
    opts: TypedBody<AbortUpdateOptions>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let log = &rqctx.log;
    let target = target.into_inner();

    let opts = opts.into_inner();
    if let Some(test_error) = opts.test_error {
        return Err(test_error.into_http_error(log, "aborting update").await);
    }

    match rqctx
        .context()
        .update_tracker
        .abort_update(target, opts.message)
        .await
    {
        Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
        Err(err) => Err(err.to_http_error()),
    }
}

/// Resets update state for a sled.
///
/// Use this to clear update state after a failed update.
#[endpoint {
    method = POST,
    path = "/clear-update-state",
}]
async fn post_clear_update_state(
    rqctx: RequestContext<ServerContext>,
    params: TypedBody<ClearUpdateStateParams>,
) -> Result<HttpResponseOk<ClearUpdateStateResponse>, HttpError> {
    let log = &rqctx.log;
    let rqctx = rqctx.context();
    let params = params.into_inner();

    if params.targets.is_empty() {
        return Err(HttpError::for_bad_request(
            None,
            "No targets specified".into(),
        ));
    }

    if let Some(test_error) = params.options.test_error {
        return Err(test_error
            .into_http_error(log, "clearing update state")
            .await);
    }

    match rqctx.update_tracker.clear_update_state(params.targets).await {
        Ok(response) => Ok(HttpResponseOk(response)),
        Err(err) => Err(err.to_http_error()),
    }
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct PathSpIgnitionCommand {
    #[serde(rename = "type")]
    type_: SpType,
    slot: u32,
    command: IgnitionCommand,
}

/// Send an ignition command targeting a specific SP.
///
/// This endpoint acts as a proxy to the MGS endpoint performing the same
/// function, allowing wicket to communicate exclusively with wicketd (even
/// though wicketd adds no meaningful functionality here beyond what MGS
/// offers).
#[endpoint {
    method = POST,
    path = "/ignition/{type}/{slot}/{command}",
}]
async fn post_ignition_command(
    rqctx: RequestContext<ServerContext>,
    path: Path<PathSpIgnitionCommand>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let PathSpIgnitionCommand { type_, slot, command } = path.into_inner();

    apictx
        .mgs_client
        .ignition_command(type_, slot, command)
        .await
        .map_err(http_error_from_client_error)?;

    Ok(HttpResponseUpdatedNoContent())
}

/// Options provided to the preflight uplink check.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PreflightUplinkCheckOptions {
    /// DNS name to query.
    pub dns_name_to_query: Option<String>,
}

/// An endpoint to start a preflight check for uplink configuration.
#[endpoint {
    method = POST,
    path = "/preflight/uplink",
}]
async fn post_start_preflight_uplink_check(
    rqctx: RequestContext<ServerContext>,
    body: TypedBody<PreflightUplinkCheckOptions>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let rqctx = rqctx.context();
    let options = body.into_inner();

    let our_switch_location = match rqctx.local_switch_id().await {
        Some(SpIdentifier { slot, type_: SpType::Switch }) => match slot {
            0 => SwitchLocation::Switch0,
            1 => SwitchLocation::Switch1,
            _ => {
                return Err(HttpError::for_internal_error(format!(
                    "unexpected switch slot {slot}"
                )));
            }
        },
        Some(other) => {
            return Err(HttpError::for_internal_error(format!(
                "unexpected switch SP identifier {other:?}"
            )));
        }
        None => {
            return Err(HttpError::for_unavail(
                Some("UnknownSwitchLocation".to_string()),
                "local switch location not yet determined".to_string(),
            ));
        }
    };

    let (network_config, dns_servers, ntp_servers) = {
        let rss_config = rqctx.rss_config.lock().unwrap();

        let network_config = rss_config
            .user_specified_rack_network_config()
            .cloned()
            .ok_or_else(|| {
                HttpError::for_bad_request(
                    None,
                    "uplink preflight check requires setting \
                     the uplink config for RSS"
                        .to_string(),
                )
            })?;

        (
            network_config,
            rss_config.dns_servers().to_vec(),
            rss_config.ntp_servers().to_vec(),
        )
    };

    match rqctx
        .preflight_checker
        .uplink_start(
            network_config,
            dns_servers,
            ntp_servers,
            our_switch_location,
            options.dns_name_to_query,
        )
        .await
    {
        Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
        Err(err) => Err(HttpError::for_client_error(
            None,
            StatusCode::TOO_MANY_REQUESTS,
            err.to_string(),
        )),
    }
}

/// An endpoint to get the report for the most recent (or still running)
/// preflight uplink check.
#[endpoint {
    method = GET,
    path = "/preflight/uplink",
}]
async fn get_preflight_uplink_report(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<UplinkEventReport>, HttpError> {
    let rqctx = rqctx.context();

    match rqctx.preflight_checker.uplink_event_report() {
        Some(report) => Ok(HttpResponseOk(report)),
        None => Err(HttpError::for_bad_request(
            None,
            "no preflight uplink report available - have you started a check?"
                .to_string(),
        )),
    }
}

/// An endpoint instructing wicketd to reload its SMF config properties.
///
/// The only expected client of this endpoint is `curl` from wicketd's SMF
/// `refresh` method, but other clients hitting it is harmless.
#[endpoint {
    method = POST,
    path = "/reload-config",
}]
async fn post_reload_config(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let smf_values = SmfConfigValues::read_current().map_err(|err| {
        HttpError::for_unavail(
            None,
            format!("failed to read SMF values: {err}"),
        )
    })?;

    let rqctx = rqctx.context();

    // We do not allow a config reload to change our bound address; return an
    // error if the caller is attempting to do so.
    if rqctx.bind_address != smf_values.address {
        return Err(HttpError::for_bad_request(
            None,
            "listening address cannot be reconfigured".to_string(),
        ));
    }

    if let Some(rack_subnet) = smf_values.rack_subnet {
        let resolver = Resolver::new_from_subnet(
            rqctx.log.new(o!("component" => "InternalDnsResolver")),
            rack_subnet,
        )
        .map_err(|err| {
            HttpError::for_unavail(
                None,
                format!("failed to create internal DNS resolver: {err}"),
            )
        })?;

        *rqctx.internal_dns_resolver.lock().unwrap() = Some(resolver);
    }

    Ok(HttpResponseUpdatedNoContent())
}

fn http_error_from_client_error(
    err: gateway_client::Error<gateway_client::types::Error>,
) -> HttpError {
    // Most errors have a status code; the only one that definitely doesn't is
    // `Error::InvalidRequest`, for which we'll use `BAD_REQUEST`.
    let status_code = err.status().unwrap_or(StatusCode::BAD_REQUEST);

    let message = format!("request to MGS failed: {err}");

    HttpError {
        status_code,
        error_code: None,
        external_message: message.clone(),
        internal_message: message,
    }
}
