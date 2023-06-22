// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::mgs::GetInventoryError;
use crate::mgs::GetInventoryResponse;
use crate::mgs::MgsHandle;
use crate::RackV1Inventory;
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
use futures::TryStreamExt;
use gateway_client::types::IgnitionCommand;
use gateway_client::types::SpIdentifier;
use gateway_client::types::SpType;
use http::StatusCode;
use omicron_common::address;
use omicron_common::api::external::SemverVersion;
use omicron_common::api::internal::shared::RackNetworkConfig;
use omicron_common::update::ArtifactId;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_hardware::Baseboard;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use std::time::Duration;
use uuid::Uuid;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::update_events::EventReport;

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
        api.register(delete_rss_config)?;
        api.register(get_rack_setup_state)?;
        api.register(post_run_rack_setup)?;
        api.register(post_run_rack_reset)?;
        api.register(get_inventory)?;
        api.register(put_repository)?;
        api.register(get_artifacts_and_event_reports)?;
        api.register(get_baseboard)?;
        api.register(post_start_update)?;
        api.register(post_abort_update)?;
        api.register(post_clear_update_state)?;
        api.register(get_update_sp)?;
        api.register(post_ignition_command)?;
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
pub struct BootstrapSledDescription {
    pub id: SpIdentifier,
    pub baseboard: Baseboard,
    /// The sled's bootstrap address, if the host is on and we've discovered it
    /// on the bootstrap network.
    pub bootstrap_ip: Option<Ipv6Addr>,
}

// This is the subset of `RackInitializeRequest` that the user fills in in clear
// text (e.g., via an uploaded config file).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CurrentRssUserConfigInsensitive {
    pub bootstrap_sleds: BTreeSet<BootstrapSledDescription>,
    pub ntp_servers: Vec<String>,
    pub dns_servers: Vec<String>,
    pub internal_services_ip_pool_ranges: Vec<address::IpRange>,
    pub external_dns_zone_name: String,
    pub rack_network_config: Option<RackNetworkConfig>,
}

// This is a summary of the subset of `RackInitializeRequest` that is sensitive;
// we only report a summary instead of returning actual data.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CurrentRssUserConfigSensitive {
    pub num_external_certificates: usize,
    pub recovery_silo_password_set: bool,
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
    match mgs_handle.get_inventory(Vec::new()).await {
        Ok(GetInventoryResponse::Response { inventory, .. }) => Ok(inventory),
        Ok(GetInventoryResponse::Unavailable) => Err(HttpError::for_unavail(
            None,
            "Rack inventory not yet available".into(),
        )),
        Err(GetInventoryError::ShutdownInProgress) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
        Err(GetInventoryError::InvalidSpIdentifier) => {
            // We didn't provide any SP identifiers to refresh, so they can't be
            // invalid.
            unreachable!()
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
    config.populate_available_bootstrap_sleds_from_inventory(
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
    config.populate_available_bootstrap_sleds_from_inventory(
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
    body: TypedBody<Vec<u8>>,
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
    body: TypedBody<Vec<u8>>,
) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError> {
    let ctx = rqctx.context();

    let mut config = ctx.rss_config.lock().unwrap();
    let response = config
        .push_key(body.into_inner())
        .map_err(|err| HttpError::for_bad_request(None, err))?;

    Ok(HttpResponseOk(response))
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

    let sled_agent_addr = ctx
        .bootstrap_agent_addr()
        .map_err(|err| HttpError::for_bad_request(None, format!("{err:#}")))?;

    let request = {
        let config = ctx.rss_config.lock().unwrap();
        config.start_rss_request(&ctx.bootstrap_peers).map_err(|err| {
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
    match rqctx.context().mgs_handle.get_inventory(force_refresh).await {
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

    // TODO: do we need to return more information with the response?

    let bytes = body.into_stream().try_collect().await?;
    rqctx.update_tracker.put_repository(bytes).await?;

    Ok(HttpResponseUpdatedNoContent())
}

/// The response to a `get_artifacts` call: the system version, and the list of
/// all artifacts currently held by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetArtifactsAndEventReportsResponse {
    pub system_version: Option<SemverVersion>,
    pub artifacts: Vec<ArtifactId>,
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
pub(crate) struct StartUpdateOptions {
    /// If passed in, fails the update with a simulated error.
    pub(crate) test_error: Option<UpdateTestError>,

    /// If passed in, creates a test step that lasts these many seconds long.
    ///
    /// This is used for testing.
    pub(crate) test_step_seconds: Option<u64>,

    /// If true, skip the check on the current RoT version and always update it
    /// regardless of whether the update appears to be neeeded.
    #[allow(dead_code)] // TODO actually use this
    pub(crate) skip_rot_version_check: bool,

    /// If true, skip the check on the current SP version and always update it
    /// regardless of whether the update appears to be neeeded.
    pub(crate) skip_sp_version_check: bool,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub(crate) struct ClearUpdateStateOptions {
    /// If passed in, fails the clear update state operation with a simulated
    /// error.
    pub(crate) test_error: Option<UpdateTestError>,
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
        match self {
            UpdateTestError::Fail => HttpError::for_bad_request(
                None,
                format!("Simulated failure while {reason}"),
            ),
            UpdateTestError::Timeout { secs } => {
                slog::info!(log, "Simulating timeout while {reason}");
                // 15 seconds should be enough to cause a timeout.
                tokio::time::sleep(Duration::from_secs(secs)).await;
                HttpError::for_bad_request(
                    None,
                    "XXX request should time out before this is hit".into(),
                )
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

/// An endpoint to start updating a sled.
#[endpoint {
    method = POST,
    path = "/update/{type}/{slot}",
}]
async fn post_start_update(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpIdentifier>,
    opts: TypedBody<StartUpdateOptions>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let log = &rqctx.log;
    let rqctx = rqctx.context();
    let target = target.into_inner();

    // Can we update the target SP? We refuse to update if:
    //
    // 1. We haven't pulled its state in our inventory (most likely cause: the
    //    cubby is empty; less likely cause: the SP is misbehaving, which will
    //    make updating it very unlikely to work anyway)
    // 2. We have pulled its state but our hardware manager says we can't update
    //    it (most likely cause: the target is the sled we're currently running
    //    on; less likely cause: our hardware manager failed to get our local
    //    identifying information, and it refuses to update this target out of
    //    an abundance of caution).
    //
    // First, get our most-recently-cached inventory view.
    let inventory = match rqctx.mgs_handle.get_inventory(Vec::new()).await {
        Ok(inventory) => inventory,
        Err(GetInventoryError::ShutdownInProgress) => {
            return Err(HttpError::for_unavail(
                None,
                "Server is shutting down".into(),
            ));
        }
        // We didn't specify any SP ids to refresh, so this error is impossible.
        Err(GetInventoryError::InvalidSpIdentifier) => unreachable!(),
    };

    // Next, do we have the state of the target SP?
    let sp_state = match inventory {
        GetInventoryResponse::Response { inventory, .. } => inventory
            .sps
            .into_iter()
            .filter_map(|sp| if sp.id == target { sp.state } else { None })
            .next(),
        GetInventoryResponse::Unavailable => None,
    };
    let Some(sp_state) = sp_state else {
        return Err(HttpError::for_bad_request(
            None,
            "cannot update target sled (no inventory state present)"
                .into(),
        ));
    };

    // If we have the state of the SP, are we allowed to update it? We
    // refuse to try to update our own sled.
    match &rqctx.baseboard {
        Some(baseboard) => {
            if baseboard.identifier() == sp_state.serial_number
                && baseboard.model() == sp_state.model
                && baseboard.revision() == i64::from(sp_state.revision)
            {
                return Err(HttpError::for_bad_request(
                    None,
                    "cannot update sled where wicketd is running".into(),
                ));
            }
        }
        None => {
            // We don't know our own baseboard, which is a very
            // questionable state to be in! For now, we will hard-code
            // the possibly locations where we could be running:
            // scrimlets can only be in cubbies 14 or 16, so we refuse
            // to update either of those.
            let target_is_scrimlet =
                matches!((target.type_, target.slot), (SpType::Sled, 14 | 16));
            if target_is_scrimlet {
                return Err(HttpError::for_bad_request(
                    None,
                    "wicketd does not know its own baseboard details: \
                     refusing to update either scrimlet"
                        .into(),
                ));
            }
        }
    }

    let opts = opts.into_inner();
    if let Some(test_error) = opts.test_error {
        return Err(test_error.into_http_error(log, "starting update").await);
    }

    // All pre-flight update checks look OK: start the update.
    //
    // Generate an ID for this update; the update tracker will send it to the
    // sled as part of the InstallinatorImageId, and installinator will send it
    // back to our artifact server with its progress reports.
    let update_id = Uuid::new_v4();

    match rqctx.update_tracker.start(target, update_id, opts).await {
        Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
        Err(err) => Err(err.to_http_error()),
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
    path = "/clear-update-state/{type}/{slot}",
}]
async fn post_clear_update_state(
    rqctx: RequestContext<ServerContext>,
    target: Path<SpIdentifier>,
    opts: TypedBody<ClearUpdateStateOptions>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let log = &rqctx.log;
    let target = target.into_inner();

    let opts = opts.into_inner();
    if let Some(test_error) = opts.test_error {
        return Err(test_error
            .into_http_error(log, "clearing update state")
            .await);
    }

    match rqctx.context().update_tracker.clear_update_state(target).await {
        Ok(()) => Ok(HttpResponseUpdatedNoContent {}),
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
