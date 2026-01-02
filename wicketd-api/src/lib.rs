// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use bootstrap_agent_client::types::RackOperationStatus;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::StreamingBody;
use dropshot::TypedBody;
use gateway_client::types::IgnitionCommand;
use omicron_common::update::ArtifactId;
use omicron_uuid_kinds::RackInitUuid;
use omicron_uuid_kinds::RackResetUuid;
use schemars::JsonSchema;
use semver::Version;
use serde::Deserialize;
use serde::Serialize;
use sled_hardware_types::Baseboard;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use tufaceous_artifact::ArtifactHashId;
use wicket_common::inventory::RackV1Inventory;
use wicket_common::inventory::SpIdentifier;
use wicket_common::inventory::SpType;
use wicket_common::preflight_check;
use wicket_common::rack_setup::BgpAuthKey;
use wicket_common::rack_setup::BgpAuthKeyId;
use wicket_common::rack_setup::CurrentRssUserConfigInsensitive;
use wicket_common::rack_setup::GetBgpAuthKeyInfoResponse;
use wicket_common::rack_setup::PutRssUserConfigInsensitive;
use wicket_common::rack_update::AbortUpdateOptions;
use wicket_common::rack_update::ClearUpdateStateOptions;
use wicket_common::rack_update::ClearUpdateStateResponse;
use wicket_common::rack_update::StartUpdateOptions;
use wicket_common::update_events::EventReport;

/// Full release repositories are currently (Dec 2024) 1.8 GiB and are likely to
/// continue growing.
const PUT_REPOSITORY_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;

#[dropshot::api_description]
pub trait WicketdApi {
    type Context;

    /// Get wicketd's current view of all sleds visible on the bootstrap network.
    #[endpoint {
        method = GET,
        path = "/bootstrap-sleds"
    }]
    async fn get_bootstrap_sleds(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<BootstrapSledIps>, HttpError>;

    /// Get the current status of the user-provided (or system-default-provided, in
    /// some cases) RSS configuration.
    #[endpoint {
        method = GET,
        path = "/rack-setup/config"
    }]
    async fn get_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CurrentRssUserConfig>, HttpError>;

    /// Update (a subset of) the current RSS configuration.
    ///
    /// Sensitive values (certificates and password hash) are not set through
    /// this endpoint.
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config"
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
        path = "/rack-setup/config/cert"
    }]
    async fn post_rss_config_cert(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError>;

    /// Add the private key of an external certificate.
    ///
    /// This must be paired with its certificate. They may be posted in either
    /// order, but one cannot post two keys in a row (or two certs in a row).
    #[endpoint {
        method = POST,
        path = "/rack-setup/config/key"
    }]
    async fn post_rss_config_key(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<String>,
    ) -> Result<HttpResponseOk<CertificateUploadResponse>, HttpError>;

    // -- BGP authentication key management

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
        rqctx: RequestContext<Self::Context>,
        // A bit weird for a GET request to have a TypedBody, but there's no other
        // nice way to transmit this information as a batch.
        params: TypedBody<GetBgpAuthKeyParams>,
    ) -> Result<HttpResponseOk<GetBgpAuthKeyInfoResponse>, HttpError>;

    /// Set the BGP authentication key for a particular key ID.
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config/bgp/auth-key/{key_id}"
    }]
    async fn put_bgp_auth_key(
        rqctx: RequestContext<Self::Context>,
        params: Path<PutBgpAuthKeyParams>,
        body: TypedBody<PutBgpAuthKeyBody>,
    ) -> Result<HttpResponseOk<PutBgpAuthKeyResponse>, HttpError>;

    /// Update the RSS config recovery silo user password hash.
    #[endpoint {
        method = PUT,
        path = "/rack-setup/config/recovery-user-password-hash"
    }]
    async fn put_rss_config_recovery_user_password_hash(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PutRssRecoveryUserPasswordHash>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Reset all RSS configuration to their default values.
    #[endpoint {
        method = DELETE,
        path = "/rack-setup/config"
    }]
    async fn delete_rss_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Query current state of rack setup.
    #[endpoint {
        method = GET,
        path = "/rack-setup"
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
        path = "/rack-setup"
    }]
    async fn post_run_rack_setup(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackInitUuid>, HttpError>;

    /// Run rack reset.
    #[endpoint {
        method = DELETE,
        path = "/rack-setup"
    }]
    async fn post_run_rack_reset(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<RackResetUuid>, HttpError>;

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
        path = "/inventory"
    }]
    async fn get_inventory(
        rqctx: RequestContext<Self::Context>,
        body_params: TypedBody<GetInventoryParams>,
    ) -> Result<HttpResponseOk<GetInventoryResponse>, HttpError>;

    /// Upload a TUF repository to the server.
    ///
    /// At any given time, wicketd will keep at most one TUF repository in
    /// memory. Any previously-uploaded repositories will be discarded.
    #[endpoint {
        method = PUT,
        path = "/repository",
        request_body_max_bytes = PUT_REPOSITORY_MAX_BYTES,
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
    }]
    async fn get_artifacts_and_event_reports(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetArtifactsAndEventReportsResponse>, HttpError>;

    /// Report the configured baseboard details.
    #[endpoint {
        method = GET,
        path = "/baseboard",
    }]
    async fn get_baseboard(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetBaseboardResponse>, HttpError>;

    /// Report the identity of the sled and switch we're currently running on /
    /// connected to.
    #[endpoint {
        method = GET,
        path = "/location",
    }]
    async fn get_location(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<GetLocationResponse>, HttpError>;

    /// An endpoint to start updating one or more sleds, switches and PSCs.
    #[endpoint {
        method = POST,
        path = "/update",
    }]
    async fn post_start_update(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<StartUpdateParams>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// An endpoint to get the status of any update being performed or recently
    /// completed on a single SP.
    #[endpoint {
        method = GET,
        path = "/update/{type}/{slot}",
    }]
    async fn get_update_sp(
        rqctx: RequestContext<Self::Context>,
        target: Path<SpIdentifier>,
    ) -> Result<HttpResponseOk<EventReport>, HttpError>;

    /// Forcibly cancels a running update.
    ///
    /// This is a potentially dangerous operation, but one that is sometimes
    /// required. A machine reset might be required after this operation completes.
    #[endpoint {
        method = POST,
        path = "/abort-update/{type}/{slot}",
    }]
    async fn post_abort_update(
        rqctx: RequestContext<Self::Context>,
        target: Path<SpIdentifier>,
        opts: TypedBody<AbortUpdateOptions>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Resets update state for a sled.
    ///
    /// Use this to clear update state after a failed update.
    #[endpoint {
        method = POST,
        path = "/clear-update-state",
    }]
    async fn post_clear_update_state(
        rqctx: RequestContext<Self::Context>,
        params: TypedBody<ClearUpdateStateParams>,
    ) -> Result<HttpResponseOk<ClearUpdateStateResponse>, HttpError>;

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
        rqctx: RequestContext<Self::Context>,
        path: Path<PathSpIgnitionCommand>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Start a preflight check for uplink configuration.
    #[endpoint {
        method = POST,
        path = "/preflight/uplink",
    }]
    async fn post_start_preflight_uplink_check(
        rqctx: RequestContext<Self::Context>,
        body: TypedBody<PreflightUplinkCheckOptions>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// Get the report for the most recent (or still running) preflight uplink
    /// check.
    #[endpoint {
        method = GET,
        path = "/preflight/uplink",
    }]
    async fn get_preflight_uplink_report(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<preflight_check::EventReport>, HttpError>;

    /// Instruct wicketd to reload its SMF config properties.
    ///
    /// The only expected client of this endpoint is `curl` from wicketd's SMF
    /// `refresh` method, but other clients hitting it is harmless.
    #[endpoint {
        method = POST,
        path = "/reload-config",
    }]
    async fn post_reload_config(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;
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

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct GetBgpAuthKeyParams {
    /// Checks that these keys are valid.
    pub check_valid: BTreeSet<BgpAuthKeyId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PutBgpAuthKeyParams {
    pub key_id: BgpAuthKeyId,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PutBgpAuthKeyBody {
    pub key: BgpAuthKey,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct PutBgpAuthKeyResponse {
    pub status: SetBgpAuthKeyStatus,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SetBgpAuthKeyStatus {
    /// The key was accepted and replaced an old key.
    Replaced,

    /// The key was accepted, and is the same as the existing key.
    Unchanged,

    /// The key was accepted and is new.
    Added,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PutRssRecoveryUserPasswordHash {
    pub hash: omicron_passwords::NewPasswordHash,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct GetInventoryParams {
    /// Refresh the state of these SPs from MGS prior to returning (instead of
    /// returning cached data).
    pub force_refresh: Vec<SpIdentifier>,
}

/// The response to a `get_inventory` call: the inventory known to wicketd, or a
/// notification that data is unavailable.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum GetInventoryResponse {
    Response { inventory: RackV1Inventory },
    Unavailable,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct InstallableArtifacts {
    pub artifact_id: ArtifactId,
    pub installable: Vec<ArtifactHashId>,
    pub sign: Option<Vec<u8>>,
}

/// The response to a `get_artifacts` call: the system version, and the list of
/// all artifacts currently held by wicketd.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetArtifactsAndEventReportsResponse {
    pub system_version: Option<Version>,

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

    pub event_reports: BTreeMap<SpType, BTreeMap<u16, EventReport>>,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub struct StartUpdateParams {
    /// The SP identifiers to start the update with. Must be non-empty.
    pub targets: BTreeSet<SpIdentifier>,

    /// Options for the update.
    pub options: StartUpdateOptions,
}

#[derive(Clone, Debug, JsonSchema, Deserialize)]
pub struct ClearUpdateStateParams {
    /// The SP identifiers to clear the update state for. Must be non-empty.
    pub targets: BTreeSet<SpIdentifier>,

    /// Options for clearing update state
    pub options: ClearUpdateStateOptions,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct GetBaseboardResponse {
    pub baseboard: Option<Baseboard>,
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

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct PathSpIgnitionCommand {
    #[serde(rename = "type")]
    pub type_: SpType,
    pub slot: u16,
    pub command: IgnitionCommand,
}

/// Options provided to the preflight uplink check.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PreflightUplinkCheckOptions {
    /// DNS name to query.
    pub dns_name_to_query: Option<String>,
}
