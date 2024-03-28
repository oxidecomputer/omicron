// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! HTTP entrypoint functions for the gateway service

mod component_details;
mod conversions;

use self::component_details::SpComponentDetails;
use self::conversions::component_from_str;
use crate::error::SpCommsError;
use crate::http_err_with_message;
use crate::ServerContext;
use base64::Engine;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::TypedBody;
use dropshot::UntypedBody;
use dropshot::WebsocketEndpointResult;
use dropshot::WebsocketUpgrade;
use futures::TryFutureExt;
use gateway_messages::SpComponent;
use gateway_sp_comms::HostPhase2Provider;
use omicron_common::update::ArtifactHash;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct SpState {
    pub serial_number: String,
    pub model: String,
    pub revision: u32,
    pub hubris_archive_id: String,
    pub base_mac_address: [u8; 6],
    pub power_state: PowerState,
    pub rot: RotState,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RotState {
    Enabled {
        active: RotSlot,
        persistent_boot_preference: RotSlot,
        pending_persistent_boot_preference: Option<RotSlot>,
        transient_boot_preference: Option<RotSlot>,
        slot_a_sha3_256_digest: Option<String>,
        slot_b_sha3_256_digest: Option<String>,
    },
    CommunicationFailed {
        message: String,
    },
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "slot", rename_all = "snake_case")]
pub enum RotSlot {
    A,
    B,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct RotImageDetails {
    pub digest: String,
    pub version: ImageVersion,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct RotCmpa {
    pub base64_data: String,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "slot", rename_all = "snake_case")]
pub enum RotCfpaSlot {
    Active,
    Inactive,
    Scratch,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct GetCfpaParams {
    pub slot: RotCfpaSlot,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct RotCfpa {
    pub base64_data: String,
    pub slot: RotCfpaSlot,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct SpIgnitionInfo {
    pub id: SpIdentifier,
    pub details: SpIgnition,
}

/// State of an ignition target.
///
/// TODO: Ignition returns much more information than we're reporting here: do
/// we want to expand this?
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "present")]
pub enum SpIgnition {
    #[serde(rename = "no")]
    Absent,
    #[serde(rename = "yes")]
    Present {
        id: SpIgnitionSystemType,
        power: bool,
        ctrl_detect_0: bool,
        ctrl_detect_1: bool,
        flt_a3: bool,
        flt_a2: bool,
        flt_rot: bool,
        flt_sp: bool,
    },
}

/// Ignition command.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum IgnitionCommand {
    PowerOn,
    PowerOff,
    PowerReset,
}

/// TODO: Do we want to bake in specific board names, or use raw u16 ID numbers?
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "system_type", rename_all = "snake_case")]
pub enum SpIgnitionSystemType {
    Gimlet,
    Sidecar,
    Psc,
    Unknown { id: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
enum SpUpdateStatus {
    /// The SP has no update status.
    None,
    /// The SP is preparing to receive an update.
    ///
    /// May or may not include progress, depending on the capabilities of the
    /// component being updated.
    Preparing { id: Uuid, progress: Option<UpdatePreparationProgress> },
    /// The SP is currently receiving an update.
    InProgress { id: Uuid, bytes_received: u32, total_bytes: u32 },
    /// The SP has completed receiving an update.
    Complete { id: Uuid },
    /// The SP has aborted an in-progress update.
    Aborted { id: Uuid },
    /// The update process failed.
    Failed { id: Uuid, code: u32 },
    /// The update process failed with an RoT-specific error.
    RotError { id: Uuid, message: String },
}

/// Progress of an SP preparing to update.
///
/// The units of `current` and `total` are unspecified and defined by the SP;
/// e.g., if preparing for an update requires erasing a flash device, this may
/// indicate progress of that erasure without defining units (bytes, pages,
/// sectors, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
struct UpdatePreparationProgress {
    current: u32,
    total: u32,
}

/// Result of reading an SP sensor.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct SpSensorReading {
    /// SP-centric timestamp of when `result` was recorded from this sensor.
    ///
    /// Currently this value represents "milliseconds since the last SP boot"
    /// and is primarily useful as a delta between sensors on this SP (assuming
    /// no reboot in between). The meaning could change with future SP releases.
    pub timestamp: u64,
    /// Value (or error) from the sensor.
    pub result: SpSensorReadingResult,
}

/// Single reading (or error) from an SP sensor.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpSensorReadingResult {
    Success { value: f32 },
    DeviceOff,
    DeviceError,
    DeviceNotPresent,
    DeviceUnavailable,
    DeviceTimeout,
}

/// List of components from a single SP.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpComponentList {
    pub components: Vec<SpComponentInfo>,
}

/// Overview of a single SP component.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpComponentInfo {
    /// The unique identifier for this component.
    pub component: String,
    /// The name of the physical device.
    pub device: String,
    /// The component's serial number, if it has one.
    pub serial_number: Option<String>,
    /// A human-readable description of the component.
    pub description: String,
    /// `capabilities` is a bitmask; interpret it via
    /// [`gateway_messages::DeviceCapabilities`].
    pub capabilities: u32,
    /// Whether or not the component is present, to the best of the SP's ability
    /// to judge.
    pub presence: SpComponentPresence,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
/// Description of the presence or absence of a component.
///
/// The presence of some components may vary based on the power state of the
/// sled (e.g., components that time out or appear unavailable if the sled is in
/// A2 may become present when the sled moves to A0).
pub enum SpComponentPresence {
    /// The component is present.
    Present,
    /// The component is not present.
    NotPresent,
    /// The component is present but in a failed or faulty state.
    Failed,
    /// The SP is unable to determine the presence of the component.
    Unavailable,
    /// The SP's attempt to determine the presence of the component timed out.
    Timeout,
    /// The SP's attempt to determine the presence of the component failed.
    Error,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum SpType {
    Sled,
    Power,
    Switch,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct SpIdentifier {
    #[serde(rename = "type")]
    pub typ: SpType,
    #[serde(deserialize_with = "deserializer_u32_from_string")]
    pub slot: u32,
}

impl Display for SpIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} {}", self.typ, self.slot)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct HostStartupOptions {
    pub phase2_recovery_mode: bool,
    pub kbm: bool,
    pub bootrd: bool,
    pub prom: bool,
    pub kmdb: bool,
    pub kmdb_boot: bool,
    pub boot_ramdisk: bool,
    pub boot_net: bool,
    pub verbose: bool,
}

/// See RFD 81.
///
/// This enum only lists power states the SP is able to control; higher power
/// states are controlled by ignition.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub enum PowerState {
    A0,
    A1,
    A2,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
pub struct ImageVersion {
    pub epoch: u32,
    pub version: u32,
}

// This type is a duplicate of the type in `ipcc`, and we provide a
// `From<_>` impl to convert to it. We keep these types distinct to allow us to
// choose different representations for MGS's HTTP API (this type) and the wire
// format passed through the SP to installinator
// (`ipcc::InstallinatorImageId`), although _currently_ they happen to
// be defined identically.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct InstallinatorImageId {
    pub update_id: Uuid,
    pub host_phase_2: ArtifactHash,
    pub control_plane: ArtifactHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "progress", rename_all = "snake_case")]
pub enum HostPhase2Progress {
    Available {
        image_id: HostPhase2RecoveryImageId,
        offset: u64,
        total_size: u64,
        age: Duration,
    },
    None,
}

/// Identifier for an SP's component's firmware slot; e.g., slots 0 and 1 for
/// the host boot flash.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct SpComponentFirmwareSlot {
    pub slot: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct SpComponentCaboose {
    pub git_commit: String,
    pub board: String,
    pub name: String,
    pub version: String,
}

/// Identity of a host phase2 recovery image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HostPhase2RecoveryImageId {
    pub sha256_hash: ArtifactHash,
}

// We can't use the default `Deserialize` derivation for `SpIdentifier::slot`
// because it's embedded in other structs via `serde(flatten)`, which does not
// play well with the way dropshot parses HTTP queries/paths. serde ends up
// trying to deserialize the flattened struct as a map of strings to strings,
// which breaks on `slot` (but not on `typ` for reasons I don't entirely
// understand). We can work around by using an enum that allows either `String`
// or `u32` (which gets us past the serde map of strings), and then parsing the
// string into a u32 ourselves (which gets us to the `slot` we want). More
// background: https://github.com/serde-rs/serde/issues/1346
fn deserializer_u32_from_string<'de, D>(
    deserializer: D,
) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Unexpected};

    #[derive(Debug, Deserialize)]
    #[serde(untagged)]
    enum StringOrU32 {
        String(String),
        U32(u32),
    }

    match StringOrU32::deserialize(deserializer)? {
        StringOrU32::String(s) => s
            .parse()
            .map_err(|_| de::Error::invalid_type(Unexpected::Str(&s), &"u32")),
        StringOrU32::U32(n) => Ok(n),
    }
}

#[derive(Deserialize, JsonSchema)]
struct PathSp {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    sp: SpIdentifier,
}

#[derive(Deserialize, JsonSchema)]
struct PathSpSensorId {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    sp: SpIdentifier,
    /// ID for the sensor on the SP.
    sensor_id: u32,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct PathSpComponent {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    component: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct PathSpIgnitionCommand {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    sp: SpIdentifier,
    /// Ignition command to perform on the targeted SP.
    command: IgnitionCommand,
}

/// Get info on an SP
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}",
}]
async fn sp_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<SpState>, HttpError> {
    let apictx = rqctx.context();
    let sp_id = path.into_inner().sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;

    let state = sp.state().await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(state.into()))
}

/// Get host startup options for a sled
///
/// This endpoint will currently fail for any `SpType` other than
/// `SpType::Sled`.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/startup-options",
}]
async fn sp_startup_options_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<HostStartupOptions>, HttpError> {
    let apictx = rqctx.context();
    let mgmt_switch = &apictx.mgmt_switch;
    let sp_id = path.into_inner().sp.into();
    let sp = mgmt_switch.sp(sp_id)?;

    let options = sp.get_startup_options().await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(options.into()))
}

/// Set host startup options for a sled
///
/// This endpoint will currently fail for any `SpType` other than
/// `SpType::Sled`.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/startup-options",
}]
async fn sp_startup_options_set(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
    body: TypedBody<HostStartupOptions>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let mgmt_switch = &apictx.mgmt_switch;
    let sp_id = path.into_inner().sp.into();
    let sp = mgmt_switch.sp(sp_id)?;

    sp.set_startup_options(body.into_inner().into()).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Read the current value of a sensor by ID
///
/// Sensor IDs come from the host topo tree.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/sensor/{sensor_id}/value",
}]
async fn sp_sensor_read_value(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpSensorId>,
) -> Result<HttpResponseOk<SpSensorReading>, HttpError> {
    let apictx = rqctx.context();
    let PathSpSensorId { sp, sensor_id } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let value = sp.read_sensor_value(sensor_id).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(value.into()))
}

/// List components of an SP
///
/// A component is a distinct entity under an SP's direct control. This lists
/// all those components for a given SP.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component",
}]
async fn sp_component_list(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<SpComponentList>, HttpError> {
    let apictx = rqctx.context();
    let sp_id = path.into_inner().sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let inventory = sp.inventory().await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(inventory.into()))
}

/// Get info for an SP component
///
/// This can be useful, for example, to poll the state of a component if
/// another interface has changed the power state of a component or updated a
/// component.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}",
}]
async fn sp_component_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseOk<Vec<SpComponentDetails>>, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;

    let details = sp.component_details(component).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(details.entries.into_iter().map(Into::into).collect()))
}

// Implementation notes:
//
// 1. As of the time of this comment, the cannonical keys written to the hubris
//    caboose are defined in https://github.com/oxidecomputer/hubtools; see
//    `write_default_caboose()`.
// 2. We currently assume that the caboose always includes the same set of
//    fields regardless of the component (e.g., the SP and RoT caboose have the
//    same fields). If that becomes untrue, we may need to split this endpoint
//    up to allow differently-typed responses.
/// Get the caboose of an SP component
///
/// Not all components have a caboose.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/caboose",
}]
async fn sp_component_caboose_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
    query_params: Query<ComponentCabooseSlot>,
) -> Result<HttpResponseOk<SpComponentCaboose>, HttpError> {
    const CABOOSE_KEY_GIT_COMMIT: [u8; 4] = *b"GITC";
    const CABOOSE_KEY_BOARD: [u8; 4] = *b"BORD";
    const CABOOSE_KEY_NAME: [u8; 4] = *b"NAME";
    const CABOOSE_KEY_VERSION: [u8; 4] = *b"VERS";

    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let ComponentCabooseSlot { firmware_slot } = query_params.into_inner();
    let component = component_from_str(&component)?;

    let from_utf8 = |key: &[u8], bytes| {
        // This helper closure is only called with the ascii-printable [u8; 4]
        // key constants we define above, so we can unwrap this conversion.
        let key = str::from_utf8(key).unwrap();
        String::from_utf8(bytes).map_err(|_| {
            http_err_with_message(
                http::StatusCode::SERVICE_UNAVAILABLE,
                "InvalidCaboose",
                format!("non-utf8 data returned for caboose key {key}"),
            )
        })
    };

    let git_commit = sp
        .read_component_caboose(
            component,
            firmware_slot,
            CABOOSE_KEY_GIT_COMMIT,
        )
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;
    let board = sp
        .read_component_caboose(component, firmware_slot, CABOOSE_KEY_BOARD)
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;
    let name = sp
        .read_component_caboose(component, firmware_slot, CABOOSE_KEY_NAME)
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;
    let version = sp
        .read_component_caboose(component, firmware_slot, CABOOSE_KEY_VERSION)
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;

    let git_commit = from_utf8(&CABOOSE_KEY_GIT_COMMIT, git_commit)?;
    let board = from_utf8(&CABOOSE_KEY_BOARD, board)?;
    let name = from_utf8(&CABOOSE_KEY_NAME, name)?;
    let version = from_utf8(&CABOOSE_KEY_VERSION, version)?;

    let caboose = SpComponentCaboose { git_commit, board, name, version };

    Ok(HttpResponseOk(caboose))
}

/// Clear status of a component
///
/// For components that maintain event counters (e.g., the sidecar `monorail`),
/// this will reset the event counters to zero.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/clear-status",
}]
async fn sp_component_clear_status(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;

    sp.component_clear_status(component).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Get the currently-active slot for an SP component
///
/// Note that the meaning of "current" in "currently-active" may vary depending
/// on the component: for example, it may mean "the actively-running slot" or
/// "the slot that will become active the next time the component is booted".
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/active-slot",
}]
async fn sp_component_active_slot_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseOk<SpComponentFirmwareSlot>, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;

    let slot = sp.component_active_slot(component).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(SpComponentFirmwareSlot { slot }))
}

#[derive(Deserialize, JsonSchema)]
pub struct SetComponentActiveSlotParams {
    /// Persist this choice of active slot.
    pub persist: bool,
}

/// Set the currently-active slot for an SP component
///
/// Note that the meaning of "current" in "currently-active" may vary depending
/// on the component: for example, it may mean "the actively-running slot" or
/// "the slot that will become active the next time the component is booted".
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/active-slot",
}]
async fn sp_component_active_slot_set(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
    query_params: Query<SetComponentActiveSlotParams>,
    body: TypedBody<SpComponentFirmwareSlot>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;
    let slot = body.into_inner().slot;
    let persist = query_params.into_inner().persist;

    sp.set_component_active_slot(component, slot, persist).await.map_err(
        |err| SpCommsError::SpCommunicationFailed { sp: sp_id, err },
    )?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Upgrade into a websocket connection attached to the given SP component's
/// serial console.
// This is a websocket endpoint; normally we'd expect to use `dropshot::channel`
// with `protocol = WEBSOCKETS` instead of `dropshot::endpoint`, but
// `dropshot::channel` doesn't allow us to return an error _before_ upgrading
// the connection, and we want to bail out ASAP if the SP doesn't allow us to
// attach. Therefore, we use `dropshot::endpoint` with the special argument type
// `WebsocketUpgrade`: this inserts the correct marker for progenitor to know
// this is a websocket endpoint, and it allows us to call
// `WebsocketUpgrade::handle()` (the method `dropshot::channel` would call for
// us to upgrade the connection) by hand after our error checking.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/serial-console/attach",
}]
async fn sp_component_serial_console_attach(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
    websocket: WebsocketUpgrade,
) -> WebsocketEndpointResult {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let component = component_from_str(&component)?;

    // Ensure we can attach to this SP's serial console.
    let console = apictx
        .mgmt_switch
        .sp(sp_id)?
        .serial_console_attach(component)
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;

    let log = apictx.log.new(slog::o!("sp" => format!("{sp:?}")));

    // We've successfully attached to the SP's serial console: upgrade the
    // websocket and run our side of that connection.
    websocket.handle(move |conn| {
        crate::serial_console::run(sp_id, console, conn, log)
    })
}

/// Detach the websocket connection attached to the given SP component's serial
/// console, if such a connection exists.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/serial-console/detach",
}]
async fn sp_component_serial_console_detach(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();

    // TODO-cleanup: "component" support for the serial console is half baked;
    // we don't use it at all to detach.
    let PathSpComponent { sp, component: _ } = path.into_inner();
    let sp_id = sp.into();

    let sp = apictx.mgmt_switch.sp(sp_id)?;
    sp.serial_console_detach().await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseUpdatedNoContent {})
}

#[derive(Deserialize, JsonSchema)]
pub struct ComponentUpdateIdSlot {
    /// An identifier for this update.
    ///
    /// This ID applies to this single instance of the API call; it is not an
    /// ID of `image` itself. Multiple API calls with the same `image` should
    /// use different IDs.
    pub id: Uuid,
    /// The update slot to apply this image to. Supply 0 if the component only
    /// has one update slot.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct ComponentCabooseSlot {
    /// The firmware slot to for which we want to request caboose information.
    pub firmware_slot: u16,
}

#[derive(Deserialize, JsonSchema)]
pub struct UpdateAbortBody {
    /// The ID of the update to abort.
    ///
    /// If the SP is currently receiving an update with this ID, it will be
    /// aborted.
    ///
    /// If the SP is currently receiving an update with a different ID, the
    /// abort request will fail.
    ///
    /// If the SP is not currently receiving any update, the request to abort
    /// should succeed but will not have actually done anything.
    pub id: Uuid,
}

/// Reset an SP component (possibly the SP itself).
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/reset",
}]
async fn sp_component_reset(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;

    sp.reset_component_prepare(component)
        .and_then(|()| sp.reset_component_trigger(component))
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Update an SP component
///
/// Update a component of an SP according to its specific update mechanism.
/// This interface is generic for all component types, but resolves to a
/// mechanism specific to the given component type. This may fail for a variety
/// of reasons including the update bundle being invalid or improperly
/// specified or due to an error originating from the SP itself.
///
/// Note that not all components may be updated; components without known
/// update mechanisms will return an error without any inspection of the
/// update bundle.
///
/// Updating the SP itself is done via the component name `sp`.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/update",
}]
async fn sp_component_update(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
    query_params: Query<ComponentUpdateIdSlot>,
    body: UntypedBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();

    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;
    let ComponentUpdateIdSlot { id, firmware_slot } = query_params.into_inner();

    // TODO-performance: this makes a full copy of the uploaded data
    let image = body.as_bytes().to_vec();

    sp.start_update(component, id, firmware_slot, image)
        .await
        .map_err(|err| SpCommsError::UpdateFailed { sp: sp_id, err })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Get the status of an update being applied to an SP component
///
/// Getting the status of an update to the SP itself is done via the component
/// name `sp`.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/update-status",
}]
async fn sp_component_update_status(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseOk<SpUpdateStatus>, HttpError> {
    let apictx = rqctx.context();

    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;

    let status = sp.update_status(component).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(status.into()))
}

/// Abort any in-progress update an SP component
///
/// Aborting an update to the SP itself is done via the component name `sp`.
///
/// On a successful return, the update corresponding to the given UUID will no
/// longer be in progress (either aborted or applied).
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/update-abort",
}]
async fn sp_component_update_abort(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
    body: TypedBody<UpdateAbortBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();

    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let component = component_from_str(&component)?;

    let UpdateAbortBody { id } = body.into_inner();
    sp.update_abort(component, id).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Read the CMPA from a root of trust.
///
/// This endpoint is only valid for the `rot` component.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/cmpa",
}]
async fn sp_rot_cmpa_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseOk<RotCmpa>, HttpError> {
    let apictx = rqctx.context();

    let PathSpComponent { sp, component } = path.into_inner();
    let sp_id = sp.into();

    // Ensure the caller knows they're asking for the RoT
    if component_from_str(&component)? != SpComponent::ROT {
        return Err(HttpError::for_bad_request(
            Some("RequestUnsupportedForComponent".to_string()),
            "Only the RoT has a CFPA".into(),
        ));
    }

    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let data = sp.read_rot_cmpa().await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    let base64_data = base64::engine::general_purpose::STANDARD.encode(data);

    Ok(HttpResponseOk(RotCmpa { base64_data }))
}

/// Read the requested CFPA slot from a root of trust.
///
/// This endpoint is only valid for the `rot` component.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/cfpa",
}]
async fn sp_rot_cfpa_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
    params: TypedBody<GetCfpaParams>,
) -> Result<HttpResponseOk<RotCfpa>, HttpError> {
    let apictx = rqctx.context();

    let PathSpComponent { sp, component } = path.into_inner();
    let GetCfpaParams { slot } = params.into_inner();
    let sp_id = sp.into();

    // Ensure the caller knows they're asking for the RoT
    if component_from_str(&component)? != SpComponent::ROT {
        return Err(HttpError::for_bad_request(
            Some("RequestUnsupportedForComponent".to_string()),
            "Only the RoT has a CFPA".into(),
        ));
    }

    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let data = match slot {
        RotCfpaSlot::Active => sp.read_rot_active_cfpa().await,
        RotCfpaSlot::Inactive => sp.read_rot_inactive_cfpa().await,
        RotCfpaSlot::Scratch => sp.read_rot_scratch_cfpa().await,
    }
    .map_err(|err| SpCommsError::SpCommunicationFailed { sp: sp_id, err })?;

    let base64_data = base64::engine::general_purpose::STANDARD.encode(data);

    Ok(HttpResponseOk(RotCfpa { base64_data, slot }))
}

/// List SPs via Ignition
///
/// Retreive information for all SPs via the Ignition controller. This is lower
/// latency and has fewer possible failure modes than querying the SP over the
/// management network.
#[endpoint {
    method = GET,
    path = "/ignition",
}]
async fn ignition_list(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<Vec<SpIgnitionInfo>>, HttpError> {
    let apictx = rqctx.context();
    let mgmt_switch = &apictx.mgmt_switch;

    let out = mgmt_switch
        .bulk_ignition_state()
        .await?
        .map(|(id, state)| SpIgnitionInfo {
            id: id.into(),
            details: state.into(),
        })
        .collect();

    Ok(HttpResponseOk(out))
}

/// Get SP info via Ignition
///
/// Retreive information for an SP via the Ignition controller. This is lower
/// latency and has fewer possible failure modes than querying the SP over the
/// management network.
#[endpoint {
    method = GET,
    path = "/ignition/{type}/{slot}",
}]
async fn ignition_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<SpIgnitionInfo>, HttpError> {
    let apictx = rqctx.context();
    let mgmt_switch = &apictx.mgmt_switch;

    let sp_id = path.into_inner().sp.into();
    let ignition_target = mgmt_switch.ignition_target(sp_id)?;

    let state = mgmt_switch
        .ignition_controller()
        .ignition_state(ignition_target)
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;

    let info = SpIgnitionInfo { id: sp_id.into(), details: state.into() };
    Ok(HttpResponseOk(info))
}

/// Send an ignition command targeting a specific SP.
///
/// This endpoint can be used to transition a target between A2 and A3 (via
/// power-on / power-off) or reset it.
///
/// The management network traffic caused by requests to this endpoint is
/// between this MGS instance and its local ignition controller, _not_ the SP
/// targeted by the command.
#[endpoint {
    method = POST,
    path = "/ignition/{type}/{slot}/{command}",
}]
async fn ignition_command(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpIgnitionCommand>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let mgmt_switch = &apictx.mgmt_switch;
    let PathSpIgnitionCommand { sp, command } = path.into_inner();
    let sp_id = sp.into();
    let ignition_target = mgmt_switch.ignition_target(sp_id)?;

    mgmt_switch
        .ignition_controller()
        .ignition_command(ignition_target, command.into())
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Get the current power state of a sled via its SP.
///
/// Note that if the sled is in A3, the SP is powered off and will not be able
/// to respond; use the ignition control endpoints for those cases.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/power-state",
}]
async fn sp_power_state_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<PowerState>, HttpError> {
    let apictx = rqctx.context();
    let sp_id = path.into_inner().sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;

    let power_state = sp.power_state().await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseOk(power_state.into()))
}

/// Set the current power state of a sled via its SP.
///
/// Note that if the sled is in A3, the SP is powered off and will not be able
/// to respond; use the ignition control endpoints for those cases.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power-state",
}]
async fn sp_power_state_set(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
    body: TypedBody<PowerState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp_id = path.into_inner().sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;
    let power_state = body.into_inner();

    sp.set_power_state(power_state.into()).await.map_err(|err| {
        SpCommsError::SpCommunicationFailed { sp: sp_id, err }
    })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Set the installinator image ID the sled should use for recovery.
///
/// This value can be read by the host via IPCC; see the `ipcc` crate.
#[endpoint {
    method = PUT,
    path = "/sp/{type}/{slot}/ipcc/installinator-image-id",
}]
async fn sp_installinator_image_id_set(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
    body: TypedBody<InstallinatorImageId>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    use ipcc::Key;

    let apictx = rqctx.context();
    let sp_id = path.into_inner().sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;

    let image_id = ipcc::InstallinatorImageId::from(body.into_inner());

    sp.set_ipcc_key_lookup_value(
        Key::InstallinatorImageId as u8,
        image_id.serialize(),
    )
    .await
    .map_err(|err| SpCommsError::SpCommunicationFailed { sp: sp_id, err })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Clear any previously-set installinator image ID on the target sled.
#[endpoint {
    method = DELETE,
    path = "/sp/{type}/{slot}/ipcc/installinator-image-id",
}]
async fn sp_installinator_image_id_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    use ipcc::Key;

    let apictx = rqctx.context();
    let sp_id = path.into_inner().sp.into();
    let sp = apictx.mgmt_switch.sp(sp_id)?;

    // We clear the image ID by setting it to a 0-length vec.
    sp.set_ipcc_key_lookup_value(Key::InstallinatorImageId as u8, Vec::new())
        .await
        .map_err(|err| SpCommsError::SpCommunicationFailed {
            sp: sp_id,
            err,
        })?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Get the most recent host phase2 request we've seen from the target SP.
///
/// This method can be used as an indirect progress report for how far along a
/// host is when it is booting via the MGS -> SP -> UART recovery path. This
/// path is used to install the trampoline image containing installinator to
/// recover a sled.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/host-phase2-progress",
}]
async fn sp_host_phase2_progress_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<HostPhase2Progress>, HttpError> {
    let apictx = rqctx.context();
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

    let Some(progress) = sp.most_recent_host_phase2_request().await else {
        return Ok(HttpResponseOk(HostPhase2Progress::None));
    };

    // Our `host_phase2_provider` is using an in-memory cache, so the only way
    // we can fail to get the total size is if we no longer have the image that
    // this SP most recently requested. We'll treat that as "no progress
    // information", since it almost certainly means our progress info on this
    // SP is very stale.
    let Ok(total_size) =
        apictx.host_phase2_provider.total_size(progress.hash).await
    else {
        return Ok(HttpResponseOk(HostPhase2Progress::None));
    };

    let image_id =
        HostPhase2RecoveryImageId { sha256_hash: ArtifactHash(progress.hash) };

    // `progress` tells us the offset the SP requested and the amount of data we
    // sent starting at that offset; report the end of that chunk to our caller.
    let offset = progress.offset.saturating_add(progress.data_sent);

    Ok(HttpResponseOk(HostPhase2Progress::Available {
        image_id,
        offset,
        total_size,
        age: progress.received.elapsed(),
    }))
}

/// Clear the most recent host phase2 request we've seen from the target SP.
#[endpoint {
    method = DELETE,
    path = "/sp/{type}/{slot}/host-phase2-progress",
}]
async fn sp_host_phase2_progress_delete(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

    sp.clear_most_recent_host_phase2_request().await;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Upload a host phase2 image that can be served to recovering hosts via the
/// host/SP control uart.
///
/// MGS caches this image in memory and is limited to a small, fixed number of
/// images (potentially 1). Uploading a new image may evict the
/// least-recently-requested image if our cache is already full.
#[endpoint {
    method = POST,
    path = "/recovery/host-phase2",
}]
async fn recovery_host_phase2_upload(
    rqctx: RequestContext<Arc<ServerContext>>,
    body: UntypedBody,
) -> Result<HttpResponseOk<HostPhase2RecoveryImageId>, HttpError> {
    let apictx = rqctx.context();

    // TODO: this makes a full copy of the host image, potentially unnecessarily
    // if it's malformed.
    let image = body.as_bytes().to_vec();

    let sha256_hash =
        apictx.host_phase2_provider.insert(image).await.map_err(|err| {
            // Any cache-insertion failure indicates a malformed image; map them
            // to bad requests.
            HttpError::for_bad_request(
                Some("BadHostPhase2Image".to_string()),
                err.to_string(),
            )
        })?;
    let sha256_hash = ArtifactHash(sha256_hash);

    Ok(HttpResponseOk(HostPhase2RecoveryImageId { sha256_hash }))
}

/// Get the identifier for the switch this MGS instance is connected to.
///
/// Note that most MGS endpoints behave identically regardless of which scrimlet
/// the MGS instance is running on; this one, however, is intentionally
/// different. This endpoint is _probably_ only useful for clients communicating
/// with MGS over localhost (i.e., other services in the switch zone) who need
/// to know which sidecar they are connected to.
#[endpoint {
    method = GET,
    path = "/local/switch-id",
}]
async fn sp_local_switch_id(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<SpIdentifier>, HttpError> {
    let apictx = rqctx.context();

    let id = apictx.mgmt_switch.local_switch()?;

    Ok(HttpResponseOk(id.into()))
}

/// Get the complete list of SP identifiers this MGS instance is configured to
/// find and communicate with.
///
/// Note that unlike most MGS endpoints, this endpoint does not send any
/// communication on the management network.
#[endpoint {
    method = GET,
    path = "/local/all-sp-ids",
}]
async fn sp_all_ids(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<Vec<SpIdentifier>>, HttpError> {
    let apictx = rqctx.context();

    let all_ids =
        apictx.mgmt_switch.all_sps()?.map(|(id, _)| id.into()).collect();

    Ok(HttpResponseOk(all_ids))
}

// TODO
// The gateway service will get asynchronous notifications both from directly
// SPs over the management network and indirectly from Ignition via the Sidecar
// SP.
// TODO The Ignition controller will send an interrupt to its local SP. Will
// that SP then notify both gateway services or just its local gateway service?
// Both Ignition controller should both do the same thing at about the same
// time so is there a real benefit to them both sending messages to both
// gateways? This would cause a single message to effectively be replicated 4x
// (Nexus would need to dedup these).

type GatewayApiDescription = ApiDescription<Arc<ServerContext>>;

/// Returns a description of the gateway API
pub fn api() -> GatewayApiDescription {
    fn register_endpoints(
        api: &mut GatewayApiDescription,
    ) -> Result<(), String> {
        api.register(sp_get)?;
        api.register(sp_startup_options_get)?;
        api.register(sp_startup_options_set)?;
        api.register(sp_component_reset)?;
        api.register(sp_power_state_get)?;
        api.register(sp_power_state_set)?;
        api.register(sp_installinator_image_id_set)?;
        api.register(sp_installinator_image_id_delete)?;
        api.register(sp_sensor_read_value)?;
        api.register(sp_component_list)?;
        api.register(sp_component_get)?;
        api.register(sp_component_caboose_get)?;
        api.register(sp_component_clear_status)?;
        api.register(sp_component_active_slot_get)?;
        api.register(sp_component_active_slot_set)?;
        api.register(sp_component_serial_console_attach)?;
        api.register(sp_component_serial_console_detach)?;
        api.register(sp_component_update)?;
        api.register(sp_component_update_status)?;
        api.register(sp_component_update_abort)?;
        api.register(sp_rot_cmpa_get)?;
        api.register(sp_rot_cfpa_get)?;
        api.register(sp_host_phase2_progress_get)?;
        api.register(sp_host_phase2_progress_delete)?;
        api.register(ignition_list)?;
        api.register(ignition_get)?;
        api.register(ignition_command)?;
        api.register(recovery_host_phase2_upload)?;
        api.register(sp_local_switch_id)?;
        api.register(sp_all_ids)?;
        Ok(())
    }

    let mut api = GatewayApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}
