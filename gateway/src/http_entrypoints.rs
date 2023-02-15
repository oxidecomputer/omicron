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
use crate::ServerContext;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RawRequest;
use dropshot::RequestContext;
use dropshot::TypedBody;
use dropshot::UntypedBody;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::TryFutureExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tokio_util::either::Either;
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
pub struct SpInfo {
    pub info: SpIgnitionInfo,
    pub details: SpState,
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
pub enum SpState {
    Enabled {
        serial_number: String,
        model: String,
        revision: u32,
        hubris_archive_id: String,
        base_mac_address: [u8; 6],
        version: ImageVersion,
        power_state: PowerState,
        rot: RotState,
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
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RotState {
    // TODO gateway_messages's RotState includes a couple nested structures that
    // I've flattened here because they only contain one field each. When those
    // structures grow we'll need to expand/change this.
    Enabled {
        active: RotSlot,
        slot_a: Option<RotImageDetails>,
        slot_b: Option<RotImageDetails>,
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
    #[serde(rename = "error")]
    CommunicationFailed { message: String },
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

// This type is a duplicate of the type in `ipcc-key-value`, and we provide a
// `From<_>` impl to convert to it. We keep these types distinct to allow us to
// choose different representations for MGS's HTTP API (this type) and the wire
// format passed through the SP to installinator
// (`ipcc_key_value::InstallinatorImageId`), although _currently_ they happen to
// be defined identically.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct InstallinatorImageId {
    pub update_id: Uuid,
    pub host_phase_2: [u8; 32],
    pub control_plane: [u8; 32],
}

/// Identifier for an SP's component's firmware slot; e.g., slots 0 and 1 for
/// the host boot flash.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct SpComponentFirmwareSlot {
    pub slot: u16,
}

/// Identity of a host phase2 recovery image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HostPhase2RecoveryImageId {
    pub sha256_hash: String,
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

/// List SPs
///
/// Since communication with SPs may be unreliable, consumers may specify an
/// optional timeout to override the default.
///
/// This interface makes use of Ignition as well as the management network.
/// SPs that are powered off (and therefore cannot respond over the
/// management network) are represented in the output set. SPs that Ignition
/// reports as powered on, but that do not respond within the allotted timeout
/// will similarly be represented in the output; these will only be included in
/// the output when the allotted timeout has expired.
///
/// Note that Ignition provides the full set of SPs that are plugged into the
/// system so the gateway service knows prior to waiting for responses the
/// expected cardinality.
#[endpoint {
    method = GET,
    path = "/sp",
}]
async fn sp_list(
    rqctx: RequestContext<Arc<ServerContext>>,
) -> Result<HttpResponseOk<Vec<SpInfo>>, HttpError> {
    let apictx = rqctx.context();
    let mgmt_switch = &apictx.mgmt_switch;

    // Build a `FuturesUnordered` to query every SP for its state.
    let all_sps_stream = mgmt_switch
        .all_sps()?
        .map(|(id, sp)| async move {
            let result = sp.state().await.map_err(SpCommsError::from);
            Either::Left((id, result))
        })
        .collect::<FuturesUnordered<_>>();

    // Build a future to query our local ignition controller for the ignition
    // state of all SPs.
    let bulk_ignition_stream =
        mgmt_switch.bulk_ignition_state().into_stream().map(Either::Right);

    let combo_stream = all_sps_stream.merge(bulk_ignition_stream);
    tokio::pin!(combo_stream);

    // Wait for all our results to come back. As SP states return, we stash them
    // in `sp_state`. When the one and only ignition result comes back, we put
    // it into `bulk_ignition_state`.
    let mut sp_state = HashMap::new();
    let mut bulk_ignition_state = None;

    while let Some(item) = combo_stream.next().await {
        match item {
            // Result from a single SP.
            Either::Left((id, state)) => {
                sp_state.insert(id, state);
            }
            // Result from our ignition controller.
            Either::Right(ignition_state_result) => {
                // If `bulk_ignition_state` succeeded, it returns an iterator
                // of `(id, state)` pairs; convert that into a HashMap for quick
                // lookups below.
                bulk_ignition_state = Some(
                    ignition_state_result
                        .map(|iter| iter.collect::<HashMap<_, _>>()),
                );
            }
        }
    }

    // We inserted exactly one future for the bulk ignition state into
    // combo_stream; if combo_stream is exhausted, all our futures have
    // completed, and we know we've populated `bulk_ignition_state`.
    let bulk_ignition_state = bulk_ignition_state.unwrap();

    // Build up a list of responses. For any given SP, we might or might not
    // have its state, and we might or might not have what our ignition
    // controller thinks its ignition state is.
    let mut responses = Vec::with_capacity(sp_state.len());
    for (id, state) in sp_state {
        let ignition_details =
            match bulk_ignition_state.as_ref().map(|m| m.get(&id)) {
                // Happy path
                Ok(Some(state)) => SpIgnition::from(*state),
                // Confusing path - we got a response from our ignition
                // controller, but it didn't include the state for SP `id`. If
                // we're on a rev-b sidecar, this could be the 36th ignition
                // target (i.e., the ignition controller does not return
                // information about itself as a target). For now we'll just
                // mark this as a failure; hopefully future sidecar revisions
                // add the 36th target
                // (https://github.com/oxidecomputer/hardware-sidecar/issues/735).
                Ok(None) => SpIgnition::CommunicationFailed {
                    message: format!(
                        "ignition response missing info for SP {:?}",
                        id
                    ),
                },
                Err(err) => {
                    SpIgnition::CommunicationFailed { message: err.to_string() }
                }
            };
        responses.push(SpInfo {
            info: SpIgnitionInfo { id: id.into(), details: ignition_details },
            details: state.into(),
        });
    }

    Ok(HttpResponseOk(responses))
}

/// Get info on an SP
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}",
}]
async fn sp_get(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<SpInfo>, HttpError> {
    let apictx = rqctx.context();
    let mgmt_switch = &apictx.mgmt_switch;
    let sp_id = path.into_inner().sp;
    let ignition_target = mgmt_switch.ignition_target(sp_id.into())?;
    let sp = mgmt_switch.sp(sp_id.into())?;

    // Send concurrent requests to our ignition controller and the target SP.
    let ignition_fut =
        mgmt_switch.ignition_controller().ignition_state(ignition_target);
    let sp_fut = sp.state();

    let (ignition_state, sp_state) = tokio::join!(ignition_fut, sp_fut);

    let info = SpInfo {
        info: SpIgnitionInfo { id: sp_id, details: ignition_state.into() },
        details: sp_state.into(),
    };

    Ok(HttpResponseOk(info))
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
    let sp = mgmt_switch.sp(path.into_inner().sp.into())?;

    let options = sp.get_startup_options().await.map_err(SpCommsError::from)?;

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
    let sp = mgmt_switch.sp(path.into_inner().sp.into())?;

    sp.set_startup_options(body.into_inner().into())
        .await
        .map_err(SpCommsError::from)?;

    Ok(HttpResponseUpdatedNoContent {})
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
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;
    let inventory = sp.inventory().await.map_err(SpCommsError::from)?;

    Ok(HttpResponseOk(inventory.into()))
}

/// Get info for an SP component
///
/// This can be useful, for example, to poll the state of a component if
/// another interface has changed the power state of a component or updated a
/// component.
///
/// As communication with SPs maybe unreliable, consumers may specify a timeout
/// to override the default. This interface will return an error when the
/// timeout is reached.
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
    let sp = apictx.mgmt_switch.sp(sp.into())?;
    let component = component_from_str(&component)?;

    let details =
        sp.component_details(component).await.map_err(SpCommsError::from)?;

    Ok(HttpResponseOk(details.entries.into_iter().map(Into::into).collect()))
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
    let sp = apictx.mgmt_switch.sp(sp.into())?;
    let component = component_from_str(&component)?;

    sp.component_clear_status(component).await.map_err(SpCommsError::from)?;

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
    let sp = apictx.mgmt_switch.sp(sp.into())?;
    let component = component_from_str(&component)?;

    let slot = sp
        .component_active_slot(component)
        .await
        .map_err(SpCommsError::from)?;

    Ok(HttpResponseOk(SpComponentFirmwareSlot { slot }))
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
    body: TypedBody<SpComponentFirmwareSlot>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();
    let sp = apictx.mgmt_switch.sp(sp.into())?;
    let component = component_from_str(&component)?;
    let slot = body.into_inner().slot;

    sp.set_component_active_slot(component, slot)
        .await
        .map_err(SpCommsError::from)?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Upgrade into a websocket connection attached to the given SP component's
/// serial console.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/serial-console/attach",
}]
async fn sp_component_serial_console_attach(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSpComponent>,
    raw_request: RawRequest,
) -> Result<http::Response<hyper::Body>, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();

    let component = component_from_str(&component)?;
    let mut request = raw_request.into_inner();

    let sp = sp.into();
    Ok(crate::serial_console::attach(
        &apictx.mgmt_switch,
        sp,
        component,
        &mut request,
        apictx.log.new(slog::o!("sp" => format!("{sp:?}"))),
    )
    .await?)
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

    let sp = apictx.mgmt_switch.sp(sp.into())?;
    sp.serial_console_detach().await.map_err(SpCommsError::from)?;

    Ok(HttpResponseUpdatedNoContent {})
}

// TODO: how can we make this generic enough to support any update mechanism?
#[derive(Deserialize, JsonSchema)]
pub struct UpdateBody {
    /// An identifier for this update.
    ///
    /// This ID applies to this single instance of the API call; it is not an
    /// ID of `image` itself. Multiple API calls with the same `image` should
    /// use different IDs.
    pub id: Uuid,
    /// The binary blob containing the update image (component-specific).
    pub image: Vec<u8>,
    /// The update slot to apply this image to. Supply 0 if the component only
    /// has one update slot.
    pub slot: u16,
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

/// Reset an SP
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/reset",
}]
async fn sp_reset(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

    sp.reset_prepare()
        .and_then(|()| sp.reset_trigger())
        .await
        .map_err(SpCommsError::from)?;

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
    body: TypedBody<UpdateBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();

    let PathSpComponent { sp, component } = path.into_inner();
    let sp = apictx.mgmt_switch.sp(sp.into())?;
    let component = component_from_str(&component)?;

    let UpdateBody { id, image, slot } = body.into_inner();
    sp.start_update(component, id, slot, image)
        .await
        .map_err(SpCommsError::from)?;

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
    let sp = apictx.mgmt_switch.sp(sp.into())?;
    let component = component_from_str(&component)?;

    let status =
        sp.update_status(component).await.map_err(SpCommsError::from)?;

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
    let sp = apictx.mgmt_switch.sp(sp.into())?;
    let component = component_from_str(&component)?;

    let UpdateAbortBody { id } = body.into_inner();
    sp.update_abort(component, id).await.map_err(SpCommsError::from)?;

    Ok(HttpResponseUpdatedNoContent {})
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

    let sp = path.into_inner().sp;
    let ignition_target = mgmt_switch.ignition_target(sp.into())?;

    let state = mgmt_switch
        .ignition_controller()
        .ignition_state(ignition_target)
        .await
        .map_err(SpCommsError::from)?;

    let info = SpIgnitionInfo { id: sp, details: state.into() };
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
    let ignition_target = mgmt_switch.ignition_target(sp.into())?;

    mgmt_switch
        .ignition_controller()
        .ignition_command(ignition_target, command.into())
        .await
        .map_err(SpCommsError::from)?;

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
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

    let power_state = sp.power_state().await.map_err(SpCommsError::from)?;

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
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;
    let power_state = body.into_inner();

    sp.set_power_state(power_state.into()).await.map_err(SpCommsError::from)?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Set the installinator image ID the sled should use for recovery.
///
/// This value can be read by the host via IPCC; see the `ipcc-key-value` crate.
#[endpoint {
    method = PUT,
    path = "/sp/{type}/{slot}/ipcc/installinator-image-id",
}]
async fn sp_installinator_image_id_set(
    rqctx: RequestContext<Arc<ServerContext>>,
    path: Path<PathSp>,
    body: TypedBody<InstallinatorImageId>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    use ipcc_key_value::Key;

    let apictx = rqctx.context();
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

    let image_id =
        ipcc_key_value::InstallinatorImageId::from(body.into_inner());

    sp.set_ipcc_key_lookup_value(
        Key::InstallinatorImageId as u8,
        image_id.serialize(),
    )
    .await
    .map_err(SpCommsError::from)?;

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
    use ipcc_key_value::Key;

    let apictx = rqctx.context();
    let sp = apictx.mgmt_switch.sp(path.into_inner().sp.into())?;

    // We clear the image ID by setting it to a 0-length vec.
    sp.set_ipcc_key_lookup_value(Key::InstallinatorImageId as u8, Vec::new())
        .await
        .map_err(SpCommsError::from)?;

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

    let sha2 =
        apictx.host_phase2_provider.insert(image).await.map_err(|err| {
            // Any cache-insertion failure indicates a malformed image; map them
            // to bad requests.
            HttpError::for_bad_request(
                Some("BadHostPhase2Image".to_string()),
                err.to_string(),
            )
        })?;

    Ok(HttpResponseOk(HostPhase2RecoveryImageId {
        sha256_hash: hex::encode(&sha2),
    }))
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
        api.register(sp_list)?;
        api.register(sp_get)?;
        api.register(sp_startup_options_get)?;
        api.register(sp_startup_options_set)?;
        api.register(sp_reset)?;
        api.register(sp_power_state_get)?;
        api.register(sp_power_state_set)?;
        api.register(sp_installinator_image_id_set)?;
        api.register(sp_installinator_image_id_delete)?;
        api.register(sp_component_list)?;
        api.register(sp_component_get)?;
        api.register(sp_component_clear_status)?;
        api.register(sp_component_active_slot_get)?;
        api.register(sp_component_active_slot_set)?;
        api.register(sp_component_serial_console_attach)?;
        api.register(sp_component_serial_console_detach)?;
        api.register(sp_component_update)?;
        api.register(sp_component_update_status)?;
        api.register(sp_component_update_abort)?;
        api.register(ignition_list)?;
        api.register(ignition_get)?;
        api.register(ignition_command)?;
        api.register(recovery_host_phase2_upload)?;
        Ok(())
    }

    let mut api = GatewayApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}
