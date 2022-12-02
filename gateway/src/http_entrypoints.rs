// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! HTTP entrypoint functions for the gateway service

mod conversions;

use self::conversions::component_from_str;
use crate::error::http_err_from_comms_err;
use crate::ServerContext;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::TypedBody;
use futures::StreamExt;
use gateway_messages::ignition;
use gateway_messages::IgnitionCommand;
use gateway_sp_comms::error::Error as SpCommsError;
use gateway_sp_comms::Timeout as SpTimeout;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog::warn;
use std::fmt::Display;
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
    Disabled,
    Unresponsive,
    Enabled {
        serial_number: String,
        // TODO more stuff
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

#[derive(Deserialize, JsonSchema)]
struct Timeout {
    timeout_millis: Option<u32>,
}

#[derive(Serialize, Deserialize)]
struct TimeoutSelector<T> {
    last: T,
    start_time: u64, // TODO
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
enum PowerState {
    A0,
    A1,
    A2,
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query: Query<Timeout>,
) -> Result<HttpResponseOk<Vec<SpInfo>>, HttpError> {
    let apictx = rqctx.context();
    let timeout = query.into_inner();

    let timeout = timeout
        .timeout_millis
        .map(|t| Duration::from_millis(u64::from(t)))
        .unwrap_or(apictx.timeouts.bulk_request_default);
    let timeout = SpTimeout::from_now(timeout);

    // query ignition controller to find out which SPs are powered on
    let all_sps = apictx
        .sp_comms
        .get_ignition_state_all()
        .await
        .map_err(http_err_from_comms_err)?;
    let communicator = Arc::clone(&apictx.sp_comms);
    let response_stream =
        apictx.sp_comms.query_all_online_sps(&all_sps, timeout, move |sp| {
            let communicator = Arc::clone(&communicator);
            async move { communicator.get_state(sp).await }
        });

    // Convert the response tuple of (SpIdentifier, IgnitionState,
    // Option<Result<Result<SpState, Error>, Elapsed>>) to Vec<SpInfo>;
    let responses: Vec<_> = response_stream
        .map(|(id, ignition_details, result)| {
            // Unpack the nasty nested type:
            // 1. None => ignition indicated power was off; treat that as
            //    success (with state = disabled)
            // 2. Outer err => timeout; treat that as "success"
            //    (with state = unresponsive)
            // 3. Inner success => true success
            // 4. Inner error => Log it (with state = unresponsive)
            let id = id.into();
            let details = ignition_details.into();
            let state = match result {
                None => SpState::Disabled,
                Some(Err(_)) => SpState::Unresponsive,
                Some(Ok(result)) => match result {
                    Ok(state) => SpState::from(state),
                    Err(err) => {
                        warn!(apictx.log, "SP {id}: {err}");
                        SpState::Unresponsive
                    }
                },
            };
            SpInfo { info: SpIgnitionInfo { id, details }, details: state }
        })
        .collect()
        .await;

    Ok(HttpResponseOk(responses))
}

/// Get info on an SP
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}",
}]
async fn sp_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<SpInfo>, HttpError> {
    let apictx = rqctx.context();
    let comms = &apictx.sp_comms;
    let sp = path.into_inner().sp;

    // ping the ignition controller first; if it says the SP is off or otherwise
    // unavailable, we're done.
    let state = comms
        .get_ignition_state(sp.into())
        .await
        .map_err(http_err_from_comms_err)?;

    let details = if state
        .target
        .map(|t| t.power_state == ignition::SystemPowerState::On)
        .unwrap_or(false)
    {
        // ignition indicates the SP is on; ask it for its state
        match comms.get_state(sp.into()).await {
            Ok(state) => SpState::from(state),
            Err(SpCommsError::Timeout { .. }) => SpState::Unresponsive,
            Err(other) => return Err(http_err_from_comms_err(other)),
        }
    } else {
        SpState::Disabled
    };

    let info = SpInfo {
        info: SpIgnitionInfo { id: sp, details: state.into() },
        details,
    };

    Ok(HttpResponseOk(info))
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<SpComponentList>, HttpError> {
    let apictx = rqctx.context();
    let comms = &apictx.sp_comms;
    let sp = path.into_inner().sp;

    let inventory =
        comms.inventory(sp.into()).await.map_err(http_err_from_comms_err)?;

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
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSpComponent>,
    _query: Query<Timeout>,
) -> Result<HttpResponseOk<SpComponentInfo>, HttpError> {
    todo!()
}

/// Upgrade into a websocket connection attached to the given SP component's
/// serial console.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/serial-console/attach",
}]
async fn sp_component_serial_console_attach(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSpComponent>,
) -> Result<http::Response<hyper::Body>, HttpError> {
    let apictx = rqctx.context();
    let PathSpComponent { sp, component } = path.into_inner();

    let component = component_from_str(&component)?;
    let mut request = rqctx.request.lock().await;

    let sp = sp.into();
    Ok(crate::serial_console::attach(
        &apictx.sp_comms,
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let comms = &rqctx.context().sp_comms;

    // TODO-cleanup: "component" support for the serial console is half baked;
    // we don't use it at all to detach.
    let PathSpComponent { sp, component: _ } = path.into_inner();

    comms
        .serial_console_detach(sp.into())
        .await
        .map_err(http_err_from_comms_err)?;

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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let comms = &rqctx.context().sp_comms;
    let sp = path.into_inner().sp;

    comms.reset(sp.into()).await.map_err(http_err_from_comms_err)?;

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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSpComponent>,
    body: TypedBody<UpdateBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let comms = Arc::clone(&rqctx.context().sp_comms);
    let PathSpComponent { sp, component } = path.into_inner();
    let component = component_from_str(&component)?;
    let UpdateBody { id, image, slot } = body.into_inner();

    comms
        .start_update(sp.into(), component, id, slot, image)
        .await
        .map_err(http_err_from_comms_err)?;

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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseOk<SpUpdateStatus>, HttpError> {
    let comms = Arc::clone(&rqctx.context().sp_comms);
    let PathSpComponent { sp, component } = path.into_inner();
    let component = component_from_str(&component)?;

    let status = comms
        .update_status(sp.into(), component)
        .await
        .map_err(http_err_from_comms_err)?;

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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSpComponent>,
    body: TypedBody<UpdateAbortBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let comms = &rqctx.context().sp_comms;
    let PathSpComponent { sp, component } = path.into_inner();
    let component = component_from_str(&component)?;
    let UpdateAbortBody { id } = body.into_inner();

    comms
        .update_abort(sp.into(), component, id)
        .await
        .map_err(http_err_from_comms_err)?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Power on an SP component
///
/// Components whose power state cannot be changed will always return an error.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/power-on",
}]
async fn sp_component_power_on(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSpComponent>,
    // TODO do we need a timeout?
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    todo!()
}

/// Power off an SP component
///
/// Components whose power state cannot be changed will always return an error.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/power-off",
}]
async fn sp_component_power_off(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSpComponent>,
    // TODO do we need a timeout?
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    todo!()
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
) -> Result<HttpResponseOk<Vec<SpIgnitionInfo>>, HttpError> {
    let apictx = rqctx.context();
    let sp_comms = &apictx.sp_comms;

    let all_state = sp_comms
        .get_ignition_state_all()
        .await
        .map_err(http_err_from_comms_err)?;

    let mut out = Vec::with_capacity(all_state.len());
    for (id, state) in all_state {
        out.push(SpIgnitionInfo { id: id.into(), details: state.into() });
    }
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<SpIgnitionInfo>, HttpError> {
    let apictx = rqctx.context();
    let sp = path.into_inner().sp;

    let state = apictx
        .sp_comms
        .get_ignition_state(sp.into())
        .await
        .map_err(http_err_from_comms_err)?;

    let info = SpIgnitionInfo { id: sp, details: state.into() };
    Ok(HttpResponseOk(info))
}

/// Power on a sled via a request to its SP.
///
/// This corresponds to moving the sled into A2.
#[endpoint {
    method = POST,
    path = "/ignition/{type}/{slot}/power-on",
}]
async fn ignition_power_on(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp = path.into_inner().sp;

    apictx
        .sp_comms
        .send_ignition_command(sp.into(), IgnitionCommand::PowerOn)
        .await
        .map_err(http_err_from_comms_err)?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Power off a sled via Ignition
///
/// This corresponds to moving the sled into A3.
#[endpoint {
    method = POST,
    path = "/ignition/{type}/{slot}/power-off",
}]
async fn ignition_power_off(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp = path.into_inner().sp;

    apictx
        .sp_comms
        .send_ignition_command(sp.into(), IgnitionCommand::PowerOff)
        .await
        .map_err(http_err_from_comms_err)?;

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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseOk<PowerState>, HttpError> {
    let apictx = rqctx.context();
    let sp = path.into_inner().sp;

    let power_state = apictx
        .sp_comms
        .power_state(sp.into())
        .await
        .map_err(http_err_from_comms_err)?;

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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
    body: TypedBody<PowerState>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp = path.into_inner().sp;
    let power_state = body.into_inner();

    apictx
        .sp_comms
        .set_power_state(sp.into(), power_state.into())
        .await
        .map_err(http_err_from_comms_err)?;

    Ok(HttpResponseUpdatedNoContent {})
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
        api.register(sp_reset)?;
        api.register(sp_power_state_get)?;
        api.register(sp_power_state_set)?;
        api.register(sp_component_list)?;
        api.register(sp_component_get)?;
        api.register(sp_component_serial_console_attach)?;
        api.register(sp_component_serial_console_detach)?;
        api.register(sp_component_update)?;
        api.register(sp_component_update_status)?;
        api.register(sp_component_update_abort)?;
        api.register(sp_component_power_on)?;
        api.register(sp_component_power_off)?;
        api.register(ignition_list)?;
        api.register(ignition_get)?;
        api.register(ignition_power_on)?;
        api.register(ignition_power_off)?;
        Ok(())
    }

    let mut api = GatewayApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}
