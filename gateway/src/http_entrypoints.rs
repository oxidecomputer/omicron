// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! HTTP entrypoint functions for the gateway service

use crate::config::KnownSps;
use crate::error::Error;
use crate::sp_comms::{self, SerialConsoleContents};
use crate::ServerContext;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, PaginationParams, Path, Query,
    RequestContext, ResultsPage, TypedBody, UntypedBody,
};
use gateway_messages::{IgnitionFlags, SpComponent};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, JsonSchema)]
struct SpInfo {
    info: SpIgnitionInfo,
    details: SpState,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "state")]
pub(crate) enum SpState {
    Disabled,
    Unresponsive,
    Enabled {
        serial_number: String,
        // TODO more stuff
    },
}

#[derive(Serialize, JsonSchema)]
struct SpIgnitionInfo {
    id: SpIdentifier,
    details: SpIgnition,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "present")]
#[allow(dead_code)] // TODO remove once `Absent` is used
enum SpIgnition {
    #[serde(rename = "no")]
    Absent,
    #[serde(rename = "yes")]
    Present {
        id: u16,
        power: bool,
        ctrl_detect_0: bool,
        ctrl_detect_1: bool,
        flt_a3: bool,
        flt_a2: bool,
        flt_rot: bool,
        flt_sp: bool,
    },
}

impl From<gateway_messages::IgnitionState> for SpIgnition {
    fn from(state: gateway_messages::IgnitionState) -> Self {
        // if we have a state, the SP was present
        Self::Present {
            id: state.id,
            power: state.flags.intersects(IgnitionFlags::POWER),
            ctrl_detect_0: state.flags.intersects(IgnitionFlags::CTRL_DETECT_0),
            ctrl_detect_1: state.flags.intersects(IgnitionFlags::CTRL_DETECT_1),
            flt_a3: state.flags.intersects(IgnitionFlags::FLT_A3),
            flt_a2: state.flags.intersects(IgnitionFlags::FLT_A2),
            flt_rot: state.flags.intersects(IgnitionFlags::FLT_ROT),
            flt_sp: state.flags.intersects(IgnitionFlags::FLT_SP),
        }
    }
}

#[derive(Serialize, JsonSchema)]
struct SpComponentInfo;

#[derive(Deserialize, JsonSchema)]
struct Timeout {
    timeout_millis: Option<u32>,
}

#[derive(Serialize, Deserialize)]
struct TimeoutSelector<T> {
    last: T,
    start_time: u64, // TODO
}

#[derive(Serialize, Deserialize, JsonSchema, PartialEq, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SpType {
    Sled,
    Power,
    Switch,
}

#[derive(Serialize, Deserialize, JsonSchema, PartialEq, Debug, Clone, Copy)]
pub(crate) struct SpIdentifier {
    #[serde(rename = "type")]
    pub(crate) typ: SpType,
    #[serde(deserialize_with = "deserializer_u32_from_string")]
    pub(crate) slot: u32,
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

impl SpIdentifier {
    fn placeholder_map_to_target(
        &self,
        known_sps: &KnownSps,
    ) -> Result<u8, Error> {
        // TODO This is wrong in all kinds of ways, but is just a placeholder
        // for now until we have a better story for bootstrapping how MGS knows
        // which SP is which.
        //
        // Maps `self` to a target number by assuming target numbers are indexed
        // from 0 starting with switches followed by sleds followed by power
        // controllers.

        let slot: usize = usize::try_from(self.slot)
            .map_err(|_| Error::SpDoesNotExist(*self))?;

        let mut base = 0;
        for (typ, count) in [
            (SpType::Switch, known_sps.switches.len()),
            (SpType::Sled, known_sps.sleds.len()),
            (SpType::Power, known_sps.power_controllers.len()),
        ] {
            if self.typ != typ {
                base += count;
                continue;
            }

            if slot < count {
                let slot = u8::try_from(slot + base).map_err(|_| {
                    Error::InternalError {
                        internal_message:
                            "too many total configured SP slots (must be < 256)"
                                .to_string(),
                    }
                })?;
                return Ok(slot);
            } else {
                return Err(Error::SpDoesNotExist(*self));
            }
        }

        // above loop returns once we match on `typ`
        unreachable!()
    }
}

fn placeholder_map_from_target(
    known_sps: &KnownSps,
    mut target: usize,
) -> Result<SpIdentifier, Error> {
    for (typ, count) in [
        (SpType::Switch, known_sps.switches.len()),
        (SpType::Sled, known_sps.sleds.len()),
        (SpType::Power, known_sps.power_controllers.len()),
    ] {
        if target < count {
            return Ok(SpIdentifier { typ, slot: target as u32 });
        }
        target -= count;
    }

    Err(Error::InternalError {
        internal_message: format!("invalid ignition target index {}", target),
    })
}

type TimeoutPaginationParams<T> = PaginationParams<Timeout, TimeoutSelector<T>>;

#[derive(Deserialize, JsonSchema)]
struct PathSp {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    sp: SpIdentifier,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub(crate) struct PathSpComponent {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    pub(crate) sp: SpIdentifier,
    /// ID for the component of the SP; this is the internal identifier used by
    /// the SP itself to identify its components.
    pub(crate) component: String,
}

/// List SPs
///
/// Since communication with SPs may be unreliable, consumers may specify an
/// optional timeout to override the default.
///
/// This interface may return a page of SPs prior to reaching either the
/// timeout with the expectation that callers will keep calling this interface
/// until the terminal page is reached. If the timeout is reached, the final
/// call will result in an error.
///
/// This interface makes use of Ignition as well as the management network.
/// SPs that are powered off (and therefore cannot respond over the
/// management network) are represented in the output set. SPs that Ignition
/// reports as powered on, but that do not respond within the allotted timeout
/// will similarly be represented in the output; these will only be included in
/// the terminal output page when the allotted timeout has expired.
///
/// Note that Ignition provides the full set of SPs that are plugged into the
/// system so the gateway service knows prior to waiting for responses the
/// expected cardinality.
#[endpoint {
    method = GET,
    path = "/sp",
}]
async fn sp_list(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _query: Query<TimeoutPaginationParams<SpIdentifier>>,
) -> Result<HttpResponseOk<ResultsPage<SpInfo>>, HttpError> {
    todo!()
}

/// Get info on an SP
///
/// As communication with SPs may be unreliable, consumers may specify an
/// optional timeout to override the default.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}",
}]
async fn sp_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
    query: Query<Timeout>,
) -> Result<HttpResponseOk<SpInfo>, HttpError> {
    let apictx = rqctx.context();
    let comms = &apictx.sp_comms;
    let sp = path.into_inner().sp;

    let target = sp.placeholder_map_to_target(comms.placeholder_known_sps())?;
    let sp_addr = comms
        .placeholder_known_sps()
        .addr_for(&sp)
        .ok_or(Error::SpDoesNotExist(sp))?;

    // ping the ignition controller first; if it says the SP is off or otherwise
    // unavailable, we're done.
    let state =
        comms.ignition_get(target, apictx.ignition_controller_timeout).await?;

    let details = if state.flags.intersects(IgnitionFlags::POWER) {
        // ignition indicates the SP is on; ask it for its state
        let timeout = query
            .into_inner()
            .timeout_millis
            .map(|n| Duration::from_millis(u64::from(n)))
            .unwrap_or(apictx.sp_request_timeout);
        match comms.state_get(sp_addr, timeout).await {
            Ok(state) => state,
            Err(sp_comms::Error::Timeout(_)) => SpState::Unresponsive,
            Err(other) => return Err(other.into()),
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
///
/// As communication with SPs may be unreliable, consumers may optionally
/// override the timeout. This interface may return a page of components prior
/// to reaching either the timeout with the expectation that callers will keep
/// calling this interface until the terminal page is reached. If the timeout
/// is reached, the final call will result in an error.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component",
}]
async fn sp_component_list(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSp>,
    _query: Query<TimeoutPaginationParams<PathSpComponent>>,
) -> Result<HttpResponseOk<ResultsPage<SpComponentInfo>>, HttpError> {
    todo!()
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

/// Get the currently-buffered data for an SP component's serial console.
///
/// This does not require any communication with the target SP; it returns any
/// buffered data we have received from that SP (subject to limitations on how
/// much we keep around; see [`SerialConsoleContents`] for details.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component/{component}/serial_console",
}]
async fn sp_component_serial_console_get(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSpComponent>,
) -> Result<HttpResponseOk<SerialConsoleContents>, HttpError> {
    let comms = &rqctx.context().sp_comms;
    let PathSpComponent { sp, component } = path.into_inner();

    let sp = comms
        .placeholder_known_sps()
        .addr_for(&sp)
        .ok_or(Error::SpDoesNotExist(sp))?;
    let component = SpComponent::try_from(component.as_str())
        .map_err(|_| Error::InvalidSpComponentId(component))?;
    let contents = comms.serial_console_get(sp, &component)?;

    // TODO With `unwrap_or_default()`, our caller can't tell the difference
    // between "this component hasn't sent us any console information yet" and
    // "this component does not have a serial console and will never send data"
    // - both cases send back an empty `SerialConsoleContents`. To handle this
    // more gracefully, we will need to know which components have a serial
    // console.
    Ok(HttpResponseOk(contents.unwrap_or_default()))
}

/// Send data to an SP component's serial console.
///
/// If this request returns successfully, the SP acknowledged that the data
/// arrived. If it fails, we do not know whether or not the data arrived (or
/// will eventually arrive).
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/serial_console",
}]
async fn sp_component_serial_console_post(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSpComponent>,
    data: UntypedBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let comms = &apictx.sp_comms;
    let PathSpComponent { sp, component } = path.into_inner();

    let sp = comms
        .placeholder_known_sps()
        .addr_for(&sp)
        .ok_or(Error::SpDoesNotExist(sp))?;
    let component = SpComponent::try_from(component.as_str())
        .map_err(|_| Error::InvalidSpComponentId(component))?;

    // TODO What is our recourse if we hit a timeout here? We don't know whether
    // the SP received none, some, or all of the data we sent, only that it
    // failed to ack (at least) the last packet in time. Hopefully the user can
    // manually detect this by inspecting the serial console output from this
    // component (if whatever they sent triggers some kind of output)? But maybe
    // we should try to do a little better - if we had to packetize `data`, for
    // example, we could at least report how much data was ack'd and how much we
    // sent that hasn't been ack'd yet?
    comms
        .serial_console_post(
            sp,
            component,
            data.as_bytes(),
            apictx.sp_request_timeout,
        )
        .await?;

    Ok(HttpResponseUpdatedNoContent {})
}

// TODO: how can we make this generic enough to support any update mechanism?
#[derive(Deserialize, JsonSchema)]
struct UpdateBody;

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
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/update",
}]
async fn sp_component_update(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSpComponent>,
    _body: TypedBody<UpdateBody>,
) -> Result<HttpResponseOk<ResultsPage<SpComponentInfo>>, HttpError> {
    todo!()
}

/// Power on an SP component
///
/// Components whose power state cannot be changed will always return an error.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/power_on",
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
    path = "/sp/{type}/{slot}/component/{component}/power_off",
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

    let all_state = apictx
        .sp_comms
        .bulk_ignition_get(apictx.ignition_controller_timeout)
        .await?;

    let mut out = Vec::with_capacity(all_state.len());
    for (i, state) in all_state.into_iter().enumerate() {
        out.push(SpIgnitionInfo {
            id: placeholder_map_from_target(
                apictx.sp_comms.placeholder_known_sps(),
                i,
            )?,
            details: state.into(),
        });
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

    let target =
        sp.placeholder_map_to_target(apictx.sp_comms.placeholder_known_sps())?;

    let state = apictx
        .sp_comms
        .ignition_get(target, apictx.ignition_controller_timeout)
        .await?;

    let info = SpIgnitionInfo { id: sp, details: state.into() };
    Ok(HttpResponseOk(info))
}

/// Power on an SP via Ignition
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power_on",
}]
async fn ignition_power_on(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp = path.into_inner().sp;

    let target =
        sp.placeholder_map_to_target(apictx.sp_comms.placeholder_known_sps())?;

    apictx
        .sp_comms
        .ignition_power_on(target, apictx.ignition_controller_timeout)
        .await?;

    Ok(HttpResponseUpdatedNoContent {})
}

/// Power off an SP via Ignition
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power_off",
}]
async fn ignition_power_off(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let apictx = rqctx.context();
    let sp = path.into_inner().sp;

    let target =
        sp.placeholder_map_to_target(apictx.sp_comms.placeholder_known_sps())?;

    apictx
        .sp_comms
        .ignition_power_off(target, apictx.ignition_controller_timeout)
        .await?;

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
        api.register(sp_component_list)?;
        api.register(sp_component_get)?;
        api.register(sp_component_serial_console_get)?;
        api.register(sp_component_serial_console_post)?;
        api.register(sp_component_update)?;
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
