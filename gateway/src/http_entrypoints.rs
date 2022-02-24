// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! HTTP entrypoint functions for the gateway service

use crate::config::KnownSps;
use crate::error::Error;
use crate::ServerContext;
use dropshot::{
    endpoint, ApiDescription, EmptyScanParams, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, PaginationParams, Path, Query,
    RequestContext, ResultsPage, TypedBody,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::sync::Arc;

#[derive(Serialize, JsonSchema)]
struct SpInfo {
    info: SpIgnitionInfo,
    details: SpState,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "state")]
#[allow(dead_code)] // TODO remove once this is used
enum SpState {
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
        use gateway_messages::IgnitionFlags;
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
#[allow(dead_code)] // TODO remove once this is used
struct Timeout {
    timeout: Option<u32>,
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

#[derive(Serialize, JsonSchema, PartialEq, Debug, Clone)]
pub(crate) struct SpIdentifier {
    #[serde(rename = "type")]
    pub(crate) typ: SpType,
    pub(crate) slot: u32,
}

// We can't `#[derive(Deserialize)]` for `SpIdentifier` because it's embedded in
// other structs via `serde(flatten)`, which does not play well with the way
// dropshot parses HTTP queries/paths. serde ends up trying to deserialize the
// flattened struct as a map of strings to strings, which breaks on our `slot`
// (but not on `typ` for reasons I don't entirely understand). We can work
// around this by manually implementing `Deserialize` with a custom enum that
// allows either `String` or `u32` for `slot` (which gets us past the serde map
// of strings), and then parsing the string into a u32 ourselves (which gets us
// to the `slot` we want). More background:
// https://github.com/serde-rs/serde/issues/1346
impl<'de> Deserialize<'de> for SpIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Unexpected, Visitor};

        const FIELDS: &[&str] = &["type", "slot"];

        // Workaround for serde(flatten) issue part 1: We want to be able to
        // accept a `slot` as either a `String` or a `u32` from serde.
        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum StringOrU32 {
            String(String),
            U32(u32),
        }

        struct SpIdentifierVisitor;

        impl<'de> Visitor<'de> for SpIdentifierVisitor {
            type Value = SpIdentifier;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter.write_str("struct SpIdentifier")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut typ = None;
                let mut slot = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "type" => {
                            if typ.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            typ = Some(map.next_value()?);
                        }
                        "slot" => {
                            if slot.is_some() {
                                return Err(de::Error::duplicate_field("slot"));
                            }
                            // serde(flatten) workaround part 2 - accept either
                            // `String` or `u32` here; if we get a string, parse
                            // it as a u32.
                            let val = map.next_value::<StringOrU32>()?;
                            match val {
                                StringOrU32::String(s) => {
                                    slot = Some(s.parse().map_err(|_| {
                                        de::Error::invalid_type(
                                            Unexpected::Str(&s),
                                            &"u32",
                                        )
                                    })?);
                                }
                                StringOrU32::U32(n) => slot = Some(n),
                            }
                        }
                        other => {
                            return Err(de::Error::unknown_field(other, FIELDS))
                        }
                    }
                }

                let typ =
                    typ.ok_or_else(|| de::Error::missing_field("type"))?;
                let slot =
                    slot.ok_or_else(|| de::Error::missing_field("slot"))?;
                Ok(SpIdentifier { typ, slot })
            }
        }

        deserializer.deserialize_struct(
            "SpIdentifier",
            FIELDS,
            SpIdentifierVisitor,
        )
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
            .map_err(|_| Error::SpDoesNotExist(self.clone()))?;

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
                return Err(Error::SpDoesNotExist(self.clone()));
            }
        }

        // above loop returns once we match on `typ`
        unreachable!()
    }
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
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSp>,
    _query: Query<Timeout>,
) -> Result<HttpResponseOk<SpInfo>, HttpError> {
    todo!()
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
/// List the SPs via the Ignition controller. This mechanism retrieves less
/// information than over the management network, however it is lower latency
/// and has fewer moving pieces that could result in delayed responses or
/// unknown states.
///
/// This interface queries ignition via its associated SP. As this interface
/// may be unreliable, consumers may optionally override the default.
#[endpoint {
    method = GET,
    path = "/ignition",
}]
async fn ignition_list(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _query: Query<PaginationParams<EmptyScanParams, SpIdentifier>>,
) -> Result<HttpResponseOk<ResultsPage<SpIgnitionInfo>>, HttpError> {
    todo!()
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
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSp>,
) -> Result<HttpResponseOk<SpIgnitionInfo>, HttpError> {
    todo!()
}

/// Power off an SP via Ignition
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power_off",
}]
async fn ignition_power_off(
    _rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    _path: Path<PathSp>,
) -> Result<HttpResponseOk<SpIgnitionInfo>, HttpError> {
    todo!()
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
