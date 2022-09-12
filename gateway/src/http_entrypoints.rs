// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! HTTP entrypoint functions for the gateway service

mod conversions;

use self::conversions::component_from_str;
use crate::bulk_state_get::BulkSpStateSingleResult;
use crate::bulk_state_get::BulkStateProgress;
use crate::bulk_state_get::SpStateRequestId;
use crate::error::http_err_from_comms_err;
use crate::ServerContext;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::PaginationParams;
use dropshot::Path;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ResultsPage;
use dropshot::TypedBody;
use dropshot::WhichPage;
use gateway_messages::IgnitionCommand;
use gateway_messages::SpComponent;
use gateway_sp_comms::error::Error as SpCommsError;
use gateway_sp_comms::Timeout as SpTimeout;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

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
#[allow(dead_code)] // TODO remove once `Absent` is used
pub enum SpIgnition {
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

#[derive(Serialize, JsonSchema)]
struct SpComponentInfo {}

#[derive(Deserialize, JsonSchema)]
struct Timeout {
    timeout_millis: Option<u32>,
}

#[derive(Serialize, Deserialize)]
struct SpStatePageSelector {
    last: SpIdentifier,
    request_id: SpStateRequestId,
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
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    query: Query<PaginationParams<Timeout, SpStatePageSelector>>,
) -> Result<HttpResponseOk<ResultsPage<SpInfo>>, HttpError> {
    let apictx = rqctx.context();
    let page_params = query.into_inner();
    let page_limit = rqctx.page_limit(&page_params)?.get() as usize;

    let (request_id, last_seen_target) = match page_params.page {
        WhichPage::First(timeout) => {
            // build overall timeout for the entire request
            let timeout = timeout
                .timeout_millis
                .map(|t| Duration::from_millis(u64::from(t)))
                .unwrap_or(apictx.timeouts.bulk_request_default)
                // TODO do we also want a floor for the timeout?
                .min(apictx.timeouts.bulk_request_max);
            let timeout = SpTimeout::from_now(timeout);

            let request_id = apictx
                .bulk_sp_state_requests
                .start(
                    timeout,
                    apictx.timeouts.bulk_request_retain_grace_period,
                )
                .await?;

            (request_id, None)
        }
        WhichPage::Next(page_selector) => {
            (page_selector.request_id, Some(page_selector.last))
        }
    };

    let progress = apictx
        .bulk_sp_state_requests
        .get(
            &request_id,
            last_seen_target.map(Into::into),
            SpTimeout::from_now(apictx.timeouts.bulk_request_page),
            page_limit,
        )
        .await?;

    // TODO it's weird that we're dropping information here. maybe "page timeout
    // reached" and "page limit reached" really are equivalent from the client's
    // point of view (as long as it doesn't interpret "fewer than limit" items
    // as the end), but ideally we'd omit sending a page token back if we're in
    // "complete". this is dependent on dropshot changes; see
    // <https://github.com/oxidecomputer/dropshot/issues/20>.
    let items = match progress {
        BulkStateProgress::PageTimeoutReached(items) => items,
        BulkStateProgress::PageLimitReached(items) => items,
        BulkStateProgress::Complete(items) => items,
    };

    let items = items
        .into_iter()
        .map(|BulkSpStateSingleResult { sp, state, result }| {
            let details = match result {
                Ok(details) => details,
                Err(err) => match &*err {
                    // TODO Treating "communication failed" and "we don't know
                    // the IP address" as "unresponsive" may not be right. Do we
                    // need more refined errors?
                    SpCommsError::Timeout { .. }
                    | SpCommsError::SpCommunicationFailed(_)
                    | SpCommsError::BadIgnitionTarget(_)
                    | SpCommsError::LocalIgnitionControllerAddressUnknown
                    | SpCommsError::SpAddressUnknown(_) => {
                        SpState::Unresponsive
                    }
                    // These errors should not be possible for the request we
                    // made.
                    SpCommsError::SpDoesNotExist(_)
                    | SpCommsError::UpdateFailed(_) => {
                        unreachable!("impossible error {}", err)
                    }
                },
            };
            Ok(SpInfo {
                info: SpIgnitionInfo { id: sp.into(), details: state.into() },
                details,
            })
        })
        .collect::<Result<Vec<_>, Arc<SpCommsError>>>()
        .map_err(http_err_from_comms_err)?;

    Ok(HttpResponseOk(ResultsPage::new(
        items,
        &request_id,
        |sp_info, &request_id| SpStatePageSelector {
            last: sp_info.info.id,
            request_id,
        },
    )?))
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

    let details = if state.is_powered_on() {
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
    pub image: Vec<u8>,
}

/// Update an SP
///
/// Copies a new image to the alternate bank of the SP flash.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/update",
}]
async fn sp_update(
    rqctx: Arc<RequestContext<Arc<ServerContext>>>,
    path: Path<PathSp>,
    body: TypedBody<UpdateBody>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let comms = &rqctx.context().sp_comms;
    let sp = path.into_inner().sp;
    let image = body.into_inner().image;

    comms
        .update(sp.into(), SpComponent::SP_ITSELF, 0, image)
        .await
        .map_err(http_err_from_comms_err)?;

    Ok(HttpResponseUpdatedNoContent {})
}

// TODO-completeness: Either add a new endpoint to allow for updating
// components, or expand the above endpoint to cover that case in addition to
// updating the SP itself.

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

/// Power on an SP via Ignition
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power-on",
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

/// Power off an SP via Ignition
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power-off",
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
        api.register(sp_update)?;
        api.register(sp_reset)?;
        api.register(sp_component_list)?;
        api.register(sp_component_get)?;
        api.register(sp_component_serial_console_attach)?;
        api.register(sp_component_serial_console_detach)?;
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
