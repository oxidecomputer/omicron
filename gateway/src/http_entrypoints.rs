// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

//! HTTP entrypoint functions for the gateway service

use std::sync::Arc;

use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, PaginationParams, Path, Query,
    RequestContext, ResultsPage, TypedBody,
};

use crate::GatewayService;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, JsonSchema)]
struct SpInfo;

#[derive(Serialize, JsonSchema)]
#[serde(tag = "present")]
enum SpIgnitionInfo {
    #[serde(rename = "no")]
    Absent,
    #[serde(rename = "yes")]
    Present { id: u8, status: u8 },
}

#[derive(Serialize, JsonSchema)]
struct SpComponentInfo;

#[derive(Deserialize, JsonSchema)]
struct TimeoutAndCount {
    timeout_ms: Option<u32>,
    expected_count: Option<u32>,
}

#[derive(Deserialize, JsonSchema)]
struct Timeout {
    timeout: Option<u32>,
}

#[derive(Serialize, Deserialize)]
struct TimeoutAndCountSelector<T> {
    last: T,
    start_time: u64, // TODO
    count_so_far: u32,
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum SpType {
    Sled,
    Power,
    Switch,
}

#[derive(Serialize, Deserialize, JsonSchema)]
struct SpIdentifier {
    #[serde(rename = "type")]
    typ: SpType,
    slot: u32,
}

type TimeoutAndCountPaginationParams<T> =
    PaginationParams<TimeoutAndCount, TimeoutAndCountSelector<T>>;

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

/// List SPs on the management network
///
/// Since communication with SPs may be unreliable, consumers may specify an
/// optional timeout to override the default and/or an optional value for the
/// expected number of SPs (i.e. the number after which we don't expect
/// additional waiting to reveal more nodes on the management network).
///
/// This interface may return a page of SPs prior to reaching either the
/// timeout or expected count with the expectation that callers will keep
/// calling this interface until the terminal page is reached.
///
/// TODO this could use Ignition to detect presense of each component along
/// with its power state. We could merge that with data from the management
/// network.
#[endpoint {
    method = GET,
    path = "/sp",
}]
async fn sp_list(
    _rqctx: Arc<RequestContext<GatewayService>>,
    _query: Query<TimeoutAndCountPaginationParams<SpIdentifier>>,
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
    _rqctx: Arc<RequestContext<GatewayService>>,
    _path: Path<PathSp>,
    _query: Query<Timeout>,
) -> Result<HttpResponseOk<SpInfo>, HttpError> {
    todo!()
}

/// List components of an SP
///
/// A components is a distinct entity under an SP's direct control. This lists
/// all those components for an SP.
///
/// As communication with SPs may be unreliable, consumers may specify a
/// timeout and/or a count of the expected number of components. This interface
/// may return a page of components prior to reaching either the timeout or
/// expected count with the expectation that callers will keep calling this
/// interface until the terminal page is reached.
#[endpoint {
    method = GET,
    path = "/sp/{type}/{slot}/component",
}]
async fn sp_component_list(
    _rqctx: Arc<RequestContext<GatewayService>>,
    _path: Path<PathSp>,
    _query: Query<TimeoutAndCountPaginationParams<PathSpComponent>>,
) -> Result<HttpResponseOk<ResultsPage<SpComponentInfo>>, HttpError> {
    todo!()
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
    _rqctx: Arc<RequestContext<GatewayService>>,
    _path: Path<PathSpComponent>,
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
/// update mechanisms will return an error without any considertion of the
/// update bundle.
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/update",
}]
async fn sp_component_update(
    _rqctx: Arc<RequestContext<GatewayService>>,
    _path: Path<PathSpComponent>,
    _body: TypedBody<UpdateBody>,
) -> Result<HttpResponseOk<ResultsPage<SpComponentInfo>>, HttpError> {
    todo!()
}

/// Power on an SP component
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/power_on",
}]
async fn sp_component_power_on(
    _rqctx: Arc<RequestContext<GatewayService>>,
    _path: Path<PathSpComponent>,
    // TODO do we need a timeout?
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    todo!()
}

/// Power off an SP component
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/component/{component}/power_off",
}]
async fn sp_component_power_off(
    _rqctx: Arc<RequestContext<GatewayService>>,
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
#[endpoint {
    method = GET,
    path = "/ignition",
}]
async fn ignition_list(
    _rqctx: Arc<RequestContext<GatewayService>>,
    // TODO these pagination params aren't quite right
    _query: Query<TimeoutAndCountPaginationParams<SpIdentifier>>,
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
    _rqctx: Arc<RequestContext<GatewayService>>,
    _path: Path<PathSp>,
) -> Result<HttpResponseOk<SpIgnitionInfo>, HttpError> {
    todo!()
}

/// Power on an SP via Ignition
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power_on",
}]
async fn ignition_power_on(
    _rqctx: Arc<RequestContext<GatewayService>>,
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
    _rqctx: Arc<RequestContext<GatewayService>>,
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

type GatewayApiDescription = ApiDescription<GatewayService>;

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
