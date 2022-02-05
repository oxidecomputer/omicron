// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the gateway service

// Copyright 2022 Oxide Computer Company

use std::sync::Arc;

use dropshot::{
    endpoint, HttpError, HttpResponseOk, HttpResponseUpdatedNoContent,
    PaginationParams, Path, Query, RequestContext, ResultsPage, TypedBody,
};
use uuid::Uuid;

use crate::GatewayService;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, JsonSchema)]
struct ManagementNode;

#[derive(Serialize, JsonSchema)]
struct ManagementNodeComponent;

#[derive(Deserialize, JsonSchema)]
struct TimeoutAndCount {
    timeout_ms: Option<u32>,
    expected_count: Option<u32>,
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
) -> Result<HttpResponseOk<ResultsPage<ManagementNode>>, HttpError> {
    todo!()
}

#[derive(Deserialize, JsonSchema)]
struct PathSp {
    /// ID for the SP that the gateway service translates into the appropriate
    /// port for communicating with the given SP.
    #[serde(flatten)]
    sp: SpIdentifier,
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
) -> Result<HttpResponseOk<ResultsPage<ManagementNodeComponent>>, HttpError> {
    todo!()
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
) -> Result<HttpResponseOk<ManagementNodeComponent>, HttpError> {
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
) -> Result<HttpResponseOk<ResultsPage<ManagementNodeComponent>>, HttpError> {
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

/// Power on an SP via Ignition
#[endpoint {
    method = POST,
    path = "/sp/{type}/{slot}/power_on",
}]
async fn sp_power_on(
    _rqctx: Arc<RequestContext<GatewayService>>,
    _path: Path<PathSp>,
    // TODO do we need a timeout?
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    todo!()
}

// TODO
// - Do we want a separate collection of interfaces for Ignition facilities
// other than power on/off/cycle?
