// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's API.
//!
//! Note that the bootstrap agent also communicates over Sprockets,
//! and has a separate interface for establishing the trust quorum.

use super::context::BootstrapContext;
use crate::bootstrap::params::RackInitializeRequest;
use crate::updates::Component;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, RequestContext, TypedBody,
};
use omicron_common::api::external::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::Baseboard;
use std::sync::Arc;

type BootstrapApiDescription = ApiDescription<BootstrapContext>;

/// Returns a description of the bootstrap agent API
pub(super) fn api() -> BootstrapApiDescription {
    fn register_endpoints(
        api: &mut BootstrapApiDescription,
    ) -> Result<(), String> {
        api.register(baseboard_get)?;
        api.register(components_get)?;
        api.register(rack_initialization_status)?;
        api.register(rack_initialize)?;
        api.register(rack_reset)?;
        api.register(sled_reset_status)?;
        api.register(sled_reset)?;
        Ok(())
    }

    let mut api = BootstrapApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// Return the baseboard identity of this sled.
#[endpoint {
    method = GET,
    path = "/baseboard",
}]
async fn baseboard_get(
    rqctx: RequestContext<BootstrapContext>,
) -> Result<HttpResponseOk<Baseboard>, HttpError> {
    let ctx = rqctx.context();
    Ok(HttpResponseOk(ctx.agent.baseboard().clone()))
}

/// Provides a list of components known to the bootstrap agent.
///
/// This API is intended to allow early boot services (such as Wicket)
/// to query the underlying component versions installed on a sled.
#[endpoint {
    method = GET,
    path = "/components",
}]
async fn components_get(
    rqctx: RequestContext<BootstrapContext>,
) -> Result<HttpResponseOk<Vec<Component>>, HttpError> {
    let ctx = rqctx.context();
    let components =
        ctx.agent.components_get().await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseOk(components))
}

#[derive(
    Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RackInitializationStatus {
    NotRunning,
    Initializing,
    Initialized,
    InitializationFailed { reason: String },
    Resetting,
    Reset,
    ResetFailed { reason: String },
}

/// Get the current status of rack initialization (or reset).
#[endpoint {
    method = GET,
    path = "/rack-initialize",
}]
async fn rack_initialization_status(
    rqctx: RequestContext<BootstrapContext>,
) -> Result<HttpResponseOk<RackInitializationStatus>, HttpError> {
    let ctx = rqctx.context();
    let status = ctx.operation_interlock.rack_initialization_status();
    Ok(HttpResponseOk(status))
}

/// Initializes the rack with the provided configuration.
#[endpoint {
    method = POST,
    path = "/rack-initialize",
}]
async fn rack_initialize(
    rqctx: RequestContext<BootstrapContext>,
    body: TypedBody<RackInitializeRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ctx = rqctx.context();
    let request = body.into_inner();
    ctx.operation_interlock
        .rack_initialize(Arc::clone(&ctx.agent), request)
        .await
        .map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Resets the rack to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/rack-initialize",
}]
async fn rack_reset(
    rqctx: RequestContext<BootstrapContext>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ctx = rqctx.context();
    ctx.operation_interlock
        .rack_reset(Arc::clone(&ctx.agent))
        .await
        .map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}

#[derive(
    Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SledResetStatus {
    NotRunning,
    Resetting,
    Reset,
    ResetFailed { reason: String },
}

/// Get the current status of rack initialization (or reset).
#[endpoint {
    method = GET,
    path = "/sled-initialize",
}]
async fn sled_reset_status(
    rqctx: RequestContext<BootstrapContext>,
) -> Result<HttpResponseOk<SledResetStatus>, HttpError> {
    let ctx = rqctx.context();
    let status = ctx.operation_interlock.sled_reset_status();
    Ok(HttpResponseOk(status))
}

/// Resets this particular sled to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/sled-initialize",
}]
async fn sled_reset(
    rqctx: RequestContext<BootstrapContext>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ctx = rqctx.context();
    ctx.operation_interlock
        .sled_reset(Arc::clone(&ctx.agent))
        .await
        .map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}
