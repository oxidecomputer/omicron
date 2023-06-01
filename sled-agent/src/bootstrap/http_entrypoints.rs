// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's API.
//!
//! Note that the bootstrap agent also communicates over Sprockets,
//! and has a separate interface for establishing the trust quorum.

use crate::bootstrap::agent::Agent;
use crate::bootstrap::params::RackInitializeRequest;
use crate::updates::Component;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk,
    HttpResponseUpdatedNoContent, RequestContext, TypedBody,
};
use omicron_common::api::external::Error;
use sled_hardware::Baseboard;
use std::sync::Arc;

type BootstrapApiDescription = ApiDescription<Arc<Agent>>;

/// Returns a description of the bootstrap agent API
pub(crate) fn api() -> BootstrapApiDescription {
    fn register_endpoints(
        api: &mut BootstrapApiDescription,
    ) -> Result<(), String> {
        api.register(baseboard_get)?;
        api.register(components_get)?;
        api.register(rack_initialize)?;
        api.register(rack_reset)?;
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
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseOk<Baseboard>, HttpError> {
    let ba = rqctx.context();
    Ok(HttpResponseOk(ba.baseboard().clone()))
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
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseOk<Vec<Component>>, HttpError> {
    let ba = rqctx.context();
    let components = ba.components_get().await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseOk(components))
}

/// Initializes the rack with the provided configuration.
#[endpoint {
    method = POST,
    path = "/rack-initialize",
}]
async fn rack_initialize(
    rqctx: RequestContext<Arc<Agent>>,
    body: TypedBody<RackInitializeRequest>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ba = rqctx.context();
    let request = body.into_inner();
    ba.rack_initialize(request).await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Resets the rack to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/rack-initialize",
}]
async fn rack_reset(
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ba = rqctx.context();
    ba.rack_reset().await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}

/// Resets this particular sled to an unconfigured state.
#[endpoint {
    method = DELETE,
    path = "/sled-initialize",
}]
async fn sled_reset(
    rqctx: RequestContext<Arc<Agent>>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let ba = rqctx.context();
    ba.sled_reset().await.map_err(|e| Error::from(e))?;
    Ok(HttpResponseUpdatedNoContent())
}
