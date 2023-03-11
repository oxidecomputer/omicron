// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's API.
//!
//! Note that the bootstrap agent also communicates over Sprockets,
//! and has a separate interface for establishing the trust quorum.

use crate::bootstrap::agent::Agent;
use crate::updates::Component;
use dropshot::{
    endpoint, ApiDescription, HttpError, HttpResponseOk, RequestContext,
};
use omicron_common::api::external::Error;
use std::sync::Arc;

type BootstrapApiDescription = ApiDescription<Arc<Agent>>;

/// Returns a description of the bootstrap agent API
pub(crate) fn api() -> BootstrapApiDescription {
    fn register_endpoints(
        api: &mut BootstrapApiDescription,
    ) -> Result<(), String> {
        api.register(components_get)?;
        Ok(())
    }

    let mut api = BootstrapApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
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
