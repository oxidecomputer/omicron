// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for the bootstrap agent's exposed API
//!
//! Note that care must be taken when evolving this interface. In particular,
//! changes need to consider forward- and backward-compatibility of bootstrap
//! agents on other sleds that may be running different versions.
//!
//! Note also that changes to the interface will necessarily require updates to
//! code where we use the client. To do this, follow the method prescribed in
//! the repo README:
//!
//! 1. Update the interface
//! 2. `cargo build` will now succeed but `cargo test` will fail because the
//! `bootstrap-agent.json` file is out of date (and because the client calls no
//! longer match the server)
//! 3. Update the `bootstrap-agent.json` file: `EXPECTORATE=overwrite cargo
//! test`
//! 4. The build will now fail, so update the client calls as needed.
//!
//! Do not update client calls before updating `bootstrap-agent.json` or else
//! you won't be able to build the crate and therefore won't be able to
//! automatically updates `boostrap-agent.json`.

use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use dropshot::TypedBody;
use omicron_common::api::external::Error as ExternalError;
use std::sync::Arc;

use super::agent::Agent;
use super::{params::ShareRequest, views::ShareResponse};

/// Returns a description of the bootstrap agent API
pub(crate) fn ba_api() -> ApiDescription<Arc<Agent>> {
    fn register_endpoints(
        api: &mut ApiDescription<Arc<Agent>>,
    ) -> Result<(), String> {
        api.register(api_request_share)?;
        Ok(())
    }

    let mut api = ApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

#[endpoint {
    method = GET,
    path = "/request_share",
}]
async fn api_request_share(
    rqctx: Arc<RequestContext<Arc<Agent>>>,
    request: TypedBody<ShareRequest>,
) -> Result<HttpResponseOk<ShareResponse>, HttpError> {
    let bootstrap_agent = rqctx.context();

    let request = request.into_inner();
    Ok(HttpResponseOk(
        bootstrap_agent
            .request_share(request.identity)
            .await
            .map_err(|e| ExternalError::from(e))?,
    ))
}
