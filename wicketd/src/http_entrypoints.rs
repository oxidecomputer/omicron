// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::artifacts::ArtifactId;
use crate::RackV1Inventory;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::FreeformBody;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::Path;
use dropshot::RequestContext;
use hyper::Body;
use std::sync::Arc;

use crate::ServerContext;

type WicketdApiDescription = ApiDescription<ServerContext>;

/// Return a description of the wicketd api for use in generating an OpenAPI spec
pub fn api() -> WicketdApiDescription {
    fn register_endpoints(
        api: &mut WicketdApiDescription,
    ) -> Result<(), String> {
        api.register(get_inventory)?;
        api.register(get_artifact)?;
        Ok(())
    }

    let mut api = WicketdApiDescription::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// A status endpoint used to report high level information known to wicketd.
///
/// This endpoint can be polled to see if there have been state changes in the
/// system that are useful to report to wicket.
///
/// Wicket, and possibly other callers, will retrieve the changed information,
/// with follow up calls.
#[endpoint {
    method = GET,
    path = "/inventory"
}]
async fn get_inventory(
    rqctx: Arc<RequestContext<ServerContext>>,
) -> Result<HttpResponseOk<RackV1Inventory>, HttpError> {
    match rqctx.context().mgs_handle.get_inventory().await {
        Ok(inventory) => Ok(HttpResponseOk(inventory)),
        Err(_) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
    }
}

/// Fetch an artifact from the in-memory cache.
#[endpoint {
    method = GET,
    path = "/artifacts/{name}/{version}"
}]
async fn get_artifact(
    rqctx: Arc<RequestContext<ServerContext>>,
    path: Path<ArtifactId>,
) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
    match rqctx.context().artifact_store.get_artifact(&path.into_inner()) {
        Some(bytes) => Ok(HttpResponseOk(Body::from(bytes).into())),
        None => {
            Err(HttpError::for_not_found(None, "Artifact not found".into()))
        }
    }
}

// TODO: hash verification/fetch artifact by hash?
