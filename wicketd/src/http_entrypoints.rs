// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! HTTP entrypoint functions for wicketd

use crate::artifacts::TufRepositoryId;
use crate::mgs::GetInventoryResponse;
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::HttpResponseUpdatedNoContent;
use dropshot::Path;
use dropshot::RequestContext;
use dropshot::UntypedBody;
use omicron_common::api::internal::nexus::UpdateArtifactId;

use crate::ServerContext;

type WicketdApiDescription = ApiDescription<ServerContext>;

/// Return a description of the wicketd api for use in generating an OpenAPI spec
pub fn api() -> WicketdApiDescription {
    fn register_endpoints(
        api: &mut WicketdApiDescription,
    ) -> Result<(), String> {
        api.register(get_inventory)?;
        api.register(put_repository)?;
        api.register(get_artifacts)?;
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
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<GetInventoryResponse>, HttpError> {
    match rqctx.context().mgs_handle.get_inventory().await {
        Ok(response) => Ok(HttpResponseOk(response)),
        Err(_) => {
            Err(HttpError::for_unavail(None, "Server is shutting down".into()))
        }
    }
}

/// An endpoint used to upload TUF repositories to the server.
#[endpoint {
    method = PUT,
    path = "/repositories/{name}/{version}",
}]
async fn put_repository(
    rqctx: RequestContext<ServerContext>,
    path: Path<TufRepositoryId>,
    body: UntypedBody,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    // TODO: do we need to return more information with the response?

    // TODO: `UntypedBody` is currently inefficient for large request bodies -- it does many copies
    // and allocations. Replace this with a better solution once it's available in dropshot.
    rqctx
        .context()
        .artifact_store
        .add_repository(path.into_inner(), body.as_bytes())?;
    Ok(HttpResponseUpdatedNoContent())
}

/// An endpoint used to report all available artifacts.
///
/// The order of the returned artifacts is unspecified, and may change between
/// calls even if the total set of artifacts has not.
#[endpoint {
    method = GET,
    path = "/artifacts",
}]
async fn get_artifacts(
    rqctx: RequestContext<ServerContext>,
) -> Result<HttpResponseOk<Vec<UpdateArtifactId>>, HttpError> {
    let artifacts = rqctx
        .context()
        .artifact_store
        .artifact_ids();
    Ok(HttpResponseOk(artifacts))
}
