// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use std::sync::Arc;

use dropshot::{
    endpoint, ApiDescription, FreeformBody, HttpError, HttpResponseOk, Path,
    RequestContext,
};

use crate::{context::ServerContext, store::ArtifactId};

type ArtifactServerApiDesc = ApiDescription<ServerContext>;

/// Return a description of the artifact server api for use in generating an OpenAPI spec
pub fn api() -> ArtifactServerApiDesc {
    fn register_endpoints(
        api: &mut ArtifactServerApiDesc,
    ) -> Result<(), String> {
        api.register(get_artifact)?;
        Ok(())
    }

    let mut api = ArtifactServerApiDesc::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
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
    match rqctx.context().artifact_store.get_artifact(&path.into_inner()).await
    {
        Some(body) => Ok(HttpResponseOk(body.into())),
        None => {
            Err(HttpError::for_not_found(None, "Artifact not found".into()))
        }
    }
}
