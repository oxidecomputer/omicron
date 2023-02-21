// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use dropshot::{
    endpoint, ApiDescription, FreeformBody, HttpError, HttpResponseOk, Path,
    RequestContext,
};
use omicron_common::update::{ArtifactHashId, ArtifactId};

use crate::context::ServerContext;

type ArtifactServerApiDesc = ApiDescription<ServerContext>;

/// Return a description of the artifact server api for use in generating an OpenAPI spec
pub fn api() -> ArtifactServerApiDesc {
    fn register_endpoints(
        api: &mut ArtifactServerApiDesc,
    ) -> Result<(), String> {
        api.register(get_artifact_by_id)?;
        api.register(get_artifact_by_hash)?;
        Ok(())
    }

    let mut api = ArtifactServerApiDesc::new();
    if let Err(err) = register_endpoints(&mut api) {
        panic!("failed to register entrypoints: {}", err);
    }
    api
}

/// Fetch an artifact from this server.
#[endpoint {
    method = GET,
    path = "/artifacts/by-id/{kind}/{name}/{version}"
}]
async fn get_artifact_by_id(
    rqctx: RequestContext<ServerContext>,
    // NOTE: this is an `ArtifactId` and not an `UpdateArtifactId`, because this
    // code might be dealing with an unknown artifact kind. This can happen
    // if a new artifact kind is introduced across version changes.
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

/// Fetch an artifact by hash.
#[endpoint {
    method = GET,
    path = "/artifacts/by-hash/{kind}/{hash}",
}]
async fn get_artifact_by_hash(
    rqctx: RequestContext<ServerContext>,
    path: Path<ArtifactHashId>,
) -> Result<HttpResponseOk<FreeformBody>, HttpError> {
    match rqctx
        .context()
        .artifact_store
        .get_artifact_by_hash(&path.into_inner())
        .await
    {
        Some(body) => Ok(HttpResponseOk(body.into())),
        None => {
            Err(HttpError::for_not_found(None, "Artifact not found".into()))
        }
    }
}
