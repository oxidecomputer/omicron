// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use dropshot::{
    endpoint, ApiDescription, FreeformBody, HttpError, HttpResponseHeaders,
    HttpResponseOk, HttpResponseUpdatedNoContent, Path, RequestContext,
    TypedBody,
};
use hyper::{header, Body, StatusCode};
use installinator_common::ProgressReport;
use omicron_common::update::{ArtifactHashId, ArtifactId};
use schemars::JsonSchema;
use serde::Deserialize;
use uuid::Uuid;

use crate::{context::ServerContext, ProgressReportStatus};

type ArtifactServerApiDesc = ApiDescription<ServerContext>;

/// Return a description of the artifact server api for use in generating an OpenAPI spec
pub fn api() -> ArtifactServerApiDesc {
    fn register_endpoints(
        api: &mut ArtifactServerApiDesc,
    ) -> Result<(), String> {
        api.register(get_artifact_by_id)?;
        api.register(get_artifact_by_hash)?;
        api.register(report_progress)?;
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
) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError> {
    match rqctx.context().artifact_store.get_artifact(&path.into_inner()).await
    {
        Some((body, size)) => Ok(body_to_artifact_response(body, size)),
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
) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError> {
    match rqctx
        .context()
        .artifact_store
        .get_artifact_by_hash(&path.into_inner())
        .await
    {
        Some((body, size)) => Ok(body_to_artifact_response(body, size)),
        None => {
            Err(HttpError::for_not_found(None, "Artifact not found".into()))
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(crate) struct ReportQuery {
    /// A unique identifier for the update.
    pub(crate) update_id: Uuid,
}

/// Report progress and completion to the server.
///
/// This method requires an `update_id` path parameter. This update ID is
/// matched against the server currently performing an update. If the server
/// is unaware of the update ID, it will return an HTTP 422 Unprocessable Entity
/// code.
#[endpoint {
    method = POST,
    path = "/report-progress/{update_id}",
}]
async fn report_progress(
    rqctx: RequestContext<ServerContext>,
    path: Path<ReportQuery>,
    event: TypedBody<ProgressReport>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let update_id = path.into_inner().update_id;
    match rqctx
        .context()
        .artifact_store
        .report_progress(update_id, event.into_inner())
        .await?
    {
        ProgressReportStatus::Processed => Ok(HttpResponseUpdatedNoContent()),
        ProgressReportStatus::UnrecognizedUpdateId => {
            Err(HttpError::for_client_error(
                None,
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("update ID {update_id} unrecognized by this server"),
            ))
        }
    }
}

fn body_to_artifact_response(
    body: Body,
    size: usize,
) -> HttpResponseHeaders<HttpResponseOk<FreeformBody>> {
    let mut response =
        HttpResponseHeaders::new_unnamed(HttpResponseOk(body.into()));
    let headers = response.headers_mut();
    headers.append(header::CONTENT_LENGTH, size.into());
    response
}
