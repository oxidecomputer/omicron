// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

use dropshot::{
    endpoint, ApiDescription, ApiDescriptionRegisterError, FreeformBody,
    HttpError, HttpResponseHeaders, HttpResponseOk,
    HttpResponseUpdatedNoContent, Path, RequestContext, TypedBody,
};
use hyper::{header, Body, StatusCode};
use installinator_common::EventReport;
use omicron_common::update::ArtifactHashId;
use schemars::JsonSchema;
use serde::Deserialize;
use uuid::Uuid;

use crate::{context::ServerContext, EventReportStatus};

type ArtifactServerApiDesc = ApiDescription<ServerContext>;

/// Return a description of the artifact server api for use in generating an OpenAPI spec
pub fn api() -> ArtifactServerApiDesc {
    fn register_endpoints(
        api: &mut ArtifactServerApiDesc,
    ) -> Result<(), ApiDescriptionRegisterError> {
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
        Some((size, body)) => Ok(body_to_artifact_response(size, body)),
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
    report: TypedBody<EventReport>,
) -> Result<HttpResponseUpdatedNoContent, HttpError> {
    let update_id = path.into_inner().update_id;
    match rqctx
        .context()
        .artifact_store
        .report_progress(update_id, report.into_inner())
        .await?
    {
        EventReportStatus::Processed => Ok(HttpResponseUpdatedNoContent()),
        EventReportStatus::UnrecognizedUpdateId => {
            Err(HttpError::for_client_error(
                None,
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("update ID {update_id} unrecognized by this server"),
            ))
        }
        EventReportStatus::ReceiverClosed => Err(HttpError::for_client_error(
            None,
            StatusCode::GONE,
            format!("update ID {update_id}: receiver closed"),
        )),
    }
}

fn body_to_artifact_response(
    size: u64,
    body: Body,
) -> HttpResponseHeaders<HttpResponseOk<FreeformBody>> {
    let mut response =
        HttpResponseHeaders::new_unnamed(HttpResponseOk(body.into()));
    let headers = response.headers_mut();
    headers.append(header::CONTENT_LENGTH, size.into());
    response
}
