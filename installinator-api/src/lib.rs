// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The REST API that installinator is a client of.
//!
//! Note that most of our APIs are named by their server. This one is instead
//! named by the client, since it is expected that multiple services will
//! implement it.

use anyhow::Result;
use dropshot::{
    Body, ConfigDropshot, FreeformBody, HandlerTaskMode, HttpError,
    HttpResponseHeaders, HttpResponseOk, HttpResponseUpdatedNoContent, Path,
    RequestContext, TypedBody,
};
use hyper::header;
use installinator_common::EventReport;
use omicron_uuid_kinds::MupdateUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use tufaceous_artifact::ArtifactHashId;

const PROGRESS_REPORT_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReportQuery {
    /// A unique identifier for the update.
    pub update_id: MupdateUuid,
}

#[dropshot::api_description]
pub trait InstallinatorApi {
    type Context;

    /// Fetch an artifact by hash.
    #[endpoint {
        method = GET,
        path = "/artifacts/by-hash/{kind}/{hash}",
    }]
    async fn get_artifact_by_hash(
        rqctx: RequestContext<Self::Context>,
        path: Path<ArtifactHashId>,
    ) -> Result<HttpResponseHeaders<HttpResponseOk<FreeformBody>>, HttpError>;

    /// Report progress and completion to the server.
    ///
    /// This method requires an `update_id` path parameter. This update ID is
    /// matched against the server currently performing an update. If the
    /// server is unaware of the update ID, it will return an HTTP 422
    /// Unprocessable Entity code.
    #[endpoint {
        method = POST,
        path = "/report-progress/{update_id}",
        request_body_max_bytes = PROGRESS_REPORT_MAX_BYTES,
    }]
    async fn report_progress(
        rqctx: RequestContext<Self::Context>,
        path: Path<ReportQuery>,
        report: TypedBody<EventReport>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;
}

/// Add a content length header to a response.
///
/// Intended to be called by `get_artifact_by_hash` implementations.
pub fn body_to_artifact_response(
    size: u64,
    body: Body,
) -> HttpResponseHeaders<HttpResponseOk<FreeformBody>> {
    let mut response =
        HttpResponseHeaders::new_unnamed(HttpResponseOk(body.into()));
    let headers = response.headers_mut();
    headers.append(header::CONTENT_LENGTH, size.into());
    response
}

/// The result of processing an installinator event report.
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[must_use]
pub enum EventReportStatus {
    /// This report was processed by the server.
    Processed,

    /// The update ID was not recognized by the server.
    UnrecognizedUpdateId,

    /// The progress receiver is closed.
    ReceiverClosed,
}

impl EventReportStatus {
    /// Convert this status to an HTTP result.
    ///
    /// Intended to be called by `report_progress` implementations.
    pub fn to_http_result(
        self,
        update_id: MupdateUuid,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        match self {
            EventReportStatus::Processed => Ok(HttpResponseUpdatedNoContent()),
            EventReportStatus::UnrecognizedUpdateId => {
                Err(HttpError::for_client_error(
                    None,
                    dropshot::ClientErrorStatusCode::UNPROCESSABLE_ENTITY,
                    format!(
                        "update ID {update_id} unrecognized by this server"
                    ),
                ))
            }
            EventReportStatus::ReceiverClosed => {
                Err(HttpError::for_client_error(
                    None,
                    dropshot::ClientErrorStatusCode::GONE,
                    format!("update ID {update_id}: receiver closed"),
                ))
            }
        }
    }
}

/// Creates a default `ConfigDropshot` for the installinator API.
pub fn default_config(bind_address: std::net::SocketAddr) -> ConfigDropshot {
    ConfigDropshot {
        bind_address,
        default_request_body_max_bytes: 1024,
        default_handler_task_mode: HandlerTaskMode::Detached,
        log_headers: vec![],
    }
}
