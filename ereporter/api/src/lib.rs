// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::{
    EmptyScanParams, HttpError, HttpResponseDeleted, HttpResponseOk,
    HttpResponseUpdatedNoContent, PaginationParams, Path, Query,
    RequestContext, ResultsPage, TypedBody,
};
pub use ereporter_types::*;
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[dropshot::api_description]
pub trait EreporterApi {
    type Context;

    /// Get a list of ereports from `reporter_id`, paginated by sequence number.
    #[endpoint {
        method = GET,
        path = "/ereports/{id}/{generation}"
    }]
    async fn ereports_list(
        request_context: RequestContext<Self::Context>,
        path: Path<ReporterId>,
        query: Query<PaginationParams<EmptyScanParams, Generation>>,
    ) -> Result<HttpResponseOk<ResultsPage<Entry>>, HttpError>;

    /// Informs the reporter with the given `reporter_id` that its ereports up
    /// to the sequence number `seq` have been successfully ingested into
    /// persistant storage.
    ///
    /// The reporter may now freely discard ereports with sequence numbers less
    /// than or equal to `seq`.
    #[endpoint {
        method = DELETE,
        path = "/ereports/{id}/{generation}/{seq}"
    }]
    async fn ereports_acknowledge(
        request_context: RequestContext<Self::Context>,
        path: Path<AcknowledgePathParams>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    #[endpoint {
        method = PUT,
        path = "/ereporter/{id}",
    }]
    async fn ereporter_put_generation(
        request_context: RequestContext<Self::Context>,
        path: Path<PutGenerationPathParams>,
        body: TypedBody<PutGenerationBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;
}

/// Path parameters to the [`EreporterApi::ereports_acknowledge`] endpoint.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AcknowledgePathParams {
    #[serde(flatten)]
    pub reporter: ReporterId,

    pub seq: Generation,
}

/// Request body for the [`EreporterApi::ereporter_put_generation`] endpoint.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct PutGenerationBody {
    /// ID of the reporter generation.
    pub generation_id: Uuid,

    /// The generation number assigned by Nexus to the ereporter at this
    /// generation.
    pub generation: Generation,
}

#[derive(
    Copy, Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq,
)]
pub struct PutGenerationPathParams {
    /// The reporter's UUID.
    pub id: Uuid,
}
