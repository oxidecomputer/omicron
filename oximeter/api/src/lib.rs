// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{
    EmptyScanParams, HttpError, HttpResponseDeleted, HttpResponseOk,
    HttpResponseUpdatedNoContent, PaginationParams, Query, RequestContext,
    ResultsPage, TypedBody,
};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[dropshot::api_description]
pub trait OximeterApi {
    type Context;

    /// Handle a request from Nexus to register a new producer with this collector.
    #[endpoint {
        method = POST,
        path = "/producers",
    }]
    async fn producers_post(
        request_context: RequestContext<Self::Context>,
        body: TypedBody<ProducerEndpoint>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError>;

    /// List all producers.
    #[endpoint {
        method = GET,
        path = "/producers",
    }]
    async fn producers_list(
        request_context: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, ProducerPage>>,
    ) -> Result<HttpResponseOk<ResultsPage<ProducerEndpoint>>, HttpError>;

    /// Delete a producer by ID.
    #[endpoint {
        method = DELETE,
        path = "/producers/{producer_id}",
    }]
    async fn producer_delete(
        request_context: RequestContext<Self::Context>,
        path: dropshot::Path<ProducerIdPathParams>,
    ) -> Result<HttpResponseDeleted, HttpError>;

    /// Return identifying information about this collector.
    #[endpoint {
        method = GET,
        path = "/info",
    }]
    async fn collector_info(
        request_context: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<CollectorInfo>, HttpError>;
}

/// Parameters for paginating the list of producers.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProducerPage {
    pub id: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProducerIdPathParams {
    pub producer_id: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CollectorInfo {
    /// The collector's UUID.
    pub id: Uuid,
    /// Last time we refreshed our producer list with Nexus.
    pub last_refresh: Option<DateTime<Utc>>,
}
