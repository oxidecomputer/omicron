// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{
    EmptyScanParams, HttpError, HttpResponseDeleted, HttpResponseOk,
    PaginationParams, Query, RequestContext, ResultsPage,
};
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[dropshot::api_description]
pub trait EreporterApi {
    type Context;

    /// Get a list of ereports, paginated by sequence number.
    #[endpoint {
        method = GET,
        path = "/ereports"
    }]
    async fn ereports_list(
        request_context: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, Generation>>,
    ) -> Result<HttpResponseOk<ResultsPage<Ereport>>, HttpError>;

    /// Informs the reporter that it may freely discard ereports with sequence
    /// numbers less than or equal to `seq`.
    #[endpoint {
        method = DELETE,
        path = "/ereports/{seq}"
    }]
    async fn ereports_truncate(
        request_context: RequestContext<Self::Context>,
        path: dropshot::Path<SeqPathParam>,
    ) -> Result<HttpResponseDeleted, HttpError>;
}

/// Path parameter to select a sequence number for
/// [`EreporterApi::ereports_truncate`].
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SeqPathParam {
    pub seq: Generation,
}

/// An error report.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Ereport {
    /// UUID of the entity that generated this ereport.
    pub reporter_id: Uuid,
    /// The ereport's sequence number, unique with regards to ereports generated
    /// by the entity with the `reporter_id`.
    pub seq: Generation,
    /// A string indicating the kind of ereport.
    ///
    /// This may be used by diagnosis engines as an indication of what `facts`
    /// to expect.
    pub class: String,
    /// The UTC timestamp when this ereport was observed, as determined by the reporter.
    pub time_created: DateTime<Utc>,
    /// The set of facts (key-value pairs) associated with this ereport.
    pub facts: HashMap<String, String>,
}
