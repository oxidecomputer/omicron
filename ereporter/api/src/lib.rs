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

    /// Get a list of ereports from `reporter_id`, paginated by sequence number.
    #[endpoint {
        method = GET,
        path = "/ereports/{id}/{generation}"
    }]
    async fn ereports_list(
        request_context: RequestContext<Self::Context>,
        path: dropshot::Path<ReporterId>,
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
        path: dropshot::Path<AcknowledgePathParams>,
    ) -> Result<HttpResponseDeleted, HttpError>;
}

/// Path parameters to  the [`EreporterApi::ereports_acknowledge`] endpoint.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AcknowledgePathParams {
    #[serde(flatten)]
    pub reporter: ReporterId,

    pub seq: Generation,
}

/// An entry in the ereport batch returned by a reporter.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Entry {
    /// The identifier of the entity that generated this ereport.
    pub reporter: ReporterId,

    /// The ereport's sequence number, unique with regards to ereports generated
    /// by the entity with the `reporter_id`.
    pub seq: Generation,

    pub value: EntryKind,
}

/// A reporter is uniquely identified by its UUID (preserved across restarts)
/// and its generation number (incremented when the reporter re-registers itself
/// after a restart).
#[derive(
    Copy, Clone, Debug, Deserialize, JsonSchema, Serialize, Eq, PartialEq,
)]
pub struct ReporterId {
    /// The reporter's UUID.
    pub id: Uuid,
    /// The reporter's generation.
    pub generation: Generation,
}

/// Kinds of entry in a batch.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    /// An ereport.
    Ereport(Ereport),
    /// Ereports may have been lost.
    DataLoss {
        /// The number of ereports that were discarded, if it is known.
        ///
        /// If ereports are dropped because a buffer has reached its capacity,
        /// the reporter is strongly encouraged to attempt to count the number
        /// of ereports lost. In other cases, such as a reporter crashing and
        /// restarting, the reporter may not be capable of determining the
        /// number of ereports that were lost, or even *if* data loss actually
        /// occurred. Therefore, a `None` here indicates *possible* data loss,
        /// while a `Some(u32)` indicates *known* data loss.
        dropped: Option<u32>,
    },
}

/// An error report.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Ereport {
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
