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
    ) -> Result<HttpResponseOk<ResultsPage<Entry>>, HttpError>;

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

/// An entry in the ereport batch returned by a reporter.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct Entry {
    /// UUID of the entity that generated this ereport.
    pub reporter_id: Uuid,
    /// The ereport's sequence number, unique with regards to ereports generated
    /// by the entity with the `reporter_id`.
    pub seq: Generation,

    pub value: EntryKind,
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
