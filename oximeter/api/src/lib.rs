// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{
    EmptyScanParams, HttpError, HttpResponseDeleted, HttpResponseOk,
    PaginationParams, Query, RequestContext, ResultsPage,
};
use dropshot_api_manager_types::api_versions;
use omicron_common::api::internal::nexus::ProducerEndpoint;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use uuid::Uuid;

api_versions!([
    // WHEN CHANGING THE API (part 1 of 2):
    //
    // +- Pick a new semver and define it in the list below.  The list MUST
    // |  remain sorted, which generally means that your version should go at
    // |  the very top.
    // |
    // |  Duplicate this line, uncomment the *second* copy, update that copy for
    // |  your new API version, and leave the first copy commented out as an
    // |  example for the next person.
    // v
    // (next_int, IDENT),
    (1, INITIAL),
]);

// WHEN CHANGING THE API (part 2 of 2):
//
// The call to `api_versions!` above defines constants of type
// `semver::Version` that you can use in your Dropshot API definition to specify
// the version when a particular endpoint was added or removed.  For example, if
// you used:
//
//     (2, ADD_FOOBAR)
//
// Then you could use `VERSION_ADD_FOOBAR` as the version in which endpoints
// were added or removed.

#[dropshot::api_description]
pub trait OximeterApi {
    type Context;

    /// List all producers.
    #[endpoint {
        method = GET,
        path = "/producers",
    }]
    async fn producers_list(
        request_context: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, ProducerPage>>,
    ) -> Result<HttpResponseOk<ResultsPage<ProducerEndpoint>>, HttpError>;

    /// Get details about a producer by ID.
    #[endpoint {
        method = GET,
        path = "/producers/{producer_id}",
    }]
    async fn producer_details(
        request_context: RequestContext<Self::Context>,
        path: dropshot::Path<ProducerIdPathParams>,
    ) -> Result<HttpResponseOk<ProducerDetails>, HttpError>;

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

/// Details about a previous successful collection.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct SuccessfulCollection {
    /// The time at which we started a collection.
    ///
    /// Note that this is the time we queued a request to collect for processing
    /// by a background task. The `time_queued` can be added to this time to
    /// figure out when processing began, and `time_collecting` can be added to
    /// that to figure out how long the actual collection process took.
    pub started_at: DateTime<Utc>,

    /// The time this request spent queued before being processed.
    pub time_queued: Duration,

    /// The time it took for the actual collection.
    pub time_collecting: Duration,

    /// The number of samples collected.
    pub n_samples: u64,
}

/// Details about a previous failed collection.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct FailedCollection {
    /// The time at which we started a collection.
    ///
    /// Note that this is the time we queued a request to collect for processing
    /// by a background task. The `time_queued` can be added to this time to
    /// figure out when processing began, and `time_collecting` can be added to
    /// that to figure out how long the actual collection process took.
    pub started_at: DateTime<Utc>,

    /// The time this request spent queued before being processed.
    pub time_queued: Duration,

    /// The time it took for the actual collection.
    pub time_collecting: Duration,

    /// The reason the collection failed.
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProducerDetails {
    /// The producer's ID.
    pub id: Uuid,

    /// The current collection interval.
    pub interval: Duration,

    /// The current collection address.
    pub address: SocketAddr,

    /// The time the producer was first registered with us.
    pub registered: DateTime<Utc>,

    /// The last time the producer's information was updated.
    pub updated: DateTime<Utc>,

    /// Details about the last successful collection.
    ///
    /// This is None if we've never successfully collected from the producer.
    pub last_success: Option<SuccessfulCollection>,

    /// Details about the last failed collection.
    ///
    /// This is None if we've never failed to collect from the producer.
    pub last_failure: Option<FailedCollection>,

    /// The total number of successful collections we've made.
    pub n_collections: u64,

    /// The total number of failed collections.
    pub n_failures: u64,
}

impl ProducerDetails {
    pub fn new(info: &ProducerEndpoint) -> Self {
        let now = Utc::now();
        Self {
            id: info.id,
            interval: info.interval,
            address: info.address,
            registered: now,
            updated: now,
            last_success: None,
            last_failure: None,
            n_collections: 0,
            n_failures: 0,
        }
    }

    /// Update with new producer information.
    ///
    /// # Panics
    ///
    /// This panics if the new information refers to a different ID.
    pub fn update(&mut self, new: &ProducerEndpoint) {
        assert_eq!(self.id, new.id);
        self.updated = Utc::now();
        self.address = new.address;
        self.interval = new.interval;
    }

    /// Update when we successfully complete a collection.
    pub fn on_success(&mut self, success: SuccessfulCollection) {
        self.last_success = Some(success);
        self.n_collections += 1;
    }

    /// Update when we fail to complete a collection.
    pub fn on_failure(&mut self, failure: FailedCollection) {
        self.last_failure = Some(failure);
        self.n_failures += 1;
    }
}
