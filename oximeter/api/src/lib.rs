// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::{DateTime, Utc};
use dropshot::{
    EmptyScanParams, HttpError, HttpResponseDeleted, HttpResponseOk,
    PaginationParams, Query, RequestContext, ResultsPage,
};
use omicron_common::api::internal::nexus::ProducerEndpoint;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};
use uuid::Uuid;

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

    /// The last time we started to collect from this producer.
    ///
    /// This is None if we've never attempted to collect from the producer.
    pub last_collection_started: Option<DateTime<Utc>>,

    /// The last time we successfully completed a collection from this producer.
    ///
    /// This is None if we've never successfully collected from the producer.
    /// Note that this can be before `last_collection_started`, when we're in
    /// the middle of a collection or the last collection has failed.
    pub last_successful_collection: Option<DateTime<Utc>>,

    /// The number of samples collected in the last successful collection.
    pub n_samples_in_last_collection: Option<u64>,

    /// The duration of the last successful collection.
    pub last_collection_duration: Option<Duration>,

    /// The last time we failed to collect from this producer.
    ///
    /// This is None if we've never failed to collect from the producer. Note
    /// that this can be before `last_collection_started`, when we're in the
    /// middle of a collection or the last collection was successful.
    pub last_failed_collection: Option<DateTime<Utc>>,

    /// A string describing why the last collection failed.
    pub last_failure_reason: Option<String>,

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
            last_collection_started: None,
            last_successful_collection: None,
            n_samples_in_last_collection: None,
            last_collection_duration: None,
            last_failed_collection: None,
            last_failure_reason: None,
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

    /// Update when we start a collection
    pub fn start_collection(&mut self) {
        self.last_collection_started = Some(Utc::now());
    }

    /// Update when we successfully complete a collection.
    ///
    /// # Panics
    ///
    /// This panics if no collection was started.
    pub fn on_success(&mut self, n_samples: u64) {
        let start = self.last_collection_started.expect("Should have started");
        let now = Utc::now();
        self.last_successful_collection = Some(now);
        self.n_samples_in_last_collection = Some(n_samples);
        self.last_collection_duration =
            Some((now - start).to_std().unwrap_or(Duration::ZERO));
        self.n_collections += 1;
    }

    /// Update when we fail to complete a collection.
    ///
    /// # Panics
    ///
    /// This panics if no collection was started.
    pub fn on_failure(&mut self, reason: impl Into<String>) {
        assert!(self.last_collection_started.is_some());
        self.last_failed_collection = Some(Utc::now());
        self.last_failure_reason = Some(reason.into());
        self.n_failures += 1;
    }
}
