// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Producer-related types for the Oximeter collector.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// Parameters for paginating the list of producers.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProducerPage {
    pub id: Uuid,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProducerIdPathParams {
    pub producer_id: Uuid,
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
