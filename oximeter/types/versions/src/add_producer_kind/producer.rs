// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Producer-related types for the Oximeter collector.

use crate::v1;
use chrono::{DateTime, Utc};
use omicron_common::api::internal::nexus::ProducerKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ProducerDetails {
    /// The producer's ID.
    pub id: Uuid,

    /// The kind of producer.
    pub kind: ProducerKind,

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
    pub last_success: Option<v1::producer::SuccessfulCollection>,

    /// Details about the last failed collection.
    ///
    /// This is None if we've never failed to collect from the producer.
    pub last_failure: Option<v1::producer::FailedCollection>,

    /// The total number of successful collections we've made.
    pub n_collections: u64,

    /// The total number of failed collections.
    pub n_failures: u64,
}

impl From<ProducerDetails> for v1::producer::ProducerDetails {
    fn from(new: ProducerDetails) -> Self {
        Self {
            id: new.id,
            interval: new.interval,
            address: new.address,
            registered: new.registered,
            updated: new.updated,
            last_success: new.last_success,
            last_failure: new.last_failure,
            n_collections: new.n_collections,
            n_failures: new.n_failures,
        }
    }
}
