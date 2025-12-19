// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for producer types.

use crate::latest::producer::FailedCollection;
use crate::latest::producer::ProducerDetails;
use crate::latest::producer::SuccessfulCollection;
use chrono::Utc;
use omicron_common::api::internal::nexus::ProducerEndpoint;

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
