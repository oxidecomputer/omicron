// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Durable record of "what services should be initialized on this Sled?"

use crate::params::ServiceZoneRequest;

use camino::{Utf8Path, Utf8PathBuf};
use omicron_common::api::external::Generation;
use omicron_common::ledger::Ledgerable;

/// A wrapper around `ZoneRequest`, which allows it to be serialized
/// to a JSON file.
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct AllZoneRequests {
    generation: Generation,
    requests: Vec<ZoneRequest>,
}

impl AllZoneRequests {
    pub fn generation(&self) -> &Generation {
        &self.generation
    }

    pub fn set_generation(&mut self, generation: Generation) {
        self.generation = generation;
    }

    pub fn requests(&self) -> &Vec<ZoneRequest> {
        &self.requests
    }

    pub fn requests_mut(&mut self) -> &mut Vec<ZoneRequest> {
        &mut self.requests
    }

    pub fn take_requests(self) -> Vec<ZoneRequest> {
        self.requests
    }
}

impl Default for AllZoneRequests {
    fn default() -> Self {
        Self { generation: Generation::new(), requests: vec![] }
    }
}

impl Ledgerable for AllZoneRequests {
    fn is_newer_than(&self, other: &AllZoneRequests) -> bool {
        self.generation >= other.generation
    }

    fn generation_bump(&mut self) {
        self.generation = self.generation.next();
    }
}

/// The combo of "what zone did you ask for" + "where did we put it".
#[derive(Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ZoneRequest {
    zone: ServiceZoneRequest,
    // TODO: Consider collapsing "root" into ServiceZoneRequest
    #[schemars(with = "String")]
    root: Utf8PathBuf,
}

impl ZoneRequest {
    pub fn new(zone: ServiceZoneRequest, root: Utf8PathBuf) -> Self {
        ZoneRequest { zone, root }
    }

    pub fn zone(&self) -> &ServiceZoneRequest {
        &self.zone
    }

    pub fn root(&self) -> &Utf8Path {
        &self.root
    }
}
