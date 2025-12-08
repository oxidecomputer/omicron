// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Artifact types for Sled Agent API v1.
//!
//! This module contains types related to artifacts used by the sled agent.
//!
//! Per RFD 619, these types are defined in the earliest version they appear in
//! and are used by business logic via floating identifiers in sled-agent-types.

use std::collections::BTreeSet;

use omicron_common::api::external::Generation;
use omicron_common::ledger::Ledgerable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;

/// Artifact configuration.
///
/// This type is used in both GET (response) and PUT (request) operations.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ArtifactConfig {
    pub generation: Generation,
    pub artifacts: BTreeSet<ArtifactHash>,
}

impl Ledgerable for ArtifactConfig {
    fn is_newer_than(&self, other: &ArtifactConfig) -> bool {
        self.generation > other.generation
    }

    // No need to do this, the generation number is provided externally.
    fn generation_bump(&mut self) {}
}
