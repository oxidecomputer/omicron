// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Artifact types for Sled Agent API v1.

use std::collections::{BTreeMap, BTreeSet};

use omicron_common::api::external::Generation;
use omicron_common::ledger::Ledgerable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;

/// Path parameters for Artifact requests.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactPathParam {
    pub sha256: ArtifactHash,
}

/// Query parameters for artifact requests.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactQueryParam {
    pub generation: Generation,
}

/// Request body for copying artifacts from a depot.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactCopyFromDepotBody {
    pub depot_base_url: String,
}

/// Response for listing artifacts.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactListResponse {
    pub generation: Generation,
    pub list: BTreeMap<ArtifactHash, usize>,
}

/// Response for copying artifacts from a depot.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactCopyFromDepotResponse {}

/// Response for putting an artifact.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactPutResponse {
    /// The number of valid M.2 artifact datasets we found on the sled. There is
    /// typically one of these datasets for each functional M.2.
    pub datasets: usize,

    /// The number of valid writes to the M.2 artifact datasets. This should be
    /// less than or equal to the number of artifact datasets.
    pub successful_writes: usize,
}

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
