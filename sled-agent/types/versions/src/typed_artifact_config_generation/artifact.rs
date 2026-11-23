// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Artifact types for Sled Agent API v52.

use std::collections::{BTreeMap, BTreeSet};

use omicron_generation_kinds::{ArtifactConfigGeneration, GenericGeneration};
use omicron_ledger::Ledgerable;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tufaceous_artifact::ArtifactHash;

use crate::v1;

/// Query parameters for artifact requests.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactQueryParam {
    pub generation: ArtifactConfigGeneration,
}

impl From<v1::artifact::ArtifactQueryParam> for ArtifactQueryParam {
    fn from(value: v1::artifact::ArtifactQueryParam) -> Self {
        let v1::artifact::ArtifactQueryParam { generation } = value;
        Self {
            generation: ArtifactConfigGeneration::from_untyped_generation(
                generation,
            ),
        }
    }
}

/// Response for listing artifacts.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ArtifactListResponse {
    pub generation: ArtifactConfigGeneration,
    pub list: BTreeMap<ArtifactHash, usize>,
}

impl From<ArtifactListResponse> for v1::artifact::ArtifactListResponse {
    fn from(value: ArtifactListResponse) -> Self {
        let ArtifactListResponse { generation, list } = value;
        Self { generation: generation.into_untyped_generation(), list }
    }
}

/// Artifact configuration.
///
/// This type is used in both GET (response) and PUT (request) operations.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct ArtifactConfig {
    pub generation: ArtifactConfigGeneration,
    pub artifacts: BTreeSet<ArtifactHash>,
}

impl Ledgerable for ArtifactConfig {
    fn is_newer_than(&self, other: &ArtifactConfig) -> bool {
        self.generation > other.generation
    }

    // No need to do this, the generation number is provided externally.
    fn generation_bump(&mut self) {}
}

impl From<ArtifactConfig> for v1::artifact::ArtifactConfig {
    fn from(value: ArtifactConfig) -> Self {
        let ArtifactConfig { generation, artifacts } = value;
        Self { generation: generation.into_untyped_generation(), artifacts }
    }
}

impl From<v1::artifact::ArtifactConfig> for ArtifactConfig {
    fn from(value: v1::artifact::ArtifactConfig) -> Self {
        let v1::artifact::ArtifactConfig { generation, artifacts } = value;
        Self {
            generation: ArtifactConfigGeneration::from_untyped_generation(
                generation,
            ),
            artifacts,
        }
    }
}
