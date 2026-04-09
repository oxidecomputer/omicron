// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact::Artifact;
use tufaceous_artifact::ArtifactVersion;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ArtifactId {
    pub version: ArtifactVersion,
    pub tags: BTreeMap<String, String>,
}

impl ArtifactId {
    pub fn new(artifact: &Artifact) -> Self {
        Self { version: artifact.version.clone(), tags: artifact.tags.clone() }
    }
}
