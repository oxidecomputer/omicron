// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact::Artifact;
use tufaceous_artifact::ArtifactVersion;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct ArtifactId {
    pub tags: BTreeMap<String, String>,
    pub version: ArtifactVersion,
}

impl ArtifactId {
    pub fn new(artifact: &Artifact) -> Self {
        Self { tags: artifact.tags.clone(), version: artifact.version.clone() }
    }
}
