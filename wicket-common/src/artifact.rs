// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact_v2::Artifact;
use tufaceous_artifact_v2::ArtifactVersion;

/// The identifying components of an [`Artifact`], sent over the wire from
/// wicketd to the wicket client to show versions of artifacts we will be
/// updating to.
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
    /// The Tufaceous tags used to select the artifact.
    pub tags: BTreeMap<String, String>,

    /// The artifact's version string.
    pub version: ArtifactVersion,
}

impl ArtifactId {
    pub fn new(artifact: &Artifact) -> Self {
        Self { tags: artifact.tags.clone(), version: artifact.version.clone() }
    }
}
