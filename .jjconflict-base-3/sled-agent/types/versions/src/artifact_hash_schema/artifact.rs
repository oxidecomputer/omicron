// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Deserialize;
use tufaceous_artifact::ArtifactHash;

use crate::v1;

/// Path parameters for Artifact requests.
#[derive(Deserialize, JsonSchema)]
pub struct ArtifactPathParam {
    pub sha256: ArtifactHash,
}

impl From<v1::artifact::ArtifactPathParam> for ArtifactPathParam {
    fn from(value: v1::artifact::ArtifactPathParam) -> Self {
        Self { sha256: value.sha256 }
    }
}
