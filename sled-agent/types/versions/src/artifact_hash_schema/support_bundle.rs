// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tufaceous_artifact::ArtifactHash;

use crate::v1;

/// Query parameters for support bundle finalization.
#[derive(Deserialize, Serialize, JsonSchema)]
pub struct SupportBundleFinalizeQueryParams {
    pub hash: ArtifactHash,
}

impl From<v1::support_bundle::SupportBundleFinalizeQueryParams>
    for SupportBundleFinalizeQueryParams
{
    fn from(
        value: v1::support_bundle::SupportBundleFinalizeQueryParams,
    ) -> Self {
        Self { hash: value.hash }
    }
}
