// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that changed in `SKIP_DEFAULT_VPC`.

use crate::v2025_11_20_00;
use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Create-time parameters for a `Project`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub skip_default_vpc: bool,
}

impl From<v2025_11_20_00::project::ProjectCreate> for ProjectCreate {
    fn from(old: v2025_11_20_00::project::ProjectCreate) -> Self {
        Self { identity: old.identity, skip_default_vpc: false }
    }
}
