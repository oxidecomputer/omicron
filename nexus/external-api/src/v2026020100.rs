// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that changed in v2026020100.

use nexus_types::external_api::params;
use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

/// Create-time parameters for a `Project`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,
}

impl From<ProjectCreate> for params::ProjectCreate {
    fn from(ProjectCreate { identity }: ProjectCreate) -> Self {
        Self { identity, skip_default_vpc: false }
    }
}
