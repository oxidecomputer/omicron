// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types that changed in `PROJECT_CREATE_DEFAULTS`.

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

    /// Default resources to create.
    ///
    /// If omitted, all default resources are created. An empty list creates no
    /// default resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<Vec<ProjectDefault>>,
}

/// A default resource that can be created with a project.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectDefault {
    /// The default VPC and the selected defaults within it.
    Vpc(Vec<VpcDefault>),
}

/// A default resource that can be created within a VPC.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum VpcDefault {
    Subnet,
}

impl From<v2025_11_20_00::project::ProjectCreate> for ProjectCreate {
    fn from(old: v2025_11_20_00::project::ProjectCreate) -> Self {
        Self { identity: old.identity, defaults: None }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectDefault, VpcDefault};

    #[test]
    fn nested_defaults_wire_format() {
        let defaults = vec![ProjectDefault::Vpc(vec![VpcDefault::Subnet])];
        let value = serde_json::to_value(&defaults).unwrap();

        assert_eq!(value, serde_json::json!([{ "vpc": ["subnet"] }]));
        assert_eq!(
            serde_json::from_value::<Vec<ProjectDefault>>(value).unwrap(),
            defaults
        );
    }
}
