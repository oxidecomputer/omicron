// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::vpc::VpcCreateDefaults;
use crate::v2025_11_20_00;
use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default resources to create with a project.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectCreateDefaults {
    /// Create a default VPC with the selected VPC defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vpc: Option<VpcCreateDefaults>,
}

/// Create-time parameters for a `Project`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// Default resources to create.
    ///
    /// If omitted, all default resources are created. If provided, only the
    /// selected default resources are created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<ProjectCreateDefaults>,
}

impl From<v2025_11_20_00::project::ProjectCreate> for ProjectCreate {
    fn from(old: v2025_11_20_00::project::ProjectCreate) -> Self {
        Self { identity: old.identity, defaults: None }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectCreate, ProjectCreateDefaults};
    use crate::v2026_08_19_02::vpc::{SubnetCreateDefaults, VpcCreateDefaults};
    use serde_json::json;

    #[test]
    fn defaults_wire_format() {
        let base = json!({
            "name": "my-project",
            "description": "My project",
        });

        let omitted: ProjectCreate =
            serde_json::from_value(base.clone()).unwrap();
        assert!(omitted.defaults.is_none());
        assert_eq!(serde_json::to_value(omitted).unwrap(), base);

        let empty: ProjectCreate = serde_json::from_value(json!({
            "name": "my-project",
            "description": "My project",
            "defaults": {},
        }))
        .unwrap();
        assert_eq!(
            empty.defaults.unwrap(),
            ProjectCreateDefaults { vpc: None }
        );

        let vpc_only: ProjectCreate = serde_json::from_value(json!({
            "name": "my-project",
            "description": "My project",
            "defaults": { "vpc": {} },
        }))
        .unwrap();
        assert_eq!(
            vpc_only.defaults.unwrap(),
            ProjectCreateDefaults {
                vpc: Some(VpcCreateDefaults { subnet: None }),
            }
        );

        let all: ProjectCreate = serde_json::from_value(json!({
            "name": "my-project",
            "description": "My project",
            "defaults": { "vpc": { "subnet": {} } },
        }))
        .unwrap();
        assert_eq!(
            all.defaults.unwrap(),
            ProjectCreateDefaults {
                vpc: Some(VpcCreateDefaults {
                    subnet: Some(SubnetCreateDefaults {}),
                }),
            }
        );

        let duplicate_vpc = r#"{
            "name": "my-project",
            "description": "My project",
            "defaults": { "vpc": {}, "vpc": { "subnet": {} } }
        }"#;
        assert!(serde_json::from_str::<ProjectCreate>(duplicate_vpc).is_err());

        let unknown_default = json!({
            "name": "my-project",
            "description": "My project",
            "defaults": { "vpcc": {} },
        });
        assert!(
            serde_json::from_value::<ProjectCreate>(unknown_default).is_err()
        );
    }
}
