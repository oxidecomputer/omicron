// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::vpc::VpcCreateDefaultsSelection;
use crate::v2025_11_20_00;
use omicron_common::api::external::IdentityMetadataCreateParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default resources to create in a project
///
/// Each field corresponds to one resource. Set a field to an object to create
/// that resource. Omit it or pass `null` to skip it.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectCreateDefaults {
    /// Create the default VPC. Omit this field or pass `null` to skip it.
    ///
    /// When present, the value also determines which of the VPC's own defaults
    /// to create: `{"type": "all"}` creates all of them, and `{"type":
    /// "explicit", "defaults": {...}}` creates only those specified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vpc: Option<VpcCreateDefaultsSelection>,
}

/// Create-time parameters for a `Project`
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct ProjectCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// Default resources to create in the project
    ///
    /// Omit this field or pass `null` to create all defaults: currently, a
    /// default VPC with its own defaults. Pass an object to specify which
    /// resources to create. `{}` creates none.
    ///
    /// For example, to create the default VPC but not its default subnet, pass
    /// `{"vpc": {"type": "explicit", "defaults": {}}}`.
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
    use crate::v2026_09_08_00::vpc::{
        SubnetCreateDefaults, VpcCreateDefaults, VpcCreateDefaultsSelection,
    };
    use serde_json::json;

    #[test]
    fn defaults_selection_rejects_unknown_fields() {
        for selection in [
            json!({ "type": "all", "defaults": {} }),
            json!({ "type": "explicit", "defaults": {}, "subnet": {} }),
        ] {
            let request = json!({
                "name": "my-project",
                "description": "My project",
                "defaults": { "vpc": selection },
            });
            let error = serde_json::from_value::<ProjectCreate>(request)
                .expect_err("unknown selection fields must be rejected");
            assert!(error.to_string().contains("unknown field"), "{error}");
        }
    }

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

        let vpc_all: ProjectCreate = serde_json::from_value(json!({
            "name": "my-project",
            "description": "My project",
            "defaults": { "vpc": { "type": "all" } },
        }))
        .unwrap();
        assert_eq!(
            vpc_all.defaults.unwrap(),
            ProjectCreateDefaults {
                vpc: Some(VpcCreateDefaultsSelection::All {}),
            }
        );

        let vpc_only: ProjectCreate = serde_json::from_value(json!({
            "name": "my-project",
            "description": "My project",
            "defaults": {
                "vpc": { "type": "explicit", "defaults": {} },
            },
        }))
        .unwrap();
        assert_eq!(
            vpc_only.defaults.unwrap(),
            ProjectCreateDefaults {
                vpc: Some(VpcCreateDefaultsSelection::Explicit {
                    defaults: VpcCreateDefaults { subnet: None },
                }),
            }
        );

        let selected: ProjectCreate = serde_json::from_value(json!({
            "name": "my-project",
            "description": "My project",
            "defaults": {
                "vpc": {
                    "type": "explicit",
                    "defaults": { "subnet": {} },
                },
            },
        }))
        .unwrap();
        assert_eq!(
            selected.defaults.unwrap(),
            ProjectCreateDefaults {
                vpc: Some(VpcCreateDefaultsSelection::Explicit {
                    defaults: VpcCreateDefaults {
                        subnet: Some(SubnetCreateDefaults {}),
                    },
                }),
            }
        );

        let duplicate_vpc = r#"{
            "name": "my-project",
            "description": "My project",
            "defaults": {
                "vpc": { "type": "all" },
                "vpc": { "type": "explicit", "defaults": {} }
            }
        }"#;
        assert!(serde_json::from_str::<ProjectCreate>(duplicate_vpc).is_err());

        let missing_selection_type = json!({
            "name": "my-project",
            "description": "My project",
            "defaults": { "vpc": {} },
        });
        assert!(
            serde_json::from_value::<ProjectCreate>(missing_selection_type)
                .is_err()
        );

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
