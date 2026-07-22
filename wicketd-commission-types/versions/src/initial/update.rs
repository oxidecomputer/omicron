// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Update types shared across wicketd's update APIs.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use gateway_types_versions::v1::component::SpIdentifier;

/// A non-empty set of service processors to act on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdateTargets(pub(crate) BTreeSet<SpIdentifier>);

impl UpdateTargets {
    pub fn new(
        targets: BTreeSet<SpIdentifier>,
    ) -> Result<Self, EmptyUpdateTargets> {
        if targets.is_empty() {
            Err(EmptyUpdateTargets)
        } else {
            Ok(Self(targets))
        }
    }
}

impl<'de> Deserialize<'de> for UpdateTargets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let targets = BTreeSet::<SpIdentifier>::deserialize(deserializer)?;
        UpdateTargets::new(targets).map_err(|EmptyUpdateTargets| {
            serde::de::Error::invalid_length(0, &"at least one target")
        })
    }
}

impl JsonSchema for UpdateTargets {
    fn schema_name() -> String {
        "UpdateTargets".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        use schemars::schema::{
            ArrayValidation, InstanceType, Metadata, Schema, SchemaObject,
        };

        Schema::Object(SchemaObject {
            metadata: Some(Box::new(Metadata {
                description: Some(
                    "A non-empty set of service processors to act on."
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(InstanceType::Array.into()),
            array: Some(Box::new(ArrayValidation {
                items: Some(generator.subschema_for::<SpIdentifier>().into()),
                min_items: Some(1),
                unique_items: Some(true),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

/// Error returned when UpdateTargets is constructed from an empty set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmptyUpdateTargets;

impl std::fmt::Display for EmptyUpdateTargets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("at least one target must be specified")
    }
}

impl std::error::Error for EmptyUpdateTargets {}
