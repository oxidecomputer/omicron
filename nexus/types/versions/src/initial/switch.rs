// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Switch types for version INITIAL.

use super::asset::AssetIdentityMetadata;
use super::hardware::Baseboard;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Opaque object representing link state. The contents of this object are not
/// yet stable.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SwitchLinkState {
    pub(crate) link: serde_json::Value,
    pub(crate) monitors: Option<serde_json::Value>,
}

impl schemars::JsonSchema for SwitchLinkState {
    fn json_schema(
        r#gen: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        let obj = schemars::schema::Schema::Object(
            schemars::schema::SchemaObject::default(),
        );
        r#gen.definitions_mut().insert(Self::schema_name(), obj.clone());
        obj
    }

    fn schema_name() -> String {
        "SwitchLinkState".to_owned()
    }
}

/// An operator's view of a Switch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Switch {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    pub baseboard: Baseboard,
    /// The rack to which this Switch is currently attached
    pub rack_id: Uuid,
}
