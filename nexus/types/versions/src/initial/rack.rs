// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Rack types for version INITIAL.

use super::asset::AssetIdentityMetadata;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// View of an Rack
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Rack {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
}
