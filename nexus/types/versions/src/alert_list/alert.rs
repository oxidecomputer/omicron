// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::asset::AssetIdentityMetadata;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Alert {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    /// The alert's class.
    pub class: String,
    /// The schema version of the payload for this alert class.
    pub version: u32,
    /// The alert's data payload.
    pub alert: serde_json::Value,
}
