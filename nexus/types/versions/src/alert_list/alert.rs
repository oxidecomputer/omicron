// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v2025_11_20_00::asset::AssetIdentityMetadata;

pub mod power_shelf;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Alert {
    #[serde(flatten)]
    pub identity: AssetIdentityMetadata,
    #[serde(flatten)]
    pub alert: AlertPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "class")]
pub enum AlertPayload {
    PsuInserted(power_shelf::PsuInsertedVersions),
    PsuRemoved(power_shelf::PsuRemovedVersions),
}
