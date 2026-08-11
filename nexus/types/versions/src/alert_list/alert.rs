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
    pub alert: AlertPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "class", content = "alert")]
// TODO ELIZA remove this and put back the explicit `serde(rename)`s once
// https://github.com/oxidecomputer/omicron/issues/11005#issuecomment-5257792617
// is figured out...
#[serde(rename_all = "snake_case")]
// EXTREMELY IMPORTANT NOTE: Each variant of this enum *must* have a
// `#[serde(rename = "")]` that renames it to the same string as the
// `AlertClass` variant that corresponds to that alert class in
// `nexus-alert-types`.
pub enum AlertPayload {
    // #[serde(rename = "hardware.power_shelf.psu.insert")]
    PsuInserted(power_shelf::PsuInsertedVersions),
    // #[serde(rename = "hardware.power_shelf.psu.remove")]
    PsuRemoved(power_shelf::PsuRemovedVersions),
}
