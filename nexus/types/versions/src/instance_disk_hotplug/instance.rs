// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq,
)]
pub struct InstanceSlotNumber(pub u16);

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceAttachDisk {
    pub disk: NameOrId,
    pub slot_number: Option<InstanceSlotNumber>,
}

impl From<crate::v2025_11_20_00::path_params::DiskPath> for InstanceAttachDisk {
    fn from(value: crate::v2025_11_20_00::path_params::DiskPath) -> Self {
        Self { disk: value.disk, slot_number: None }
    }
}
