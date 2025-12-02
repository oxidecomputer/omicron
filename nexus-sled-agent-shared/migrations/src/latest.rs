// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Aliases for the latest version of each public type

use crate::v1;
use crate::v10;

pub type BootPartitionContents = v1::BootPartitionContents;
pub type Inventory = v10::Inventory;
pub type OmicronSledConfig = v10::OmicronSledConfig;
pub type ConfigReconcilerInventory = v10::ConfigReconcilerInventory;
pub type OmicronZoneConfig = v10::OmicronZoneConfig;
pub type OmicronZonesConfig = v10::OmicronZonesConfig;
pub type OmicronZoneType = v10::OmicronZoneType;
