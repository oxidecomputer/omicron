// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Aliases for the latest version of each public type

use crate::v1;
use crate::v10;

// Initial types moved from `inventory.rs`
pub type SledRole = v1::SledRole;
pub type BootPartitionContents = v1::BootPartitionContents;
pub type BootImageHeader = v1::BootImageHeader;
pub type InventoryDisk = v1::InventoryDisk;
pub type InventoryDataset = v1::InventoryDataset;
pub type InventoryZpool = v1::InventoryZpool;
pub type OrphanedDataset = v1::OrphanedDataset;
pub type RemoveMupdateOverrideInventory = v1::RemoveMupdateOverrideInventory;
pub type ConfigReconcilerInventoryResult = v1::ConfigReconcilerInventoryResult;
pub type ZoneImageResolverInventory = v1::ZoneImageResolverInventory;
pub type HostPhase2DesiredSlots = v1::HostPhase2DesiredSlots;
pub type HostPhase2DesiredContents = v1::HostPhase2DesiredContents;
pub type OmicronZoneDataset = v1::OmicronZoneDataset;

// Types updated in v10
pub type Inventory = v10::Inventory;
pub type OmicronSledConfig = v10::OmicronSledConfig;
pub type ConfigReconcilerInventory = v10::ConfigReconcilerInventory;
pub type OmicronZoneConfig = v10::OmicronZoneConfig;
pub type OmicronZonesConfig = v10::OmicronZonesConfig;
pub type OmicronZoneType = v10::OmicronZoneType;
