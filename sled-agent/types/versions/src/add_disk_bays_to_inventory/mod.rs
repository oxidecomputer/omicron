// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_DISK_BAYS_TO_INVENTORY` of the Sled Agent API.
//!
//! This version adds `disk_bays` to `Inventory`: every U.2 bay and M.2
//! socket of the sled's chassis and what the hardware topology found behind
//! it, including bays that are empty or hold a device sled-agent cannot use
//! as a disk. It also makes `InventoryDisk::location` required, since the
//! topology that supplies the label is now where sled-agent finds disks.

pub mod inventory;
