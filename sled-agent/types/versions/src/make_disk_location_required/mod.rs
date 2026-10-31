// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MAKE_DISK_LOCATION_REQUIRED` of the Sled Agent API.
//!
//! This version makes `InventoryDisk::location` required: sled-agent finds
//! disks through the hardware topology, so every disk it reports has one.

pub mod inventory;
