// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_DISK_LOCATION_TO_INVENTORY` of the Sled Agent API.
//!
//! This version adds a `location` field to `InventoryDisk`, carrying the
//! chassis label the platform's hardware topology gives the disk's bay or
//! socket, and renames its `slot` field to `pcie_slot` to say what that
//! number is.

pub mod inventory;
