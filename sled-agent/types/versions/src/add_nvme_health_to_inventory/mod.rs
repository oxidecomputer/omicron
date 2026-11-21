// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_NVME_HEALTH_TO_INVENTORY` of the Sled Agent API.
//!
//! This version adds an `nvme_health` field to `InventoryDisk`, carrying the
//! disk's NVMe SMART / Health Information log page.

pub mod inventory;
