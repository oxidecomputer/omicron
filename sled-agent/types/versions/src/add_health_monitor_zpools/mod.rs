// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_HEALTH_MONITOR_ZPOOLS` of the Sled Agent API.
//!
//! This version modifies the `HealthMonitorInventory` type. It adds a new
//! `unhealthy_zpools` field. Additionally, the `Inventory` type is updated
//! to use the new `HealthMonitorInventory` type.

pub mod inventory;
