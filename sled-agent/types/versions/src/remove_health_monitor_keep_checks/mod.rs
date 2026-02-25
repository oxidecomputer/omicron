// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `REMOVE_HEALTH_MONITOR_KEEP_CHECKS` of the Sled Agent API.
//!
//! This version removes the `HealthMonitorInventory` type, and modifies
//! `Inventory` to include the health check directly.

pub mod inventory;
