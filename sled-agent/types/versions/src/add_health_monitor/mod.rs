// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_HEALTH_MONITOR` of the Sled Agent API.
//!
//! This version adds a new `HealthMonitorInventory` type, and modifies
//! `Inventory` to include it as a field.

pub mod inventory;
