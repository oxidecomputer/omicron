// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `INVENTORY_BASEBOARD_ID` of the Sled Agent API.
//!
//! The sled inventory now reports its baseboard as a `BaseboardId` (part
//! number and serial number) rather than the full `Baseboard` enum.

pub mod inventory;
