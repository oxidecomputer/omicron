// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_RACK_ID_TO_INVENTORY` of the Sled Agent API.
//!
//! This version adds the `rack_id` field to inventory. This field reports this
//! sled's rack ID, which is a necessary step toward supporting multirack
//! regional clusters.
//!
//! When a new rack joins the underlay network, there will be no existing `rack`
//! row with the new sled-agents' rack ID, so we will be able to determine which
//! sled agents have not yet been adopted into the cluster by doing an anti-join
//! on the `rack` table.

pub mod inventory;
