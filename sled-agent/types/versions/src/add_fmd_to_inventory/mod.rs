// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_FMD_TO_INVENTORY` of the Sled Agent API.
//!
//! This version adds FMD (Fault Management Daemon) data to the sled inventory
//! response, exposing diagnosed faults and affected resources.

pub mod inventory;
