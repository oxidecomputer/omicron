// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `ADD_INSTANCE_PRIMARY_NIC_MTU` of the Sled Agent API.
//!
//! This version adds an MTU field for the instance's primary OPTE NIC so
//! Nexus can request an 8500 byte MTU when jumbo frames are enabled.

pub mod instance;
