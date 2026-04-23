// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MCAST_M2P_FORWARDING` of the Sled Agent API.
//!
//! Adds multicast-to-physical mapping and forwarding types, and moves the
//! multicast subscription endpoints from VMM-keyed to instance-keyed.

pub mod instance;
pub mod multicast;
