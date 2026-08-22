// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `MCAST_M2P_FORWARDING` of the Sled Agent API.
//!
//! This version adds the multicast-to-physical (M2P) mapping and
//! forwarding types used by the new networking endpoints. It also changes
//! the multicast subscription endpoints to take an `InstanceMulticastMembership`
//! request body in place of `InstanceMulticastBody`.

pub mod multicast;
