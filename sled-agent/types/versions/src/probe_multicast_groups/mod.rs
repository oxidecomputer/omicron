// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `PROBE_MULTICAST_GROUPS` of the Sled Agent API.
//!
//! This version adds `multicast_groups` to `ProbeCreate` so a probe's OPTE
//! port is subscribed to its groups at zone provisioning time.

pub mod probes;
