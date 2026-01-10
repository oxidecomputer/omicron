// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `BGP_PEER_COLLISION_STATE` of the Nexus external API.
//!
//! This version (2025121200) adds collision state for BGP peers. Instance
//! creation types here don't have the `ip_version` field that was added
//! in later versions for default IP pool selection.

pub mod floating_ip;
pub mod instance;
