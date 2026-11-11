// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `BGP_PEER_COLLISION_STATE` of the Nexus external API.
//!
//! This version (2025_12_12_00) adds the `ConnectionCollision` state to
//! `BgpPeerState`, but does not yet include the `peer_id` field on
//! `BgpPeerStatus`.

pub mod networking;
