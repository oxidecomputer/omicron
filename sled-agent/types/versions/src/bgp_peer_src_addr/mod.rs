// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `BGP_PEER_SRC_ADDR` of the Sled Agent API.
//!
//! This version adds a `src_addr` field to `RouterPeerType::Numbered`,
//! allowing the BGP session source address to be stored in the bootstore.

pub mod early_networking;
pub mod system_networking;
