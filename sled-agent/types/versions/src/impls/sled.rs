// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for sled types.

use std::net::{Ipv6Addr, SocketAddrV6};

use omicron_common::address;
use sha3::{Digest, Sha3_256};

use crate::latest::sled::StartSledAgentRequest;

impl StartSledAgentRequest {
    /// Returns the sled's address.
    pub fn sled_address(&self) -> SocketAddrV6 {
        address::get_sled_address(self.body.subnet)
    }

    /// Returns the switch zone's IP address.
    pub fn switch_zone_ip(&self) -> Ipv6Addr {
        address::get_switch_zone_address(self.body.subnet)
    }

    /// Compute the sha3_256 digest of `self.rack_id` to use as a `salt`
    /// for disk encryption. We don't want to include other values that are
    /// consistent across sleds as it would prevent us from moving drives
    /// between sleds.
    pub fn hash_rack_id(&self) -> [u8; 32] {
        // We know the unwrap succeeds as a Sha3_256 digest is 32 bytes
        Sha3_256::digest(self.body.rack_id.as_bytes())
            .as_slice()
            .try_into()
            .unwrap()
    }
}
