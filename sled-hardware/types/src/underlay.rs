// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/// Initial octet of IPv6 for bootstrap addresses.
pub const BOOTSTRAP_PREFIX: u16 = 0xfdb0;

/// IPv6 prefix mask for bootstrap addresses.
pub const BOOTSTRAP_MASK: u8 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapInterface {
    GlobalZone,
    SwitchZone,
}

impl BootstrapInterface {
    pub fn interface_id(self) -> u64 {
        match self {
            BootstrapInterface::GlobalZone => 1,
            BootstrapInterface::SwitchZone => 2,
        }
    }
}
