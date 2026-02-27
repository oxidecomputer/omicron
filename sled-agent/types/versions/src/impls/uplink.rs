// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for uplink types.

use crate::latest::early_networking::PortConfig;
use crate::latest::uplink::HostPortConfig;

impl From<PortConfig> for HostPortConfig {
    fn from(x: PortConfig) -> Self {
        Self {
            port: x.port,
            addrs: x.addresses,
            lldp: x.lldp.clone(),
            tx_eq: x.tx_eq,
        }
    }
}
