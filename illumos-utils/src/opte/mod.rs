// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

cfg_if::cfg_if! {
    if #[cfg(target_os = "illumos")] {
        mod illumos;
        pub use illumos::*;
    } else {
        mod non_illumos;
        pub use non_illumos::*;
    }
}

mod firewall_rules;
pub mod params;
mod port;
mod port_manager;

pub use firewall_rules::opte_firewall_rules;
pub use port::Port;
pub use port_manager::PortManager;
pub use port_manager::PortTicket;

use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
pub use oxide_vpc::api::BoundaryServices;
pub use oxide_vpc::api::DhcpCfg;
pub use oxide_vpc::api::Vni;
use std::net::IpAddr;

/// Information about the gateway for an OPTE port
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct Gateway {
    mac: MacAddr6,
    ip: IpAddr,
}

// The MAC address that OPTE exposes to guest NICs, i.e., the MAC of the virtual
// gateway OPTE operates as for each guest. See
// https://github.com/oxidecomputer/omicron/pull/955#discussion_r856432498 for
// more context on the genesis of this, but this is just a reserved address
// within the "system" portion of the virtual MAC address space.
// See https://github.com/oxidecomputer/omicron/issues/1381
const OPTE_VIRTUAL_GATEWAY_MAC: MacAddr6 =
    MacAddr6::new(0xa8, 0x40, 0x25, 0xff, 0x77, 0x77);

impl Gateway {
    pub fn from_subnet(subnet: &IpNetwork) -> Self {
        Self {
            mac: OPTE_VIRTUAL_GATEWAY_MAC,

            // See RFD 21, section 2.2 table 1
            ip: subnet
                .iter()
                .nth(1)
                .expect("IP subnet must have at least 2 addresses"),
        }
    }

    pub fn ip(&self) -> &IpAddr {
        &self.ip
    }
}
