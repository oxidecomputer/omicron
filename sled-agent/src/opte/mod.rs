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

use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use std::net::IpAddr;
use std::net::Ipv6Addr;

/// Location information for reaching Boundary Services, for directing
/// inter-sled or off-rack traffic from guests.
#[derive(Debug, Clone, Copy)]
pub struct BoundaryServices {
    pub ip: Ipv6Addr,
    pub vni: Vni,
}

impl Default for BoundaryServices {
    fn default() -> Self {
        // TODO-completeness: Don't hardcode this.
        //
        // Boundary Services will be started on several Sidecars during rack
        // setup, and those addresses will need to be propagated here.
        // See https://github.com/oxidecomputer/omicron/issues/1382
        const BOUNDARY_SERVICES_ADDR: Ipv6Addr =
            Ipv6Addr::new(0xfd00, 0x99, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
        let boundary_services_vni = Vni::new(99_u32).unwrap();

        Self { ip: BOUNDARY_SERVICES_ADDR, vni: boundary_services_vni }
    }
}

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
                .expect("IP subnet must have at least 1 address"),
        }
    }
}
