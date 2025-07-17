// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

cfg_if::cfg_if! {
    if #[cfg(all(target_os = "illumos", not(test)))] {
        mod illumos;
        pub use illumos::*;
    } else {
        mod non_illumos;
        pub use non_illumos::*;
    }
}

mod firewall_rules;
mod port;
mod port_manager;
mod route;
mod stat;

pub use firewall_rules::opte_firewall_rules;
use ipnetwork::IpNetwork;
use macaddr::MacAddr6;
use omicron_common::api::internal::shared;
pub use oxide_vpc::api::BoundaryServices;
pub use oxide_vpc::api::DhcpCfg;
use oxide_vpc::api::IpCidr;
use oxide_vpc::api::Ipv4Cidr;
use oxide_vpc::api::Ipv4PrefixLen;
use oxide_vpc::api::Ipv6Cidr;
use oxide_vpc::api::Ipv6PrefixLen;
use oxide_vpc::api::RouterTarget;
pub use oxide_vpc::api::Vni;
use oxnet::IpNet;
pub use port::Port;
pub use port_manager::PortCreateParams;
pub use port_manager::PortManager;
pub use port_manager::PortTicket;
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

/// Convert a nexus `IpNet` to an OPTE `IpCidr`.
fn net_to_cidr(net: IpNet) -> IpCidr {
    match net {
        IpNet::V4(net) => IpCidr::Ip4(Ipv4Cidr::new(
            net.addr().into(),
            Ipv4PrefixLen::new(net.width()).unwrap(),
        )),
        IpNet::V6(net) => IpCidr::Ip6(Ipv6Cidr::new(
            net.addr().into(),
            Ipv6PrefixLen::new(net.width()).unwrap(),
        )),
    }
}

/// Convert a nexus `RouterTarget` to an OPTE `RouterTarget`.
///
/// This is effectively a `From` impl, but defined for two out-of-crate types.
/// We map internet gateways that target the (single) "system" VPC IG to
/// `InternetGateway(None)`. Everything else is mapped directly, translating IP
/// address types as needed.
fn router_target_opte(target: &shared::RouterTarget) -> RouterTarget {
    use shared::InternetGatewayRouterTarget;
    use shared::RouterTarget::*;
    match target {
        Drop => RouterTarget::Drop,
        InternetGateway(InternetGatewayRouterTarget::System) => {
            RouterTarget::InternetGateway(None)
        }
        InternetGateway(InternetGatewayRouterTarget::Instance(id)) => {
            RouterTarget::InternetGateway(Some(*id))
        }
        Ip(ip) => RouterTarget::Ip((*ip).into()),
        VpcSubnet(net) => RouterTarget::VpcSubnet(net_to_cidr(*net)),
    }
}
