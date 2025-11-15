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

pub use firewall_rules::opte_firewall_rules;
use macaddr::MacAddr6;
use omicron_common::api::internal::shared;
use omicron_common::api::internal::shared::PrivateIpConfig;
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
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

/// Information about the gateway for an OPTE port
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct Gateway {
    mac: MacAddr6,
    ips: GatewayIps,
}

// IP addresses for an OPTE gateway.
#[derive(Clone, Copy, Debug)]
enum GatewayIps {
    // IPv4-only gateway.
    V4(Ipv4Addr),
    // IPv6-only gateway.
    V6(Ipv6Addr),
    // Dual-stack gateway.
    DualStack { v4: Ipv4Addr, v6: Ipv6Addr },
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
    /// Construct information about the gateway from an IP configuration.
    pub fn from_ip_config(ip: &PrivateIpConfig) -> Self {
        let ips =
            match ip {
                PrivateIpConfig::V4(v4) => {
                    let ip = v4.subnet().first_host();
                    GatewayIps::V4(ip)
                }
                PrivateIpConfig::V6(v6) => {
                    let ip =
                        v6.subnet().iter().nth(1).expect(
                            "IPv6 subnet must have at least 2 addresses",
                        );
                    GatewayIps::V6(ip)
                }
                PrivateIpConfig::DualStack { v4, v6 } => {
                    let v4 = v4.subnet().first_host();
                    let v6 =
                        v6.subnet().iter().nth(1).expect(
                            "IPv6 subnet must have at least 2 addresses",
                        );
                    GatewayIps::DualStack { v4, v6 }
                }
            };
        Self { mac: OPTE_VIRTUAL_GATEWAY_MAC, ips }
    }

    /// Return the gateway's IPv4 address, if it exists.
    pub fn ipv4_addr(&self) -> Option<&Ipv4Addr> {
        match &self.ips {
            GatewayIps::V4(v4) | GatewayIps::DualStack { v4, .. } => Some(&v4),
            GatewayIps::V6(_) => None,
        }
    }

    /// Return the gateway's IPv6 address, if it exists.
    pub fn ipv6_addr(&self) -> Option<&Ipv6Addr> {
        match &self.ips {
            GatewayIps::V6(v6) | GatewayIps::DualStack { v6, .. } => Some(&v6),
            GatewayIps::V4(_) => None,
        }
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

#[cfg(test)]
mod tests {
    use super::Gateway;
    use super::GatewayIps;
    use omicron_common::api::internal::shared::PrivateIpConfig;
    use omicron_common::api::internal::shared::PrivateIpv4Config;
    use omicron_common::api::internal::shared::PrivateIpv6Config;
    use oxnet::Ipv4Net;
    use oxnet::Ipv6Net;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    #[test]
    fn convert_private_ip_config_to_gateway_ips() {
        let v4 = PrivateIpv4Config::new(
            Ipv4Addr::new(10, 0, 0, 5),
            Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap(),
        )
        .unwrap();
        let v6 = PrivateIpv6Config::new(
            Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 5),
            Ipv6Net::new(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 0), 64)
                .unwrap(),
        )
        .unwrap();
        let cfg = PrivateIpConfig::DualStack { v4: v4.clone(), v6: v6.clone() };

        let expected_v4_gateway = Ipv4Addr::new(10, 0, 0, 1);
        let expected_v6_gateway = Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1);
        let GatewayIps::V4(gateway) =
            Gateway::from_ip_config(&PrivateIpConfig::V4(v4)).ips
        else {
            panic!("Expected IPv4 OPTE gateway IP addresses");
        };
        assert_eq!(gateway, expected_v4_gateway);
        let GatewayIps::V6(gateway) =
            Gateway::from_ip_config(&PrivateIpConfig::V6(v6)).ips
        else {
            panic!("Expected IPv6 OPTE gateway IP addresses");
        };
        assert_eq!(gateway, expected_v6_gateway);

        let GatewayIps::DualStack { v4: ipv4, v6: ipv6 } =
            Gateway::from_ip_config(&cfg).ips
        else {
            panic!("Expected dual-stack OPTE gateway IP addresses");
        };
        assert_eq!(ipv4, expected_v4_gateway);
        assert_eq!(ipv6, expected_v6_gateway);
    }
}
