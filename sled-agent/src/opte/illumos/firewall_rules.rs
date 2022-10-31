// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Convert Omicron VPC firewall rules to OPTE firewall rules.

use crate::opte::Vni;
use crate::params::VpcFirewallRule;
use macaddr::MacAddr6;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::VpcAddress;
use omicron_common::api::external::VpcFirewallRuleAction;
use omicron_common::api::external::VpcFirewallRuleDirection;
use omicron_common::api::external::VpcFirewallRuleProtocol;
use omicron_common::api::external::VpcFirewallRuleStatus;
use oxide_vpc::api::Address;
use oxide_vpc::api::Direction;
use oxide_vpc::api::Filters;
use oxide_vpc::api::FirewallAction;
use oxide_vpc::api::FirewallRule;
use oxide_vpc::api::Ipv4Cidr;
use oxide_vpc::api::Ipv4PrefixLen;
use oxide_vpc::api::Ports;
use oxide_vpc::api::ProtoFilter;
use oxide_vpc::api::Protocol;

trait FromVpcFirewallRule {
    fn action(&self) -> FirewallAction;
    fn direction(&self) -> Direction;
    fn disabled(&self) -> bool;
    fn hosts(&self) -> Vec<Address>;
    fn ports(&self) -> Ports;
    fn priority(&self) -> u16;
    fn protos(&self) -> Vec<ProtoFilter>;
}

impl FromVpcFirewallRule for VpcFirewallRule {
    fn action(&self) -> FirewallAction {
        match self.action {
            VpcFirewallRuleAction::Allow => FirewallAction::Allow,
            VpcFirewallRuleAction::Deny => FirewallAction::Deny,
        }
    }

    fn direction(&self) -> Direction {
        match self.direction {
            VpcFirewallRuleDirection::Inbound => Direction::In,
            VpcFirewallRuleDirection::Outbound => Direction::Out,
        }
    }

    fn disabled(&self) -> bool {
        match self.status {
            VpcFirewallRuleStatus::Disabled => false,
            VpcFirewallRuleStatus::Enabled => true,
        }
    }

    fn hosts(&self) -> Vec<Address> {
        self.filter_hosts.as_ref().map_or_else(
            || vec![Address::Any],
            |hosts| {
                hosts
                    .iter()
                    .map(|host| match host {
                        VpcAddress::Ip(IpNet::V4(net))
                            if net.prefix() == 32 =>
                        {
                            Address::Ip(net.ip().into())
                        }
                        VpcAddress::Ip(IpNet::V4(net)) => {
                            Address::Subnet(Ipv4Cidr::new(
                                net.ip().into(),
                                Ipv4PrefixLen::new(net.prefix()).unwrap(),
                            ))
                        }
                        VpcAddress::Ip(IpNet::V6(_net)) => {
                            todo!("IPv6 host filters")
                        }
                        VpcAddress::Vpc(vni) => {
                            Address::Vni(Vni::new(u32::from(*vni)).unwrap())
                        }
                    })
                    .collect::<Vec<Address>>()
            },
        )
    }

    fn ports(&self) -> Ports {
        match self.filter_ports {
            Some(ref ports) if ports.len() > 0 => Ports::PortList(
                ports
                    .iter()
                    .flat_map(|range| {
                        (range.first.0.get()..=range.last.0.get())
                            .collect::<Vec<u16>>()
                    })
                    .collect::<Vec<u16>>(),
            ),
            _ => Ports::Any,
        }
    }

    fn priority(&self) -> u16 {
        self.priority.0
    }

    fn protos(&self) -> Vec<ProtoFilter> {
        self.filter_protocols.as_ref().map_or_else(
            || vec![ProtoFilter::Any],
            |protos| {
                protos
                    .iter()
                    .map(|proto| {
                        ProtoFilter::Proto(match proto {
                            VpcFirewallRuleProtocol::Tcp => Protocol::TCP,
                            VpcFirewallRuleProtocol::Udp => Protocol::UDP,
                            VpcFirewallRuleProtocol::Icmp => Protocol::ICMP,
                        })
                    })
                    .collect::<Vec<ProtoFilter>>()
            },
        )
    }
}

/// Translate from a slice of VPC firewall rules to a vector of OPTE rules
/// that match a given port's VNI and MAC address. OPTE rules can only encode
/// a single host address and protocol, so we must unroll rules with multiple
/// hosts/protocols.
pub fn opte_firewall_rules(
    rules: &[VpcFirewallRule],
    vni: &Vni,
    mac: &MacAddr6,
) -> Vec<FirewallRule> {
    #[allow(clippy::map_flatten)]
    rules
        .iter()
        .filter(|rule| rule.disabled())
        .filter(|rule| {
            rule.targets.is_empty() // no targets means apply everywhere
                || rule.targets.iter().any(|nic| {
                    // (VNI, MAC) is a unique identifier for the NIC.
                    u32::from(nic.vni) == u32::from(*vni) && nic.mac.0 == *mac
                })
        })
        .map(|rule| {
            let priority = rule.priority();
            let action = rule.action();
            let direction = rule.direction();
            let ports = rule.ports();
            let protos = rule.protos();
            let hosts = rule.hosts();
            protos
                .iter()
                .map(|proto| {
                    hosts
                        .iter()
                        .map(|hosts| FirewallRule {
                            priority,
                            action,
                            direction,
                            filters: {
                                let mut filters = Filters::new();
                                filters
                                    .set_hosts(*hosts)
                                    .set_protocol(*proto)
                                    .set_ports(ports.clone());
                                filters
                            },
                        })
                        .collect::<Vec<FirewallRule>>()
                })
                .collect::<Vec<Vec<FirewallRule>>>()
        })
        .flatten()
        .flatten()
        .collect::<Vec<FirewallRule>>()
}
