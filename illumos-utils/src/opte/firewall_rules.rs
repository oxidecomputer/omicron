// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Convert Omicron VPC firewall rules to OPTE firewall rules.

use crate::opte::params::VpcFirewallRule;
use crate::opte::Vni;
use macaddr::MacAddr6;
use omicron_common::api::external::VpcFirewallRuleAction;
use omicron_common::api::external::VpcFirewallRuleDirection;
use omicron_common::api::external::VpcFirewallRuleProtocol;
use omicron_common::api::external::VpcFirewallRuleStatus;
use omicron_common::api::internal::nexus::HostIdentifier;
use oxide_vpc::api::Address;
use oxide_vpc::api::Direction;
use oxide_vpc::api::Filters;
use oxide_vpc::api::FirewallAction;
use oxide_vpc::api::FirewallRule;
use oxide_vpc::api::IpAddr;
use oxide_vpc::api::IpCidr;
use oxide_vpc::api::Ipv4Cidr;
use oxide_vpc::api::Ipv4PrefixLen;
use oxide_vpc::api::Ipv6Cidr;
use oxide_vpc::api::Ipv6PrefixLen;
use oxide_vpc::api::Ports;
use oxide_vpc::api::ProtoFilter;
use oxide_vpc::api::Protocol;
use oxnet::IpNet;

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
        match self.filter_hosts {
            Some(ref hosts) if !hosts.is_empty() => hosts
                .iter()
                .map(|host| match host {
                    HostIdentifier::Ip(IpNet::V4(net)) if net.is_host_net() => {
                        Address::Ip(IpAddr::Ip4(net.addr().into()))
                    }
                    HostIdentifier::Ip(IpNet::V4(net)) => {
                        Address::Subnet(IpCidr::Ip4(Ipv4Cidr::new(
                            net.addr().into(),
                            Ipv4PrefixLen::new(net.width()).unwrap(),
                        )))
                    }
                    HostIdentifier::Ip(IpNet::V6(net)) if net.is_host_net() => {
                        Address::Ip(IpAddr::Ip6(net.addr().into()))
                    }
                    HostIdentifier::Ip(IpNet::V6(net)) => {
                        Address::Subnet(IpCidr::Ip6(Ipv6Cidr::new(
                            net.addr().into(),
                            Ipv6PrefixLen::new(net.width()).unwrap(),
                        )))
                    }
                    HostIdentifier::Vpc(vni) => {
                        Address::Vni(Vni::new(u32::from(*vni)).unwrap())
                    }
                })
                .collect(),
            _ => vec![Address::Any],
        }
    }

    fn ports(&self) -> Ports {
        match self.filter_ports {
            Some(ref ports) if !ports.is_empty() => Ports::PortList(
                ports
                    .iter()
                    .flat_map(|range| {
                        (range.first.0.get()..=range.last.0.get())
                            .collect::<Vec<u16>>()
                    })
                    .collect(),
            ),
            _ => Ports::Any,
        }
    }

    fn priority(&self) -> u16 {
        self.priority.0
    }

    fn protos(&self) -> Vec<ProtoFilter> {
        match self.filter_protocols {
            Some(ref protos) if !protos.is_empty() => protos
                .iter()
                .map(|proto| {
                    ProtoFilter::Proto(match proto {
                        VpcFirewallRuleProtocol::Tcp => Protocol::TCP,
                        VpcFirewallRuleProtocol::Udp => Protocol::UDP,
                        VpcFirewallRuleProtocol::Icmp => Protocol::ICMP,
                    })
                })
                .collect(),
            _ => vec![ProtoFilter::Any],
        }
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
