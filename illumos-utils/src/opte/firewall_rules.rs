// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Convert Omicron VPC firewall rules to OPTE firewall rules.

use super::net_to_cidr;
use super::stat::PortStats;
use crate::opte::Vni;
use macaddr::MacAddr6;
use omicron_common::api::external::VpcEntity;
use omicron_common::api::external::VpcFirewallRuleAction;
use omicron_common::api::external::VpcFirewallRuleDirection;
use omicron_common::api::external::VpcFirewallRuleProtocol;
use omicron_common::api::external::VpcFirewallRuleStatus;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use oxide_vpc::api::Address;
use oxide_vpc::api::Direction;
use oxide_vpc::api::Filters;
use oxide_vpc::api::FirewallAction;
use oxide_vpc::api::FirewallRule;
use oxide_vpc::api::IpAddr;
use oxide_vpc::api::Ports;
use oxide_vpc::api::ProtoFilter;
use oxnet::IpNet;
use std::collections::HashSet;

trait FromVpcFirewallRule {
    fn action(&self) -> FirewallAction;
    fn direction(&self) -> Direction;
    fn disabled(&self) -> bool;
    fn hosts(&self) -> Vec<Address>;
    fn ports(&self) -> Ports;
    fn priority(&self) -> u16;
    fn protos(&self) -> Vec<ProtoFilter>;
}

impl FromVpcFirewallRule for ResolvedVpcFirewallRule {
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
                    HostIdentifier::Ip(IpNet::V6(net)) if net.is_host_net() => {
                        Address::Ip(IpAddr::Ip6(net.addr().into()))
                    }
                    HostIdentifier::Ip(ip) => Address::Subnet(net_to_cidr(*ip)),
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
                .map(|proto| match proto {
                    VpcFirewallRuleProtocol::Tcp => ProtoFilter::Tcp,
                    VpcFirewallRuleProtocol::Udp => ProtoFilter::Udp,
                    VpcFirewallRuleProtocol::Icmp(v) => {
                        ProtoFilter::Icmp(v.map(|v| {
                            oxide_vpc::api::IcmpFilter {
                                ty: v.icmp_type,
                                codes: v.code.map(Into::into),
                            }
                        }))
                    }
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
    rules: &[ResolvedVpcFirewallRule],
    vni: &Vni,
    mac: &MacAddr6,
    stats: &PortStats,
) -> Vec<FirewallRule> {
    let initial_stat_entities = stats.entities();
    let initial_firewall_rules: HashSet<_> = initial_stat_entities
        .into_iter()
        .filter(|v| matches!(v, VpcEntity::FirewallRule(_)))
        .collect();
    let mut expected_rules = HashSet::new();

    #[allow(clippy::map_flatten)]
    let out = rules
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

            let ent = VpcEntity::FirewallRule(rule.id);
            let stat_id = stats.register_entity(ent.clone());
            expected_rules.insert(ent);

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

                                // Port assignments are incompatible with non
                                // TCP/UDP protocols.
                                if matches!(
                                    proto,
                                    ProtoFilter::Tcp | ProtoFilter::Udp
                                ) {
                                    filters.set_ports(ports.clone());
                                }

                                filters
                                    .set_hosts(*hosts)
                                    .set_protocol(proto.clone());
                                filters
                            },
                            stat_id: Some(stat_id),
                        })
                        .collect::<Vec<FirewallRule>>()
                })
                .collect::<Vec<Vec<FirewallRule>>>()
        })
        .flatten()
        .flatten()
        .collect::<Vec<FirewallRule>>();

    for absent_entity in initial_firewall_rules.difference(&expected_rules) {
        stats.deregister_entity(absent_entity.clone());
    }

    out
}
