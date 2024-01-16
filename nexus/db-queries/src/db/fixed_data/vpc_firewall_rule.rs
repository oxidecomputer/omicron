// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_types::identity::Resource;
use omicron_common::api::external::{
    L4PortRange, VpcFirewallRuleAction, VpcFirewallRuleDirection,
    VpcFirewallRuleFilter, VpcFirewallRulePriority, VpcFirewallRuleProtocol,
    VpcFirewallRuleStatus, VpcFirewallRuleTarget, VpcFirewallRuleUpdate,
};
use once_cell::sync::Lazy;

/// Built-in VPC firewall rule for External DNS.
pub static DNS_VPC_FW_RULE: Lazy<VpcFirewallRuleUpdate> =
    Lazy::new(|| VpcFirewallRuleUpdate {
        name: "external-dns-inbound".parse().unwrap(),
        description: "allow inbound connections for DNS from anywhere"
            .to_string(),
        status: VpcFirewallRuleStatus::Enabled,
        direction: VpcFirewallRuleDirection::Inbound,
        targets: vec![VpcFirewallRuleTarget::Subnet(
            super::vpc_subnet::DNS_VPC_SUBNET.name().clone(),
        )],
        filters: VpcFirewallRuleFilter {
            hosts: None,
            protocols: Some(vec![VpcFirewallRuleProtocol::Udp]),
            ports: Some(vec![L4PortRange {
                first: 53.try_into().unwrap(),
                last: 53.try_into().unwrap(),
            }]),
        },
        action: VpcFirewallRuleAction::Allow,
        priority: VpcFirewallRulePriority(65534),
    });

/// Built-in VPC firewall rule for Nexus.
pub static NEXUS_VPC_FW_RULE: Lazy<VpcFirewallRuleUpdate> =
    Lazy::new(|| VpcFirewallRuleUpdate {
        name: "nexus-inbound".parse().unwrap(),
        description:
            "allow inbound connections for console & api from anywhere"
                .to_string(),
        status: VpcFirewallRuleStatus::Enabled,
        direction: VpcFirewallRuleDirection::Inbound,
        targets: vec![VpcFirewallRuleTarget::Subnet(
            super::vpc_subnet::NEXUS_VPC_SUBNET.name().clone(),
        )],
        filters: VpcFirewallRuleFilter {
            hosts: None,
            protocols: Some(vec![VpcFirewallRuleProtocol::Tcp]),
            ports: Some(vec![
                L4PortRange {
                    first: 80.try_into().unwrap(),
                    last: 80.try_into().unwrap(),
                },
                L4PortRange {
                    first: 443.try_into().unwrap(),
                    last: 443.try_into().unwrap(),
                },
            ]),
        },
        action: VpcFirewallRuleAction::Allow,
        priority: VpcFirewallRulePriority(65534),
    });
