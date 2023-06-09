// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use lazy_static::lazy_static;
use nexus_types::identity::Resource;
use omicron_common::api::external::{
    L4PortRange, VpcFirewallRuleAction, VpcFirewallRuleDirection,
    VpcFirewallRuleFilter, VpcFirewallRulePriority, VpcFirewallRuleProtocol,
    VpcFirewallRuleStatus, VpcFirewallRuleTarget, VpcFirewallRuleUpdate,
};

lazy_static! {
    /// Built-in VPC firewall rule for External DNS.
    pub static ref DNS_VPC_FW_RULE: VpcFirewallRuleUpdate = VpcFirewallRuleUpdate {
        name: "external-dns-inbound".parse().unwrap(),
        description: "allow inbound connections for DNS from anywhere"
            .to_string(),
        status: VpcFirewallRuleStatus::Enabled,
        direction: VpcFirewallRuleDirection::Inbound,
        targets: vec![
            VpcFirewallRuleTarget::Subnet(
                super::vpc_subnet::DNS_VPC_SUBNET.name().clone(),
            ),
        ],
        filters: VpcFirewallRuleFilter {
            hosts: None,
            protocols: Some(vec![VpcFirewallRuleProtocol::Udp]),
            ports: Some(
                vec![
                    L4PortRange {
                        first: 53.try_into().unwrap(),
                        last: 53.try_into().unwrap(),
                    },
                ],
            ),
        },
        action: VpcFirewallRuleAction::Allow,
        priority: VpcFirewallRulePriority(65534),
    };

    /// Built-in VPC firewall rule for Nexus.
    pub static ref NEXUS_VPC_FW_RULE: VpcFirewallRuleUpdate = VpcFirewallRuleUpdate {
        name: "nexus-inbound".parse().unwrap(),
        description: "allow inbound connections for console & api from anywhere"
            .to_string(),
        status: VpcFirewallRuleStatus::Enabled,
        direction: VpcFirewallRuleDirection::Inbound,
        targets: vec![
            VpcFirewallRuleTarget::Subnet(
                super::vpc_subnet::NEXUS_VPC_SUBNET.name().clone(),
            ),
        ],
        filters: VpcFirewallRuleFilter {
            hosts: None,
            protocols: Some(vec![VpcFirewallRuleProtocol::Tcp]),
            ports: Some(
                vec![
                    L4PortRange {
                        first: 80.try_into().unwrap(),
                        last: 80.try_into().unwrap(),
                    },
                    L4PortRange {
                        first: 443.try_into().unwrap(),
                        last: 443.try_into().unwrap(),
                    },
                ],
            ),
        },
        action: VpcFirewallRuleAction::Allow,
        priority: VpcFirewallRulePriority(65534),
    };
}
