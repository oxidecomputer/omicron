// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use nexus_types::identity::Resource;
use omicron_common::api::external::{
    L4PortRange, VpcFirewallIcmpFilter, VpcFirewallRuleAction,
    VpcFirewallRuleDirection, VpcFirewallRuleFilter, VpcFirewallRulePriority,
    VpcFirewallRuleProtocol, VpcFirewallRuleStatus, VpcFirewallRuleTarget,
    VpcFirewallRuleUpdate,
};
use std::sync::LazyLock;

/// Built-in VPC firewall rule for External DNS.
pub static DNS_VPC_FW_RULE: LazyLock<VpcFirewallRuleUpdate> =
    LazyLock::new(|| VpcFirewallRuleUpdate {
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

/// The name for the built-in VPC firewall rule for Nexus.
pub const NEXUS_VPC_FW_RULE_NAME: &str = "nexus-inbound";

/// Built-in VPC firewall rule for Nexus.
///
/// Note that we currently rely on this being exactly one rule to implement the
/// Nexus allowlist. See `nexus/networking/src/firewall_rules.rs` for more
/// details.
pub static NEXUS_VPC_FW_RULE: LazyLock<VpcFirewallRuleUpdate> =
    LazyLock::new(|| VpcFirewallRuleUpdate {
        name: NEXUS_VPC_FW_RULE_NAME.parse().unwrap(),
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

/// The name for the built-in VPC firewall rule for Nexus.
pub const NEXUS_ICMP_FW_RULE_NAME: &str = "nexus-icmp";

/// Built-in VPC firewall rule for Nexus.
///
/// This rule allows *arbitrary forwarding nodes* on the network to inform the
/// Nexus/DNS zones that packets have been explicitly dropped. This is a key part
/// in enabling path MTU discovery, such as when customers are accessing Nexus
/// over a VPN.
///
/// We currently rely on this being exactly one rule to implement the system-level
/// enable/disable endpoint. See `nexus/networking/src/firewall_rules.rs`.
pub static NEXUS_ICMP_FW_RULE: LazyLock<VpcFirewallRuleUpdate> =
    LazyLock::new(|| VpcFirewallRuleUpdate {
        name: NEXUS_ICMP_FW_RULE_NAME.parse().unwrap(),
        description:
            "allow typical inbound ICMP error codes for outbound flows"
                .to_string(),
        status: VpcFirewallRuleStatus::Enabled,
        direction: VpcFirewallRuleDirection::Inbound,
        targets: vec![
            VpcFirewallRuleTarget::Subnet(
                super::vpc_subnet::NEXUS_VPC_SUBNET.name().clone(),
            ),
            VpcFirewallRuleTarget::Subnet(
                super::vpc_subnet::DNS_VPC_SUBNET.name().clone(),
            ),
        ],
        filters: VpcFirewallRuleFilter {
            hosts: None,
            protocols: Some(vec![
                VpcFirewallRuleProtocol::Icmp(Some(VpcFirewallIcmpFilter {
                    // Type 3 -- Destination Unreachable
                    icmp_type: 3,
                    // Codes 3,4 -- Port Unreachable, Fragmentation needed
                    code: Some((3..=4).into()),
                })),
                VpcFirewallRuleProtocol::Icmp(Some(VpcFirewallIcmpFilter {
                    // Type 5 -- Redirect
                    icmp_type: 5,
                    code: None,
                })),
                VpcFirewallRuleProtocol::Icmp(Some(VpcFirewallIcmpFilter {
                    // Type 11 -- Time Exceeded
                    icmp_type: 11,
                    code: None,
                })),
            ]),
            ports: None,
        },
        action: VpcFirewallRuleAction::Allow,
        priority: VpcFirewallRulePriority(65534),
    });
