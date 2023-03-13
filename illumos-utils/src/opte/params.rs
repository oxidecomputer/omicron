// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external;
use omicron_common::api::internal::nexus::HostIdentifier;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

pub use omicron_common::api::internal::sled_agent::NetworkInterface;

/// An IP address and port range used for instance source NAT, i.e., making
/// outbound network connections from guests.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
pub struct SourceNatConfig {
    /// The external address provided to the instance
    pub ip: IpAddr,
    /// The first port used for instance NAT, inclusive.
    pub first_port: u16,
    /// The last port used for instance NAT, also inclusive.
    pub last_port: u16,
}

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub rules: Vec<VpcFirewallRule>,
}

/// VPC firewall rule after object name resolution has been performed by Nexus
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRule {
    pub status: external::VpcFirewallRuleStatus,
    pub direction: external::VpcFirewallRuleDirection,
    pub targets: Vec<NetworkInterface>,
    pub filter_hosts: Option<Vec<HostIdentifier>>,
    pub filter_ports: Option<Vec<external::L4PortRange>>,
    pub filter_protocols: Option<Vec<external::VpcFirewallRuleProtocol>>,
    pub action: external::VpcFirewallRuleAction,
    pub priority: external::VpcFirewallRulePriority,
}
