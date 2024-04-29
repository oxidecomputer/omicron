// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::shared::NetworkInterface;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::net::Ipv6Addr;

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub vni: external::Vni,
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

/// A mapping from a virtual NIC to a physical host
#[derive(
    Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct VirtualNetworkInterfaceHost {
    pub virtual_ip: IpAddr,
    pub virtual_mac: external::MacAddr,
    pub physical_host_ip: Ipv6Addr,
    pub vni: external::Vni,
}

/// DHCP configuration for a port
///
/// Not present here: Hostname (DHCPv4 option 12; used in DHCPv6 option 39); we
/// use `InstanceRuntimeState::hostname` for this value.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct DhcpConfig {
    /// DNS servers to send to the instance
    ///
    /// (DHCPv4 option 6; DHCPv6 option 23)
    pub dns_servers: Vec<IpAddr>,

    /// DNS zone this instance's hostname belongs to (e.g. the `project.example`
    /// part of `instance1.project.example`)
    ///
    /// (DHCPv4 option 15; used in DHCPv6 option 39)
    pub host_domain: Option<String>,

    /// DNS search domains
    ///
    /// (DHCPv4 option 119; DHCPv6 option 24)
    pub search_domains: Vec<String>,
}
