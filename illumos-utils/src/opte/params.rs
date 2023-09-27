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
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SetVirtualNetworkInterfaceHost {
    pub virtual_ip: IpAddr,
    pub virtual_mac: external::MacAddr,
    pub physical_host_ip: Ipv6Addr,
    pub vni: external::Vni,
}

/// The data needed to identify a virtual IP for which a sled maintains an OPTE
/// virtual-to-physical mapping such that that mapping can be deleted.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct DeleteVirtualNetworkInterfaceHost {
    /// The virtual IP whose mapping should be deleted.
    pub virtual_ip: IpAddr,

    /// The VNI for the network containing the virtual IP whose mapping should
    /// be deleted.
    pub vni: external::Vni,
}
