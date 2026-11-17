// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;

use omicron_common::api::external::Hostname;
use omicron_common::api::internal::shared::DhcpConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1;
use crate::v1::instance::ResolvedVpcFirewallRule;
use crate::v1::inventory::NetworkInterface;
use crate::v1::inventory::SourceNatConfig;

/// Describes sled-local configuration that a sled-agent must establish to make
/// the instance's virtual hardware fully functional.
///
/// Added in v7: `multicast_groups` field.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceSledLocalConfig {
    pub hostname: Hostname,
    pub nics: Vec<NetworkInterface>,
    pub source_nat: SourceNatConfig,
    /// Zero or more external IP addresses (either floating or ephemeral),
    /// provided to an instance to allow inbound connectivity.
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: Vec<IpAddr>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
}

/// Represents a multicast group membership for an instance.
///
/// Introduced in v7.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct InstanceMulticastMembership {
    pub group_ip: IpAddr,
    // For Source-Specific Multicast (SSM)
    pub sources: Vec<IpAddr>,
}

impl From<v1::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(v1: v1::instance::InstanceSledLocalConfig) -> Self {
        Self {
            hostname: v1.hostname,
            nics: v1.nics,
            source_nat: v1.source_nat,
            ephemeral_ip: v1.ephemeral_ip,
            floating_ips: v1.floating_ips,
            multicast_groups: Vec::new(), // Added in v7
            firewall_rules: v1.firewall_rules,
            dhcp_config: v1.dhcp_config,
        }
    }
}

/// Request body for multicast group operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstanceMulticastBody {
    Join(InstanceMulticastMembership),
    Leave(InstanceMulticastMembership),
}
