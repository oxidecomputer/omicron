// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashSet;
use std::net::IpAddr;

use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::shared::DhcpConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::inventory::NetworkInterface;
use crate::v1::inventory::SourceNatConfig;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v9;
use crate::v9::instance::DelegatedZvol;

/// The protocols that may be specified in a firewall rule's filter.
//
// This is the version of the enum without `Icmp6`, for versions up through
// `ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES`.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum VpcFirewallRuleProtocol {
    Tcp,
    Udp,
    Icmp(Option<external::VpcFirewallIcmpFilter>),
}

impl From<crate::v1::instance::VpcFirewallRuleProtocol>
    for VpcFirewallRuleProtocol
{
    fn from(v1: crate::v1::instance::VpcFirewallRuleProtocol) -> Self {
        match v1 {
            crate::v1::instance::VpcFirewallRuleProtocol::Tcp => Self::Tcp,
            crate::v1::instance::VpcFirewallRuleProtocol::Udp => Self::Udp,
            crate::v1::instance::VpcFirewallRuleProtocol::Icmp(f) => {
                Self::Icmp(f)
            }
        }
    }
}

/// VPC firewall rule after object name resolution has been performed by Nexus.
//
// This is the version of the struct without `Icmp6` in the protocol filter,
// for versions up through `ADD_DUAL_STACK_SHARED_NETWORK_INTERFACES`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ResolvedVpcFirewallRule {
    pub status: external::VpcFirewallRuleStatus,
    pub direction: external::VpcFirewallRuleDirection,
    pub targets: Vec<NetworkInterface>,
    pub filter_hosts: Option<HashSet<HostIdentifier>>,
    pub filter_ports: Option<Vec<external::L4PortRange>>,
    pub filter_protocols: Option<Vec<VpcFirewallRuleProtocol>>,
    pub action: external::VpcFirewallRuleAction,
    pub priority: external::VpcFirewallRulePriority,
}

/// Describes sled-local configuration that a sled-agent must establish to make
/// the instance's virtual hardware fully functional.
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
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl TryFrom<v9::instance::InstanceSledLocalConfig>
    for InstanceSledLocalConfig
{
    type Error = external::Error;

    fn try_from(
        v9: v9::instance::InstanceSledLocalConfig,
    ) -> Result<Self, Self::Error> {
        let firewall_rules = v9
            .firewall_rules
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            hostname: v9.hostname,
            nics: v9
                .nics
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            source_nat: v9.source_nat,
            ephemeral_ip: v9.ephemeral_ip,
            floating_ips: v9.floating_ips,
            multicast_groups: v9.multicast_groups,
            firewall_rules,
            dhcp_config: v9.dhcp_config,
            delegated_zvols: v9.delegated_zvols,
        })
    }
}

impl TryFrom<crate::v1::instance::ResolvedVpcFirewallRule>
    for ResolvedVpcFirewallRule
{
    type Error = external::Error;

    fn try_from(
        v1: crate::v1::instance::ResolvedVpcFirewallRule,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            status: v1.status,
            direction: v1.direction,
            targets: v1
                .targets
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            filter_hosts: v1.filter_hosts,
            filter_ports: v1.filter_ports,
            filter_protocols: v1.filter_protocols.map(|ps| {
                ps.into_iter().map(VpcFirewallRuleProtocol::from).collect()
            }),
            action: v1.action,
            priority: v1.priority,
        })
    }
}
