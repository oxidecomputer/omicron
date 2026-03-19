// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026_03_14_00 that cannot live in `nexus-types-versions`
//! because they convert to/from `omicron-common` types (orphan rule).

use api_identity::ObjectIdentity;
use omicron_common::api::external;
use omicron_common::api::external::Error;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::L4PortRange;
use omicron_common::api::external::Name;
use omicron_common::api::external::ObjectIdentity;
use omicron_common::api::external::VpcFirewallIcmpFilter;
use omicron_common::api::external::VpcFirewallRuleAction;
use omicron_common::api::external::VpcFirewallRuleDirection;
use omicron_common::api::external::VpcFirewallRuleHostFilter;
use omicron_common::api::external::VpcFirewallRulePriority;
use omicron_common::api::external::VpcFirewallRuleStatus;
use omicron_common::api::external::VpcFirewallRuleTarget;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// The protocols that may be specified in a firewall rule's filter.
//
// This is the version of the enum without `Icmp6`, for API versions up
// through `MULTICAST_DROP_MVLAN`.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum VpcFirewallRuleProtocol {
    Tcp,
    Udp,
    Icmp(Option<VpcFirewallIcmpFilter>),
}

impl From<VpcFirewallRuleProtocol> for external::VpcFirewallRuleProtocol {
    fn from(p: VpcFirewallRuleProtocol) -> Self {
        match p {
            VpcFirewallRuleProtocol::Tcp => Self::Tcp,
            VpcFirewallRuleProtocol::Udp => Self::Udp,
            VpcFirewallRuleProtocol::Icmp(v) => Self::Icmp(v),
        }
    }
}

/// Filters reduce the scope of a firewall rule.
//
// This is the version of the filter without `Icmp6` protocol support, for API
// versions up through `MULTICAST_DROP_MVLAN`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleFilter {
    /// If present, host filters match the "other end" of traffic from the
    /// target's perspective: for an inbound rule, they match the source of
    /// traffic. For an outbound rule, they match the destination.
    #[schemars(length(max = 256))]
    pub hosts: Option<Vec<VpcFirewallRuleHostFilter>>,

    /// If present, the networking protocols this rule applies to.
    #[schemars(length(max = 256))]
    pub protocols: Option<Vec<VpcFirewallRuleProtocol>>,

    /// If present, the destination ports or port ranges this rule applies to.
    #[schemars(length(max = 256))]
    pub ports: Option<Vec<L4PortRange>>,
}

impl TryFrom<external::VpcFirewallRuleFilter> for VpcFirewallRuleFilter {
    type Error = Error;

    fn try_from(f: external::VpcFirewallRuleFilter) -> Result<Self, Error> {
        let protocols = f
            .protocols
            .map(|ps| {
                ps.into_iter()
                    .map(|p| match p {
                        external::VpcFirewallRuleProtocol::Tcp => {
                            Ok(VpcFirewallRuleProtocol::Tcp)
                        }
                        external::VpcFirewallRuleProtocol::Udp => {
                            Ok(VpcFirewallRuleProtocol::Udp)
                        }
                        external::VpcFirewallRuleProtocol::Icmp(v) => {
                            Ok(VpcFirewallRuleProtocol::Icmp(v))
                        }
                        external::VpcFirewallRuleProtocol::Icmp6(_) => {
                            Err(Error::invalid_value(
                                "vpc_firewall_rule_protocol",
                                format!("unrecognized protocol: {p}"),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        Ok(Self { hosts: f.hosts, protocols, ports: f.ports })
    }
}

impl From<VpcFirewallRuleFilter> for external::VpcFirewallRuleFilter {
    fn from(f: VpcFirewallRuleFilter) -> Self {
        Self {
            hosts: f.hosts,
            protocols: f
                .protocols
                .map(|ps| ps.into_iter().map(Into::into).collect()),
            ports: f.ports,
        }
    }
}

/// A single rule in a VPC firewall.
//
// This is the version of the rule without `Icmp6` protocol support, for API
// versions up through `MULTICAST_DROP_MVLAN`.
#[derive(ObjectIdentity, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRule {
    /// Common identifying metadata
    #[serde(flatten)]
    pub identity: IdentityMetadata,
    /// Whether this rule is in effect
    pub status: VpcFirewallRuleStatus,
    /// Whether this rule is for incoming or outgoing traffic
    pub direction: VpcFirewallRuleDirection,
    /// Determine the set of instances that the rule applies to
    pub targets: Vec<VpcFirewallRuleTarget>,
    /// Reductions on the scope of the rule
    pub filters: VpcFirewallRuleFilter,
    /// Whether traffic matching the rule should be allowed or dropped
    pub action: VpcFirewallRuleAction,
    /// The relative priority of this rule
    pub priority: VpcFirewallRulePriority,
    /// The VPC to which this rule belongs
    pub vpc_id: Uuid,
}

impl TryFrom<external::VpcFirewallRule> for VpcFirewallRule {
    type Error = Error;

    fn try_from(r: external::VpcFirewallRule) -> Result<Self, Error> {
        Ok(Self {
            identity: r.identity,
            status: r.status,
            direction: r.direction,
            targets: r.targets,
            filters: r.filters.try_into()?,
            action: r.action,
            priority: r.priority,
            vpc_id: r.vpc_id,
        })
    }
}

/// Collection of a VPC's firewall rules.
//
// This is the version of the collection without `Icmp6` protocol support, for
// API versions up through `MULTICAST_DROP_MVLAN`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRules {
    pub rules: Vec<VpcFirewallRule>,
}

impl TryFrom<external::VpcFirewallRules> for VpcFirewallRules {
    type Error = Error;

    fn try_from(r: external::VpcFirewallRules) -> Result<Self, Error> {
        let rules = r
            .rules
            .into_iter()
            .map(VpcFirewallRule::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { rules })
    }
}

/// A single rule in a VPC firewall update request.
//
// This is the version of the update without `Icmp6` protocol support, for API
// versions up through `MULTICAST_DROP_MVLAN`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleUpdate {
    /// Name of the rule, unique to this VPC
    pub name: Name,
    /// Human-readable free-form text about a resource
    pub description: String,
    /// Whether this rule is in effect
    pub status: VpcFirewallRuleStatus,
    /// Whether this rule is for incoming or outgoing traffic
    pub direction: VpcFirewallRuleDirection,
    /// Determine the set of instances that the rule applies to
    #[schemars(length(max = 256))]
    pub targets: Vec<VpcFirewallRuleTarget>,
    /// Reductions on the scope of the rule
    pub filters: VpcFirewallRuleFilter,
    /// Whether traffic matching the rule should be allowed or dropped
    pub action: VpcFirewallRuleAction,
    /// The relative priority of this rule
    pub priority: VpcFirewallRulePriority,
}

impl From<VpcFirewallRuleUpdate> for external::VpcFirewallRuleUpdate {
    fn from(u: VpcFirewallRuleUpdate) -> Self {
        Self {
            name: u.name,
            description: u.description,
            status: u.status,
            direction: u.direction,
            targets: u.targets,
            filters: u.filters.into(),
            action: u.action,
            priority: u.priority,
        }
    }
}

/// Updated list of firewall rules. Will replace all existing rules.
//
// This is the version of the params without `Icmp6` protocol support, for API
// versions up through `MULTICAST_DROP_MVLAN`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRuleUpdateParams {
    #[schemars(length(max = 1024))]
    #[serde(default)]
    pub rules: Vec<VpcFirewallRuleUpdate>,
}

impl From<VpcFirewallRuleUpdateParams>
    for external::VpcFirewallRuleUpdateParams
{
    fn from(p: VpcFirewallRuleUpdateParams) -> Self {
        Self { rules: p.rules.into_iter().map(Into::into).collect() }
    }
}
