// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types from API version 2026_06_16_00 that cannot live in `nexus-types-versions`
//! because they convert to/from `omicron-common` types (orphan rule).

use api_identity::ObjectIdentity;
use omicron_common::api::external;
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
// This is the version of the enum that serializes ICMP protocols as `icmp` and
// `icmp6`, for API versions from `ADD_ICMPV6_FIREWALL_SUPPORT` up through
// `INSTANCE_CPU_TYPE_TURIN_V2`. The current version renames these to
// `icmp_v4` and `icmp_v6`.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type", content = "value")]
pub enum VpcFirewallRuleProtocol {
    Tcp,
    Udp,
    Icmp(Option<VpcFirewallIcmpFilter>),
    Icmp6(Option<VpcFirewallIcmpFilter>),
}

impl From<VpcFirewallRuleProtocol> for external::VpcFirewallRuleProtocol {
    fn from(p: VpcFirewallRuleProtocol) -> Self {
        match p {
            VpcFirewallRuleProtocol::Tcp => Self::Tcp,
            VpcFirewallRuleProtocol::Udp => Self::Udp,
            VpcFirewallRuleProtocol::Icmp(v) => Self::IcmpV4(v),
            VpcFirewallRuleProtocol::Icmp6(v) => Self::IcmpV6(v),
        }
    }
}

impl From<external::VpcFirewallRuleProtocol> for VpcFirewallRuleProtocol {
    fn from(p: external::VpcFirewallRuleProtocol) -> Self {
        match p {
            external::VpcFirewallRuleProtocol::Tcp => Self::Tcp,
            external::VpcFirewallRuleProtocol::Udp => Self::Udp,
            external::VpcFirewallRuleProtocol::IcmpV4(v) => Self::Icmp(v),
            external::VpcFirewallRuleProtocol::IcmpV6(v) => Self::Icmp6(v),
        }
    }
}

/// Filters reduce the scope of a firewall rule.
//
// This is the version of the filter that serializes ICMP protocols as `icmp`
// and `icmp6`, for API versions from `ADD_ICMPV6_FIREWALL_SUPPORT` up through
// `INSTANCE_CPU_TYPE_TURIN_V2`.
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

impl From<external::VpcFirewallRuleFilter> for VpcFirewallRuleFilter {
    fn from(f: external::VpcFirewallRuleFilter) -> Self {
        Self {
            hosts: f.hosts,
            protocols: f
                .protocols
                .map(|ps| ps.into_iter().map(Into::into).collect()),
            ports: f.ports,
        }
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
// This is the version of the rule that serializes ICMP protocols as `icmp` and
// `icmp6`, for API versions from `ADD_ICMPV6_FIREWALL_SUPPORT` up through
// `INSTANCE_CPU_TYPE_TURIN_V2`.
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

impl From<external::VpcFirewallRule> for VpcFirewallRule {
    fn from(r: external::VpcFirewallRule) -> Self {
        Self {
            identity: r.identity,
            status: r.status,
            direction: r.direction,
            targets: r.targets,
            filters: r.filters.into(),
            action: r.action,
            priority: r.priority,
            vpc_id: r.vpc_id,
        }
    }
}

/// Collection of a VPC's firewall rules.
//
// This is the version of the collection that serializes ICMP protocols as
// `icmp` and `icmp6`, for API versions from `ADD_ICMPV6_FIREWALL_SUPPORT` up
// through `INSTANCE_CPU_TYPE_TURIN_V2`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VpcFirewallRules {
    pub rules: Vec<VpcFirewallRule>,
}

impl From<external::VpcFirewallRules> for VpcFirewallRules {
    fn from(r: external::VpcFirewallRules) -> Self {
        Self { rules: r.rules.into_iter().map(VpcFirewallRule::from).collect() }
    }
}

/// A single rule in a VPC firewall update request.
//
// This is the version of the update that serializes ICMP protocols as `icmp`
// and `icmp6`, for API versions from `ADD_ICMPV6_FIREWALL_SUPPORT` up through
// `INSTANCE_CPU_TYPE_TURIN_V2`.
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
// This is the version of the params that serializes ICMP protocols as `icmp`
// and `icmp6`, for API versions from `ADD_ICMPV6_FIREWALL_SUPPORT` up through
// `INSTANCE_CPU_TYPE_TURIN_V2`.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn firewall_protocol_uses_legacy_wire_spelling() {
        let icmp = VpcFirewallRuleProtocol::Icmp(None);
        assert_eq!(
            serde_json::to_value(icmp).unwrap(),
            serde_json::json!({
                "type": "icmp",
                "value": null,
            })
        );

        let icmp6 =
            VpcFirewallRuleProtocol::Icmp6(Some(VpcFirewallIcmpFilter {
                icmp_type: 128,
                code: None,
            }));
        assert_eq!(
            serde_json::to_value(icmp6).unwrap(),
            serde_json::json!({
                "type": "icmp6",
                "value": {
                    "icmp_type": 128,
                    "code": null,
                },
            })
        );
    }

    #[test]
    fn firewall_protocol_converts_to_latest_variants() {
        let icmp: external::VpcFirewallRuleProtocol =
            VpcFirewallRuleProtocol::Icmp(None).into();
        assert_eq!(icmp, external::VpcFirewallRuleProtocol::IcmpV4(None));

        let icmp6: external::VpcFirewallRuleProtocol =
            VpcFirewallRuleProtocol::Icmp6(None).into();
        assert_eq!(icmp6, external::VpcFirewallRuleProtocol::IcmpV6(None));

        let old: VpcFirewallRuleProtocol =
            external::VpcFirewallRuleProtocol::IcmpV4(None).into();
        assert_eq!(old, VpcFirewallRuleProtocol::Icmp(None));

        let old6: VpcFirewallRuleProtocol =
            external::VpcFirewallRuleProtocol::IcmpV6(None).into();
        assert_eq!(old6, VpcFirewallRuleProtocol::Icmp6(None));
    }
}
