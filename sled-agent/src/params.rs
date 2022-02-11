// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::{
    L4PortRange, NetworkInterface, VpcFirewallRuleAction,
    VpcFirewallRuleDirection, VpcFirewallRulePriority, VpcFirewallRuleProtocol,
    VpcFirewallRuleStatus,
};
use omicron_common::api::internal::nexus::DiskRuntimeState;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

///Used to request a Disk state change
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase", tag = "state", content = "instance")]
pub enum DiskStateRequested {
    Detached,
    Attached(Uuid),
    Destroyed,
    Faulted,
}

impl DiskStateRequested {
    /// Returns whether the requested state is attached to an Instance or not.
    pub fn is_attached(&self) -> bool {
        match self {
            DiskStateRequested::Detached => false,
            DiskStateRequested::Destroyed => false,
            DiskStateRequested::Faulted => false,

            DiskStateRequested::Attached(_) => true,
        }
    }
}

/// Sent from to a sled agent to establish the runtime state of a Disk
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DiskEnsureBody {
    /// Last runtime state of the Disk known to Nexus (used if the agent has
    /// never seen this Disk before).
    pub initial_runtime: DiskRuntimeState,
    /// requested runtime state of the Disk
    pub target: DiskStateRequested,
}

/// Sent to a sled agent to establish the current firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub rules: Vec<VpcFirewallRule>,
}

/// VPC firewall rule after object name resolution has been performed by Nexus
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRule {
    pub status: VpcFirewallRuleStatus,
    pub direction: VpcFirewallRuleDirection,
    pub targets: Vec<NetworkInterface>,
    #[schemars(with = "Option<Vec<IpNetworkDef>>")]
    pub filter_hosts: Option<Vec<ipnetwork::IpNetwork>>,
    pub filter_ports: Option<Vec<L4PortRange>>,
    pub filter_protocols: Option<Vec<VpcFirewallRuleProtocol>>,
    pub action: VpcFirewallRuleAction,
    pub priority: VpcFirewallRulePriority,
}

// NOTE: Newtype so that we can derive JsonSchema.
#[derive(JsonSchema)]
#[serde(remote = "ipnetwork::IpNetwork")]
/// IPv4 (in dotted quad) or IPv6address, followed by a slash and prefix length
struct IpNetworkDef(String);
