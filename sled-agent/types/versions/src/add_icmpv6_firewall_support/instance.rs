// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version `ADD_ICMPV6_FIREWALL_SUPPORT`.

use std::collections::HashSet;
use std::net::SocketAddr;

use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::DelegatedZvol;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::external_ip::v1::ExternalIpConfig;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v10;
use crate::v18;
use crate::v18::attached_subnet::AttachedSubnet;
use crate::v29;
use crate::v29::instance::VmmSpec;

/// VPC firewall rule after object name resolution has been performed by Nexus.
//
// This version supports `Icmp6` in the protocol filter, added in
// `ADD_ICMPV6_FIREWALL_SUPPORT`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct ResolvedVpcFirewallRule {
    pub status: external::VpcFirewallRuleStatus,
    pub direction: external::VpcFirewallRuleDirection,
    pub targets: Vec<NetworkInterface>,
    pub filter_hosts: Option<HashSet<HostIdentifier>>,
    pub filter_ports: Option<Vec<external::L4PortRange>>,
    pub filter_protocols: Option<Vec<external::VpcFirewallRuleProtocol>>,
    pub action: external::VpcFirewallRuleAction,
    pub priority: external::VpcFirewallRulePriority,
}

impl From<v10::instance::ResolvedVpcFirewallRule> for ResolvedVpcFirewallRule {
    fn from(old: v10::instance::ResolvedVpcFirewallRule) -> Self {
        Self {
            status: old.status,
            direction: old.direction,
            targets: old.targets,
            filter_hosts: old.filter_hosts,
            filter_ports: old.filter_ports,
            filter_protocols: old.filter_protocols.map(|ps| {
                ps.into_iter()
                    .map(|p| match p {
                        v10::instance::VpcFirewallRuleProtocol::Tcp => {
                            external::VpcFirewallRuleProtocol::Tcp
                        }
                        v10::instance::VpcFirewallRuleProtocol::Udp => {
                            external::VpcFirewallRuleProtocol::Udp
                        }
                        v10::instance::VpcFirewallRuleProtocol::Icmp(f) => {
                            external::VpcFirewallRuleProtocol::Icmp(f)
                        }
                    })
                    .collect()
            }),
            action: old.action,
            priority: old.priority,
        }
    }
}

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// The virtual hardware configuration this virtual machine should have when
    /// it is started.
    pub vmm_spec: VmmSpec,

    /// Information about the sled-local configuration that needs to be
    /// established to make the VM's virtual hardware fully functional.
    pub local_config: InstanceSledLocalConfig,

    /// The initial VMM runtime state for the VMM being registered.
    pub vmm_runtime: VmmRuntimeState,

    /// The ID of the instance for which this VMM is being created.
    pub instance_id: InstanceUuid,

    /// The ID of the migration in to this VMM, if this VMM is being
    /// ensured is part of a migration in. If this is `None`, the VMM is not
    /// being created due to a migration.
    pub migration_id: Option<Uuid>,

    /// The address at which this VMM should serve a Propolis server API.
    pub propolis_addr: SocketAddr,

    /// Metadata used to track instance statistics.
    pub metadata: InstanceMetadata,
}

/// Describes sled-local configuration that a sled-agent must establish to make
/// the instance's virtual hardware fully functional.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceSledLocalConfig {
    pub hostname: Hostname,
    pub nics: Vec<NetworkInterface>,
    pub external_ips: Option<ExternalIpConfig>,
    pub attached_subnets: Vec<AttachedSubnet>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl From<v29::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(old: v29::instance::InstanceEnsureBody) -> Self {
        Self {
            vmm_spec: old.vmm_spec,
            local_config: old.local_config.into(),
            vmm_runtime: old.vmm_runtime,
            instance_id: old.instance_id,
            migration_id: old.migration_id,
            propolis_addr: old.propolis_addr,
            metadata: old.metadata,
        }
    }
}

impl From<ResolvedVpcFirewallRule>
    for omicron_common::api::internal::shared::ResolvedVpcFirewallRule
{
    fn from(rule: ResolvedVpcFirewallRule) -> Self {
        Self {
            status: rule.status,
            direction: rule.direction,
            targets: rule.targets,
            filter_hosts: rule.filter_hosts,
            filter_ports: rule.filter_ports,
            filter_protocols: rule.filter_protocols,
            action: rule.action,
            priority: rule.priority,
        }
    }
}

impl From<v18::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(v18: v18::instance::InstanceSledLocalConfig) -> Self {
        Self {
            hostname: v18.hostname,
            nics: v18.nics,
            external_ips: v18.external_ips,
            attached_subnets: v18.attached_subnets,
            multicast_groups: v18.multicast_groups,
            firewall_rules: v18
                .firewall_rules
                .into_iter()
                .map(ResolvedVpcFirewallRule::from)
                .collect(),
            dhcp_config: v18.dhcp_config,
            delegated_zvols: v18.delegated_zvols,
        }
    }
}
