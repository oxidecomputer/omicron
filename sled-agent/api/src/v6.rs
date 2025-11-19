// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent types (version 6)
//!
//! Version 6 types (before multicast support was added in version 7).

use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::HostIdentifier;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::SourceNatConfig;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use sled_agent_types::instance::InstanceMetadata;
use sled_agent_types::instance::VmmSpec;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::SocketAddr;
use uuid::Uuid;

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent (version 6, before multicast support).
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
/// the instance's virtual hardware fully functional (version 6, before multicast).
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceSledLocalConfig {
    pub hostname: Hostname,
    pub nics: Vec<NetworkInterface>,
    pub source_nat: SourceNatConfig,
    /// Zero or more external IP addresses (either floating or ephemeral),
    /// provided to an instance to allow inbound connectivity.
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: Vec<IpAddr>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
}

impl From<InstanceSledLocalConfig>
    for sled_agent_types::inventory::v8::InstanceSledLocalConfig
{
    fn from(v6: InstanceSledLocalConfig) -> Self {
        let InstanceSledLocalConfig {
            hostname,
            nics,
            source_nat,
            ephemeral_ip,
            floating_ips,
            firewall_rules,
            dhcp_config,
        } = v6;
        let firewall_rules =
            firewall_rules.into_iter().map(Into::into).collect();

        Self {
            hostname,
            nics,
            source_nat,
            ephemeral_ip,
            floating_ips,
            multicast_groups: Vec::new(),
            firewall_rules,
            dhcp_config,
        }
    }
}

impl From<InstanceEnsureBody>
    for sled_agent_types::inventory::v8::InstanceEnsureBody
{
    fn from(v6: InstanceEnsureBody) -> Self {
        let InstanceEnsureBody {
            vmm_spec,
            local_config,
            vmm_runtime,
            instance_id,
            migration_id,
            propolis_addr,
            metadata,
        } = v6;

        Self {
            vmm_spec,
            local_config: local_config.into(),
            vmm_runtime,
            instance_id,
            migration_id,
            propolis_addr,
            metadata,
        }
    }
}

/// VPC firewall rule after object name resolution has been performed by Nexus
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

impl From<ResolvedVpcFirewallRule>
    for sled_agent_types::inventory::v8::ResolvedVpcFirewallRule
{
    fn from(v6: ResolvedVpcFirewallRule) -> Self {
        let ResolvedVpcFirewallRule {
            status,
            direction,
            targets,
            filter_hosts,
            filter_ports,
            filter_protocols,
            action,
            priority,
        } = v6;
        Self {
            status,
            direction,
            targets,
            filter_hosts,
            filter_ports,
            filter_protocols,
            action,
            priority,
        }
    }
}
