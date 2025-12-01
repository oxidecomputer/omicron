// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent types that changed from version 8 to version 9

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
use sled_agent_types::instance::InstanceMulticastMembership;
use sled_agent_types::instance::VmmSpec;
use sled_agent_types::inventory::v9;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::SocketAddr;
use uuid::Uuid;

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
    pub source_nat: SourceNatConfig,
    /// Zero or more external IP addresses (either floating or ephemeral),
    /// provided to an instance to allow inbound connectivity.
    pub ephemeral_ip: Option<IpAddr>,
    pub floating_ips: Vec<IpAddr>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
}

impl From<InstanceEnsureBody> for v9::InstanceEnsureBody {
    fn from(v8: InstanceEnsureBody) -> Self {
        Self {
            vmm_spec: v8.vmm_spec,
            local_config: v8.local_config.into(),
            vmm_runtime: v8.vmm_runtime,
            instance_id: v8.instance_id,
            migration_id: v8.migration_id,
            propolis_addr: v8.propolis_addr,
            metadata: v8.metadata,
        }
    }
}

impl From<InstanceSledLocalConfig> for v9::InstanceSledLocalConfig {
    fn from(v8: InstanceSledLocalConfig) -> Self {
        let firewall_rules =
            v8.firewall_rules.into_iter().map(Into::into).collect();
        Self {
            hostname: v8.hostname,
            nics: v8.nics,
            source_nat: v8.source_nat,
            ephemeral_ip: v8.ephemeral_ip,
            floating_ips: v8.floating_ips,
            multicast_groups: v8.multicast_groups,
            firewall_rules,
            dhcp_config: v8.dhcp_config,
            delegated_zvols: vec![],
        }
    }
}

/// VPC firewall rule after object name resolution has been performed by Nexus
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
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

impl From<ResolvedVpcFirewallRule> for v9::ResolvedVpcFirewallRule {
    fn from(v8: ResolvedVpcFirewallRule) -> Self {
        Self {
            status: v8.status,
            direction: v8.direction,
            targets: v8.targets,
            filter_hosts: v8.filter_hosts,
            filter_ports: v8.filter_ports,
            filter_protocols: v8.filter_protocols,
            action: v8.action,
            priority: v8.priority,
        }
    }
}
