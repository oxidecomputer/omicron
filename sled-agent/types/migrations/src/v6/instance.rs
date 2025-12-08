// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for Sled Agent API version 6.
//!
//! This version does NOT have multicast_groups (added in v7) or
//! delegated_zvols (added in v9). Uses NetworkInterface v1.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};

use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::{HostIdentifier, VmmRuntimeState};
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_common::api::internal::shared::{DhcpConfig, SourceNatConfig};
use omicron_uuid_kinds::InstanceUuid;
use propolis_client::instance_spec::InstanceSpecV0;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
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

/// Metadata used to track statistics about an instance.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct InstanceMetadata {
    pub silo_id: Uuid,
    pub project_id: Uuid,
}

/// Specifies the virtual hardware configuration of a new Propolis VMM in the
/// form of a Propolis instance specification.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VmmSpec(pub InstanceSpecV0);

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

impl From<InstanceEnsureBody> for crate::v7::instance::InstanceEnsureBody {
    fn from(v6: InstanceEnsureBody) -> Self {
        Self {
            vmm_spec: crate::v7::instance::VmmSpec(v6.vmm_spec.0),
            local_config: v6.local_config.into(),
            vmm_runtime: v6.vmm_runtime,
            instance_id: v6.instance_id,
            migration_id: v6.migration_id,
            propolis_addr: v6.propolis_addr,
            metadata: crate::v7::instance::InstanceMetadata {
                silo_id: v6.metadata.silo_id,
                project_id: v6.metadata.project_id,
            },
        }
    }
}

impl From<InstanceSledLocalConfig>
    for crate::v7::instance::InstanceSledLocalConfig
{
    fn from(v6: InstanceSledLocalConfig) -> Self {
        Self {
            hostname: v6.hostname,
            nics: v6.nics,
            source_nat: v6.source_nat,
            ephemeral_ip: v6.ephemeral_ip,
            floating_ips: v6.floating_ips,
            multicast_groups: Vec::new(), // Added in v7
            firewall_rules: v6
                .firewall_rules
                .into_iter()
                .map(Into::into)
                .collect(),
            dhcp_config: v6.dhcp_config,
        }
    }
}

impl From<ResolvedVpcFirewallRule>
    for crate::v7::instance::ResolvedVpcFirewallRule
{
    fn from(v6: ResolvedVpcFirewallRule) -> Self {
        Self {
            status: v6.status,
            direction: v6.direction,
            targets: v6.targets,
            filter_hosts: v6.filter_hosts,
            filter_ports: v6.filter_ports,
            filter_protocols: v6.filter_protocols,
            action: v6.action,
            priority: v6.priority,
        }
    }
}
