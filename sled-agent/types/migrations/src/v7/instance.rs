// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for Sled Agent API version 7.
//!
//! This version adds multicast_groups to InstanceSledLocalConfig.
//!
//! Types that are unchanged from v1 are referenced from there:
//! - VmmSpec
//! - InstanceMetadata
//! - ResolvedVpcFirewallRule

use std::net::{IpAddr, SocketAddr};

use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_common::api::internal::shared::{DhcpConfig, SourceNatConfig};
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Unchanged types from v1
use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::ResolvedVpcFirewallRule;
use crate::v1::instance::VmmSpec;

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

impl From<crate::v1::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(v1: crate::v1::instance::InstanceEnsureBody) -> Self {
        Self {
            vmm_spec: v1.vmm_spec,
            local_config: v1.local_config.into(),
            vmm_runtime: v1.vmm_runtime,
            instance_id: v1.instance_id,
            migration_id: v1.migration_id,
            propolis_addr: v1.propolis_addr,
            metadata: v1.metadata,
        }
    }
}

impl From<crate::v1::instance::InstanceSledLocalConfig>
    for InstanceSledLocalConfig
{
    fn from(v1: crate::v1::instance::InstanceSledLocalConfig) -> Self {
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
