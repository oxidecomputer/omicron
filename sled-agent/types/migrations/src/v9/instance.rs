// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for Sled Agent API version 9.
//!
//! This version adds delegated_zvols to InstanceSledLocalConfig.
//!
//! Types that are unchanged from v1 are referenced from there:
//! - VmmSpec
//! - InstanceMetadata
//! - ResolvedVpcFirewallRule

use std::net::{IpAddr, SocketAddr};

use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::network_interface::v1::NetworkInterface;
use omicron_common::api::internal::shared::{
    DelegatedZvol, DhcpConfig, SourceNatConfig,
};
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::ResolvedVpcFirewallRule;
use crate::v1::instance::VmmSpec;
use crate::v7::instance::InstanceMulticastMembership;

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
/// Added in v9: `delegated_zvols` field.
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

/// Update firewall rules for a VPC
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct VpcFirewallRulesEnsureBody {
    pub vni: external::Vni,
    pub rules: Vec<ResolvedVpcFirewallRule>,
}

impl From<crate::v7::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(v7: crate::v7::instance::InstanceEnsureBody) -> Self {
        Self {
            vmm_spec: v7.vmm_spec,
            local_config: v7.local_config.into(),
            vmm_runtime: v7.vmm_runtime,
            instance_id: v7.instance_id,
            migration_id: v7.migration_id,
            propolis_addr: v7.propolis_addr,
            metadata: v7.metadata,
        }
    }
}

impl From<crate::v7::instance::InstanceSledLocalConfig>
    for InstanceSledLocalConfig
{
    fn from(v7: crate::v7::instance::InstanceSledLocalConfig) -> Self {
        Self {
            hostname: v7.hostname,
            nics: v7.nics,
            source_nat: v7.source_nat,
            ephemeral_ip: v7.ephemeral_ip,
            floating_ips: v7.floating_ips,
            multicast_groups: v7.multicast_groups,
            firewall_rules: v7.firewall_rules,
            dhcp_config: v7.dhcp_config,
            delegated_zvols: Vec::new(), // Added in v9
        }
    }
}
