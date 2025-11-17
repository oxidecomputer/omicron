// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Sled agent API types (version 7)
//!
//! Version 7 adds support for multicast group management on instances.

use std::net::{IpAddr, SocketAddr};

use omicron_common::api::{
    external::Hostname,
    internal::{
        nexus::VmmRuntimeState,
        shared::{
            DhcpConfig, NetworkInterface, ResolvedVpcFirewallRule,
            SourceNatConfig,
        },
    },
};
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use sled_agent_types::instance::{InstanceMetadata, VmmSpec};

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
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
}

impl From<InstanceEnsureBody>
    for sled_agent_types::instance::InstanceEnsureBody
{
    fn from(
        v6: InstanceEnsureBody,
    ) -> sled_agent_types::instance::InstanceEnsureBody {
        sled_agent_types::instance::InstanceEnsureBody {
            vmm_spec: v6.vmm_spec,
            local_config: sled_agent_types::instance::InstanceSledLocalConfig {
                hostname: v6.local_config.hostname,
                nics: v6.local_config.nics,
                source_nat: v6.local_config.source_nat,
                ephemeral_ip: v6.local_config.ephemeral_ip,
                floating_ips: v6.local_config.floating_ips,
                multicast_groups: Vec::new(),
                firewall_rules: v6.local_config.firewall_rules,
                dhcp_config: v6.local_config.dhcp_config,
            },
            vmm_runtime: v6.vmm_runtime,
            instance_id: v6.instance_id,
            migration_id: v6.migration_id,
            propolis_addr: v6.propolis_addr,
            metadata: v6.metadata,
        }
    }
}
