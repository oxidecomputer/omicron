// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, SocketAddr};

use crate::{v1, v9};
use omicron_common::api::{
    external,
    external::Hostname,
    internal::{
        nexus::VmmRuntimeState,
        shared::{
            DelegatedZvol, DhcpConfig, NetworkInterface,
            ResolvedVpcFirewallRule, SourceNatConfig,
        },
    },
};
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The body of a request to ensure that a instance and VMM are known to a sled
/// agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// The virtual hardware configuration this virtual machine should have when
    /// it is started.
    pub vmm_spec: v1::VmmSpec,

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
    pub metadata: v1::InstanceMetadata,
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
    pub multicast_groups: Vec<v1::InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl TryFrom<v9::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    type Error = external::Error;

    fn try_from(value: InstanceSledLocalConfig) -> Result<Self, Self::Error> {
        let nics = value
            .nics
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        let firewall_rules = value
            .firewall_rules
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            hostname: value.hostname,
            nics,
            source_nat: value.source_nat,
            ephemeral_ip: value.ephemeral_ip,
            floating_ips: value.floating_ips,
            multicast_groups: value.multicast_groups,
            firewall_rules,
            dhcp_config: value.dhcp_config,
            delegated_zvols: value.delegated_zvols,
        })
    }
}

/// Request body for multicast group operations.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstanceMulticastBody {
    Join(v1::InstanceMulticastMembership),
    Leave(v1::InstanceMulticastMembership),
}
