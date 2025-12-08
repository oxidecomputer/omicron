// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for Sled Agent API version 10+.
//!
//! This version uses NetworkInterface v2 (dual-stack, multiple IP addresses).

use std::net::{IpAddr, SocketAddr};

use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use omicron_common::api::internal::shared::{
    DelegatedZvol, DhcpConfig, SourceNatConfig,
};
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Unchanged types from earlier versions
use crate::v1::instance::InstanceMetadata;
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

impl TryFrom<crate::v9::instance::InstanceEnsureBody> for InstanceEnsureBody {
    type Error = external::Error;

    fn try_from(
        v9: crate::v9::instance::InstanceEnsureBody,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            vmm_spec: v9.vmm_spec,
            local_config: v9.local_config.try_into()?,
            vmm_runtime: v9.vmm_runtime,
            instance_id: v9.instance_id,
            migration_id: v9.migration_id,
            propolis_addr: v9.propolis_addr,
            metadata: v9.metadata,
        })
    }
}

impl TryFrom<crate::v9::instance::InstanceSledLocalConfig>
    for InstanceSledLocalConfig
{
    type Error = external::Error;

    fn try_from(
        v9: crate::v9::instance::InstanceSledLocalConfig,
    ) -> Result<Self, Self::Error> {
        let firewall_rules = v9
            .firewall_rules
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            hostname: v9.hostname,
            nics: v9
                .nics
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            source_nat: v9.source_nat,
            ephemeral_ip: v9.ephemeral_ip,
            floating_ips: v9.floating_ips,
            multicast_groups: v9.multicast_groups,
            firewall_rules,
            dhcp_config: v9.dhcp_config,
            delegated_zvols: v9.delegated_zvols,
        })
    }
}

impl TryFrom<crate::v1::instance::ResolvedVpcFirewallRule>
    for ResolvedVpcFirewallRule
{
    type Error = external::Error;

    fn try_from(
        v1: crate::v1::instance::ResolvedVpcFirewallRule,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            status: v1.status,
            direction: v1.direction,
            targets: v1
                .targets
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            filter_hosts: v1.filter_hosts,
            filter_ports: v1.filter_ports,
            filter_protocols: v1.filter_protocols,
            action: v1.action,
            priority: v1.priority,
        })
    }
}

impl TryFrom<crate::v9::instance::VpcFirewallRulesEnsureBody>
    for VpcFirewallRulesEnsureBody
{
    type Error = external::Error;

    fn try_from(
        v9: crate::v9::instance::VpcFirewallRulesEnsureBody,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            vni: v9.vni,
            rules: v9
                .rules
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
        })
    }
}
