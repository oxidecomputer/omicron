// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version `ADD_INSTANCE_PRIMARY_NIC_MTU`.

use std::net::SocketAddr;

use omicron_common::api::external::Hostname;
use omicron_common::api::internal::shared::DelegatedZvol;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::VmmRuntimeState;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v10::inventory::NetworkInterface;
use crate::v18::attached_subnet::AttachedSubnet;
use crate::v29::instance::VmmSpec;
use crate::v31::instance::ResolvedVpcFirewallRule;
use crate::v32;
use crate::v32::instance::ExternalIpConfig;

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
    pub external_ips: ExternalIpConfig,
    pub attached_subnets: Vec<AttachedSubnet>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
    /// The MTU to apply to the instance's primary OPTE port, in bytes. If
    /// `None`, the OPTE default is used (1500). Set when the fleet has
    /// enabled external jumbo frames and the instance has opted in.
    pub primary_nic_mtu: Option<u32>,
}

impl From<v32::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(old: v32::instance::InstanceEnsureBody) -> Self {
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

impl From<v32::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(old: v32::instance::InstanceSledLocalConfig) -> Self {
        Self {
            hostname: old.hostname,
            nics: old.nics,
            external_ips: old.external_ips,
            attached_subnets: old.attached_subnets,
            multicast_groups: old.multicast_groups,
            firewall_rules: old.firewall_rules,
            dhcp_config: old.dhcp_config,
            delegated_zvols: old.delegated_zvols,
            primary_nic_mtu: None,
        }
    }
}
