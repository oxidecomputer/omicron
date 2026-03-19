// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version `MAKE_ALL_EXTERNAL_IP_FIELDS_OPTIONAL`.

use std::net::SocketAddr;

use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::DelegatedZvol;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::ExternalIpConfig;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::VmmSpec;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v18::attached_subnet::AttachedSubnet;
use crate::v29;
use crate::v29::instance::ResolvedVpcFirewallRule;

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
}

impl From<v29::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(v29: v29::instance::InstanceEnsureBody) -> Self {
        Self {
            vmm_spec: v29.vmm_spec,
            local_config: v29.local_config.into(),
            vmm_runtime: v29.vmm_runtime,
            instance_id: v29.instance_id,
            migration_id: v29.migration_id,
            propolis_addr: v29.propolis_addr,
            metadata: v29.metadata,
        }
    }
}

impl From<v29::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(v29: v29::instance::InstanceSledLocalConfig) -> Self {
        let external_ips = match v29.external_ips {
            None => ExternalIpConfig::default(),
            Some(old) => old.into(),
        };
        Self {
            hostname: v29.hostname,
            nics: v29.nics,
            external_ips,
            attached_subnets: v29.attached_subnets,
            multicast_groups: v29.multicast_groups,
            firewall_rules: v29.firewall_rules,
            dhcp_config: v29.dhcp_config,
            delegated_zvols: v29.delegated_zvols,
        }
    }
}
