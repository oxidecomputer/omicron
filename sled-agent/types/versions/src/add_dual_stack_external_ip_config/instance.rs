// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::SocketAddr;

use omicron_common::api::external;
use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::ExternalIpConfig;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::VmmSpec;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v9::instance::DelegatedZvol;
use crate::v10;

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
    pub external_ips: Option<ExternalIpConfig>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl TryFrom<v10::instance::InstanceEnsureBody> for InstanceEnsureBody {
    type Error = external::Error;

    fn try_from(
        v10: v10::instance::InstanceEnsureBody,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            vmm_spec: v10.vmm_spec,
            local_config: v10.local_config.try_into()?,
            vmm_runtime: v10.vmm_runtime,
            instance_id: v10.instance_id,
            migration_id: v10.migration_id,
            propolis_addr: v10.propolis_addr,
            metadata: v10.metadata,
        })
    }
}

impl TryFrom<v10::instance::InstanceSledLocalConfig>
    for InstanceSledLocalConfig
{
    type Error = external::Error;

    fn try_from(
        v10: v10::instance::InstanceSledLocalConfig,
    ) -> Result<Self, Self::Error> {
        // v10.source_nat is already a v1::SourceNatConfig, so we can use it directly
        let external_ips = ExternalIpConfig::try_from_generic(
            Some(v10.source_nat),
            v10.ephemeral_ip,
            v10.floating_ips.clone(),
        )
        .map_err(|e| {
            external::Error::invalid_request(format!(
                "invalid external IP config: {e}"
            ))
        })?;

        Ok(Self {
            hostname: v10.hostname,
            nics: v10.nics,
            external_ips: Some(external_ips),
            multicast_groups: v10.multicast_groups,
            firewall_rules: v10.firewall_rules,
            dhcp_config: v10.dhcp_config,
            delegated_zvols: v10.delegated_zvols,
        })
    }
}
