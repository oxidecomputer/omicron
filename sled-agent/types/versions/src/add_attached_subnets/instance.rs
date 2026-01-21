use std::net::SocketAddr;

use omicron_common::api::external::Hostname;
use omicron_common::api::internal::nexus::VmmRuntimeState;
use omicron_common::api::internal::shared::DelegatedZvol;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_common::api::internal::shared::ExternalIpConfig;
use omicron_common::api::internal::shared::NetworkInterface;
use omicron_common::api::internal::shared::ResolvedVpcFirewallRule;
use omicron_uuid_kinds::InstanceUuid;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::v1::instance::InstanceMetadata;
use crate::v1::instance::VmmSpec;
use crate::v7::instance::InstanceMulticastMembership;
use crate::v18::attached_subnet::AttachedSubnet;

use crate::v17;

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
    pub attached_subnets: Vec<AttachedSubnet>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
}

impl From<v17::instance::InstanceEnsureBody> for InstanceEnsureBody {
    fn from(v17: v17::instance::InstanceEnsureBody) -> InstanceEnsureBody {
        InstanceEnsureBody {
            vmm_spec: v17.vmm_spec,
            local_config: v17.local_config.into(),
            vmm_runtime: v17.vmm_runtime,
            instance_id: v17.instance_id,
            migration_id: v17.migration_id,
            propolis_addr: v17.propolis_addr,
            metadata: v17.metadata,
        }
    }
}

impl From<v17::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(
        v17: v17::instance::InstanceSledLocalConfig,
    ) -> InstanceSledLocalConfig {
        InstanceSledLocalConfig {
            hostname: v17.hostname,
            nics: v17.nics,
            external_ips: v17.external_ips,
            attached_subnets: vec![],
            multicast_groups: v17.multicast_groups,
            firewall_rules: v17.firewall_rules,
            dhcp_config: v17.dhcp_config,
            delegated_zvols: v17.delegated_zvols,
        }
    }
}
