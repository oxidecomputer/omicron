// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Instance types for version `INSTANCE_ATTACHED_SUBNETS_MAP`.

use std::net::SocketAddr;

use iddqd::IdOrdMap;
use iddqd::errors::DuplicateItem;
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
use crate::v31::instance::ResolvedVpcFirewallRule;
use crate::v32::instance::ExternalIpConfig;
use crate::v41;
use crate::v44;
use crate::v44::instance::VmmSpec;

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
    pub attached_subnets: IdOrdMap<AttachedSubnet>,
    pub multicast_groups: Vec<InstanceMulticastMembership>,
    pub firewall_rules: Vec<ResolvedVpcFirewallRule>,
    pub dhcp_config: DhcpConfig,
    pub delegated_zvols: Vec<DelegatedZvol>,
    /// The MTU to apply to the instance's primary OPTE port, in bytes. If
    /// `None`, the OPTE default is used (1500). Set when the fleet has
    /// enabled external jumbo frames and the instance has opted in.
    pub primary_nic_mtu: Option<u32>,
}

impl TryFrom<v44::instance::InstanceEnsureBody> for InstanceEnsureBody {
    type Error = DuplicateItem<AttachedSubnet>;

    fn try_from(
        old: v44::instance::InstanceEnsureBody,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            vmm_spec: old.vmm_spec,
            local_config: old.local_config.try_into()?,
            vmm_runtime: old.vmm_runtime,
            instance_id: old.instance_id,
            migration_id: old.migration_id,
            propolis_addr: old.propolis_addr,
            metadata: old.metadata,
        })
    }
}

impl TryFrom<v41::instance::InstanceSledLocalConfig>
    for InstanceSledLocalConfig
{
    type Error = DuplicateItem<AttachedSubnet>;

    fn try_from(
        old: v41::instance::InstanceSledLocalConfig,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            hostname: old.hostname,
            nics: old.nics,
            external_ips: old.external_ips,
            attached_subnets: IdOrdMap::from_iter_unique(old.attached_subnets)?,
            multicast_groups: old.multicast_groups,
            firewall_rules: old.firewall_rules,
            dhcp_config: old.dhcp_config,
            delegated_zvols: old.delegated_zvols,
            primary_nic_mtu: old.primary_nic_mtu,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v18::attached_subnet::AttachedSubnetKind;
    use iddqd::id_ord_map;
    use oxnet::IpNet;
    use std::str::FromStr;

    fn v41_local_config(
        attached_subnets: Vec<AttachedSubnet>,
    ) -> v41::instance::InstanceSledLocalConfig {
        v41::instance::InstanceSledLocalConfig {
            hostname: Hostname::from_str("bert").unwrap(),
            nics: vec![],
            external_ips: ExternalIpConfig::default(),
            attached_subnets,
            multicast_groups: vec![],
            firewall_rules: vec![],
            dhcp_config: DhcpConfig {
                dns_servers: vec![],
                host_domain: None,
                search_domains: vec![],
            },
            delegated_zvols: vec![],
            primary_nic_mtu: Some(8500),
        }
    }

    #[test]
    fn local_config_from_v41_keeps_distinct_subnets() {
        let external = AttachedSubnet {
            subnet: "10.0.0.0/24".parse().unwrap(),
            kind: AttachedSubnetKind::External,
        };
        let vpc = AttachedSubnet {
            subnet: "10.0.1.0/24".parse().unwrap(),
            kind: AttachedSubnetKind::Vpc,
        };
        let config = InstanceSledLocalConfig::try_from(v41_local_config(vec![
            external, vpc,
        ]))
        .expect("distinct subnets are accepted");
        assert_eq!(config.attached_subnets, id_ord_map! { external, vpc });
        assert_eq!(config.primary_nic_mtu, Some(8500));
    }

    #[test]
    fn local_config_from_v41_rejects_duplicate_subnets() {
        let subnet: IpNet = "10.0.0.0/24".parse().unwrap();
        let first =
            AttachedSubnet { subnet, kind: AttachedSubnetKind::External };
        let second = AttachedSubnet { subnet, kind: AttachedSubnetKind::Vpc };
        let err = InstanceSledLocalConfig::try_from(v41_local_config(vec![
            first, second,
        ]))
        .expect_err("duplicate subnets are rejected");
        assert_eq!(err.new_item(), &second);
        assert_eq!(err.duplicates(), [first]);
    }
}
