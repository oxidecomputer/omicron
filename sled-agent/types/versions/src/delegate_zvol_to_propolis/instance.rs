// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::IpAddr;

use omicron_common::api::external::Hostname;
use omicron_common::api::internal::shared::DhcpConfig;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalZpoolUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v1::instance::ResolvedVpcFirewallRule;
use crate::v1::inventory::NetworkInterface;
use crate::v1::inventory::SourceNatConfig;
use crate::v7;
use crate::v7::instance::InstanceMulticastMembership;

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

/// Delegate a ZFS volume to a zone
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DelegatedZvol {
    /// Delegate a slice of the local storage dataset present on this pool into
    /// the zone.
    LocalStorage { zpool_id: ExternalZpoolUuid, dataset_id: DatasetUuid },
}

impl From<v7::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(v7: v7::instance::InstanceSledLocalConfig) -> Self {
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
