// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::api::external::Hostname;
use omicron_common::api::internal::shared::DelegatedZvol;
use omicron_common::api::internal::shared::DhcpConfig;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::v7::instance::InstanceMulticastMembership;
use crate::v9;
use crate::v10::instance::ResolvedVpcFirewallRule;
use crate::v10::inventory::NetworkInterface;
use crate::v11;
use crate::v11::instance::ExternalIpConfig;

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

impl From<v9::instance::DelegatedZvol> for DelegatedZvol {
    fn from(v9: v9::instance::DelegatedZvol) -> Self {
        match v9 {
            v9::instance::DelegatedZvol::LocalStorage {
                zpool_id,
                dataset_id,
            } => {
                // The previous version of this API was meant to allocate local
                // storage from the encrypted dataset.
                DelegatedZvol::LocalStorageEncrypted { zpool_id, dataset_id }
            }
        }
    }
}

impl From<v11::instance::InstanceSledLocalConfig> for InstanceSledLocalConfig {
    fn from(
        v11: v11::instance::InstanceSledLocalConfig,
    ) -> InstanceSledLocalConfig {
        InstanceSledLocalConfig {
            hostname: v11.hostname,
            nics: v11.nics,
            external_ips: v11.external_ips,
            multicast_groups: v11.multicast_groups,
            firewall_rules: v11.firewall_rules,
            dhcp_config: v11.dhcp_config,
            delegated_zvols: v11
                .delegated_zvols
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}
