// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Uplink-related types for the Sled Agent API.

use super::early_networking::UplinkAddressConfig;
use crate::v1;
use crate::v1::early_networking::LldpPortConfig;
use crate::v1::early_networking::TxEqConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A set of switch uplinks.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SwitchPorts {
    pub uplinks: Vec<HostPortConfig>,
}

impl From<v1::uplink::SwitchPorts> for SwitchPorts {
    fn from(value: v1::uplink::SwitchPorts) -> Self {
        Self { uplinks: value.uplinks.into_iter().map(From::from).collect() }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct HostPortConfig {
    /// Switchport to use for external connectivity
    pub port: String,

    /// IP Address and prefix (e.g., `192.168.0.1/16`) to apply to switchport
    /// (must be in infra_ip pool).  May also include an optional VLAN ID.
    pub addrs: Vec<UplinkAddressConfig>,

    pub lldp: Option<LldpPortConfig>,

    pub tx_eq: Option<TxEqConfig>,
}

impl From<v1::uplink::HostPortConfig> for HostPortConfig {
    fn from(value: v1::uplink::HostPortConfig) -> Self {
        Self {
            port: value.port,
            addrs: value.addrs.into_iter().map(From::from).collect(),
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        }
    }
}
