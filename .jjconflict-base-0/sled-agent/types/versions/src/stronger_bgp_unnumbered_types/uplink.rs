// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Uplink-related types for the Sled Agent API.
//!
//! Changes in this version:
//! * New [`HostPortConfig`] to pick up new [`UplinkAddressConfig`].
//! * New [`SwitchPorts`] to pick up new [`HostPortConfig`].

use super::early_networking::UplinkAddressConfig;
use super::early_networking::UplinkIpNetError;
use crate::v1::early_networking::LldpPortConfig;
use crate::v1::early_networking::TxEqConfig;
use crate::v20;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A set of switch uplinks.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SwitchPorts {
    pub uplinks: Vec<HostPortConfig>,
}

impl TryFrom<v20::uplink::SwitchPorts> for SwitchPorts {
    type Error = UplinkIpNetError;

    fn try_from(value: v20::uplink::SwitchPorts) -> Result<Self, Self::Error> {
        let uplinks = value
            .uplinks
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self { uplinks })
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

impl TryFrom<v20::uplink::HostPortConfig> for HostPortConfig {
    type Error = UplinkIpNetError;

    fn try_from(
        value: v20::uplink::HostPortConfig,
    ) -> Result<Self, Self::Error> {
        let addrs = value
            .addrs
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            port: value.port,
            addrs,
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        })
    }
}
