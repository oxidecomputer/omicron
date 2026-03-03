// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Uplink-related types for the Sled Agent API.
//!
//! Changes in this version:
//! * TODO-john

use crate::v1;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct HostPortConfig {
    /// Switchport to use for external connectivity
    pub port: String,

    /// IP Address and prefix (e.g., `192.168.0.1/16`) to apply to switchport
    /// (must be in infra_ip pool).  May also include an optional VLAN ID.
    pub addrs: Vec<super::early_networking::UplinkAddressConfig>,

    pub lldp: Option<v1::early_networking::LldpPortConfig>,

    pub tx_eq: Option<v1::early_networking::TxEqConfig>,
}
