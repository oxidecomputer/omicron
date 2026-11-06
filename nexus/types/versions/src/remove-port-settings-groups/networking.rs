// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `REMOVE_PORT_SETTINGS_GROUPS` version.
//!
//! Changes in this version:
//!
//! * Remove `groups` from `SwitchPortSettingsCreate` and `SwitchPortSettings`
//! * Define new versions of types and their required `From` impls:
//!   * [`SwitchPortSettings`]
//!   * [`SwitchPortSettingsCreate`]

use crate::v2025_11_20_00::networking::{
    AddressConfig, LinkConfigCreate, RouteConfig, SwitchInterfaceConfigCreate,
    SwitchPortAddressView, SwitchPortConfig, SwitchPortConfigCreate,
    SwitchPortLinkConfig, SwitchPortRouteConfig,
};
use crate::v2026_05_07_00::networking::SwitchInterfaceConfig;
use crate::v2026_08_14_01::networking::{BgpPeer, BgpPeerConfig};
use omicron_common::api::external::{
    IdentityMetadata, IdentityMetadataCreateParams,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parameters for creating switch port settings. Switch port settings are the
/// central data structure for setting up external networking. Switch port
/// settings include link, interface, route, address and dynamic network
/// protocol configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettingsCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub port_config: SwitchPortConfigCreate,

    /// Link configurations.
    pub links: Vec<LinkConfigCreate>,

    /// Interface configurations.
    #[serde(default)]
    pub interfaces: Vec<SwitchInterfaceConfigCreate>,

    /// Route configurations.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,

    /// BGP peer configurations.
    #[serde(default)]
    pub bgp_peers: Vec<BgpPeerConfig>,

    /// Address configurations.
    pub addresses: Vec<AddressConfig>,
}

impl From<crate::v2026_08_14_01::networking::SwitchPortSettingsCreate>
    for SwitchPortSettingsCreate
{
    fn from(
        value: crate::v2026_08_14_01::networking::SwitchPortSettingsCreate,
    ) -> Self {
        Self {
            identity: value.identity,
            port_config: value.port_config,
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers,
            addresses: value.addresses,
        }
    }
}

/// This structure contains all port settings information in one place. It's a
/// convenience data structure for getting a complete view of a particular
/// port's settings.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettings {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Layer 1 physical port settings.
    pub port: SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<SwitchPortLinkConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<SwitchInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<BgpPeer>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<SwitchPortAddressView>,
}

impl From<SwitchPortSettings>
    for crate::v2026_08_14_01::networking::SwitchPortSettings
{
    fn from(value: SwitchPortSettings) -> Self {
        Self {
            identity: value.identity,
            port: value.port,
            groups: vec![],
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers,
            addresses: value.addresses,
        }
    }
}
