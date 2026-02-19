// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `BGP_UNNUMBERED_PEERS` version.
//!
//! This version:
//! - Adds `max_paths` to `BgpConfigCreate`.
//! - Uses `external::BgpPeer` (optional `addr`, `router_lifetime`) in
//!   `BgpPeerConfig`.
//! - Updates `SwitchPortSettingsCreate` to use the new `BgpPeerConfig`.

use omicron_common::api::external::{
    self, IdentityMetadataCreateParams, MaxPathConfig, Name, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Parameters for creating a BGP configuration. This includes an autonomous
/// system number (ASN) and a virtual routing and forwarding (VRF) identifier.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpConfigCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    /// The autonomous system number of this BGP configuration.
    pub asn: u32,

    pub bgp_announce_set_id: NameOrId,

    /// Optional virtual routing and forwarding identifier for this BGP
    /// configuration.
    pub vrf: Option<Name>,

    // Dynamic BGP policy is not yet available so we skip adding it to the API.
    /// A shaper program to apply to outgoing open and update messages.
    #[serde(skip)]
    pub shaper: Option<String>,
    /// A checker program to apply to incoming open and update messages.
    #[serde(skip)]
    pub checker: Option<String>,

    /// Maximum number of paths to use when multiple "best paths" exist
    #[serde(default)]
    pub max_paths: MaxPathConfig,
}

impl From<crate::v2025_11_20_00::networking::BgpConfigCreate>
    for BgpConfigCreate
{
    fn from(old: crate::v2025_11_20_00::networking::BgpConfigCreate) -> Self {
        BgpConfigCreate {
            identity: old.identity,
            asn: old.asn,
            bgp_announce_set_id: old.bgp_announce_set_id,
            vrf: old.vrf,
            shaper: old.shaper,
            checker: old.checker,
            max_paths: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpPeerConfig {
    /// Link that the peer is reachable on.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    pub peers: Vec<external::BgpPeer>,
}

impl From<crate::v2025_11_20_00::networking::BgpPeerConfig> for BgpPeerConfig {
    fn from(old: crate::v2025_11_20_00::networking::BgpPeerConfig) -> Self {
        BgpPeerConfig {
            link_name: old.link_name,
            peers: old.peers.into_iter().map(Into::into).collect(),
        }
    }
}

/// Parameters for creating switch port settings. Switch port settings are the
/// central data structure for setting up external networking. Switch port
/// settings include link, interface, route, address and dynamic network
/// protocol configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettingsCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub port_config: crate::v2025_11_20_00::networking::SwitchPortConfigCreate,

    #[serde(default)]
    pub groups: Vec<NameOrId>,

    /// Link configurations.
    pub links: Vec<crate::v2025_11_20_00::networking::LinkConfigCreate>,

    /// Interface configurations.
    #[serde(default)]
    pub interfaces:
        Vec<crate::v2025_11_20_00::networking::SwitchInterfaceConfigCreate>,

    /// Route configurations.
    #[serde(default)]
    pub routes: Vec<crate::v2025_11_20_00::networking::RouteConfig>,

    /// BGP peer configurations.
    #[serde(default)]
    pub bgp_peers: Vec<BgpPeerConfig>,

    /// Address configurations.
    pub addresses: Vec<crate::v2025_11_20_00::networking::AddressConfig>,
}

impl SwitchPortSettingsCreate {
    pub fn new(identity: IdentityMetadataCreateParams) -> Self {
        Self {
            identity,
            port_config:
                crate::v2025_11_20_00::networking::SwitchPortConfigCreate {
                    geometry:
                        crate::v2025_11_20_00::networking::SwitchPortGeometry::Qsfp28x1,
                },
            groups: Vec::new(),
            links: Vec::new(),
            interfaces: Vec::new(),
            routes: Vec::new(),
            bgp_peers: Vec::new(),
            addresses: Vec::new(),
        }
    }
}

impl From<crate::v2025_11_20_00::networking::SwitchPortSettingsCreate>
    for SwitchPortSettingsCreate
{
    fn from(
        old: crate::v2025_11_20_00::networking::SwitchPortSettingsCreate,
    ) -> Self {
        SwitchPortSettingsCreate {
            identity: old.identity,
            port_config: old.port_config,
            groups: old.groups,
            links: old.links,
            interfaces: old.interfaces,
            routes: old.routes,
            bgp_peers: old.bgp_peers.into_iter().map(Into::into).collect(),
            addresses: old.addresses,
        }
    }
}
