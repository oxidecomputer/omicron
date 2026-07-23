// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `BGP_PEER_SRC_ADDR` version.
//!
//! Changes in this version:
//!
//! * Add `src_addr` to [`RouterPeerType::Numbered`]
//! * [`BgpPeer::addr`] now uses the v43 [`RouterPeerType`] (with `src_addr`
//!   inside `Numbered`).
//! * Define new versions of types that transitively include [`BgpPeer`]:
//!   * [`BgpPeerConfig`]
//!   * [`SwitchPortSettings`]
//!   * [`SwitchPortSettingsCreate`]

use crate::v2025_11_20_00::networking::{
    AddressConfig, LinkConfigCreate, RouteConfig, SwitchInterfaceConfigCreate,
    SwitchPortAddressView, SwitchPortConfig, SwitchPortConfigCreate,
    SwitchPortLinkConfig, SwitchPortRouteConfig, SwitchPortSettingsGroups,
};
use crate::v2026_05_07_00::networking::SwitchInterfaceConfig;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types_versions::v1::early_networking::ImportExportPolicy;
use sled_agent_types_versions::v43::early_networking::RouterPeerType;

// Re-export the error type unchanged from the previous version.
pub use crate::v2026_04_16_00::networking::BgpPeerConversionError;

/// A BGP peer configuration for an interface. Includes the set of announcements
/// that will be advertised to the peer. The `bgp_config` parameter is a
/// reference to global BGP parameters.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpPeer {
    /// The global BGP configuration used for establishing a session with this
    /// peer.
    pub bgp_config: NameOrId,

    /// The address of the host to peer with, or specifying the configuration of
    /// an unnumbered BGP session.
    pub addr: RouterPeerType,

    /// How long to hold peer connections between keepalives (seconds).
    pub hold_time: u32,

    /// How long to hold a peer in idle before attempting a new session
    /// (seconds).
    pub idle_hold_time: u32,

    /// How long to delay sending an open request after establishing a TCP
    /// session (seconds).
    pub delay_open: u32,

    /// How long to wait between TCP connection retries (seconds).
    pub connect_retry: u32,

    /// How often to send keepalive requests (seconds).
    pub keepalive: u32,

    /// Require that a peer has a specified ASN.
    pub remote_asn: Option<u32>,

    /// Require messages from a peer have a minimum IP time to live field.
    pub min_ttl: Option<u8>,

    /// Use the given key for TCP-MD5 authentication with the peer.
    pub md5_auth_key: Option<String>,

    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    pub multi_exit_discriminator: Option<u32>,

    /// Include the provided communities in updates sent to the peer.
    pub communities: Vec<u32>,

    /// Apply a local preference to routes received from this peer.
    pub local_pref: Option<u32>,

    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    pub enforce_first_as: bool,

    /// Define import policy for a peer.
    pub allowed_import: ImportExportPolicy,

    /// Define export policy for a peer.
    pub allowed_export: ImportExportPolicy,

    /// Associate a VLAN ID with a peer.
    pub vlan_id: Option<u16>,
}

impl From<crate::v2026_04_16_00::networking::BgpPeer> for BgpPeer {
    fn from(value: crate::v2026_04_16_00::networking::BgpPeer) -> Self {
        Self {
            bgp_config: value.bgp_config,
            addr: value.addr.into(),
            hold_time: value.hold_time,
            idle_hold_time: value.idle_hold_time,
            delay_open: value.delay_open,
            connect_retry: value.connect_retry,
            keepalive: value.keepalive,
            remote_asn: value.remote_asn,
            min_ttl: value.min_ttl,
            md5_auth_key: value.md5_auth_key,
            multi_exit_discriminator: value.multi_exit_discriminator,
            communities: value.communities,
            local_pref: value.local_pref,
            enforce_first_as: value.enforce_first_as,
            allowed_import: value.allowed_import,
            allowed_export: value.allowed_export,
            vlan_id: value.vlan_id,
        }
    }
}

impl From<BgpPeer> for crate::v2026_04_16_00::networking::BgpPeer {
    fn from(value: BgpPeer) -> Self {
        Self {
            bgp_config: value.bgp_config,
            addr: value.addr.into(),
            hold_time: value.hold_time,
            idle_hold_time: value.idle_hold_time,
            delay_open: value.delay_open,
            connect_retry: value.connect_retry,
            keepalive: value.keepalive,
            remote_asn: value.remote_asn,
            min_ttl: value.min_ttl,
            md5_auth_key: value.md5_auth_key,
            multi_exit_discriminator: value.multi_exit_discriminator,
            communities: value.communities,
            local_pref: value.local_pref,
            enforce_first_as: value.enforce_first_as,
            allowed_import: value.allowed_import,
            allowed_export: value.allowed_export,
            vlan_id: value.vlan_id,
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

    pub peers: Vec<BgpPeer>,
}

impl From<crate::v2026_04_16_00::networking::BgpPeerConfig> for BgpPeerConfig {
    fn from(value: crate::v2026_04_16_00::networking::BgpPeerConfig) -> Self {
        Self {
            link_name: value.link_name,
            peers: value.peers.into_iter().map(From::from).collect(),
        }
    }
}

impl From<BgpPeerConfig> for crate::v2026_04_16_00::networking::BgpPeerConfig {
    fn from(value: BgpPeerConfig) -> Self {
        Self {
            link_name: value.link_name,
            peers: value.peers.into_iter().map(From::from).collect(),
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

    pub port_config: SwitchPortConfigCreate,

    #[serde(default)]
    pub groups: Vec<NameOrId>,

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

impl From<crate::v2026_04_16_00::networking::SwitchPortSettingsCreate>
    for SwitchPortSettingsCreate
{
    fn from(
        value: crate::v2026_04_16_00::networking::SwitchPortSettingsCreate,
    ) -> Self {
        Self {
            identity: value.identity,
            port_config: value.port_config,
            groups: value.groups,
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            addresses: value.addresses,
        }
    }
}

impl From<SwitchPortSettingsCreate>
    for crate::v2026_04_16_00::networking::SwitchPortSettingsCreate
{
    fn from(value: SwitchPortSettingsCreate) -> Self {
        Self {
            identity: value.identity,
            port_config: value.port_config,
            groups: value.groups,
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
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

    /// Switch port settings included from other switch port settings groups.
    pub groups: Vec<SwitchPortSettingsGroups>,

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

/// Downgrade to v2026_05_07_00 (REMOVE_DUPLICATED_NETWORKING_TYPES) —
/// drop `src_addr` from each BGP peer.
impl From<SwitchPortSettings>
    for crate::v2026_05_07_00::networking::SwitchPortSettings
{
    fn from(value: SwitchPortSettings) -> Self {
        Self {
            identity: value.identity,
            groups: value.groups,
            port: value.port,
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            addresses: value.addresses,
        }
    }
}

/// Downgrade to v2026_04_16_00 (STRONGER_BGP_UNNUMBERED_TYPES) —
/// drop `src_addr` and re-add the `vlan_interfaces` field.
impl From<SwitchPortSettings>
    for crate::v2026_04_16_00::networking::SwitchPortSettings
{
    fn from(value: SwitchPortSettings) -> Self {
        let v2026_05_07_00: crate::v2026_05_07_00::networking::SwitchPortSettings =
            value.into();
        v2026_05_07_00.into()
    }
}
