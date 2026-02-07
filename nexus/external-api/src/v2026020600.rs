// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Nexus external types that changed from 2026020600 to 2026020700.
//!
//! Version 2026020600 types (before BGP unnumbered peers support was added).
//!
//! Key differences from newer API versions:
//! - [`BgpPeer`] has a required `addr` field. Newer versions make `addr`
//!   optional to support BGP unnumbered sessions where the peer address
//!   is discovered dynamically via the interface rather than being specified.
//! - [`BgpPeerConfig`] contains the old [`BgpPeer`] type with required `addr`.
//! - [`SwitchPortSettingsCreate`] uses the old [`BgpPeerConfig`] type.
//! - [`SwitchPortSettings`] contains the old [`BgpPeer`] type with required `addr`.
//! - [`BgpConfigCreate`] lacks the `max_paths` field. Newer versions include
//!   `max_paths` to configure BGP multipath support.
//! - [`BgpConfig`] lacks the `max_paths` field. Newer versions include
//!   `max_paths` to configure BGP multipath support.
//!
//! [`BgpPeer`]: self::BgpPeer
//! [`BgpPeerConfig`]: self::BgpPeerConfig
//! [`SwitchPortSettingsCreate`]: self::SwitchPortSettingsCreate
//! [`SwitchPortSettings`]: self::SwitchPortSettings
//! [`BgpConfigCreate`]: self::BgpConfigCreate
//! [`BgpConfig`]: self::BgpConfig

use api_identity::ObjectIdentity;
use omicron_common::api::external::ObjectIdentity;
use omicron_common::api::external::{
    self, IdentityMetadata, IdentityMetadataCreateParams, ImportExportPolicy,
    Name, NameOrId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// A BGP peer configuration for an interface. Includes the set of announcements
/// that will be advertised to the peer identified by `addr`. The `bgp_config`
/// parameter is a reference to global BGP parameters. The `interface_name`
/// indicates what interface the peer should be contacted on.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpPeer {
    /// The global BGP configuration used for establishing a session with this
    /// peer.
    pub bgp_config: NameOrId,

    /// The name of interface to peer on. This is relative to the port
    /// configuration this BGP peer configuration is a part of. For example this
    /// value could be phy0 to refer to a primary physical interface. Or it
    /// could be vlan47 to refer to a VLAN interface.
    pub interface_name: Name,

    /// The address of the host to peer with.
    pub addr: IpAddr,

    /// How long to hold peer connections between keepalives (seconds).
    pub hold_time: u32,

    /// How long to hold a peer in idle before attempting a new session
    /// (seconds).
    pub idle_hold_time: u32,

    /// How long to delay sending an open request after establishing a TCP
    /// session (seconds).
    pub delay_open: u32,

    /// How long to to wait between TCP connection retries (seconds).
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

impl From<BgpPeer> for external::BgpPeer {
    fn from(old: BgpPeer) -> external::BgpPeer {
        external::BgpPeer {
            bgp_config: old.bgp_config,
            interface_name: old.interface_name,
            addr: Some(old.addr),
            hold_time: old.hold_time,
            idle_hold_time: old.idle_hold_time,
            delay_open: old.delay_open,
            connect_retry: old.connect_retry,
            keepalive: old.keepalive,
            remote_asn: old.remote_asn,
            min_ttl: old.min_ttl,
            md5_auth_key: old.md5_auth_key,
            multi_exit_discriminator: old.multi_exit_discriminator,
            communities: old.communities,
            local_pref: old.local_pref,
            enforce_first_as: old.enforce_first_as,
            allowed_import: old.allowed_import,
            allowed_export: old.allowed_export,
            vlan_id: old.vlan_id,
        }
    }
}

impl TryFrom<external::BgpPeer> for BgpPeer {
    type Error = external::Error;

    fn try_from(new: external::BgpPeer) -> Result<Self, Self::Error> {
        let addr = new.addr.ok_or_else(|| {
            external::Error::invalid_request(
                "BGP peer has no address configured, but the API version \
                 in use requires an address. Update your client to use \
                 BGP unnumbered peers.",
            )
        })?;
        Ok(BgpPeer {
            bgp_config: new.bgp_config,
            interface_name: new.interface_name,
            addr,
            hold_time: new.hold_time,
            idle_hold_time: new.idle_hold_time,
            delay_open: new.delay_open,
            connect_retry: new.connect_retry,
            keepalive: new.keepalive,
            remote_asn: new.remote_asn,
            min_ttl: new.min_ttl,
            md5_auth_key: new.md5_auth_key,
            multi_exit_discriminator: new.multi_exit_discriminator,
            communities: new.communities,
            local_pref: new.local_pref,
            enforce_first_as: new.enforce_first_as,
            allowed_import: new.allowed_import,
            allowed_export: new.allowed_export,
            vlan_id: new.vlan_id,
        })
    }
}

// --- Params types that contain BgpPeer ---

use nexus_types::external_api::params;

/// BGP peer configuration for a link (old version with required addr).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpPeerConfig {
    /// Link that the peer is reachable on.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    pub peers: Vec<BgpPeer>,
}

impl From<BgpPeerConfig> for params::BgpPeerConfig {
    fn from(old: BgpPeerConfig) -> params::BgpPeerConfig {
        params::BgpPeerConfig {
            link_name: old.link_name,
            peers: old.peers.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<params::BgpPeerConfig> for BgpPeerConfig {
    type Error = external::Error;

    fn try_from(new: params::BgpPeerConfig) -> Result<Self, Self::Error> {
        Ok(BgpPeerConfig {
            link_name: new.link_name,
            peers: new
                .peers
                .into_iter()
                .map(BgpPeer::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

/// Parameters for creating switch port settings (old version with required BgpPeer.addr).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettingsCreate {
    #[serde(flatten)]
    pub identity: external::IdentityMetadataCreateParams,

    pub port_config: params::SwitchPortConfigCreate,

    #[serde(default)]
    pub groups: Vec<NameOrId>,

    /// Link configurations.
    pub links: Vec<params::LinkConfigCreate>,

    /// Interface configurations.
    #[serde(default)]
    pub interfaces: Vec<params::SwitchInterfaceConfigCreate>,

    /// Route configurations.
    #[serde(default)]
    pub routes: Vec<params::RouteConfig>,

    /// BGP peer configurations.
    #[serde(default)]
    pub bgp_peers: Vec<BgpPeerConfig>,

    /// Address configurations.
    pub addresses: Vec<params::AddressConfig>,
}

impl From<SwitchPortSettingsCreate> for params::SwitchPortSettingsCreate {
    fn from(old: SwitchPortSettingsCreate) -> params::SwitchPortSettingsCreate {
        params::SwitchPortSettingsCreate {
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

// --- Response types that contain BgpPeer ---

/// Switch port settings (old version with required BgpPeer.addr).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettings {
    #[serde(flatten)]
    pub identity: external::IdentityMetadata,

    /// Switch port settings included from other switch port settings groups.
    pub groups: Vec<external::SwitchPortSettingsGroups>,

    /// Layer 1 physical port settings.
    pub port: external::SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<external::SwitchPortLinkConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<external::SwitchInterfaceConfig>,

    /// Vlan interface settings.
    pub vlan_interfaces: Vec<external::SwitchVlanInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<external::SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<BgpPeer>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<external::SwitchPortAddressView>,
}

impl TryFrom<external::SwitchPortSettings> for SwitchPortSettings {
    type Error = external::Error;

    fn try_from(
        new: external::SwitchPortSettings,
    ) -> Result<Self, Self::Error> {
        Ok(SwitchPortSettings {
            identity: new.identity,
            groups: new.groups,
            port: new.port,
            links: new.links,
            interfaces: new.interfaces,
            vlan_interfaces: new.vlan_interfaces,
            routes: new.routes,
            bgp_peers: new
                .bgp_peers
                .into_iter()
                .map(BgpPeer::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            addresses: new.addresses,
        })
    }
}

/// Parameters for creating a BGP configuration. This includes and autonomous
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

    // Dynamic BGP policy is not yet available so we skip adding it to the API
    /// A shaper program to apply to outgoing open and update messages.
    #[serde(skip)]
    pub shaper: Option<String>,
    /// A checker program to apply to incoming open and update messages.
    #[serde(skip)]
    pub checker: Option<String>,
}

impl From<BgpConfigCreate> for params::BgpConfigCreate {
    fn from(old: BgpConfigCreate) -> params::BgpConfigCreate {
        params::BgpConfigCreate {
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

/// A base BGP configuration.
#[derive(
    ObjectIdentity, Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq,
)]
pub struct BgpConfig {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// The autonomous system number of this BGP configuration.
    pub asn: u32,

    /// Optional virtual routing and forwarding identifier for this BGP
    /// configuration.
    pub vrf: Option<String>,
}

impl From<BgpConfig> for external::BgpConfig {
    fn from(old: BgpConfig) -> external::BgpConfig {
        external::BgpConfig {
            identity: old.identity,
            asn: old.asn,
            vrf: old.vrf,
            max_paths: Default::default(),
        }
    }
}
