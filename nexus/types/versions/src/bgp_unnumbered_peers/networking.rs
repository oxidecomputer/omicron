// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `BGP_UNNUMBERED_PEERS` version.
//!
//! This version (2026_02_13_01) adds support for BGP unnumbered peers:
//! - `BgpPeer.addr` becomes optional (unnumbered sessions).
//! - `BgpPeer.router_lifetime` is added for IPv6 router advertisement
//!   lifetime.
//! - `BgpConfigCreate` gains a `max_paths` field for BGP multipath.
//! - `BgpPeerStatus` gains a `peer_id` field.
//! - `BgpImported` replaces the IPv4-only `BgpImportedRouteIpv4`.
//! - `BgpExported` becomes per-route instead of a HashMap.
//! - `SwitchPortSettings` updated to use the new `BgpPeer`
//! - `SwitchPortSettingsCreate` updated to use the new `BgpPeerConfig`.

use crate::v2025_12_12_00::networking::BgpPeerState;
use omicron_common::api::external::{
    self, IdentityMetadata, IdentityMetadataCreateParams, MaxPathConfig, Name,
    NameOrId, SwitchLocation,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

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

    /// The address of the host to peer with. If not provided, this is an
    /// unnumbered BGP session that will be established over the interface
    /// specified by `interface_name`.
    pub addr: Option<IpAddr>,

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
    pub allowed_import: external::ImportExportPolicy,

    /// Define export policy for a peer.
    pub allowed_export: external::ImportExportPolicy,

    /// Associate a VLAN ID with a peer.
    pub vlan_id: Option<u16>,

    /// Router lifetime in seconds for unnumbered BGP peers.
    pub router_lifetime: u16,
}

impl From<crate::v2025_11_20_00::networking::BgpPeer> for BgpPeer {
    fn from(old: crate::v2025_11_20_00::networking::BgpPeer) -> Self {
        BgpPeer {
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
            router_lifetime: 0,
        }
    }
}

impl TryFrom<BgpPeer> for crate::v2025_11_20_00::networking::BgpPeer {
    type Error = external::Error;

    fn try_from(new: BgpPeer) -> Result<Self, Self::Error> {
        let addr = new.addr.ok_or_else(|| {
            external::Error::invalid_request(
                "BGP peer has no address configured, but the API version \
                 in use requires an address. Update your client to use \
                 BGP unnumbered peers.",
            )
        })?;
        Ok(Self {
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

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpPeerConfig {
    /// Link that the peer is reachable on.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    pub peers: Vec<BgpPeer>,
}

impl From<crate::v2025_11_20_00::networking::BgpPeerConfig> for BgpPeerConfig {
    fn from(old: crate::v2025_11_20_00::networking::BgpPeerConfig) -> Self {
        BgpPeerConfig {
            link_name: old.link_name,
            peers: old.peers.into_iter().map(Into::into).collect(),
        }
    }
}

/// The current status of a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpPeerStatus {
    /// IP address of the peer.
    pub addr: IpAddr,

    /// Interface name
    pub peer_id: String,

    /// Local autonomous system number.
    pub local_asn: u32,

    /// Remote autonomous system number.
    pub remote_asn: u32,

    /// State of the peer.
    pub state: BgpPeerState,

    /// Time of last state change.
    pub state_duration_millis: u64,

    /// Switch with the peer session.
    pub switch: SwitchLocation,
}

impl From<BgpPeerStatus> for crate::v2025_12_12_00::networking::BgpPeerStatus {
    fn from(new: BgpPeerStatus) -> Self {
        Self {
            addr: new.addr,
            local_asn: new.local_asn,
            remote_asn: new.remote_asn,
            state: new.state,
            state_duration_millis: new.state_duration_millis,
            switch: new.switch,
        }
    }
}

/// This structure contains all port settings information in one place. It's a
/// convenience data structure for getting a complete view of a particular
/// port's settings.
// TODO: several fields below embed `external::*` types directly from
// `omicron-common`, which means their serialized shape is not truly frozen.
// Once `omicron-common-versions` exists, replace these with version-local
// copies of the types to ensure the initial version's wire format is
// immutable.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettings {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

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

impl TryFrom<SwitchPortSettings>
    for crate::v2025_11_20_00::networking::SwitchPortSettings
{
    type Error = external::Error;

    fn try_from(new: SwitchPortSettings) -> Result<Self, Self::Error> {
        Ok(Self {
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
                .map(crate::v2025_11_20_00::networking::BgpPeer::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            addresses: new.addresses,
        })
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

/// Route exported to a peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpExported {
    /// Identifier for the BGP peer.
    pub peer_id: String,

    /// Switch the route is exported from.
    pub switch: SwitchLocation,

    /// The destination network prefix.
    pub prefix: oxnet::IpNet,
}

impl From<Vec<BgpExported>> for crate::v2025_11_20_00::networking::BgpExported {
    fn from(values: Vec<BgpExported>) -> Self {
        use std::collections::hash_map::Entry;

        let mut out = Self::default();

        for export in values {
            let oxnet::IpNet::V4(net) = export.prefix else {
                continue;
            };
            match out.exports.entry(export.peer_id) {
                Entry::Occupied(mut occupied_entry) => {
                    occupied_entry.get_mut().push(net);
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(vec![net]);
                }
            }
        }

        out
    }
}

/// A route imported from a BGP peer.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct BgpImported {
    /// The destination network prefix.
    pub prefix: oxnet::IpNet,

    /// The nexthop the prefix is reachable through.
    pub nexthop: IpAddr,

    /// BGP identifier of the originating router.
    pub id: u32,

    /// Switch the route is imported into.
    pub switch: SwitchLocation,
}

impl TryFrom<BgpImported>
    for crate::v2025_11_20_00::networking::BgpImportedRouteIpv4
{
    type Error = String;

    fn try_from(value: BgpImported) -> Result<Self, Self::Error> {
        let BgpImported { prefix, nexthop, id, switch } = value;

        let prefix = match prefix {
            oxnet::IpNet::V4(ipv4_net) => Ok(ipv4_net),
            oxnet::IpNet::V6(ipv6_net) => {
                Err(format!("prefix must be Ipv4Net but it is {ipv6_net}"))
            }
        }?;

        let nexthop = match nexthop {
            IpAddr::V4(ipv4_addr) => Ok(ipv4_addr),
            IpAddr::V6(ipv6_addr) => {
                Err(format!("nexthop must be Ipv4Addr but it is {ipv6_addr}"))
            }
        }?;

        Ok(Self { prefix, nexthop, id, switch })
    }
}
