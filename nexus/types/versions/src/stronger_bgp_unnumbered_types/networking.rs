// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `STRONGER_BGP_UNNUMBERED_TYPES` version.
//!
//! * Change fields of [`BgpPeer`] to give stronger guarantees:
//!   * [`BgpPeer::addr`] is now [`RouterPeerType`] instead of
//!     `Option<IpAddr>` (which permitted three distinct representations of
//!     "unnumbered": `None`, `Some(0.0.0.0)`, and `Some(::)`).
//!   * [`BgpPeer::router_lifetime`] is now [`RouterLifetimeConfig`] instead of
//!     `u16`, adding enforcement of bounds.
//! * Remove `BgpPeer::interface_name` (omicron#10104).
//! * Define new versions of types that transitively include [`BgpPeer`]:
//!   * [`BgpPeerConfig`]
//!   * [`SwitchPortSettings`]
//!   * [`SwitchPortSettingsCreate`]

use crate::v2025_11_20_00::networking::{
    AddressConfig, LinkConfigCreate, RouteConfig, SwitchInterfaceConfigCreate,
    SwitchPortConfigCreate,
};
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::InvalidIpAddrError;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterLifetimeConfigError;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::RouterPeerIpAddrError;
use sled_agent_types::early_networking::RouterPeerType;

/// A BGP peer configuration for an interface. Includes the set of announcements
/// that will be advertised to the peer identified by `addr`. The `bgp_config`
/// parameter is a reference to global BGP parameters. The `interface_name`
/// indicates what interface the peer should be contacted on.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpPeer {
    /// The global BGP configuration used for establishing a session with this
    /// peer.
    pub bgp_config: NameOrId,

    /// The address of the host to peer with, or specifying that an unnumbered
    /// BGP session that will be established over the interface specified by
    /// `interface_name`.
    pub addr: RouterPeerType,

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

#[derive(Debug, thiserror::Error)]
pub enum BgpPeerConversionError {
    #[error(transparent)]
    RouterPeerIpAddr(#[from] RouterPeerIpAddrError),
    #[error(transparent)]
    RouterLifetimeConfig(#[from] RouterLifetimeConfigError),
}

impl TryFrom<crate::v2026_02_13_01::networking::BgpPeer> for BgpPeer {
    type Error = BgpPeerConversionError;

    fn try_from(
        value: crate::v2026_02_13_01::networking::BgpPeer,
    ) -> Result<Self, Self::Error> {
        let addr = match value.addr.map(RouterPeerIpAddr::try_from) {
            // The expected cases: We either have a valid peer IP or `None` (an
            // unnumbered peer).
            Some(Ok(ip)) => RouterPeerType::Numbered { ip },
            None => {
                let router_lifetime =
                    RouterLifetimeConfig::new(value.router_lifetime)?;
                RouterPeerType::Unnumbered { router_lifetime }
            }

            // Unexpected cases: If `value.peer` is `Some(UNSPECIFIED)`, we'll
            // treat that as `unnumbered`, because `UNSPECIFIED` was previously
            // used as the sentinel value for unnumbered peers. For any other
            // error case, we want to reject this conversion - the peer IP isn't
            // valid.
            Some(Err(err)) => match err.err {
                InvalidIpAddrError::UnspecifiedAddress => {
                    let router_lifetime =
                        RouterLifetimeConfig::new(value.router_lifetime)?;
                    RouterPeerType::Unnumbered { router_lifetime }
                }
                InvalidIpAddrError::LoopbackAddress
                | InvalidIpAddrError::MulticastAddress
                | InvalidIpAddrError::Ipv4Broadcast
                | InvalidIpAddrError::Ipv6UnicastLinkLocal => {
                    return Err(err.into());
                }
            },
        };

        Ok(Self {
            bgp_config: value.bgp_config,
            addr,
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
        })
    }
}

impl From<BgpPeer> for crate::v2026_02_13_01::networking::BgpPeer {
    fn from(value: BgpPeer) -> Self {
        // TODO-correctness What should we backfill here? We've dropped this
        // field because we weren't actually using it. We can't return an empty
        // string because that's not a legal `Name`.
        //
        // The other option is to return an error to old API clients, but that
        // seems worse than having placeholder data in a field that was never
        // used...
        let interface_name = "interface-name-unavailable"
            .parse()
            .expect("constant is a valid Name");

        let (addr, router_lifetime) = match value.addr {
            RouterPeerType::Numbered { ip } => {
                // The previous `BgpPeer` always contained a `router_lifetime`,
                // but only used it if the peer was unnumbered. We can fill in
                // any arbitrary value here - just use the default.
                (Some(ip.into()), RouterLifetimeConfig::default().as_u16())
            }
            RouterPeerType::Unnumbered { router_lifetime } => {
                (None, router_lifetime.as_u16())
            }
        };

        Self {
            bgp_config: value.bgp_config,
            interface_name,
            addr,
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
            router_lifetime,
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

impl TryFrom<crate::v2026_02_13_01::networking::BgpPeerConfig>
    for BgpPeerConfig
{
    type Error = BgpPeerConversionError;

    fn try_from(
        value: crate::v2026_02_13_01::networking::BgpPeerConfig,
    ) -> Result<Self, Self::Error> {
        let peers = value
            .peers
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;

        Ok(Self { link_name: value.link_name, peers })
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

impl TryFrom<crate::v2026_02_13_01::networking::SwitchPortSettingsCreate>
    for SwitchPortSettingsCreate
{
    type Error = BgpPeerConversionError;

    fn try_from(
        value: crate::v2026_02_13_01::networking::SwitchPortSettingsCreate,
    ) -> Result<Self, Self::Error> {
        let bgp_peers = value
            .bgp_peers
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            identity: value.identity,
            port_config: value.port_config,
            groups: value.groups,
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers,
            addresses: value.addresses,
        })
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

impl From<SwitchPortSettings>
    for crate::v2026_02_13_01::networking::SwitchPortSettings
{
    fn from(value: SwitchPortSettings) -> Self {
        Self {
            identity: value.identity,
            groups: value.groups,
            port: value.port,
            links: value.links,
            interfaces: value.interfaces,
            vlan_interfaces: value.vlan_interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            addresses: value.addresses,
        }
    }
}
