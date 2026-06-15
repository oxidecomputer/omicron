// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * `RouterPeerType::Numbered` gains a new optional field `src_addr:
//!   Option<RouterPeerIpAddr>`, allowing the BGP session source address to
//!   be stored in the bootstore.
//! * A new [`LoopbackAddress`] struct and a matching `loopback_addresses`
//!   field on [`RackNetworkConfig`] allow switch loopback interfaces to be
//!   restored from the bootstore on reboot.
//! * Updated types that transitively contain the newly-updated
//!   `RouterPeerType`:
//!     * [`BgpPeerConfig`]
//!     * [`PortConfig`]
//!     * [`RackNetworkConfig`]
//!     * `EarlyNetworkConfigBody`

use crate::v1::early_networking as v1;
use crate::v20::early_networking as v20;
use crate::v30::early_networking as v30;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

pub use v30::InvalidIpAddrError;
pub use v30::RouterPeerIpAddr;
pub use v30::RouterPeerIpAddrError;
pub use v30::UplinkAddress;
pub use v30::UplinkAddressConfig;
pub use v30::UplinkIpNet;
pub use v30::UplinkIpNetError;

/// A loopback address to configure on a switch.
///
/// The `anycast` flag present on the database model is intentionally omitted
/// here: the DPD `loopback_ipv4_create`/`loopback_ipv6_create` API does not
/// accept it, so the sync task already discards it.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
pub struct LoopbackAddress {
    /// The switch on which this loopback address should be configured.
    pub switch: v1::SwitchSlot,
    /// The loopback IP address.
    pub address: IpAddr,
}

/// A BGP peer address: either a numbered (IP) peer or an unnumbered (interface)
/// peer.
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    JsonSchema,
    Hash,
    PartialOrd,
    Ord,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RouterPeerType {
    Unnumbered {
        /// Router lifetime in seconds for unnumbered BGP peers.
        router_lifetime: v20::RouterLifetimeConfig,
    },
    Numbered {
        /// IP address for numbered BGP peers.
        ip: RouterPeerIpAddr,
        /// Optional source address to use for the BGP session.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        src_addr: Option<RouterPeerIpAddr>,
    },
}

impl From<v30::RouterPeerType> for RouterPeerType {
    fn from(value: v30::RouterPeerType) -> Self {
        match value {
            v30::RouterPeerType::Unnumbered { router_lifetime } => {
                Self::Unnumbered { router_lifetime }
            }
            v30::RouterPeerType::Numbered { ip } => {
                // src_addr is new; default to None when upgrading from v30.
                Self::Numbered { ip, src_addr: None }
            }
        }
    }
}

impl From<RouterPeerType> for v30::RouterPeerType {
    fn from(value: RouterPeerType) -> Self {
        match value {
            RouterPeerType::Unnumbered { router_lifetime } => {
                Self::Unnumbered { router_lifetime }
            }
            RouterPeerType::Numbered { ip, src_addr: _ } => {
                // src_addr is not present in v30; drop it.
                Self::Numbered { ip }
            }
        }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct BgpPeerConfig {
    /// The autonomous system number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer.
    pub addr: RouterPeerType,
    /// How long to keep a session alive without a keepalive in seconds.
    /// Defaults to 6.
    pub hold_time: Option<u64>,
    /// How long to keep a peer in idle after a state machine reset in seconds.
    pub idle_hold_time: Option<u64>,
    /// How long to delay sending open messages to a peer. In seconds.
    pub delay_open: Option<u64>,
    /// The interval in seconds between peer connection retry attempts.
    pub connect_retry: Option<u64>,
    /// The interval to send keepalive messages at.
    pub keepalive: Option<u64>,
    /// Require that a peer has a specified ASN.
    #[serde(default)]
    pub remote_asn: Option<u32>,
    /// Require messages from a peer have a minimum IP time to live field.
    #[serde(default)]
    pub min_ttl: Option<u8>,
    /// Use the given key for TCP-MD5 authentication with the peer.
    #[serde(default)]
    pub md5_auth_key: Option<String>,
    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    #[serde(default)]
    pub multi_exit_discriminator: Option<u32>,
    /// Include the provided communities in updates sent to the peer.
    #[serde(default)]
    pub communities: Vec<u32>,
    /// Apply a local preference to routes received from this peer.
    #[serde(default)]
    pub local_pref: Option<u32>,
    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    #[serde(default)]
    pub enforce_first_as: bool,
    /// Define import policy for a peer.
    #[serde(default)]
    pub allowed_import: v1::ImportExportPolicy,
    /// Define export policy for a peer.
    #[serde(default)]
    pub allowed_export: v1::ImportExportPolicy,
    /// Associate a VLAN ID with a BGP peer session.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

impl From<v30::BgpPeerConfig> for BgpPeerConfig {
    fn from(value: v30::BgpPeerConfig) -> Self {
        Self {
            asn: value.asn,
            port: value.port,
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

impl From<BgpPeerConfig> for v30::BgpPeerConfig {
    fn from(value: BgpPeerConfig) -> Self {
        Self {
            asn: value.asn,
            port: value.port,
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

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct PortConfig {
    /// The set of routes associated with this port.
    pub routes: Vec<v1::RouteConfig>,
    /// This port's addresses and optional vlan IDs
    pub addresses: Vec<v30::UplinkAddressConfig>,
    /// Switch the port belongs to.
    pub switch: v1::SwitchSlot,
    /// Name of the port this config applies to.
    pub port: String,
    /// Port speed.
    pub uplink_port_speed: v1::LinkSpeed,
    /// Port forward error correction type.
    pub uplink_port_fec: Option<v1::LinkFec>,
    /// BGP peers on this port
    pub bgp_peers: Vec<BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
    /// LLDP configuration for this port
    pub lldp: Option<v1::LldpPortConfig>,
    /// TX-EQ configuration for this port
    pub tx_eq: Option<v1::TxEqConfig>,
}

impl From<v30::PortConfig> for PortConfig {
    fn from(value: v30::PortConfig) -> Self {
        Self {
            routes: value.routes,
            addresses: value.addresses,
            switch: value.switch,
            port: value.port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            autoneg: value.autoneg,
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        }
    }
}

impl From<PortConfig> for v30::PortConfig {
    fn from(value: PortConfig) -> Self {
        Self {
            routes: value.routes,
            addresses: value.addresses,
            switch: value.switch,
            port: value.port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            autoneg: value.autoneg,
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        }
    }
}

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct RackNetworkConfig {
    pub rack_subnet: Ipv6Net,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: IpAddr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: IpAddr,
    /// Uplinks for connecting the rack to external networks
    pub ports: Vec<PortConfig>,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<v20::BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<v1::BfdPeerConfig>,
    /// Loopback addresses to configure on each switch
    #[serde(default)]
    pub loopback_addresses: Vec<LoopbackAddress>,
}

impl From<v30::RackNetworkConfig> for RackNetworkConfig {
    fn from(value: v30::RackNetworkConfig) -> Self {
        Self {
            rack_subnet: value.rack_subnet,
            infra_ip_first: value.infra_ip_first,
            infra_ip_last: value.infra_ip_last,
            ports: value.ports.into_iter().map(From::from).collect(),
            bgp: value.bgp,
            bfd: value.bfd,
            // loopback_addresses is new; default to empty when upgrading from v30.
            loopback_addresses: vec![],
        }
    }
}

impl From<RackNetworkConfig> for v30::RackNetworkConfig {
    fn from(value: RackNetworkConfig) -> Self {
        Self {
            rack_subnet: value.rack_subnet,
            infra_ip_first: value.infra_ip_first,
            infra_ip_last: value.infra_ip_last,
            ports: value.ports.into_iter().map(From::from).collect(),
            bgp: value.bgp,
            bfd: value.bfd,
            // loopback_addresses is not present in v30; drop it.
        }
    }
}
