// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * Add an optional `src_addr` field to [`BgpPeerConfig`], specifying the
//!   local IP address used when initiating BGP sessions with a peer.
//! * Update types that transitively contain [`BgpPeerConfig`]:
//!     * [`PortConfig`]
//!     * [`RackNetworkConfig`]
//!     * [`EarlyNetworkConfigBody`]
//!     * [`WriteNetworkConfigRequest`]

use crate::v1::early_networking as v1;
use crate::v20::early_networking as v20;
use crate::v30::early_networking as v30;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// BGP peer configuration with an optional source address.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct BgpPeerConfig {
    /// The autonomous system number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer.
    pub addr: v30::RouterPeerType,
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
    /// The local IP address to use when initiating BGP sessions with this peer.
    #[serde(default)]
    pub src_addr: Option<IpAddr>,
}

impl From<v30::BgpPeerConfig> for BgpPeerConfig {
    fn from(value: v30::BgpPeerConfig) -> Self {
        Self {
            asn: value.asn,
            port: value.port,
            addr: value.addr,
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
            src_addr: None,
        }
    }
}

impl From<BgpPeerConfig> for v30::BgpPeerConfig {
    fn from(value: BgpPeerConfig) -> Self {
        Self {
            asn: value.asn,
            port: value.port,
            addr: value.addr,
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
        }
    }
}

/// This is the actual configuration of EarlyNetworking.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct EarlyNetworkConfigBody {
    // Rack network configuration as delivered from RSS or Nexus
    pub rack_network_config: RackNetworkConfig,
}

impl EarlyNetworkConfigBody {
    pub const SCHEMA_VERSION: u32 = 4;
}

impl From<v30::EarlyNetworkConfigBody> for EarlyNetworkConfigBody {
    fn from(old: v30::EarlyNetworkConfigBody) -> Self {
        Self { rack_network_config: old.rack_network_config.into() }
    }
}

impl From<EarlyNetworkConfigBody> for v30::EarlyNetworkConfigBody {
    fn from(new: EarlyNetworkConfigBody) -> Self {
        Self { rack_network_config: new.rack_network_config.into() }
    }
}

/// Structure for requests from Nexus to sled-agent to write a new
/// `EarlyNetworkConfigBody` into the replicated bootstore.
///
/// [`WriteNetworkConfigRequest`] INTENTIONALLY does not have a `From`
/// implementation from prior API versions. It is critically important that
/// sled-agent not attempt to rewrite old `EarlyNetworkConfigBody` types to the
/// latest version. For more about this, see the comments on the relevant
/// endpoint in `sled-agent-api`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct WriteNetworkConfigRequest {
    pub generation: u64,
    pub body: EarlyNetworkConfigBody,
}
