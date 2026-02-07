// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version 2 of rack init types.

// These types are used in the rack initialization lockstep API, which allows
// changes freely, and also kept serialized in the bootstore, which requires
// changes to be made very carefully to maintain backwards compatibility. We
// need a better setup for managing bootstore changes; this is tracked by
// <https://github.com/oxidecomputer/omicron/issues/9801>.

use super::v1::BfdPeerConfig;
use super::v1::LldpPortConfig;
use super::v1::PortFec;
use super::v1::PortSpeed;
use super::v1::RouteConfig;
use super::v1::SwitchLocation;
use super::v1::TxEqConfig;
use crate::api::external::ImportExportPolicy;
use oxnet::{IpNet, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
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
    pub bgp: Vec<BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<BfdPeerConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct BgpPeerConfig {
    /// The autonomous system number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer. Use `UNSPECIFIED` to indicate an unnumbered BGP
    /// session established over the interface specified by `port`.
    pub addr: IpAddr,
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
    pub allowed_import: ImportExportPolicy,
    /// Define export policy for a peer.
    #[serde(default)]
    pub allowed_export: ImportExportPolicy,
    /// Associate a VLAN ID with a BGP peer session.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct PortConfig {
    /// The set of routes associated with this port.
    pub routes: Vec<RouteConfig>,
    /// This port's addresses and optional vlan IDs
    pub addresses: Vec<UplinkAddressConfig>,
    /// Switch the port belongs to.
    pub switch: SwitchLocation,
    /// Nmae of the port this config applies to.
    pub port: String,
    /// Port speed.
    pub uplink_port_speed: PortSpeed,
    /// Port forward error correction type.
    pub uplink_port_fec: Option<PortFec>,
    /// BGP peers on this port
    pub bgp_peers: Vec<BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
    /// LLDP configuration for this port
    pub lldp: Option<LldpPortConfig>,
    /// TX-EQ configuration for this port
    pub tx_eq: Option<TxEqConfig>,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
pub struct UplinkAddressConfig {
    /// The address to be used on the uplink.
    /// Set to `None` for an Ipv6 Link Local address.
    pub address: Option<IpNet>,

    /// The VLAN id (if any) associated with this address.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

impl From<super::v1::UplinkAddressConfig> for UplinkAddressConfig {
    fn from(value: super::v1::UplinkAddressConfig) -> Self {
        Self { address: Some(value.address), vlan_id: value.vlan_id }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct UplinkAddressConfigError(pub(super) String);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct BgpConfig {
    /// The autonomous system number for the BGP configuration.
    pub asn: u32,

    /// The set of prefixes for the BGP router to originate.
    pub originate: Vec<IpNet>,

    /// Shaper to apply to outgoing messages.
    #[serde(default)]
    pub shaper: Option<String>,

    /// Checker to apply to incoming messages.
    #[serde(default)]
    pub checker: Option<String>,

    /// Maximum number of paths to use when multiple "best paths" exist
    #[serde(default)]
    pub max_paths: MaxPathConfig,
}

#[derive(
    Debug, Copy, Clone, Deserialize, Serialize, PartialEq, Eq, JsonSchema,
)]
pub struct MaxPathConfig(u8);

impl MaxPathConfig {
    pub fn new(v: u8) -> Result<Self, MaxPathConfigError> {
        if v == 0 {
            let msg = "Max path value cannot be zero".into();
            return Err(MaxPathConfigError(msg));
        }

        if v > 32 {
            let msg =
                "System does not support more than 32 paths for ECMP".into();
            return Err(MaxPathConfigError(msg));
        }

        Ok(Self(v))
    }

    pub fn new_unchecked(v: u8) -> Self {
        Self(v)
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

impl Default for MaxPathConfig {
    fn default() -> Self {
        Self(1)
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct MaxPathConfigError(pub(super) String);

/// A set of switch uplinks.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SwitchPorts {
    pub uplinks: Vec<HostPortConfig>,
}

impl From<super::v1::SwitchPorts> for SwitchPorts {
    fn from(value: super::v1::SwitchPorts) -> Self {
        Self { uplinks: value.uplinks.into_iter().map(From::from).collect() }
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

impl From<super::v1::HostPortConfig> for HostPortConfig {
    fn from(value: super::v1::HostPortConfig) -> Self {
        Self {
            port: value.port,
            addrs: value.addrs.into_iter().map(From::from).collect(),
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        }
    }
}
