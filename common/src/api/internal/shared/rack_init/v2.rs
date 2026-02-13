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
use std::num::NonZeroU8;

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
    /// Router lifetime in seconds for unnumbered BGP peers.
    #[serde(default)]
    pub router_lifetime: RouterLifetimeConfig,
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
        let address = if value.address.addr().is_unspecified() {
            None
        } else {
            Some(value.address)
        };

        Self { address, vlan_id: value.vlan_id }
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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub struct MaxPathConfig(u8);

impl MaxPathConfig {
    const MIN: u8 = 1;
    const MAX: u8 = 32;

    pub fn new(v: u8) -> Result<Self, MaxPathConfigError> {
        if v < Self::MIN {
            return Err(MaxPathConfigError::ValueTooSmall);
        }

        if v > Self::MAX {
            return Err(MaxPathConfigError::ValueTooLarge);
        }

        Ok(Self(v))
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }

    pub fn as_nonzero_u8(&self) -> NonZeroU8 {
        // By construction, we guarantee self.0 is at least Self::MIN (1), so we
        // can unwrap this conversion.
        NonZeroU8::new(self.0).unwrap()
    }
}

impl Default for MaxPathConfig {
    fn default() -> Self {
        Self(Self::MIN)
    }
}

impl<'de> Deserialize<'de> for MaxPathConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u8::deserialize(deserializer)?;
        MaxPathConfig::new(value).map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for MaxPathConfig {
    fn schema_name() -> String {
        "MaxPathConfig".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            format: Some("uint8".to_string()),
            number: Some(Box::new(schemars::schema::NumberValidation {
                minimum: Some(f64::from(Self::MIN)),
                maximum: Some(f64::from(Self::MAX)),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MaxPathConfigError {
    #[error("max path value cannot be less than {}", MaxPathConfig::MIN)]
    ValueTooSmall,
    #[error("max path value cannot be greater than {}", MaxPathConfig::MAX)]
    ValueTooLarge,
    #[error(
        "max path value must be an integer between {} and {}",
        MaxPathConfig::MIN,
        MaxPathConfig::MAX
    )]
    ParseIntError(#[from] std::num::ParseIntError),
}

/// Router lifetime in seconds for unnumbered BGP peers.
///
/// This value is used in IPv6 Router Advertisements to indicate how long
/// the router should be considered valid by neighbors.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize)]
pub struct RouterLifetimeConfig(u16);

impl RouterLifetimeConfig {
    /// Default router lifetime: 0 seconds (disabled)
    pub const DEFAULT: u16 = 0;

    // Maximum valid router lifetime is 9000 seconds (2.5 hours) per RFC 4861
    const MAX: u16 = 9000;

    pub fn new(v: u16) -> Result<Self, RouterLifetimeConfigError> {
        if v > Self::MAX {
            return Err(RouterLifetimeConfigError::ValueTooLarge);
        }

        Ok(Self(v))
    }

    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

impl Default for RouterLifetimeConfig {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl<'de> Deserialize<'de> for RouterLifetimeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        RouterLifetimeConfig::new(value).map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for RouterLifetimeConfig {
    fn schema_name() -> String {
        "RouterLifetimeConfig".to_string()
    }

    fn json_schema(
        _: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "Router lifetime in seconds for unnumbered BGP peers"
                        .to_string(),
                ),
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::InstanceType::Integer.into()),
            format: Some("uint16".to_string()),
            number: Some(Box::new(schemars::schema::NumberValidation {
                minimum: Some(f64::from(Self::DEFAULT)),
                maximum: Some(f64::from(Self::MAX)),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RouterLifetimeConfigError {
    #[error(
        "router lifetime config cannot be greater than {}",
        RouterLifetimeConfig::MAX
    )]
    ValueTooLarge,
    #[error(
        "max path value must be an integer between {} and {}",
        RouterLifetimeConfig::DEFAULT,
        RouterLifetimeConfig::MAX
    )]
    ParseIntError(#[from] std::num::ParseIntError),
}

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
