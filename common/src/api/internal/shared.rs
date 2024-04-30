// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types shared between Nexus and Sled Agent.

use crate::{
    address::NUM_SOURCE_NAT_PORTS,
    api::external::{self, BfdMode, Name},
};
use ipnetwork::{IpNetwork, Ipv4Network, Ipv6Network};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};
use uuid::Uuid;

/// The type of network interface
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
    Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NetworkInterfaceKind {
    /// A vNIC attached to a guest instance
    Instance { id: Uuid },
    /// A vNIC associated with an internal service
    Service { id: Uuid },
    /// A vNIC associated with a probe
    Probe { id: Uuid },
}

/// Information required to construct a virtual network interface
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct NetworkInterface {
    pub id: Uuid,
    pub kind: NetworkInterfaceKind,
    pub name: external::Name,
    pub ip: IpAddr,
    pub mac: external::MacAddr,
    pub subnet: external::IpNet,
    pub vni: external::Vni,
    pub primary: bool,
    pub slot: u8,
}

/// An IP address and port range used for source NAT, i.e., making
/// outbound network connections from guests or services.
// Note that `Deserialize` is manually implemented; if you make any changes to
// the fields of this structure, you must make them to that implementation too.
#[derive(Debug, Clone, Copy, Serialize, JsonSchema, PartialEq, Eq, Hash)]
pub struct SourceNatConfig {
    /// The external address provided to the instance or service.
    pub ip: IpAddr,
    /// The first port used for source NAT, inclusive.
    first_port: u16,
    /// The last port used for source NAT, also inclusive.
    last_port: u16,
}

// We implement `Deserialize` manually to add validity checking on the port
// range.
impl<'de> Deserialize<'de> for SourceNatConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // The fields of `SourceNatConfigShadow` should exactly match the fields
        // of `SourceNatConfig`. We're not really using serde's remote derive,
        // but by adding the attribute we get compile-time checking that all the
        // field names and types match. (It doesn't check the _order_, but that
        // should be fine as long as we're using JSON or similar formats.)
        #[derive(Deserialize)]
        #[serde(remote = "SourceNatConfig")]
        struct SourceNatConfigShadow {
            ip: IpAddr,
            first_port: u16,
            last_port: u16,
        }

        let shadow = SourceNatConfigShadow::deserialize(deserializer)?;
        SourceNatConfig::new(shadow.ip, shadow.first_port, shadow.last_port)
            .map_err(D::Error::custom)
    }
}

impl SourceNatConfig {
    /// Construct a `SourceNatConfig` with the given port range, both inclusive.
    ///
    /// # Errors
    ///
    /// Fails if `(first_port, last_port)` is not aligned to
    /// [`NUM_SOURCE_NAT_PORTS`].
    pub fn new(
        ip: IpAddr,
        first_port: u16,
        last_port: u16,
    ) -> Result<Self, SourceNatConfigError> {
        if first_port % NUM_SOURCE_NAT_PORTS == 0
            && last_port
                .checked_sub(first_port)
                .and_then(|diff| diff.checked_add(1))
                == Some(NUM_SOURCE_NAT_PORTS)
        {
            Ok(Self { ip, first_port, last_port })
        } else {
            Err(SourceNatConfigError::UnalignedPortPair {
                first_port,
                last_port,
            })
        }
    }

    /// Get the port range.
    ///
    /// Guaranteed to be aligned to [`NUM_SOURCE_NAT_PORTS`].
    pub fn port_range(&self) -> std::ops::RangeInclusive<u16> {
        self.first_port..=self.last_port
    }

    /// Get the port range as a raw tuple; both values are inclusive.
    ///
    /// Guaranteed to be aligned to [`NUM_SOURCE_NAT_PORTS`].
    pub fn port_range_raw(&self) -> (u16, u16) {
        self.port_range().into_inner()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SourceNatConfigError {
    #[error(
        "snat port range is not aligned to {NUM_SOURCE_NAT_PORTS}: \
         ({first_port}, {last_port})"
    )]
    UnalignedPortPair { first_port: u16, last_port: u16 },
}

// We alias [`RackNetworkConfig`] to the current version of the protocol, so
// that we can convert between versions as necessary.
pub type RackNetworkConfig = RackNetworkConfigV1;

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackNetworkConfigV1 {
    pub rack_subnet: Ipv6Network,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: Ipv4Addr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: Ipv4Addr,
    /// Uplinks for connecting the rack to external networks
    pub ports: Vec<PortConfigV1>,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<BfdPeerConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct BgpConfig {
    /// The autonomous system number for the BGP configuration.
    pub asn: u32,
    /// The set of prefixes for the BGP router to originate.
    pub originate: Vec<Ipv4Network>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct BgpPeerConfig {
    /// The autonomous sysetm number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer.
    pub addr: Ipv4Addr,
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
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct BfdPeerConfig {
    pub local: Option<IpAddr>,
    pub remote: IpAddr,
    pub detection_threshold: u8,
    pub required_rx: u64,
    pub mode: BfdMode,
    pub switch: SwitchLocation,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct RouteConfig {
    /// The destination of the route.
    pub destination: IpNetwork,
    /// The nexthop/gateway address.
    pub nexthop: IpAddr,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct PortConfigV1 {
    /// The set of routes associated with this port.
    pub routes: Vec<RouteConfig>,
    /// This port's addresses.
    pub addresses: Vec<IpNetwork>,
    /// Switch the port belongs to.
    pub switch: SwitchLocation,
    /// Nmae of the port this config applies to.
    pub port: String,
    /// Port speed.
    pub uplink_port_speed: PortSpeed,
    /// Port forward error correction type.
    pub uplink_port_fec: PortFec,
    /// BGP peers on this port
    pub bgp_peers: Vec<BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
}

impl From<UplinkConfig> for PortConfigV1 {
    fn from(value: UplinkConfig) -> Self {
        PortConfigV1 {
            routes: vec![RouteConfig {
                destination: "0.0.0.0/0".parse().unwrap(),
                nexthop: value.gateway_ip.into(),
            }],
            addresses: vec![value.uplink_cidr.into()],
            switch: value.switch,
            port: value.uplink_port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: vec![],
            autoneg: false,
        }
    }
}

/// Deprecated, use PortConfigV1 instead. Cannot actually deprecate due to
/// <https://github.com/serde-rs/serde/issues/2195>
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct UplinkConfig {
    /// Gateway address
    pub gateway_ip: Ipv4Addr,
    /// Switch to use for uplink
    pub switch: SwitchLocation,
    /// Switchport to use for external connectivity
    pub uplink_port: String,
    /// Speed for the Switchport
    pub uplink_port_speed: PortSpeed,
    /// Forward Error Correction setting for the uplink port
    pub uplink_port_fec: PortFec,
    /// IP Address and prefix (e.g., `192.168.0.1/16`) to apply to switchport
    /// (must be in infra_ip pool)
    pub uplink_cidr: Ipv4Network,
    /// VLAN id to use for uplink
    pub uplink_vid: Option<u16>,
}

/// A set of switch uplinks.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct SwitchPorts {
    pub uplinks: Vec<HostPortConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct HostPortConfig {
    /// Switchport to use for external connectivity
    pub port: String,

    /// IP Address and prefix (e.g., `192.168.0.1/16`) to apply to switchport
    /// (must be in infra_ip pool)
    pub addrs: Vec<IpNetwork>,
}

impl From<PortConfigV1> for HostPortConfig {
    fn from(x: PortConfigV1) -> Self {
        Self { port: x.port, addrs: x.addresses }
    }
}

/// Identifies switch physical location
#[derive(
    Clone,
    Copy,
    Debug,
    Deserialize,
    Serialize,
    PartialEq,
    JsonSchema,
    Hash,
    Eq,
    PartialOrd,
    Ord,
)]
#[serde(rename_all = "snake_case")]
pub enum SwitchLocation {
    /// Switch in upper slot
    Switch0,
    /// Switch in lower slot
    Switch1,
}

impl fmt::Display for SwitchLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SwitchLocation::Switch0 => write!(f, "switch0"),
            SwitchLocation::Switch1 => write!(f, "switch1"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ParseSwitchLocationError(String);

impl FromStr for SwitchLocation {
    type Err = ParseSwitchLocationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "switch0" => Ok(Self::Switch0),
            "switch1" => Ok(Self::Switch1),
            _ => Err(ParseSwitchLocationError(format!(
                "not a valid location: {s}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExternalPortDiscovery {
    // Automatically discover ports via Dendrite
    Auto(HashMap<SwitchLocation, Ipv6Addr>),
    // Static configuration pairing switches with a collection of ports
    Static(HashMap<SwitchLocation, Vec<Name>>),
}

/// Switchport Speed options
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum PortSpeed {
    #[serde(alias = "0G")]
    Speed0G,
    #[serde(alias = "1G")]
    Speed1G,
    #[serde(alias = "10G")]
    Speed10G,
    #[serde(alias = "25G")]
    Speed25G,
    #[serde(alias = "40G")]
    Speed40G,
    #[serde(alias = "50G")]
    Speed50G,
    #[serde(alias = "100G")]
    Speed100G,
    #[serde(alias = "200G")]
    Speed200G,
    #[serde(alias = "400G")]
    Speed400G,
}

impl fmt::Display for PortSpeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortSpeed::Speed0G => write!(f, "0G"),
            PortSpeed::Speed1G => write!(f, "1G"),
            PortSpeed::Speed10G => write!(f, "10G"),
            PortSpeed::Speed25G => write!(f, "25G"),
            PortSpeed::Speed40G => write!(f, "40G"),
            PortSpeed::Speed50G => write!(f, "50G"),
            PortSpeed::Speed100G => write!(f, "100G"),
            PortSpeed::Speed200G => write!(f, "200G"),
            PortSpeed::Speed400G => write!(f, "400G"),
        }
    }
}

/// Switchport FEC options
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum PortFec {
    Firecode,
    None,
    Rs,
}

impl fmt::Display for PortFec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortFec::Firecode => write!(f, "Firecode R-FEC"),
            PortFec::None => write!(f, "None"),
            PortFec::Rs => write!(f, "RS-FEC"),
        }
    }
}
