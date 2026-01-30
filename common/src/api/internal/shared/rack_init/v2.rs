// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version 2 of rack init types. The rack initialization API is now
//! lockstep, so this should be the last version of these types and
//! they now may be modified freely.

use crate::api::external::{BfdMode, ImportExportPolicy, Name};
use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackNetworkConfig {
    pub rack_subnet: Ipv6Net,
    // TODO: #3591 Consider making infra-ip ranges implicit for uplinks
    /// First ip address to be used for configuring network infrastructure
    pub infra_ip_first: Ipv4Addr,
    /// Last ip address to be used for configuring network infrastructure
    pub infra_ip_last: Ipv4Addr,
    /// Uplinks for connecting the rack to external networks
    pub ports: Vec<PortConfig>,
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
    pub originate: Vec<Ipv4Net>,

    /// Shaper to apply to outgoing messages.
    #[serde(default)]
    pub shaper: Option<String>,

    /// Checker to apply to incoming messages.
    #[serde(default)]
    pub checker: Option<String>,
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

impl BgpPeerConfig {
    /// The default hold time for a BGP peer in seconds.
    pub const DEFAULT_HOLD_TIME: u64 = 6;

    /// The default idle hold time for a BGP peer in seconds.
    pub const DEFAULT_IDLE_HOLD_TIME: u64 = 3;

    /// The default delay open time for a BGP peer in seconds.
    pub const DEFAULT_DELAY_OPEN: u64 = 0;

    /// The default connect retry time for a BGP peer in seconds.
    pub const DEFAULT_CONNECT_RETRY: u64 = 3;

    /// The default keepalive time for a BGP peer in seconds.
    pub const DEFAULT_KEEPALIVE: u64 = 2;

    pub fn hold_time(&self) -> u64 {
        self.hold_time.unwrap_or(Self::DEFAULT_HOLD_TIME)
    }

    pub fn idle_hold_time(&self) -> u64 {
        self.idle_hold_time.unwrap_or(Self::DEFAULT_IDLE_HOLD_TIME)
    }

    pub fn delay_open(&self) -> u64 {
        self.delay_open.unwrap_or(Self::DEFAULT_DELAY_OPEN)
    }

    pub fn connect_retry(&self) -> u64 {
        self.connect_retry.unwrap_or(Self::DEFAULT_CONNECT_RETRY)
    }

    pub fn keepalive(&self) -> u64 {
        self.keepalive.unwrap_or(Self::DEFAULT_KEEPALIVE)
    }
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
    pub destination: IpNet,
    /// The nexthop/gateway address.
    pub nexthop: IpAddr,
    /// The VLAN id associated with this route.
    #[serde(default)]
    pub vlan_id: Option<u16>,
    /// The RIB priority (i.e. Admin Distance) associated with this route.
    #[serde(default)]
    pub rib_priority: Option<u8>,
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
pub struct UplinkAddressConfig {
    pub address: IpNet,
    /// The VLAN id (if any) associated with this address.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

impl UplinkAddressConfig {
    pub fn addr(&self) -> IpAddr {
        self.address.addr()
    }
}

impl std::fmt::Display for UplinkAddressConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.vlan_id {
            None => write!(f, "{}", self.address),
            Some(v) => write!(f, "{};{}", self.address, v),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct UplinkAddressConfigError(String);

impl std::fmt::Display for UplinkAddressConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse switch location error: {}", self.0)
    }
}

/// Convert a string into an UplinkAddressConfig.
/// 192.168.1.1/24 => UplinkAddressConfig { 192.168.1.1/24, None }
/// 192.168.1.1/24;200 => UplinkAddressConfig { 192.168.1.1/24, Some(200) }
impl FromStr for UplinkAddressConfig {
    type Err = UplinkAddressConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fields: Vec<&str> = s.split(';').collect();
        let (address, vlan_id) = match fields.len() {
            1 => Ok((fields[0], None)),
            2 => Ok((fields[0], Some(fields[1]))),
            _ => Err(UplinkAddressConfigError(format!(
                "not a valid uplink address: {s}"
            ))),
        }?;
        let address = address.parse().map_err(|_| {
            UplinkAddressConfigError(format!(
                "not a valid ip address: {address}"
            ))
        })?;
        let vlan_id = match vlan_id {
            None => Ok(None),
            Some(v) => match v.parse() {
                Err(_) => Err(format!("invalid vlan id: {v}")),
                Ok(vlan_id) if vlan_id > 1 && vlan_id < 4096 => {
                    Ok(Some(vlan_id))
                }
                Ok(vlan_id) => Err(format!("vlan id out of range: {vlan_id}")),
            },
        }
        .map_err(|e| UplinkAddressConfigError(e))?;
        Ok(UplinkAddressConfig { address, vlan_id })
    }
}

#[derive(
    Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
/// To what extent should this port participate in LLDP
pub enum LldpAdminStatus {
    #[default]
    Enabled,
    Disabled,
    RxOnly,
    TxOnly,
}

impl fmt::Display for LldpAdminStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LldpAdminStatus::Enabled => write!(f, "enabled"),
            LldpAdminStatus::Disabled => write!(f, "disabled"),
            LldpAdminStatus::RxOnly => write!(f, "rx_only"),
            LldpAdminStatus::TxOnly => write!(f, "tx_only"),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ParseLldpAdminStatusError(String);

impl std::fmt::Display for ParseLldpAdminStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LLDP admin status error: {}", self.0)
    }
}

impl FromStr for LldpAdminStatus {
    type Err = ParseLldpAdminStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            "rxonly" | "rx_only" => Ok(Self::RxOnly),
            "txonly" | "tx_only" => Ok(Self::TxOnly),
            _ => Err(ParseLldpAdminStatusError(format!(
                "not a valid admin status: {s}"
            ))),
        }
    }
}

/// Per-port LLDP configuration settings.  Only the "status" setting is
/// mandatory.  All other fields have natural defaults or may be inherited from
/// the switch.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct LldpPortConfig {
    /// To what extent should this port participate in LLDP
    pub status: LldpAdminStatus,
    /// Chassis ID to advertise.  If this is set, it will be advertised as a
    /// LocallyAssigned ID type.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub chassis_id: Option<String>,
    /// Port ID to advertise.  If this is set, it will be advertised as a
    /// LocallyAssigned ID type.  If this is not set, it will be set to
    /// the port name. e.g., qsfp0/0.
    pub port_id: Option<String>,
    /// Port description to advertise.  If this is not set, no
    /// description will be advertised.
    pub port_description: Option<String>,
    /// System name to advertise.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub system_name: Option<String>,
    /// System description to advertise.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub system_description: Option<String>,
    /// Management IP addresses to advertise.  If this is not set, it will be
    /// inherited from the switch-level settings.
    pub management_addrs: Option<Vec<IpAddr>>,
}

/// Per-port tx-eq overrides.  This can be used to fine-tune the transceiver
/// equalization settings to improve signal integrity.
#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema,
)]
pub struct TxEqConfig {
    /// Pre-cursor tap1
    pub pre1: Option<i32>,
    /// Pre-cursor tap2
    pub pre2: Option<i32>,
    /// Main tap
    pub main: Option<i32>,
    /// Post-cursor tap2
    pub post2: Option<i32>,
    /// Post-cursor tap1
    pub post1: Option<i32>,
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
    /// (must be in infra_ip pool).  May also include an optional VLAN ID.
    pub addrs: Vec<UplinkAddressConfig>,

    pub lldp: Option<LldpPortConfig>,
    pub tx_eq: Option<TxEqConfig>,
}

impl From<PortConfig> for HostPortConfig {
    fn from(x: PortConfig) -> Self {
        Self {
            port: x.port,
            addrs: x.addresses,
            lldp: x.lldp.clone(),
            tx_eq: x.tx_eq,
        }
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

impl SwitchLocation {
    /// Return the location of the other switch, not ourself.
    pub const fn other(&self) -> Self {
        match self {
            SwitchLocation::Switch0 => SwitchLocation::Switch1,
            SwitchLocation::Switch1 => SwitchLocation::Switch0,
        }
    }
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

impl std::fmt::Display for ParseSwitchLocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse switch location error: {}", self.0)
    }
}

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
