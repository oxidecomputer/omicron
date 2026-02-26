// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version 1 of rack init types.

use crate::api::external::{BfdMode, ImportExportPolicy};
use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr};

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
    /// Address of the peer. Use `Ipv4Addr::UNSPECIFIED` to indicate an
    /// unnumbered BGP session established over the interface specified by
    /// `port`.
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

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct UplinkAddressConfigError(pub(super) String);

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

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ParseLldpAdminStatusError(pub(super) String);

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

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ParseSwitchLocationError(pub(super) String);

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
