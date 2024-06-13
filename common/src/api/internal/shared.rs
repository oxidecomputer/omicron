// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types shared between Nexus and Sled Agent.

use crate::{
    address::NUM_SOURCE_NAT_PORTS,
    api::external::{self, BfdMode, ImportExportPolicy, Name, Vni},
};
use oxnet::{IpNet, Ipv4Net, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
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
    pub name: Name,
    pub ip: IpAddr,
    pub mac: external::MacAddr,
    pub subnet: IpNet,
    pub vni: Vni,
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

// We alias [`PortConfig`] to the current version of the protocol, so
// that we can convert between versions as necessary.
pub type PortConfig = PortConfigV2;

// We alias [`RackNetworkConfig`] to the current version of the protocol, so
// that we can convert between versions as necessary.
pub type RackNetworkConfig = RackNetworkConfigV2;

/// Initial network configuration
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
pub struct RackNetworkConfigV2 {
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct PortConfigV2 {
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
    pub uplink_port_fec: PortFec,
    /// BGP peers on this port
    pub bgp_peers: Vec<BgpPeerConfig>,
    /// Whether or not to set autonegotiation
    #[serde(default)]
    pub autoneg: bool,
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
}

impl From<PortConfigV2> for HostPortConfig {
    fn from(x: PortConfigV2) -> Self {
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

/// Description of source IPs allowed to reach rack services.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "allow", content = "ips")]
pub enum AllowedSourceIps {
    /// Allow traffic from any external IP address.
    Any,
    /// Restrict access to a specific set of source IP addresses or subnets.
    ///
    /// All others are prevented from reaching rack services.
    List(IpAllowList),
}

impl TryFrom<Vec<IpNet>> for AllowedSourceIps {
    type Error = &'static str;
    fn try_from(list: Vec<IpNet>) -> Result<Self, Self::Error> {
        IpAllowList::try_from(list).map(Self::List)
    }
}

impl TryFrom<&[ipnetwork::IpNetwork]> for AllowedSourceIps {
    type Error = &'static str;
    fn try_from(list: &[ipnetwork::IpNetwork]) -> Result<Self, Self::Error> {
        IpAllowList::try_from(list).map(Self::List)
    }
}

/// A non-empty allowlist of IP subnets.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(try_from = "Vec<IpNet>", into = "Vec<IpNet>")]
#[schemars(transparent)]
pub struct IpAllowList(Vec<IpNet>);

impl IpAllowList {
    /// Return the entries of the list as a slice.
    pub fn as_slice(&self) -> &[IpNet] {
        &self.0
    }

    /// Return an iterator over the entries of the list.
    pub fn iter(&self) -> impl Iterator<Item = &IpNet> {
        self.0.iter()
    }

    /// Consume the list into an iterator.
    pub fn into_iter(self) -> impl Iterator<Item = IpNet> {
        self.0.into_iter()
    }

    /// Return the number of entries in the allowlist.
    ///
    /// Note that this is always >= 1, though we return a usize for simplicity.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<IpAllowList> for Vec<IpNet> {
    fn from(list: IpAllowList) -> Self {
        list.0
    }
}

impl TryFrom<Vec<IpNet>> for IpAllowList {
    type Error = &'static str;
    fn try_from(list: Vec<IpNet>) -> Result<Self, Self::Error> {
        if list.is_empty() {
            return Err("IP allowlist must not be empty");
        }
        Ok(Self(list))
    }
}

impl TryFrom<&[ipnetwork::IpNetwork]> for IpAllowList {
    type Error = &'static str;

    fn try_from(list: &[ipnetwork::IpNetwork]) -> Result<Self, Self::Error> {
        if list.is_empty() {
            return Err("IP allowlist must not be empty");
        }
        Ok(Self(list.into_iter().map(|net| (*net).into()).collect()))
    }
}

/// A VPC route resolved into a concrete target.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ResolvedVpcRoute {
    pub dest: IpNet,
    pub target: RouterTarget,
}

/// The target for a given router entry.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case", content = "value")]
pub enum RouterTarget {
    Drop,
    InternetGateway,
    Ip(IpAddr),
    VpcSubnet(IpNet),
}

/// Information on the current parent router (and version) of a route set
/// according to the control plane.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct RouterVersion {
    pub router_id: Uuid,
    pub version: u64,
}

impl RouterVersion {
    /// Return whether a new route set should be applied over the current
    /// values.
    ///
    /// This will occur when seeing a new version and a matching parent,
    /// or a new parent router on the control plane.
    pub fn is_replaced_by(&self, other: &Self) -> bool {
        (self.router_id != other.router_id) || self.version < other.version
    }
}

/// Identifier for a VPC and/or subnet.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct RouterId {
    pub vni: Vni,
    pub subnet: Option<IpNet>,
}

/// Version information for routes on a given VPC subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ResolvedVpcRouteState {
    pub id: RouterId,
    pub version: Option<RouterVersion>,
}

/// An updated set of routes for a given VPC and/or subnet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ResolvedVpcRouteSet {
    pub id: RouterId,
    pub version: Option<RouterVersion>,
    pub routes: HashSet<ResolvedVpcRoute>,
}

#[cfg(test)]
mod tests {
    use crate::api::internal::shared::AllowedSourceIps;
    use oxnet::{IpNet, Ipv4Net, Ipv6Net};
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn test_deserialize_allowed_source_ips() {
        let parsed: AllowedSourceIps = serde_json::from_str(
            r#"{"allow":"list","ips":["127.0.0.1/32","10.0.0.0/24","fd00::1/64"]}"#,
        )
        .unwrap();
        assert_eq!(
            parsed,
            AllowedSourceIps::try_from(vec![
                Ipv4Net::host_net(Ipv4Addr::LOCALHOST).into(),
                IpNet::V4(
                    Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap()
                ),
                IpNet::V6(
                    Ipv6Net::new(
                        Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1),
                        64
                    )
                    .unwrap()
                ),
            ])
            .unwrap()
        );
    }

    #[test]
    fn test_deserialize_unknown_string() {
        serde_json::from_str::<AllowedSourceIps>(r#"{"allow":"wat"}"#)
            .expect_err(
                "Should not be able to deserialize from unknown variant name",
            );
    }

    #[test]
    fn test_deserialize_any_into_allowed_external_ips() {
        assert_eq!(
            AllowedSourceIps::Any,
            serde_json::from_str(r#"{"allow":"any"}"#).unwrap(),
        );
    }
}
