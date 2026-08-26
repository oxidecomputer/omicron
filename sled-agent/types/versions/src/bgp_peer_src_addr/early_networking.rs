// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * Define a new [`RouterPeerType`] that adds `src_addr` to the
//!   [`RouterPeerType::Numbered`] variant. Since numbered peers can
//!   technically be reachable via multiple interfaces, this gives
//!   the operator the ability to specify a stable source for BGP
//!   sessions.
//! * [`BgpPeerConfig`] uses the new [`RouterPeerType`]

use crate::v1::early_networking as v1;
use crate::v20::early_networking as v20;
use crate::v30::early_networking as v30;
use crate::v42::early_networking as v42;
use oxnet::Ipv6Net;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RouterPeerType {
    Unnumbered(UnnumberedRouter),
    Numbered(NumberedRouter),
}

#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub struct UnnumberedRouter {
    /// Router lifetime in seconds for unnumbered BGP peers.
    pub router_lifetime: v20::RouterLifetimeConfig,
}

impl From<UnnumberedRouter> for RouterPeerType {
    fn from(value: UnnumberedRouter) -> Self {
        Self::Unnumbered(value)
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq, Hash, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct NumberedRouter {
    /// Target IP address for numbered BGP peers.
    pub(crate) target_addr: v30::RouterPeerIpAddr,
    /// Source IP address to use when initiating the BGP session with the
    /// peer. If not provided, an address is selected automatically.
    // We derive default here because this type gets shared with the
    // Nexus external api, and many users will not need to specify this
    // parameter for their configurations
    #[serde(default)]
    pub(crate) src_addr: Option<v30::RouterPeerIpAddr>,
}

impl From<NumberedRouter> for RouterPeerType {
    fn from(value: NumberedRouter) -> Self {
        RouterPeerType::Numbered(value)
    }
}

impl NumberedRouter {
    pub fn new(
        target_addr: v30::RouterPeerIpAddr,
        src_addr: Option<v30::RouterPeerIpAddr>,
    ) -> Result<Self, AddressFamilyMismatchError> {
        match src_addr {
            Some(src) if src.is_ipv4() != target_addr.is_ipv4() => {
                Err(AddressFamilyMismatchError(src.into(), target_addr.into()))
            }
            _ => Ok(Self { target_addr, src_addr }),
        }
    }
}

impl<'de> Deserialize<'de> for NumberedRouter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // The fields of `NumberedRouterShadow` should exactly match the
        // fields of `NumberedRouter`. We're not really using serde's remote
        // derive, but by adding the attribute we get compile-time checking that
        // all the field names and types match. (It doesn't check the _order_,
        // but that should be fine as long as we're using JSON or similar
        // formats.)
        #[derive(Deserialize)]
        #[serde(remote = "NumberedRouter")]
        struct NumberedRouterShadow {
            target_addr: v30::RouterPeerIpAddr,
            #[serde(default)]
            src_addr: Option<v30::RouterPeerIpAddr>,
        }

        // We deserialize, then re-run the input through the constructor
        // to ensure the input is valid
        let to_validate = NumberedRouterShadow::deserialize(deserializer)?;

        NumberedRouter::new(to_validate.target_addr, to_validate.src_addr)
            .map_err(D::Error::custom)
    }
}

/// Upgrade from v30: set `src_addr: None` for numbered peers.
impl From<v30::RouterPeerType> for RouterPeerType {
    fn from(value: v30::RouterPeerType) -> Self {
        match value {
            v30::RouterPeerType::Unnumbered { router_lifetime } => {
                UnnumberedRouter { router_lifetime }.into()
            }
            v30::RouterPeerType::Numbered { ip } => {
                NumberedRouter { target_addr: ip, src_addr: None }.into()
            }
        }
    }
}

/// Error returned when converting a [RouterPeerType] with `Some(src_addr)`
/// into a [`v30::RouterPeerType`].
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "Numbered peer has a src_addr configured, which is \
    unrepresentable in this API version"
)]
pub struct NumberedPeerWithSrcAddrError;

impl TryFrom<RouterPeerType> for v30::RouterPeerType {
    type Error = NumberedPeerWithSrcAddrError;

    fn try_from(value: RouterPeerType) -> Result<Self, Self::Error> {
        match value {
            RouterPeerType::Unnumbered(peer) => {
                Ok(Self::Unnumbered { router_lifetime: peer.router_lifetime })
            }
            RouterPeerType::Numbered(peer) => {
                if peer.src_addr.is_some() {
                    return Err(NumberedPeerWithSrcAddrError);
                };
                Ok(Self::Numbered { ip: peer.target_addr })
            }
        }
    }
}

/// A BGP peer configuration for a port.
#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct BgpPeerConfig {
    /// The autonomous system number of the router the peer belongs to.
    pub asn: u32,
    /// Switch port the peer is reachable on.
    pub port: String,
    /// Address of the peer (numbered or unnumbered).
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

impl TryFrom<BgpPeerConfig> for v30::BgpPeerConfig {
    type Error = NumberedPeerWithSrcAddrError;

    fn try_from(value: BgpPeerConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            asn: value.asn,
            port: value.port,
            addr: value.addr.try_into()?,
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

impl TryFrom<PortConfig> for v30::PortConfig {
    type Error = NumberedPeerWithSrcAddrError;

    fn try_from(value: PortConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            routes: value.routes,
            addresses: value.addresses,
            switch: value.switch,
            port: value.port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers: value
                .bgp_peers
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<_, _>>()?,
            autoneg: value.autoneg,
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        })
    }
}

/// Error returned when constructing a [`UplinkPorts`] from an empty list.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "a rack network config must contain at least one uplink port, \
     but the port list was empty"
)]
pub struct EmptyUplinkPortsError;

/// A non-empty list of uplink [`PortConfig`]s.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct UplinkPorts(pub(crate) Vec<PortConfig>);

impl UplinkPorts {
    /// Constructs an `UplinkPorts` from a list of ports, returning an error if
    /// the list is empty.
    pub fn new(ports: Vec<PortConfig>) -> Result<Self, EmptyUplinkPortsError> {
        if ports.is_empty() {
            return Err(EmptyUplinkPortsError);
        }
        Ok(Self(ports))
    }
}

impl<'de> Deserialize<'de> for UplinkPorts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let ports = Vec::<PortConfig>::deserialize(deserializer)?;
        UplinkPorts::new(ports).map_err(|EmptyUplinkPortsError| {
            serde::de::Error::invalid_length(0, &"at least one uplink port")
        })
    }
}

impl JsonSchema for UplinkPorts {
    fn schema_name() -> String {
        "UplinkPorts".to_string()
    }

    fn json_schema(
        generator: &mut schemars::r#gen::SchemaGenerator,
    ) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            instance_type: Some(schemars::schema::InstanceType::Array.into()),
            array: Some(Box::new(schemars::schema::ArrayValidation {
                items: Some(generator.subschema_for::<PortConfig>().into()),
                min_items: Some(1),
                ..Default::default()
            })),
            ..Default::default()
        })
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
    pub ports: UplinkPorts,
    /// BGP configurations for connecting the rack to external networks
    pub bgp: Vec<v20::BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<v1::BfdPeerConfig>,
}

impl From<v42::RackNetworkConfig> for RackNetworkConfig {
    fn from(old: v42::RackNetworkConfig) -> Self {
        Self {
            rack_subnet: old.rack_subnet,
            infra_ip_first: old.infra_ip_first,
            infra_ip_last: old.infra_ip_last,
            ports: old.ports.into(),
            bgp: old.bgp,
            bfd: old.bfd,
        }
    }
}

impl TryFrom<RackNetworkConfig> for v42::RackNetworkConfig {
    type Error = NumberedPeerWithSrcAddrError;

    fn try_from(new: RackNetworkConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            rack_subnet: new.rack_subnet,
            infra_ip_first: new.infra_ip_first,
            infra_ip_last: new.infra_ip_last,
            ports: v42::UplinkPorts::new(
                new.ports
                    .0
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
            )
            // Safety: we had at least one port coming in, so we have at
            // least one going out.
            .expect("non-empty UplinkPorts downgrade produced empty ports"),
            bgp: new.bgp,
            bfd: new.bfd,
        })
    }
}

impl From<v42::UplinkPorts> for UplinkPorts {
    fn from(old: v42::UplinkPorts) -> Self {
        Self(old.into_vec().into_iter().map(From::from).collect())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0} does not have the same address family as {1}")]
pub struct AddressFamilyMismatchError(IpAddr, IpAddr);
