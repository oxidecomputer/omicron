// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * Introduce [`UplinkIpNet`], a newtype wrapper around [`IpNet`] that does
//!   not allow unspecified IP addresses.
//! * Introduce [`RouterPeerIpAddr`], a newtype wrapper around [`IpAddr`] that
//!   enforces several requirements for valid peer addresses.
//! * Introduce [`UplinkAddress`], a stronger type for specifying
//!   possibly-link-local IP nets. This is the new type of
//!   [`UplinkAddressConfig::address`], which was previously an
//!   [`Option<IpNet>`] where both `None` and `Some(UNSPECIFIED)` were treated
//!   as link-local.
//! * Introduce [`RouterPeerType`], a stronger type for specifying
//!   possibly-unnumbered BGP peers. This is the new type of
//!   [`BgpPeerConfig::addr`], which was previously an [`IpAddr`] where an
//!   unspecified address was treated as unnumbered.
//! * Move `router_lifetime` from the top-level of `BgpPeerConfig` to inside the
//!   [`RouterPeerType::Unnumbered`] variant; it only applies to unnumbered
//!   peers.
//! * Update types that transitively contain the newly-updated
//!   [`UplinkAddressConfig`] or [`BgpPeerConfig`]:
//!     * [`EarlyNetworkConfigBody`]
//!     * [`PortConfig`]
//!     * [`RackNetworkConfig`]
//!     * [`WriteNetworkConfigRequest`]

use crate::v1::early_networking as v1;
use crate::v20::early_networking as v20;
use crate::v26::early_networking as v26;
use oxnet::{IpNet, Ipv6Net};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use slog_error_chain::InlineErrorChain;
use std::net::IpAddr;
use std::net::Ipv4Addr;

#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidIpAddrError {
    #[error("unspecified address is not allowed")]
    UnspecifiedAddress,
    #[error("loopback address is not allowed")]
    LoopbackAddress,
    #[error("multicast addresses are not allowed")]
    MulticastAddress,
    #[error("IPv4 broadcast address is not allowed")]
    Ipv4Broadcast,
    #[error("IPv6 unicast link-local addresses are not allowed")]
    Ipv6UnicastLinkLocal,
    #[error("IPv4-mapped IPv6 addresses are not allowed")]
    Ipv4MappedIpv6,
}

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
pub enum UplinkAddress {
    // `#[serde(rename)]` this to `addrconf` so when this shows up in
    // config-rss.toml development files, it's not phrased as `addr_conf`. This
    // also makes it consistent with our custom TOML parsing in customer-facing
    // TOML via wicket.
    #[serde(rename = "addrconf")]
    AddrConf,
    Static {
        ip_net: UplinkIpNet,
    },
}

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
    },
}

// These impls live here instead of in `crate::impls` because they're only used
// for conversions in this particular version. All these are private to ensure
// they don't leak out beyond this module.
impl RouterPeerType {
    /// In contexts where we cannot use this strong type to describe
    /// "unnumbered" addresses, we have two or three possible representations:
    ///
    /// * In a context where we need a non-optional `IpAddr`, we could use
    ///   `Ipv4Addr::UNSPECIFIED` or `Ipv6Addr::UNSPECIFIED`.
    /// * In a context where we need `Option<IpAddr>`, we could use `None`,
    ///   Some(`Ipv4Addr::UNSPECIFIED`), or Some(`Ipv6Addr::UNSPECIFIED`).
    ///
    /// In the optional case, we always prefer `None`. In the non-optional case,
    /// we choose this sentinel value.
    const UNNUMBERED_SENTINEL: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

    /// Squash this address down to an [`IpAddr`] by converting
    /// [`RouterPeerType::Unnumbered`] to
    /// [`RouterPeerType::UNNUMBERED_SENTINEL`].
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    fn ip_squashing_unnumbered_to_sentinel(&self) -> IpAddr {
        match *self {
            Self::Unnumbered { .. } => Self::UNNUMBERED_SENTINEL,
            Self::Numbered { ip } => ip.into(),
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    PartialEq,
    Eq,
    JsonSchema,
    Hash,
    PartialOrd,
    Ord,
)]
// We'd also like to have `#[serde(try_from = "IpNet")]`, but that loses the
// detailed error messages we produce. We manually implement Deserialize per
// https://github.com/serde-rs/serde/issues/2211#issuecomment-1627628399.
#[serde(into = "IpNet")]
#[schemars(with = "IpNet")]
pub struct UplinkIpNet(pub(crate) IpNet);

impl<'de> Deserialize<'de> for UplinkIpNet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        IpNet::deserialize(deserializer).and_then(|ip_net| {
            Self::try_from(ip_net).map_err(|err| {
                serde::de::Error::custom(InlineErrorChain::new(&err))
            })
        })
    }
}

// These conversion implementations are defined here instead of in
// `crate::impls::*` because they're tied to how we derive `Deserialize` and
// `Serialize`.
impl From<UplinkIpNet> for IpNet {
    fn from(value: UplinkIpNet) -> Self {
        value.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid uplink ipnet `{ip_net}`")]
pub struct UplinkIpNetError {
    pub ip_net: IpNet,
    #[source]
    pub err: InvalidIpAddrError,
}

impl TryFrom<IpNet> for UplinkIpNet {
    type Error = UplinkIpNetError;

    fn try_from(value: IpNet) -> Result<Self, Self::Error> {
        // Apply the same validation rules we use for `RouterPeerIpAddr`. If the
        // IP fails, steal the specific error out and wrap it in our error type
        // instead.
        match RouterPeerIpAddr::try_from(value.addr()) {
            Ok(_) => Ok(Self(value)),
            Err(RouterPeerIpAddrError { err, .. }) => {
                Err(UplinkIpNetError { ip_net: value, err })
            }
        }
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Serialize,
    PartialEq,
    Eq,
    JsonSchema,
    Hash,
    PartialOrd,
    Ord,
)]
// We'd also like to have `#[serde(try_from = "IpAddr")]`, but that loses the
// detailed error messages we produce. We manually implement Deserialize per
// https://github.com/serde-rs/serde/issues/2211#issuecomment-1627628399.
#[serde(into = "IpAddr")]
#[schemars(with = "IpAddr")]
pub struct RouterPeerIpAddr(pub(crate) IpAddr);

impl<'de> Deserialize<'de> for RouterPeerIpAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        IpAddr::deserialize(deserializer).and_then(|ip| {
            Self::try_from(ip).map_err(|err| {
                serde::de::Error::custom(InlineErrorChain::new(&err))
            })
        })
    }
}

// As with `UplinkIpNet`, these conversion implementations are defined here
// instead of in `crate::impls::*` because they're tied to how we derive
// `Deserialize` and `Serialize`.
impl From<RouterPeerIpAddr> for IpAddr {
    fn from(value: RouterPeerIpAddr) -> Self {
        value.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid router peer address `{ip}`")]
pub struct RouterPeerIpAddrError {
    pub ip: IpAddr,
    #[source]
    pub err: InvalidIpAddrError,
}

impl TryFrom<IpAddr> for RouterPeerIpAddr {
    type Error = RouterPeerIpAddrError;

    fn try_from(ip: IpAddr) -> Result<Self, Self::Error> {
        let err = match ip {
            IpAddr::V4(ipv4) => {
                // Perform the same validity checks we require in maghemite.
                // We deliberately do not flag Class E (240.0.0.0/4) or
                // Link-Local (169.254.0.0/16) ranges as invalid, as some
                // networks have deployed these as if they were standard
                // routable unicast addresses, which we need to handle.
                if ipv4.is_loopback() {
                    InvalidIpAddrError::LoopbackAddress
                } else if ipv4.is_multicast() {
                    InvalidIpAddrError::MulticastAddress
                } else if ipv4.is_broadcast() {
                    InvalidIpAddrError::Ipv4Broadcast
                } else if ipv4.is_unspecified() {
                    InvalidIpAddrError::UnspecifiedAddress
                } else {
                    return Ok(Self(ip));
                }
            }
            IpAddr::V6(ipv6) => {
                // As above, perform validity checks we require in maghemite.
                if ipv6.is_loopback() {
                    InvalidIpAddrError::LoopbackAddress
                } else if ipv6.is_multicast() {
                    InvalidIpAddrError::MulticastAddress
                } else if ipv6.is_unspecified() {
                    InvalidIpAddrError::UnspecifiedAddress
                } else if ipv6.is_unicast_link_local() {
                    InvalidIpAddrError::Ipv6UnicastLinkLocal
                } else if ipv6.to_ipv4_mapped().is_some() {
                    // switch to ipv6.is_ipv4_mapped() once it's stabilized
                    InvalidIpAddrError::Ipv4MappedIpv6
                } else {
                    return Ok(Self(ip));
                }
            }
        };

        Err(RouterPeerIpAddrError { ip, err })
    }
}

#[derive(
    Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema, Hash,
)]
pub struct UplinkAddressConfig {
    /// The address to be used on the uplink.
    pub address: UplinkAddress,

    /// The VLAN id (if any) associated with this address.
    #[serde(default)]
    pub vlan_id: Option<u16>,
}

impl TryFrom<v20::UplinkAddressConfig> for UplinkAddressConfig {
    type Error = UplinkIpNetError;

    fn try_from(value: v20::UplinkAddressConfig) -> Result<Self, Self::Error> {
        let address = match value.address.map(UplinkIpNet::try_from) {
            Some(Ok(ip_net)) => UplinkAddress::Static { ip_net },
            None => UplinkAddress::AddrConf,
            Some(Err(err)) => match err.err {
                // v20::UplinkAddressConfig should have represented addrconf IPs
                // as `None` (handled above), but it's also possible we could
                // have an unspecified IP as a sentinel value; peel that error
                // out and convert to addrconf. Forward any other kind of
                // invalid address out as a failure - we should not have any of
                // these, and if we do, we're going to reject them somewhere
                // down the line at runtime anyway.
                InvalidIpAddrError::UnspecifiedAddress => {
                    UplinkAddress::AddrConf
                }
                InvalidIpAddrError::LoopbackAddress
                | InvalidIpAddrError::MulticastAddress
                | InvalidIpAddrError::Ipv4Broadcast
                | InvalidIpAddrError::Ipv6UnicastLinkLocal
                | InvalidIpAddrError::Ipv4MappedIpv6 => return Err(err),
            },
        };
        Ok(Self { address, vlan_id: value.vlan_id })
    }
}

impl From<UplinkAddressConfig> for v20::UplinkAddressConfig {
    fn from(value: UplinkAddressConfig) -> Self {
        let address = match value.address {
            UplinkAddress::AddrConf => None,
            UplinkAddress::Static { ip_net } => Some(ip_net.into()),
        };
        Self { address, vlan_id: value.vlan_id }
    }
}

#[derive(
    Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, JsonSchema,
)]
pub struct PortConfig {
    /// The set of routes associated with this port.
    pub routes: Vec<v1::RouteConfig>,
    /// This port's addresses and optional vlan IDs
    pub addresses: Vec<UplinkAddressConfig>,
    /// Switch the port belongs to.
    pub switch: v1::SwitchSlot,
    /// Name of the port this config applies to.
    pub port: String,
    /// Port speed.
    pub uplink_port_speed: v1::PortSpeed,
    /// Port forward error correction type.
    pub uplink_port_fec: Option<v1::PortFec>,
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

#[derive(Debug, thiserror::Error)]
pub enum PortConfigConversionError {
    #[error(transparent)]
    UplinkIpNet(#[from] UplinkIpNetError),
    #[error(transparent)]
    RouterPeerIpAddr(#[from] RouterPeerIpAddrError),
}

impl TryFrom<v20::PortConfig> for PortConfig {
    type Error = PortConfigConversionError;

    fn try_from(value: v20::PortConfig) -> Result<Self, Self::Error> {
        let addresses = value
            .addresses
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;
        let bgp_peers = value
            .bgp_peers
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            routes: value.routes,
            addresses,
            switch: value.switch,
            port: value.port,
            uplink_port_speed: value.uplink_port_speed,
            uplink_port_fec: value.uplink_port_fec,
            bgp_peers,
            autoneg: value.autoneg,
            lldp: value.lldp,
            tx_eq: value.tx_eq,
        })
    }
}

impl From<PortConfig> for v20::PortConfig {
    fn from(value: PortConfig) -> Self {
        Self {
            routes: value.routes,
            addresses: value.addresses.into_iter().map(From::from).collect(),
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

impl TryFrom<v20::BgpPeerConfig> for BgpPeerConfig {
    type Error = RouterPeerIpAddrError;

    fn try_from(value: v20::BgpPeerConfig) -> Result<Self, Self::Error> {
        let addr = match RouterPeerIpAddr::try_from(value.addr) {
            Ok(ip) => RouterPeerType::Numbered { ip },
            Err(err) => match err.err {
                // v20::BgpPeerConfig represented unnumbered peers as
                // unspecified addresses; peel that error out and convert to an
                // unnumbered address. Forward any other kind of invalid address
                // out as a failure - we should not have any of these, and if we
                // do, we're going to reject them somewhere down the line at
                // runtime anyway.
                InvalidIpAddrError::UnspecifiedAddress => {
                    RouterPeerType::Unnumbered {
                        router_lifetime: value.router_lifetime,
                    }
                }
                InvalidIpAddrError::LoopbackAddress
                | InvalidIpAddrError::MulticastAddress
                | InvalidIpAddrError::Ipv4Broadcast
                | InvalidIpAddrError::Ipv6UnicastLinkLocal
                | InvalidIpAddrError::Ipv4MappedIpv6 => return Err(err),
            },
        };
        Ok(Self {
            asn: value.asn,
            port: value.port,
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

impl From<BgpPeerConfig> for v20::BgpPeerConfig {
    fn from(value: BgpPeerConfig) -> Self {
        let router_lifetime = match value.addr {
            RouterPeerType::Unnumbered { router_lifetime } => router_lifetime,
            // v20::BgpPeerConfig always has a `router_lifetime` field, but its
            // value is only used with unnumbered peers. For numbered peers,
            // just fill in a default value.
            RouterPeerType::Numbered { .. } => {
                v20::RouterLifetimeConfig::default()
            }
        };

        Self {
            asn: value.asn,
            port: value.port,
            addr: value.addr.ip_squashing_unnumbered_to_sentinel(),
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
    pub bgp: Vec<v20::BgpConfig>,
    /// BFD configuration for connecting the rack to external networks
    #[serde(default)]
    pub bfd: Vec<v1::BfdPeerConfig>,
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

impl TryFrom<v26::EarlyNetworkConfigBody> for EarlyNetworkConfigBody {
    type Error = anyhow::Error;

    fn try_from(old: v26::EarlyNetworkConfigBody) -> Result<Self, Self::Error> {
        let old = old.rack_network_config;

        let ports = old
            .ports
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            rack_network_config: RackNetworkConfig {
                rack_subnet: old.rack_subnet,
                infra_ip_first: old.infra_ip_first,
                infra_ip_last: old.infra_ip_last,
                ports,
                bgp: old.bgp,
                bfd: old.bfd,
            },
        })
    }
}

impl From<EarlyNetworkConfigBody> for v26::EarlyNetworkConfigBody {
    fn from(new: EarlyNetworkConfigBody) -> Self {
        let new = new.rack_network_config;

        Self {
            rack_network_config: v20::RackNetworkConfig {
                rack_subnet: new.rack_subnet,
                infra_ip_first: new.infra_ip_first,
                infra_ip_last: new.infra_ip_last,
                ports: new.ports.into_iter().map(From::from).collect(),
                bgp: new.bgp,
                bfd: new.bfd,
            },
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::net::Ipv6Addr;

    #[test]
    fn test_uplink_address_conversions() {
        // Confirm we can convert old -> new -> old. In some cases the `new ->
        // old` produces an `old` that isn't quite the same; we fill in
        // `expected_old` in those cases. Specifically: `old` could contain an
        // address of `None`, `Some("0.0.0.0/x")`, or `Some("::/x")`, which will
        // always come back as `None` after the round trip.
        for (old, new, expected_old) in [
            (
                Some("10.0.0.0/8".parse::<IpNet>().unwrap()),
                UplinkAddress::Static { ip_net: "10.0.0.0/8".parse().unwrap() },
                None,
            ),
            (
                Some("fd00:1234::/64".parse::<IpNet>().unwrap()),
                UplinkAddress::Static {
                    ip_net: "fd00:1234::/64".parse().unwrap(),
                },
                None,
            ),
            (None, UplinkAddress::AddrConf, None),
            (
                Some("0.0.0.0/8".parse::<IpNet>().unwrap()),
                UplinkAddress::AddrConf,
                Some(None),
            ),
            (
                Some("::/128".parse::<IpNet>().unwrap()),
                UplinkAddress::AddrConf,
                Some(None),
            ),
        ] {
            for vlan_id in [Some(1234), None] {
                let expected_old = expected_old.unwrap_or(old);
                let old = v20::UplinkAddressConfig { address: old, vlan_id };
                let new = UplinkAddressConfig { address: new, vlan_id };
                let expected_old =
                    v20::UplinkAddressConfig { address: expected_old, vlan_id };

                assert_eq!(UplinkAddressConfig::try_from(old).unwrap(), new);
                assert_eq!(v20::UplinkAddressConfig::from(new), expected_old);
            }
        }
    }

    #[test]
    fn test_router_peer_address_conversions() {
        fn make_new_bgp_peer_config(addr: RouterPeerType) -> BgpPeerConfig {
            BgpPeerConfig {
                asn: 1,
                port: "port".to_owned(),
                addr,
                hold_time: None,
                idle_hold_time: None,
                delay_open: None,
                connect_retry: None,
                keepalive: None,
                remote_asn: None,
                min_ttl: None,
                md5_auth_key: None,
                multi_exit_discriminator: None,
                communities: Vec::new(),
                local_pref: None,
                enforce_first_as: false,
                allowed_import: v1::ImportExportPolicy::NoFiltering,
                allowed_export: v1::ImportExportPolicy::NoFiltering,
                vlan_id: None,
            }
        }
        fn make_old_bgp_peer_config(
            addr: IpAddr,
            router_lifetime: v20::RouterLifetimeConfig,
        ) -> v20::BgpPeerConfig {
            v20::BgpPeerConfig {
                asn: 1,
                port: "port".to_owned(),
                addr,
                hold_time: None,
                idle_hold_time: None,
                delay_open: None,
                connect_retry: None,
                keepalive: None,
                remote_asn: None,
                min_ttl: None,
                md5_auth_key: None,
                multi_exit_discriminator: None,
                communities: Vec::new(),
                local_pref: None,
                enforce_first_as: false,
                allowed_import: v1::ImportExportPolicy::NoFiltering,
                allowed_export: v1::ImportExportPolicy::NoFiltering,
                vlan_id: None,
                router_lifetime,
            }
        }

        // Confirm we can convert old -> new -> old. In some cases the `new ->
        // old` produces an `old` that isn't quite the same; we fill in
        // `expected_old` in those cases. Specifically: `old` could contain an
        // address of `0.0.0.0` or `::`; either will come back as
        // `UNNUMBERED_SENTINEL` after the round trip.
        for (old_ip, old_router_lifetime, new, expected_old_ip) in [
            (
                "10.0.0.1".parse::<IpAddr>().unwrap(),
                v20::RouterLifetimeConfig::default(),
                RouterPeerType::Numbered { ip: "10.0.0.1".parse().unwrap() },
                None,
            ),
            (
                "fd00:1234::3".parse().unwrap(),
                v20::RouterLifetimeConfig::default(),
                RouterPeerType::Numbered {
                    ip: "fd00:1234::3".parse().unwrap(),
                },
                None,
            ),
            (
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                v20::RouterLifetimeConfig::default(),
                RouterPeerType::Unnumbered {
                    router_lifetime: v20::RouterLifetimeConfig::default(),
                },
                Some(RouterPeerType::UNNUMBERED_SENTINEL),
            ),
            (
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                v20::RouterLifetimeConfig::new(1234).unwrap(),
                RouterPeerType::Unnumbered {
                    router_lifetime: v20::RouterLifetimeConfig::new(1234)
                        .unwrap(),
                },
                Some(RouterPeerType::UNNUMBERED_SENTINEL),
            ),
        ] {
            let expected_old_ip = expected_old_ip.unwrap_or(old_ip);

            let old = make_old_bgp_peer_config(old_ip, old_router_lifetime);
            let new = make_new_bgp_peer_config(new);
            let expected_old =
                make_old_bgp_peer_config(expected_old_ip, old_router_lifetime);

            assert_eq!(BgpPeerConfig::try_from(old).unwrap(), new);
            assert_eq!(v20::BgpPeerConfig::from(new), expected_old);
        }
    }
}
