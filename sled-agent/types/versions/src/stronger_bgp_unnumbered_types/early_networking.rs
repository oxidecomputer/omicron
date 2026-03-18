// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.
//!
//! Changes in this version:
//!
//! * Introduce [`SpecifiedIpNet`], a newtype wrapper around [`IpNet`] that does
//!   not allow unspecified IP addresses.
//! * Introduce [`SpecifiedIpAddr`], a newtype wrapper around [`IpAddr`] that
//!   does not allow unspecified IP addresses.
//! * Introduce [`UplinkAddress`], a stronger type for specifying
//!   possibly-link-local IP nets. This is the new type of
//!   [`UplinkAddressConfig::address`], which was previously an
//!   [`Option<IpNet>`] where both `None` and `Some(UNSPECIFIED)` were treated
//!   as link-local.
//! * Introduce [`RouterPeerAddress`], a stronger type for specifying
//!   possibly-unnumbered BGP peer addresses. This is the new type of
//!   [`BgpPeerConfig::addr`], which was previously an [`IpAddr`] where an
//!   unspecified address was treated as unnumbered.
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
use std::net::{IpAddr, Ipv6Addr};

#[derive(Debug, thiserror::Error)]
#[error("IP address must not be the unspecified address (0.0.0.0 or ::)")]
pub struct UnspecifiedIpError;

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
    LinkLocal,
    Address { ip_net: SpecifiedIpNet },
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
pub enum RouterPeerAddress {
    Unnumbered,
    Numbered { ip: SpecifiedIpAddr },
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
#[serde(try_from = "IpNet", into = "IpNet")]
pub struct SpecifiedIpNet(pub(crate) IpNet);

// These conversion implementations are defined here instead of in
// `crate::impls::*` because they're tied to how we derive `Deserialize` and
// `Serialize`.
impl From<SpecifiedIpNet> for IpNet {
    fn from(value: SpecifiedIpNet) -> Self {
        value.0
    }
}

impl TryFrom<IpNet> for SpecifiedIpNet {
    type Error = UnspecifiedIpError;

    fn try_from(value: IpNet) -> Result<Self, Self::Error> {
        if value.addr().is_unspecified() {
            Err(UnspecifiedIpError)
        } else {
            Ok(Self(value))
        }
    }
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
#[serde(try_from = "IpAddr", into = "IpAddr")]
pub struct SpecifiedIpAddr(pub(crate) IpAddr);

// As with `SpecifiedIpNet`, these conversion implementations are defined here
// instead of in `crate::impls::*` because they're tied to how we derive
// `Deserialize` and `Serialize`.
impl From<SpecifiedIpAddr> for IpAddr {
    fn from(value: SpecifiedIpAddr) -> Self {
        value.0
    }
}

impl TryFrom<IpAddr> for SpecifiedIpAddr {
    type Error = UnspecifiedIpError;

    fn try_from(value: IpAddr) -> Result<Self, Self::Error> {
        if value.is_unspecified() {
            Err(UnspecifiedIpError)
        } else {
            Ok(Self(value))
        }
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

impl From<v20::UplinkAddressConfig> for UplinkAddressConfig {
    fn from(value: v20::UplinkAddressConfig) -> Self {
        let address = match value.address.map(SpecifiedIpNet::try_from) {
            Some(Ok(ip_net)) => UplinkAddress::Address { ip_net },
            Some(Err(UnspecifiedIpError)) | None => UplinkAddress::LinkLocal,
        };
        Self { address, vlan_id: value.vlan_id }
    }
}

impl From<UplinkAddressConfig> for v20::UplinkAddressConfig {
    fn from(value: UplinkAddressConfig) -> Self {
        let address = match value.address {
            UplinkAddress::LinkLocal => None,
            UplinkAddress::Address { ip_net } => Some(ip_net.into()),
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

impl From<v20::PortConfig> for PortConfig {
    fn from(value: v20::PortConfig) -> Self {
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
    pub addr: RouterPeerAddress,
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
    /// Router lifetime in seconds for unnumbered BGP peers.
    #[serde(default)]
    pub router_lifetime: v20::RouterLifetimeConfig,
}

impl From<v20::BgpPeerConfig> for BgpPeerConfig {
    fn from(value: v20::BgpPeerConfig) -> Self {
        let addr = match SpecifiedIpAddr::try_from(value.addr) {
            Ok(ip) => RouterPeerAddress::Numbered { ip },
            Err(UnspecifiedIpError) => RouterPeerAddress::Unnumbered,
        };
        Self {
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
            router_lifetime: value.router_lifetime,
        }
    }
}

impl From<BgpPeerConfig> for v20::BgpPeerConfig {
    fn from(value: BgpPeerConfig) -> Self {
        let addr = match value.addr {
            RouterPeerAddress::Unnumbered => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            RouterPeerAddress::Numbered { ip } => ip.into(),
        };
        Self {
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
            router_lifetime: value.router_lifetime,
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

// We're required to implement `TryFrom` for our deserialization machinery, but
// this conversion is infallible.
impl TryFrom<v26::EarlyNetworkConfigBody> for EarlyNetworkConfigBody {
    type Error = anyhow::Error;

    fn try_from(old: v26::EarlyNetworkConfigBody) -> Result<Self, Self::Error> {
        let old = old.rack_network_config;

        Ok(Self {
            rack_network_config: RackNetworkConfig {
                rack_subnet: old.rack_subnet,
                infra_ip_first: old.infra_ip_first,
                infra_ip_last: old.infra_ip_last,
                ports: old.ports.into_iter().map(From::from).collect(),
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
    use std::net::Ipv4Addr;

    use crate::v20::early_networking::RouterLifetimeConfig;

    use super::*;

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
                UplinkAddress::Address {
                    ip_net: "10.0.0.0/8".parse().unwrap(),
                },
                None,
            ),
            (
                Some("fe80:1234::/64".parse::<IpNet>().unwrap()),
                UplinkAddress::Address {
                    ip_net: "fe80:1234::/64".parse().unwrap(),
                },
                None,
            ),
            (None, UplinkAddress::LinkLocal, None),
            (
                Some("0.0.0.0/8".parse::<IpNet>().unwrap()),
                UplinkAddress::LinkLocal,
                Some(None),
            ),
            (
                Some("::/128".parse::<IpNet>().unwrap()),
                UplinkAddress::LinkLocal,
                Some(None),
            ),
        ] {
            for vlan_id in [Some(1234), None] {
                let expected_old = expected_old.unwrap_or(old);
                let old = v20::UplinkAddressConfig { address: old, vlan_id };
                let new = UplinkAddressConfig { address: new, vlan_id };
                let expected_old =
                    v20::UplinkAddressConfig { address: expected_old, vlan_id };

                assert_eq!(UplinkAddressConfig::from(old), new);
                assert_eq!(v20::UplinkAddressConfig::from(new), expected_old);
            }
        }
    }

    #[test]
    fn test_router_peer_address_conversions() {
        fn make_new_bgp_peer_config(addr: RouterPeerAddress) -> BgpPeerConfig {
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
                router_lifetime: RouterLifetimeConfig::default(),
            }
        }
        fn make_old_bgp_peer_config(addr: IpAddr) -> v20::BgpPeerConfig {
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
                router_lifetime: RouterLifetimeConfig::default(),
            }
        }

        // Confirm we can convert old -> new -> old. In some cases the `new ->
        // old` produces an `old` that isn't quite the same; we fill in
        // `expected_old` in those cases. Specifically: `old` could contain an
        // address of `0.0.0.0` or `::`; either will come back as `::` after the
        // round trip.
        for (old, new, expected_old) in [
            (
                "10.0.0.1".parse::<IpAddr>().unwrap(),
                RouterPeerAddress::Numbered { ip: "10.0.0.1".parse().unwrap() },
                None,
            ),
            (
                "fe80:1234::3".parse().unwrap(),
                RouterPeerAddress::Numbered {
                    ip: "fe80:1234::3".parse().unwrap(),
                },
                None,
            ),
            (
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                RouterPeerAddress::Unnumbered,
                Some(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
            ),
            (
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                RouterPeerAddress::Unnumbered,
                None,
            ),
        ] {
            let expected_old = expected_old.unwrap_or(old);

            let old = make_old_bgp_peer_config(old);
            let new = make_new_bgp_peer_config(new);
            let expected_old = make_old_bgp_peer_config(expected_old);

            assert_eq!(BgpPeerConfig::from(old), new);
            assert_eq!(v20::BgpPeerConfig::from(new), expected_old);
        }
    }
}
