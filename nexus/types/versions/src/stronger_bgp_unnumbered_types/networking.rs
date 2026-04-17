// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Networking types for the `STRONGER_BGP_UNNUMBERED_TYPES` version.
//!
//! * Change fields of [`BgpPeer`] to give stronger guarantees:
//!   * [`BgpPeer::addr`] is now [`RouterPeerType`] instead of
//!     `Option<IpAddr>` (which permitted three distinct representations of
//!     "unnumbered": `None`, `Some(0.0.0.0)`, and `Some(::)`).
//!   * `BgpPeer::router_lifetime` moved from being a top-level field to being
//!     nested inside the [`RouterPeerType::Unnumbered`] variant, and its type
//!     is now [`RouterLifetimeConfig`] instead of `u16`, adding enforcement of
//!     bounds.
//! * Remove `BgpPeer::interface_name` (omicron#10104).
//! * Define new versions of types that transitively include [`BgpPeer`]:
//!   * [`BgpPeerConfig`]
//!   * [`SwitchPortSettings`]
//!   * [`SwitchPortSettingsCreate`]

use crate::v2025_11_20_00::networking::{
    AddressConfig, LinkConfigCreate, RouteConfig, SwitchInterfaceConfigCreate,
    SwitchPortConfigCreate,
};
use omicron_common::api::external;
use omicron_common::api::external::IdentityMetadata;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::Name;
use omicron_common::api::external::NameOrId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_agent_types::early_networking::ImportExportPolicy;
use sled_agent_types::early_networking::InvalidIpAddrError;
use sled_agent_types::early_networking::RouterLifetimeConfig;
use sled_agent_types::early_networking::RouterLifetimeConfigError;
use sled_agent_types::early_networking::RouterPeerIpAddr;
use sled_agent_types::early_networking::RouterPeerIpAddrError;
use sled_agent_types::early_networking::RouterPeerType;
use std::net::IpAddr;

/// A BGP peer configuration for an interface. Includes the set of announcements
/// that will be advertised to the peer. The `bgp_config` parameter is a
/// reference to global BGP parameters.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct BgpPeer {
    /// The global BGP configuration used for establishing a session with this
    /// peer.
    pub bgp_config: NameOrId,

    /// The address of the host to peer with, or specifying the configuration of
    /// an unnumbered BGP session.
    pub addr: RouterPeerType,

    /// How long to hold peer connections between keepalives (seconds).
    pub hold_time: u32,

    /// How long to hold a peer in idle before attempting a new session
    /// (seconds).
    pub idle_hold_time: u32,

    /// How long to delay sending an open request after establishing a TCP
    /// session (seconds).
    pub delay_open: u32,

    /// How long to wait between TCP connection retries (seconds).
    pub connect_retry: u32,

    /// How often to send keepalive requests (seconds).
    pub keepalive: u32,

    /// Require that a peer has a specified ASN.
    pub remote_asn: Option<u32>,

    /// Require messages from a peer have a minimum IP time to live field.
    pub min_ttl: Option<u8>,

    /// Use the given key for TCP-MD5 authentication with the peer.
    pub md5_auth_key: Option<String>,

    /// Apply the provided multi-exit discriminator (MED) updates sent to the peer.
    pub multi_exit_discriminator: Option<u32>,

    /// Include the provided communities in updates sent to the peer.
    pub communities: Vec<u32>,

    /// Apply a local preference to routes received from this peer.
    pub local_pref: Option<u32>,

    /// Enforce that the first AS in paths received from this peer is the peer's AS.
    pub enforce_first_as: bool,

    /// Define import policy for a peer.
    pub allowed_import: ImportExportPolicy,

    /// Define export policy for a peer.
    pub allowed_export: ImportExportPolicy,

    /// Associate a VLAN ID with a peer.
    pub vlan_id: Option<u16>,
}

#[derive(Debug, thiserror::Error)]
pub enum BgpPeerConversionError {
    #[error(transparent)]
    RouterPeerIpAddr(#[from] RouterPeerIpAddrError),
    #[error(transparent)]
    RouterLifetimeConfig(#[from] RouterLifetimeConfigError),
}

/// Convert from our previous representations of BGP peer addresses: an optional
/// IP address and an unvalidated `router_lifetime`.
///
/// Converts IPs of both `None` and `Some(UNSPECIFIED)` to
/// [`RouterPeerType::Unnumbered`], and converts other `Some(ip)` values to
/// [`RouterPeerType::Numbered`] (discarding `router_lifetime`, as it only
/// applies to unnumbered peers). Fails if given an invalid IP (e.g., a loopback
/// or multicast address) or if given an IP that maps to
/// [`RouterPeerType::Unnumbered`] and `router_lifetime` is an invalid
/// [`RouterLifetimeConfig`].
///
/// This method is private and is only used by a `TryFrom` implementation and
/// the conversion unit tests in this module.
fn router_peer_type_try_from_old_representation(
    ip: Option<IpAddr>,
    router_lifetime: u16,
) -> Result<RouterPeerType, BgpPeerConversionError> {
    match ip.map(RouterPeerIpAddr::try_from) {
        // The expected cases: We either have a valid peer IP or `None` (an
        // unnumbered peer).
        Some(Ok(ip)) => Ok(RouterPeerType::Numbered { ip }),
        None => {
            let router_lifetime = RouterLifetimeConfig::new(router_lifetime)?;
            Ok(RouterPeerType::Unnumbered { router_lifetime })
        }

        // Unexpected cases: If `ip` is `Some(UNSPECIFIED)`, we'll treat that as
        // `unnumbered`, because `UNSPECIFIED` was previously used as the
        // sentinel value for unnumbered peers in some contexts. For any other
        // error case, we want to reject this conversion - the peer IP isn't
        // valid.
        Some(Err(err)) => match err.err {
            InvalidIpAddrError::UnspecifiedAddress => {
                let router_lifetime =
                    RouterLifetimeConfig::new(router_lifetime)?;
                Ok(RouterPeerType::Unnumbered { router_lifetime })
            }
            InvalidIpAddrError::LoopbackAddress
            | InvalidIpAddrError::MulticastAddress
            | InvalidIpAddrError::Ipv4Broadcast
            | InvalidIpAddrError::Ipv6UnicastLinkLocal
            | InvalidIpAddrError::Ipv4MappedIpv6 => Err(err.into()),
        },
    }
}

impl TryFrom<crate::v2026_02_13_01::networking::BgpPeer> for BgpPeer {
    type Error = BgpPeerConversionError;

    fn try_from(
        value: crate::v2026_02_13_01::networking::BgpPeer,
    ) -> Result<Self, Self::Error> {
        let addr = router_peer_type_try_from_old_representation(
            value.addr,
            value.router_lifetime,
        )?;
        Ok(Self {
            bgp_config: value.bgp_config,
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

impl From<BgpPeer> for crate::v2026_02_13_01::networking::BgpPeer {
    fn from(value: BgpPeer) -> Self {
        // We have to fill in some valid name for `interface_name` when
        // converting back to the old BgpPeer version, but the field is gone
        // because we were never actually using it. We'll use
        // `deprecated-field`: this should stand out if any person looks at it
        // and (hopefully!) convey that the field is no longer needed.
        let interface_name =
            "deprecated-field".parse().expect("constant is a valid Name");

        let (addr, router_lifetime) = match value.addr {
            RouterPeerType::Numbered { ip } => {
                // The previous `BgpPeer` always contained a `router_lifetime`,
                // but only used it if the peer was unnumbered. We can fill in
                // any arbitrary value here - just use the default.
                (Some(ip.into()), RouterLifetimeConfig::default().as_u16())
            }
            RouterPeerType::Unnumbered { router_lifetime } => {
                (None, router_lifetime.as_u16())
            }
        };

        Self {
            bgp_config: value.bgp_config,
            interface_name,
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
            router_lifetime,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct BgpPeerConfig {
    /// Link that the peer is reachable on.
    /// On ports that are not broken out, this is always phy0.
    /// On a 2x breakout the options are phy0 and phy1, on 4x
    /// phy0-phy3, etc.
    pub link_name: Name,

    pub peers: Vec<BgpPeer>,
}

impl TryFrom<crate::v2026_02_13_01::networking::BgpPeerConfig>
    for BgpPeerConfig
{
    type Error = BgpPeerConversionError;

    fn try_from(
        value: crate::v2026_02_13_01::networking::BgpPeerConfig,
    ) -> Result<Self, Self::Error> {
        let peers = value
            .peers
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;

        Ok(Self { link_name: value.link_name, peers })
    }
}

/// Parameters for creating switch port settings. Switch port settings are the
/// central data structure for setting up external networking. Switch port
/// settings include link, interface, route, address and dynamic network
/// protocol configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SwitchPortSettingsCreate {
    #[serde(flatten)]
    pub identity: IdentityMetadataCreateParams,

    pub port_config: SwitchPortConfigCreate,

    #[serde(default)]
    pub groups: Vec<NameOrId>,

    /// Link configurations.
    pub links: Vec<LinkConfigCreate>,

    /// Interface configurations.
    #[serde(default)]
    pub interfaces: Vec<SwitchInterfaceConfigCreate>,

    /// Route configurations.
    #[serde(default)]
    pub routes: Vec<RouteConfig>,

    /// BGP peer configurations.
    #[serde(default)]
    pub bgp_peers: Vec<BgpPeerConfig>,

    /// Address configurations.
    pub addresses: Vec<AddressConfig>,
}

impl TryFrom<crate::v2026_02_13_01::networking::SwitchPortSettingsCreate>
    for SwitchPortSettingsCreate
{
    type Error = BgpPeerConversionError;

    fn try_from(
        value: crate::v2026_02_13_01::networking::SwitchPortSettingsCreate,
    ) -> Result<Self, Self::Error> {
        let bgp_peers = value
            .bgp_peers
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            identity: value.identity,
            port_config: value.port_config,
            groups: value.groups,
            links: value.links,
            interfaces: value.interfaces,
            routes: value.routes,
            bgp_peers,
            addresses: value.addresses,
        })
    }
}

/// This structure contains all port settings information in one place. It's a
/// convenience data structure for getting a complete view of a particular
/// port's settings.
// TODO: several fields below embed `external::*` types directly from
// `omicron-common`, which means their serialized shape is not truly frozen.
// Once `omicron-common-versions` exists, replace these with version-local
// copies of the types to ensure the initial version's wire format is
// immutable.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct SwitchPortSettings {
    #[serde(flatten)]
    pub identity: IdentityMetadata,

    /// Switch port settings included from other switch port settings groups.
    pub groups: Vec<external::SwitchPortSettingsGroups>,

    /// Layer 1 physical port settings.
    pub port: external::SwitchPortConfig,

    /// Layer 2 link settings.
    pub links: Vec<external::SwitchPortLinkConfig>,

    /// Layer 3 interface settings.
    pub interfaces: Vec<external::SwitchInterfaceConfig>,

    /// Vlan interface settings.
    pub vlan_interfaces: Vec<external::SwitchVlanInterfaceConfig>,

    /// IP route settings.
    pub routes: Vec<external::SwitchPortRouteConfig>,

    /// BGP peer settings.
    pub bgp_peers: Vec<BgpPeer>,

    /// Layer 3 IP address settings.
    pub addresses: Vec<external::SwitchPortAddressView>,
}

impl From<SwitchPortSettings>
    for crate::v2026_02_13_01::networking::SwitchPortSettings
{
    fn from(value: SwitchPortSettings) -> Self {
        Self {
            identity: value.identity,
            groups: value.groups,
            port: value.port,
            links: value.links,
            interfaces: value.interfaces,
            vlan_interfaces: value.vlan_interfaces,
            routes: value.routes,
            bgp_peers: value.bgp_peers.into_iter().map(From::from).collect(),
            addresses: value.addresses,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    /// Helper to build a minimal old-format `BgpPeer` for conversion tests.
    fn make_old_bgp_peer(
        addr: Option<IpAddr>,
        router_lifetime: u16,
    ) -> crate::v2026_02_13_01::networking::BgpPeer {
        crate::v2026_02_13_01::networking::BgpPeer {
            bgp_config: NameOrId::Name("test-config".parse().unwrap()),
            interface_name: "phy0".parse().unwrap(),
            addr,
            hold_time: 6,
            idle_hold_time: 6,
            delay_open: 0,
            connect_retry: 3,
            keepalive: 2,
            remote_asn: None,
            min_ttl: None,
            md5_auth_key: None,
            multi_exit_discriminator: None,
            communities: Vec::new(),
            local_pref: None,
            enforce_first_as: false,
            allowed_import: ImportExportPolicy::NoFiltering,
            allowed_export: ImportExportPolicy::NoFiltering,
            vlan_id: None,
            router_lifetime,
        }
    }

    #[test]
    fn test_valid_router_peer_type_try_from_old_representation() {
        let cases: Vec<(&str, Option<IpAddr>, u16, RouterPeerType)> = vec![
            (
                "numbered IPv4",
                Some("192.168.1.1".parse().unwrap()),
                0,
                RouterPeerType::Numbered { ip: "192.168.1.1".parse().unwrap() },
            ),
            (
                "numbered IPv6",
                Some("fd00::1".parse().unwrap()),
                0,
                RouterPeerType::Numbered { ip: "fd00::1".parse().unwrap() },
            ),
            (
                "unnumbered via None",
                None,
                300,
                RouterPeerType::Unnumbered {
                    router_lifetime: RouterLifetimeConfig::new(300).unwrap(),
                },
            ),
            (
                "unnumbered via IPv4 UNSPECIFIED sentinel",
                Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
                300,
                RouterPeerType::Unnumbered {
                    router_lifetime: RouterLifetimeConfig::new(300).unwrap(),
                },
            ),
            (
                "unnumbered via IPv6 UNSPECIFIED sentinel",
                Some(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
                300,
                RouterPeerType::Unnumbered {
                    router_lifetime: RouterLifetimeConfig::new(300).unwrap(),
                },
            ),
        ];

        for (label, addr, router_lifetime, expected) in cases {
            let result = router_peer_type_try_from_old_representation(
                addr,
                router_lifetime,
            )
            .unwrap_or_else(|e| panic!("{label}: unexpected error: {e:?}"));
            assert_eq!(result, expected, "{label}");
        }
    }

    #[test]
    fn test_invalid_router_peer_type_try_from_old_representation() {
        let ip_error_cases: Vec<(&str, IpAddr, InvalidIpAddrError)> = vec![
            (
                "IPv4 loopback",
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                InvalidIpAddrError::LoopbackAddress,
            ),
            (
                "IPv6 loopback",
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                InvalidIpAddrError::LoopbackAddress,
            ),
            (
                "multicast",
                "224.0.0.1".parse().unwrap(),
                InvalidIpAddrError::MulticastAddress,
            ),
            (
                "broadcast",
                IpAddr::V4(Ipv4Addr::BROADCAST),
                InvalidIpAddrError::Ipv4Broadcast,
            ),
            (
                "IPv6 link-local",
                "fe80::1".parse().unwrap(),
                InvalidIpAddrError::Ipv6UnicastLinkLocal,
            ),
            (
                "IPv4-mapped IPv6",
                "::ffff:192.168.0.1".parse().unwrap(),
                InvalidIpAddrError::Ipv4MappedIpv6,
            ),
        ];

        for (label, ip, expected_err) in ip_error_cases {
            let err = router_peer_type_try_from_old_representation(Some(ip), 0)
                .expect_err(&format!("{label}: should have failed"));
            match err {
                BgpPeerConversionError::RouterPeerIpAddr(
                    RouterPeerIpAddrError { err, .. },
                ) => assert_eq!(
                    std::mem::discriminant(&err),
                    std::mem::discriminant(&expected_err),
                    "{label}: wrong error variant: {err:?}"
                ),
                other => panic!("{label}: wrong error type: {other:?}"),
            }
        }

        // router_lifetime too large — via None and via UNSPECIFIED sentinel
        for (label, addr) in [
            ("None", None),
            ("UNSPECIFIED", Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED))),
        ] {
            let err = router_peer_type_try_from_old_representation(addr, 9001)
                .expect_err(&format!(
                    "router_lifetime 9001 with {label} addr should fail"
                ));
            assert!(
                matches!(err, BgpPeerConversionError::RouterLifetimeConfig(_)),
                "{label}: wrong error type: {err:?}"
            );
        }
    }

    #[test]
    fn test_bgp_peer_conversion_numbered_preserves_fields() {
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let old = make_old_bgp_peer(Some(ip), 0);
        let new = BgpPeer::try_from(old.clone()).unwrap();

        assert_eq!(
            new.addr,
            RouterPeerType::Numbered { ip: ip.try_into().unwrap() }
        );
        assert_eq!(new.bgp_config, old.bgp_config);
        assert_eq!(new.hold_time, old.hold_time);
        assert_eq!(new.idle_hold_time, old.idle_hold_time);
        assert_eq!(new.delay_open, old.delay_open);
        assert_eq!(new.connect_retry, old.connect_retry);
        assert_eq!(new.keepalive, old.keepalive);
        assert_eq!(new.remote_asn, old.remote_asn);
        assert_eq!(new.enforce_first_as, old.enforce_first_as);
        assert_eq!(new.allowed_import, old.allowed_import);
        assert_eq!(new.allowed_export, old.allowed_export);
    }

    #[test]
    fn test_bgp_peer_conversion_unnumbered_preserves_fields() {
        let old = make_old_bgp_peer(None, 300);
        let new = BgpPeer::try_from(old.clone()).unwrap();

        assert_eq!(
            new.addr,
            RouterPeerType::Unnumbered {
                router_lifetime: RouterLifetimeConfig::new(300).unwrap()
            }
        );
        assert_eq!(new.bgp_config, old.bgp_config);
        assert_eq!(new.hold_time, old.hold_time);
    }

    #[test]
    fn test_bgp_peer_reverse_numbered() {
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let new = BgpPeer::try_from(make_old_bgp_peer(Some(ip), 123)).unwrap();
        let back: crate::v2026_02_13_01::networking::BgpPeer = new.into();

        assert_eq!(back.addr, Some(ip));

        // router_lifetime is filled with the default for numbered peers
        assert_eq!(
            back.router_lifetime,
            RouterLifetimeConfig::default().as_u16()
        );

        // interface_name gets the placeholder
        assert_eq!(back.interface_name.as_str(), "deprecated-field");
    }

    #[test]
    fn test_bgp_peer_reverse_unnumbered() {
        let new = BgpPeer::try_from(make_old_bgp_peer(None, 300)).unwrap();
        let back: crate::v2026_02_13_01::networking::BgpPeer = new.into();

        assert_eq!(back.addr, None);
        assert_eq!(back.router_lifetime, 300);
    }

    #[test]
    fn test_bgp_peer_round_trip_numbered() {
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let original = make_old_bgp_peer(Some(ip), 0);
        let new = BgpPeer::try_from(original.clone()).unwrap();
        let back: crate::v2026_02_13_01::networking::BgpPeer = new.into();

        // addr and all non-removed fields should survive the round trip
        assert_eq!(back.addr, original.addr);
        assert_eq!(back.bgp_config, original.bgp_config);
        assert_eq!(back.hold_time, original.hold_time);
        assert_eq!(back.communities, original.communities);
        assert_eq!(back.allowed_import, original.allowed_import);
    }

    #[test]
    fn test_bgp_peer_round_trip_unnumbered() {
        let original = make_old_bgp_peer(None, 300);
        let new = BgpPeer::try_from(original.clone()).unwrap();
        let back: crate::v2026_02_13_01::networking::BgpPeer = new.into();

        assert_eq!(back.addr, original.addr);
        assert_eq!(back.router_lifetime, original.router_lifetime);
        assert_eq!(back.bgp_config, original.bgp_config);
        assert_eq!(back.hold_time, original.hold_time);
    }

    #[test]
    fn test_bgp_peer_round_trip_sentinel_becomes_none() {
        // Old clients might send Some(0.0.0.0) for unnumbered. After round
        // trip, this normalizes to None (the canonical representation).
        let original =
            make_old_bgp_peer(Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)), 300);
        let new = BgpPeer::try_from(original).unwrap();
        let back: crate::v2026_02_13_01::networking::BgpPeer = new.into();

        assert_eq!(back.addr, None, "UNSPECIFIED should normalize to None");
        assert_eq!(back.router_lifetime, 300);
    }
}
