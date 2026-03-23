// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for early networking types.

use crate::latest::early_networking::BgpPeerConfig;
use crate::latest::early_networking::InvalidIpAddrError;
use crate::latest::early_networking::LldpAdminStatus;
use crate::latest::early_networking::MaxPathConfig;
use crate::latest::early_networking::MaxPathConfigError;
use crate::latest::early_networking::PortFec;
use crate::latest::early_networking::PortSpeed;
use crate::latest::early_networking::RouterLifetimeConfig;
use crate::latest::early_networking::RouterLifetimeConfigError;
use crate::latest::early_networking::RouterPeerIpAddr;
use crate::latest::early_networking::RouterPeerIpAddrError;
use crate::latest::early_networking::RouterPeerType;
use crate::latest::early_networking::SwitchSlot;
use crate::latest::early_networking::UplinkAddress;
use crate::latest::early_networking::UplinkAddressConfig;
use crate::latest::early_networking::UplinkIpNet;
use crate::latest::early_networking::UplinkIpNetError;
use ipnetwork::IpNetwork;
use omicron_common::api::external;
use oxnet::IpNet;
use oxnet::IpNetParseError;
use std::fmt;
use std::net::AddrParseError;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::str::FromStr;

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

impl From<PortFec> for external::LinkFec {
    fn from(x: PortFec) -> Self {
        match x {
            PortFec::Firecode => Self::Firecode,
            PortFec::None => Self::None,
            PortFec::Rs => Self::Rs,
        }
    }
}

impl From<PortSpeed> for external::LinkSpeed {
    fn from(x: PortSpeed) -> Self {
        match x {
            PortSpeed::Speed0G => Self::Speed0G,
            PortSpeed::Speed1G => Self::Speed1G,
            PortSpeed::Speed10G => Self::Speed10G,
            PortSpeed::Speed25G => Self::Speed25G,
            PortSpeed::Speed40G => Self::Speed40G,
            PortSpeed::Speed50G => Self::Speed50G,
            PortSpeed::Speed100G => Self::Speed100G,
            PortSpeed::Speed200G => Self::Speed200G,
            PortSpeed::Speed400G => Self::Speed400G,
        }
    }
}

impl FromStr for MaxPathConfig {
    type Err = MaxPathConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: u8 = s.parse()?;
        Self::new(v)
    }
}

impl std::fmt::Display for MaxPathConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u8())
    }
}

impl FromStr for RouterLifetimeConfig {
    type Err = RouterLifetimeConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v: u16 = s.parse()?;
        Self::new(v)
    }
}

impl std::fmt::Display for RouterLifetimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u16())
    }
}

impl std::fmt::Display for UplinkIpNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        IpNet::from(*self).fmt(f)
    }
}

impl std::fmt::Display for RouterPeerIpAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<RouterPeerIpAddr> for IpNetwork {
    fn from(value: RouterPeerIpAddr) -> Self {
        Self::from(IpAddr::from(value))
    }
}

impl UplinkIpNet {
    pub const fn addr(&self) -> IpAddr {
        self.0.addr()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UplinkIpNetParseError {
    #[error("invalid IP net")]
    IpNetParseError(#[from] IpNetParseError),
    #[error(transparent)]
    InvalidIpError(#[from] UplinkIpNetError),
}

impl FromStr for UplinkIpNet {
    type Err = UplinkIpNetParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ip = IpNet::from_str(s)?;
        let addr = Self::try_from(ip)?;
        Ok(addr)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RouterPeerIpAddrParseError {
    #[error(transparent)]
    AddrParseError(#[from] AddrParseError),
    #[error(transparent)]
    InvalidIpAddr(#[from] RouterPeerIpAddrError),
}

impl FromStr for RouterPeerIpAddr {
    type Err = RouterPeerIpAddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ip = IpAddr::from_str(s)?;
        let addr = Self::try_from(ip)?;
        Ok(addr)
    }
}

impl RouterPeerType {
    /// Returns true if `Self` describes a numbered peer; false otherwise.
    pub fn is_numbered(&self) -> bool {
        match self {
            Self::Unnumbered { .. } => false,
            Self::Numbered { .. } => true,
        }
    }

    /// Returns true if `Self` describes an unnumbered peer; false otherwise.
    pub fn is_unnumbered(&self) -> bool {
        !self.is_numbered()
    }
}

impl UplinkAddress {
    /// Squash this address down to a flat IP address by converting
    /// [`UplinkAddress::AddrConf`] to `::/128`.
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn ip_net_squashing_addrconf_to_unspecified(&self) -> IpNet {
        match *self {
            UplinkAddress::AddrConf => {
                IpNet::host_net(IpAddr::V6(Ipv6Addr::UNSPECIFIED))
            }
            UplinkAddress::Static { ip_net } => ip_net.into(),
        }
    }

    /// Convert an arbitrary [`IpNet`] into an [`UplinkAddress`] by converting
    /// an unspecified IP to [`UplinkAddress::AddrConf`].
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn try_from_ip_net_treating_unspecified_as_addrconf(
        ip_net: IpNet,
    ) -> Result<Self, InvalidIpAddrError> {
        match UplinkIpNet::try_from(ip_net) {
            Ok(ip_net) => Ok(Self::Static { ip_net }),
            Err(err) => match err.err {
                InvalidIpAddrError::UnspecifiedAddress => Ok(Self::AddrConf),
                InvalidIpAddrError::LoopbackAddress
                | InvalidIpAddrError::MulticastAddress
                | InvalidIpAddrError::Ipv4Broadcast
                | InvalidIpAddrError::Ipv6UnicastLinkLocal
                | InvalidIpAddrError::Ipv4MappedIpv6 => Err(err.err),
            },
        }
    }
}

impl UplinkAddressConfig {
    /// Helper to construct an `UplinkAddressConfig` with a specified IP net and
    /// no VLAN ID.
    pub fn without_vlan(ip_net: UplinkIpNet) -> Self {
        Self { address: UplinkAddress::Static { ip_net }, vlan_id: None }
    }

    /// Format `self` appropriately for passing to `uplinkd`'s SMF properties.
    pub fn to_uplinkd_smf_property(&self) -> String {
        let addr: &dyn fmt::Display = match &self.address {
            UplinkAddress::AddrConf => &"link-local",
            UplinkAddress::Static { ip_net } => ip_net,
        };

        match self.vlan_id {
            Some(v) => format!("{addr};{v}"),
            None => addr.to_string(),
        }
    }
}

impl LldpAdminStatus {
    /// Format `self` appropriately for passing to `lldpd`'s SMF properties.
    pub fn to_lldpd_smf_property(&self) -> &'static str {
        match self {
            LldpAdminStatus::Enabled => "enabled",
            LldpAdminStatus::Disabled => "disabled",
            LldpAdminStatus::RxOnly => "rx_only",
            LldpAdminStatus::TxOnly => "tx_only",
        }
    }
}

impl SwitchSlot {
    /// Return the slot of the other switch, not ourself.
    pub const fn other(&self) -> Self {
        match self {
            SwitchSlot::Switch0 => SwitchSlot::Switch1,
            SwitchSlot::Switch1 => SwitchSlot::Switch0,
        }
    }
}

// Customize `Debug` so we get lower-cased variants. We used to have a `Display`
// impl used in a variety of logging and error message contexts; we've switched
// that over to using this `Debug` impl, but it's nice for the capitalization to
// remain consistent.
impl fmt::Debug for SwitchSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SwitchSlot::Switch0 => write!(f, "switch0"),
            SwitchSlot::Switch1 => write!(f, "switch1"),
        }
    }
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

impl fmt::Display for PortFec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortFec::Firecode => write!(f, "Firecode R-FEC"),
            PortFec::None => write!(f, "None"),
            PortFec::Rs => write!(f, "RS-FEC"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::latest::early_networking::InvalidIpAddrError;
    use oxnet::{Ipv4Net, Ipv6Net};
    use proptest::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::net::Ipv4Addr;
    use test_strategy::proptest;

    #[test]
    fn test_uplink_smf_property_formatting() {
        for (address, expected_addr) in [
            (
                UplinkAddress::Static {
                    ip_net: UplinkIpNet::try_from(IpNet::V6(
                        Ipv6Net::new("fd80::123".parse().unwrap(), 16).unwrap(),
                    ))
                    .unwrap(),
                },
                "fd80::123/16",
            ),
            (
                UplinkAddress::Static {
                    ip_net: UplinkIpNet::try_from(IpNet::V4(
                        Ipv4Net::new("10.0.0.1".parse().unwrap(), 8).unwrap(),
                    ))
                    .unwrap(),
                },
                "10.0.0.1/8",
            ),
            (UplinkAddress::AddrConf, "link-local"),
        ] {
            for (vlan_id, expected_vlan) in
                [(Some(1), ";1"), (Some(1234), ";1234"), (None, "")]
            {
                let config = UplinkAddressConfig { address, vlan_id };
                let expected = format!("{expected_addr}{expected_vlan}");
                assert_eq!(
                    config.to_uplinkd_smf_property(),
                    expected,
                    "unexpected SMF property for {config:?}"
                );
            }
        }
    }

    // We want our proptests below to hit all the invalid categories of IPs, so
    // define our own input mapping that chooses from all the particular
    // categories we want to reject.
    //
    // Returns an IP and the expected kind of error we should get if we try to
    // parse it, if any.
    fn arb_ip_addr()
    -> impl Strategy<Value = (IpAddr, Option<InvalidIpAddrError>)> {
        prop_oneof![
            // ipv4 unspecified
            Just((
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                Some(InvalidIpAddrError::UnspecifiedAddress)
            )),
            // ipv6 unspecified
            Just((
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                Some(InvalidIpAddrError::UnspecifiedAddress)
            )),
            // ipv4 loopback
            Just((
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                Some(InvalidIpAddrError::LoopbackAddress)
            )),
            // ipv6 loopback
            Just((
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                Some(InvalidIpAddrError::LoopbackAddress)
            )),
            // ipv4 multicast: 224.0.0.0 – 239.255.255.255
            (224u8..=239u8, any::<[u8; 3]>()).prop_map(|(hi, rest)| {
                (
                    IpAddr::V4(Ipv4Addr::new(hi, rest[0], rest[1], rest[2])),
                    Some(InvalidIpAddrError::MulticastAddress),
                )
            }),
            // ipv6 multicast: ff00::/8
            any::<[u8; 15]>().prop_map(|rest| {
                let mut octets = [0u8; 16];
                octets[0] = 0xff;
                octets[1..].copy_from_slice(&rest);
                (
                    IpAddr::V6(Ipv6Addr::from(octets)),
                    Some(InvalidIpAddrError::MulticastAddress),
                )
            }),
            // ipv4 broadcast
            Just((
                IpAddr::V4(Ipv4Addr::BROADCAST),
                Some(InvalidIpAddrError::Ipv4Broadcast)
            )),
            // ipv6 unicast link-local: fe80::/10
            any::<[u8; 15]>().prop_map(|rest| {
                let mut octets = [0u8; 16];
                octets[0] = 0xfe;
                octets[1] = 0x80 | (rest[0] & 0x3f);
                octets[2..].copy_from_slice(&rest[1..]);
                (
                    IpAddr::V6(Ipv6Addr::from(octets)),
                    Some(InvalidIpAddrError::Ipv6UnicastLinkLocal),
                )
            }),
            // ipv4-mapped ipv6
            any::<Ipv4Addr>()
                .prop_map(|ip| ip.to_ipv6_mapped())
                .prop_map(IpAddr::V6)
                .prop_map(|ip| (ip, Some(InvalidIpAddrError::Ipv4MappedIpv6))),
            // any other ipv4 (filtered)
            any::<Ipv4Addr>()
                .prop_filter(
                    "not unspecified, loopback, multicast, or broadcast",
                    |ip| {
                        !ip.is_unspecified()
                            && !ip.is_loopback()
                            && !ip.is_multicast()
                            && !ip.is_broadcast()
                    }
                )
                .prop_map(IpAddr::V4)
                .prop_map(|ip| (ip, None)),
            // any other ipv6 (filtered)
            any::<[u8; 16]>()
                .prop_map(|b| Ipv6Addr::from(b))
                .prop_filter(
                    "not unspecified, loopback, multicast, or link-local",
                    |ip| {
                        !ip.is_unspecified()
                            && !ip.is_loopback()
                            && !ip.is_multicast()
                            && !ip.is_unicast_link_local()
                            && ip.to_ipv4_mapped().is_none()
                    }
                )
                .prop_map(IpAddr::V6)
                .prop_map(|ip| (ip, None)),
        ]
    }

    #[proptest]
    fn test_ip_parsing(
        #[strategy(arb_ip_addr())] input: (IpAddr, Option<InvalidIpAddrError>),
    ) {
        let (ip, expected_err) = input;
        // Test both RouterPeerIpAddr and UplinkIpNet; we don't bother
        // proptesting the network side of `IpNet` because that's not relevant
        // to any of our specific parsing.
        let ip_net = IpNet::new(ip, 24).unwrap();
        let ip_string = ip.to_string();
        let ip_net_string = ip_net.to_string();
        let ip_result = ip_string.parse::<RouterPeerIpAddr>();
        let ip_net_result = ip_net_string.parse::<UplinkIpNet>();

        if let Some(expected_err) = expected_err {
            let ip_err = ip_result.expect_err("parsing failed");
            match ip_err {
                RouterPeerIpAddrParseError::AddrParseError(_) => {
                    panic!("unexpected error {ip_err:?}")
                }
                RouterPeerIpAddrParseError::InvalidIpAddr(ip_err) => {
                    assert_eq!(ip_err.ip, ip);
                    assert_eq!(ip_err.err, expected_err);
                }
            }
            let ip_net_err = ip_net_result.expect_err("parsing failed");
            match ip_net_err {
                UplinkIpNetParseError::IpNetParseError(_) => {
                    panic!("unexpected error {ip_net_err:?}")
                }
                UplinkIpNetParseError::InvalidIpError(ip_net_err) => {
                    assert_eq!(ip_net_err.ip_net, ip_net);
                    assert_eq!(ip_net_err.err, expected_err);
                }
            }
        } else {
            let parsed_ip = ip_result.expect("parsing succeeded");
            assert_eq!(parsed_ip.0, ip);
            let parsed_ip_net = ip_net_result.expect("parsing succeeded");
            assert_eq!(IpNet::from(parsed_ip_net), ip_net);
        }
    }

    #[proptest]
    fn test_router_peer_ip_addr_serialization(
        #[strategy(arb_ip_addr())] input: (IpAddr, Option<InvalidIpAddrError>),
    ) {
        let (ip, expected_err) = input;

        #[derive(Debug, Serialize, Deserialize)]
        struct WrapIp {
            ip: IpAddr,
        }
        #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
        struct WrapRouterIp {
            ip: RouterPeerIpAddr,
        }

        let wrapped = WrapIp { ip };

        let jsonified = serde_json::to_string(&wrapped).unwrap();
        let tomlified = toml::to_string(&wrapped).unwrap();

        let json_result = serde_json::from_str::<WrapRouterIp>(&jsonified);
        let toml_result = toml::from_str::<WrapRouterIp>(&tomlified);

        if let Some(expected_err) = expected_err {
            let expected_err = expected_err.to_string();
            let err =
                json_result.expect_err("deserialization failed").to_string();
            assert!(
                err.contains(&expected_err),
                "got error {err:?}, but expected it to contain {expected_err:?}"
            );
            let err =
                toml_result.expect_err("deserialization failed").to_string();
            assert!(
                err.contains(&expected_err),
                "got error {err:?}, but expected it to contain {expected_err:?}"
            );
        } else {
            let json_result = json_result.expect("deserialization succeeded");
            assert_eq!(json_result.ip.0, ip);
            let toml_result = toml_result.expect("deserialization succeeded");
            assert_eq!(toml_result.ip.0, ip);
        }
    }

    #[proptest]
    fn test_uplink_ip_net_serialization(
        #[strategy(arb_ip_addr())] input: (IpAddr, Option<InvalidIpAddrError>),
    ) {
        let (ip, expected_err) = input;

        #[derive(Debug, Serialize, Deserialize)]
        struct WrapIpNet {
            ip_net: IpNet,
        }
        #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
        struct WrapUplinkIpNet {
            ip_net: UplinkIpNet,
        }

        let ip_net = IpNet::new(ip, 24).unwrap();
        let wrapped = WrapIpNet { ip_net };

        let jsonified = serde_json::to_string(&wrapped).unwrap();
        let tomlified = toml::to_string(&wrapped).unwrap();

        let json_result = serde_json::from_str::<WrapUplinkIpNet>(&jsonified);
        let toml_result = toml::from_str::<WrapUplinkIpNet>(&tomlified);

        if let Some(expected_err) = expected_err {
            let expected_err = expected_err.to_string();
            let err =
                json_result.expect_err("deserialization failed").to_string();
            assert!(
                err.contains(&expected_err),
                "got error {err:?}, but expected it to contain {expected_err:?}"
            );
            let err =
                toml_result.expect_err("deserialization failed").to_string();
            assert!(
                err.contains(&expected_err),
                "got error {err:?}, but expected it to contain {expected_err:?}"
            );
        } else {
            let json_result = json_result.expect("deserialization succeeded");
            assert_eq!(IpNet::from(json_result.ip_net), ip_net);
            let toml_result = toml_result.expect("deserialization succeeded");
            assert_eq!(IpNet::from(toml_result.ip_net), ip_net);
        }
    }
}
