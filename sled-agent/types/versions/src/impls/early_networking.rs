// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementations for early networking types.

use crate::latest::early_networking::BgpPeerConfig;
use crate::latest::early_networking::LldpAdminStatus;
use crate::latest::early_networking::MaxPathConfig;
use crate::latest::early_networking::MaxPathConfigError;
use crate::latest::early_networking::PortFec;
use crate::latest::early_networking::PortSpeed;
use crate::latest::early_networking::RouterLifetimeConfig;
use crate::latest::early_networking::RouterLifetimeConfigError;
use crate::latest::early_networking::RouterPeerAddress;
use crate::latest::early_networking::SpecifiedIpAddr;
use crate::latest::early_networking::SpecifiedIpNet;
use crate::latest::early_networking::SwitchSlot;
use crate::latest::early_networking::UnspecifiedIpError;
use crate::latest::early_networking::UplinkAddress;
use crate::latest::early_networking::UplinkAddressConfig;
use omicron_common::api::external;
use oxnet::IpNet;
use oxnet::IpNetParseError;
use oxnet::Ipv6Net;
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

impl std::fmt::Display for SpecifiedIpNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for SpecifiedIpAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl SpecifiedIpNet {
    pub const fn addr(&self) -> SpecifiedIpAddr {
        let ip = self.0.addr();

        // We're bypassing `SpecifiedIpAddr::try_from()` so we can remain a
        // `const` function, and because we enforce the same invariants. This
        // check documents that fact and provides a runtime guard if we
        // accidentally break it.
        if ip.is_unspecified() {
            panic!(
                "SpecifiedIpNet contains an unspecified IP address \
                 (this should be impossible!)"
            );
        }

        SpecifiedIpAddr(ip)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpecifiedIpNetParseError {
    #[error("invalid IP net")]
    IpNetParseError(#[from] IpNetParseError),
    #[error(transparent)]
    UnspecifiedIpError(#[from] UnspecifiedIpError),
}

impl FromStr for SpecifiedIpNet {
    type Err = SpecifiedIpNetParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ip = IpNet::from_str(s)?;
        let addr = Self::try_from(ip)?;
        Ok(addr)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpecifiedIpAddrParseError {
    #[error(transparent)]
    AddrParseError(#[from] AddrParseError),
    #[error(transparent)]
    UnspecifiedIpError(#[from] UnspecifiedIpError),
}

impl FromStr for SpecifiedIpAddr {
    type Err = SpecifiedIpAddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ip = IpAddr::from_str(s)?;
        let addr = Self::try_from(ip)?;
        Ok(addr)
    }
}

impl RouterPeerAddress {
    /// Squash this address down to an [`Option<IpAddr>`] by converting
    /// [`RouterPeerAddress::Unnumbered`] to `None`.
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn ip_squashing_unnumbered_to_none(&self) -> Option<IpAddr> {
        match *self {
            Self::Unnumbered => None,
            Self::Numbered { ip } => Some(ip.into()),
        }
    }

    /// Convert an arbitrary [`IpAddr`] into a [`RouterPeerAddress`] by
    /// converting an unspecified IP to [`RouterPeerAddress::Unnumbered`].
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn from_ip_treating_unspecified_as_unnumbered(ip: IpAddr) -> Self {
        match SpecifiedIpAddr::try_from(ip) {
            Ok(ip) => Self::Numbered { ip },
            Err(UnspecifiedIpError) => Self::Unnumbered,
        }
    }

    /// Convert an arbitrary `Option<IpAddr>` into a [`RouterPeerAddress`] by
    /// converting both `None` and `Some(UNSPECIFIED)`
    /// [`RouterPeerAddress::Unnumbered`].
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn from_optional_ip_treating_unspecified_as_unnumbered(
        ip: Option<IpAddr>,
    ) -> Self {
        let Some(ip) = ip else {
            return Self::Unnumbered;
        };
        Self::from_ip_treating_unspecified_as_unnumbered(ip)
    }
}

impl UplinkAddress {
    /// Squash this address down to a flat IP address by converting
    /// [`UplinkAddress::LinkLocal`] to `::`.
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn addr_squashing_link_local_to_unspecified(&self) -> IpAddr {
        match self {
            UplinkAddress::LinkLocal => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            UplinkAddress::Address { ip_net } => ip_net.addr().into(),
        }
    }

    /// Squash this address down to an [`IpNet`] address by converting
    /// [`UplinkAddress::LinkLocal`] to `::/128`.
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn ip_net_squashing_link_local_to_unspecified(&self) -> IpNet {
        match *self {
            UplinkAddress::LinkLocal => {
                IpNet::V6(Ipv6Net::host_net(Ipv6Addr::UNSPECIFIED))
            }
            UplinkAddress::Address { ip_net } => ip_net.into(),
        }
    }

    /// Convert an arbitrary [`IpNet`] into an [`UplinkAddress`] by converting
    /// an unspecified IP to [`UplinkAddress::LinkLocal`].
    ///
    /// Uses of this function probably indicate places where we could consider
    /// using stronger types.
    pub fn from_ip_net_treating_unspecified_as_link_local(
        ip_net: IpNet,
    ) -> Self {
        match SpecifiedIpNet::try_from(ip_net) {
            Ok(ip_net) => Self::Address { ip_net },
            Err(UnspecifiedIpError) => Self::LinkLocal,
        }
    }
}

impl UplinkAddressConfig {
    /// Helper to construct an `UplinkAddressConfig` with a specified IP net and
    /// no VLAN ID.
    pub fn without_vlan(ip_net: SpecifiedIpNet) -> Self {
        Self { address: UplinkAddress::Address { ip_net }, vlan_id: None }
    }

    /// Format `self` appropriately for passing to `uplinkd`'s SMF properties.
    pub fn to_uplinkd_smf_property(&self) -> String {
        let addr: &dyn fmt::Display = match &self.address {
            UplinkAddress::LinkLocal => &"link-local",
            UplinkAddress::Address { ip_net } => ip_net,
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
    use assert_matches::assert_matches;
    use serde::{Deserialize, Serialize};
    use test_strategy::proptest;

    #[proptest]
    fn test_specified_ip_parsing(ip: IpAddr) {
        // Test both SpecifiedIpAddr and SpecifiedIpNet; we don't bother
        // proptesting the network side of `IpNet` because that's not relevant
        // to any of our specific parsing.
        let ip_net = IpNet::new(ip, 24).unwrap();
        let ip_string = ip.to_string();
        let ip_net_string = ip_net.to_string();
        let ip_result = ip_string.parse::<SpecifiedIpAddr>();
        let ip_net_result = ip_net_string.parse::<SpecifiedIpNet>();

        if ip.is_unspecified() {
            assert_matches!(
                ip_result,
                Err(SpecifiedIpAddrParseError::UnspecifiedIpError(
                    UnspecifiedIpError
                ))
            );
            assert_matches!(
                ip_net_result,
                Err(SpecifiedIpNetParseError::UnspecifiedIpError(
                    UnspecifiedIpError
                ))
            );
        } else {
            let parsed_ip = ip_result.expect("parsing succeeded");
            assert_eq!(parsed_ip.0, ip);
            let parsed_ip_net = ip_net_result.expect("parsing succeeded");
            assert_eq!(parsed_ip_net.0, ip_net);
        }
    }

    #[proptest]
    fn test_specified_ip_serialization(ip: IpAddr) {
        // Test both SpecifiedIpAddr and SpecifiedIpNet; we don't bother
        // proptesting the network side of `IpNet` because that's not relevant
        // to any of our specific serialization.
        #[derive(Debug, Serialize, Deserialize)]
        struct PlainWrapper {
            ip: IpAddr,
            ip_net: IpNet,
        }
        #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
        struct SpecifiedWrapper {
            ip: SpecifiedIpAddr,
            ip_net: SpecifiedIpNet,
        }

        let ip_net = IpNet::new(ip, 24).unwrap();
        let plain_wrapped = PlainWrapper { ip, ip_net };

        let jsonified = serde_json::to_string(&plain_wrapped).unwrap();
        let tomlified = toml::to_string(&plain_wrapped).unwrap();

        if ip.is_unspecified() {
            // We should fail to deserialize unspecified IPs...
            let json_result =
                serde_json::from_str::<SpecifiedWrapper>(&jsonified);
            let toml_result = toml::from_str::<SpecifiedWrapper>(&tomlified);
            assert_matches!(
                json_result,
                Err(err)
                    if err
                        .to_string()
                        .contains("must not be the unspecified address")
            );
            assert_matches!(
                toml_result,
                Err(err)
                    if err
                        .to_string()
                        .contains("must not be the unspecified address")
            );
        } else {
            // ... but successfully deserialize specified ones. And our
            // serialized form should exactly match the wrapped types.
            let json_result =
                serde_json::from_str::<SpecifiedWrapper>(&jsonified)
                    .expect("deserialized");
            let toml_result = toml::from_str::<SpecifiedWrapper>(&tomlified)
                .expect("deserialized");

            assert_eq!(json_result, toml_result);
            assert_eq!(json_result.ip.0, ip);
            assert_eq!(json_result.ip_net.0, ip_net);

            let jsonified2 = serde_json::to_string(&json_result).unwrap();
            let tomlified2 = toml::to_string(&json_result).unwrap();
            assert_eq!(jsonified, jsonified2);
            assert_eq!(tomlified, tomlified2);
        }
    }
}
