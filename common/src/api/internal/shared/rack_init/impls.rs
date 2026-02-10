// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::BgpPeerConfig;
use super::HostPortConfig;
use super::LldpAdminStatus;
use super::MaxPathConfig;
use super::MaxPathConfigError;
use super::ParseLldpAdminStatusError;
use super::ParseSwitchLocationError;
use super::PortConfig;
use super::PortFec;
use super::PortSpeed;
use super::RouterLifetimeConfig;
use super::RouterLifetimeConfigError;
use super::SwitchLocation;
use super::UplinkAddressConfig;
use super::UplinkAddressConfigError;
use std::fmt;
use std::net::{IpAddr, Ipv6Addr};
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
        let v: u16 = s.parse().map_err(|e| {
            RouterLifetimeConfigError(format!(
                "failed to parse value for router lifetime config: {e}"
            ))
        })?;

        Self::new(v)
    }
}

impl std::fmt::Display for RouterLifetimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u16())
    }
}

impl UplinkAddressConfig {
    pub fn addr(&self) -> IpAddr {
        match self.address {
            Some(ipaddr) => ipaddr.addr(),
            None => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        }
    }
}

impl std::fmt::Display for UplinkAddressConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.address, self.vlan_id) {
            (Some(addr), None) => write!(f, "{addr}"),
            (Some(addr), Some(v)) => write!(f, "{addr};{v}"),
            (None, None) => write!(f, "link-local"),
            (None, Some(v)) => write!(f, "link-local;{v}"),
        }
    }
}

impl std::fmt::Display for UplinkAddressConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse switch location error: {}", self.0)
    }
}

/// Convert a string into an UplinkAddressConfig.
/// 192.168.1.1/24 => UplinkAddressConfig { Some(192.168.1.1/24), None }
/// 192.168.1.1/24;200 => UplinkAddressConfig { Some(192.168.1.1/24), Some(200) }
/// link-local => UplinkAddressConfig { None, None }
/// link-local;200 => UplinkAddressConfig { None, Some(200) }
impl FromStr for UplinkAddressConfig {
    type Err = UplinkAddressConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fields: Vec<&str> = s.split(';').collect();
        let (address_str, vlan_id) = match fields.len() {
            1 => Ok((fields[0], None)),
            2 => Ok((fields[0], Some(fields[1]))),
            _ => Err(UplinkAddressConfigError(format!(
                "not a valid uplink address: {s}"
            ))),
        }?;
        let address = if address_str == "link-local" {
            None
        } else {
            Some(address_str.parse().map_err(|_| {
                UplinkAddressConfigError(format!(
                    "not a valid ip address: {address_str}"
                ))
            })?)
        };
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
