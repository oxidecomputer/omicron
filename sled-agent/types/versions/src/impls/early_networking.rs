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
use crate::latest::early_networking::SwitchSlot;
use crate::latest::early_networking::UplinkAddressConfig;
use omicron_common::api::external;
use oxnet::IpNet;
use std::fmt;
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

impl UplinkAddressConfig {
    /// Construct an `UplinkAddressConfig` with no VLAN ID.
    pub fn without_vlan(address: IpNet) -> Self {
        // TODO-cleanup Squash unspecified addresses down to `None`. We want
        // better types here:
        // <https://github.com/oxidecomputer/omicron/issues/9832>.
        let address =
            if address.addr().is_unspecified() { None } else { Some(address) };
        Self { address, vlan_id: None }
    }

    pub fn addr(&self) -> IpAddr {
        match self.address {
            Some(ipaddr) => ipaddr.addr(),
            None => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        }
    }

    /// Format `self` appropriately for passing to `uplinkd`'s SMF properties.
    pub fn to_uplinkd_smf_property(&self) -> String {
        fn addr_string(addr: &oxnet::IpNet) -> String {
            if addr.addr().is_unspecified() {
                "link-local".into()
            } else {
                addr.to_string()
            }
        }

        // TODO-cleanup for now, squash address values of both `None` and
        // `Some(UNSPECIFIED)` down to "link-local". We want better types here:
        // <https://github.com/oxidecomputer/omicron/issues/9832>.
        match (&self.address, self.vlan_id) {
            (Some(addr), None) => addr_string(addr),
            (Some(addr), Some(v)) => format!("{};{v}", addr_string(addr)),
            (None, None) => "link-local".to_string(),
            (None, Some(v)) => format!("link-local;{v}"),
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
    /// Return the location of the other switch, not ourself.
    pub const fn other(&self) -> Self {
        match self {
            SwitchSlot::Switch0 => SwitchSlot::Switch1,
            SwitchSlot::Switch1 => SwitchSlot::Switch0,
        }
    }
}

impl fmt::Display for SwitchSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SwitchSlot::Switch0 => write!(f, "switch0"),
            SwitchSlot::Switch1 => write!(f, "switch1"),
        }
    }
}

impl SwitchSlot {
    // TODO-correctness enum in external API
    //
    // We should remove this function after changing the external API to use
    // `SwitchSlot` instead of `Name`.
    pub fn parse_from_external_api(
        name: &external::Name,
    ) -> Result<Self, external::Error> {
        match name.as_str() {
            "switch0" => Ok(Self::Switch0),
            "switch1" => Ok(Self::Switch1),
            _ => Err(external::Error::invalid_request(format!(
                "invalid switch location `{name}` \
                 (expected `switch0` or `switch1`)",
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
