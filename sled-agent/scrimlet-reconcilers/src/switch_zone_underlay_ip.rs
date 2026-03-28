// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_common::address::get_switch_zone_address;
use sled_agent_types::sled::StartSledAgentRequest;
use std::fmt;
use std::net::IpAddr;
use std::net::Ipv6Addr;

/// Newtype wrapper around [`Ipv6Addr`]. This type is always the IP address
/// of our own, local switch zone.
///
/// That switch zone will only exist if we are a scrimlet, but we always
/// know what the IP would be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ThisSledSwitchZoneUnderlayIpAddr(Ipv6Addr);

impl ThisSledSwitchZoneUnderlayIpAddr {
    /// Construct a [`ThisSledSwitchZoneUnderlayIpAddr`] from the request to
    /// start this sled agent.
    ///
    /// This takes a full request object instead of something smaller (like just
    /// a sled subnet) to put up a roadblock to accidentally constructing a
    /// [`ThisSledSwitchZoneUnderlayIpAddr`] that points to any address other
    /// than our own. `sled-agent` has ready access to the subnets and addresses
    /// of other sleds, but doesn't have ready access to other sleds'
    /// [`StartSledAgentRequest`]s.
    pub fn from_sled_agent_request(request: &StartSledAgentRequest) -> Self {
        ThisSledSwitchZoneUnderlayIpAddr(get_switch_zone_address(
            request.body.subnet,
        ))
    }
}

// NOTE: We impl `From` only in this direction: constructing a
// `ThisSledSwitchZoneUnderlayIpAddr` must happen only via
// `from_sled_agent_request()`.
impl From<ThisSledSwitchZoneUnderlayIpAddr> for Ipv6Addr {
    fn from(value: ThisSledSwitchZoneUnderlayIpAddr) -> Self {
        value.0
    }
}

impl From<ThisSledSwitchZoneUnderlayIpAddr> for IpAddr {
    fn from(value: ThisSledSwitchZoneUnderlayIpAddr) -> Self {
        value.0.into()
    }
}

impl PartialEq<IpAddr> for ThisSledSwitchZoneUnderlayIpAddr {
    fn eq(&self, other: &IpAddr) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<ThisSledSwitchZoneUnderlayIpAddr> for IpAddr {
    fn eq(&self, other: &ThisSledSwitchZoneUnderlayIpAddr) -> bool {
        self.eq(&other.0)
    }
}

impl PartialEq<Ipv6Addr> for ThisSledSwitchZoneUnderlayIpAddr {
    fn eq(&self, other: &Ipv6Addr) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<ThisSledSwitchZoneUnderlayIpAddr> for Ipv6Addr {
    fn eq(&self, other: &ThisSledSwitchZoneUnderlayIpAddr) -> bool {
        self.eq(&other.0)
    }
}

impl fmt::Display for ThisSledSwitchZoneUnderlayIpAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
