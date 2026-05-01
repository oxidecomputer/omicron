// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types related to operating on sleds.

pub use sled_agent_types_versions::latest::sled::*;

pub const SWITCH_ZONE_BASEBOARD_FILE: &str = "/opt/oxide/baseboard.json";

pub use self::local_switch_zone_ip::ThisSledSwitchZoneUnderlayIpAddr;

/// Private module to enforce construction of
/// [`ThisSledSwitchZoneUnderlayIpAddr`] only happens via the constructors we
/// define.
mod local_switch_zone_ip {
    use omicron_common::address::get_switch_zone_address;
    use sled_agent_types_versions::latest::sled::StartSledAgentRequest;
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
        #[cfg(any(test, feature = "testing"))]
        pub const TEST_FAKE: Self = Self(Ipv6Addr::LOCALHOST);

        /// Construct a [`ThisSledSwitchZoneUnderlayIpAddr`] from the request to
        /// start this sled agent.
        ///
        /// This function MUST only be called by a `sled-agent` with its own
        /// `StartSledAgentRequest`. We can't statically enforce this
        /// requirement, but failure to adhere to this convention defeats the
        /// entire point of this type: it is supposed to provide a way for
        /// `sled-agent` (and related crates) to statically "tag" an IP address
        /// as being the IP of its own, local switch zone.
        pub fn from_sled_agent_request(
            request: &StartSledAgentRequest,
        ) -> Self {
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

    impl fmt::Display for ThisSledSwitchZoneUnderlayIpAddr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt(f)
        }
    }
}
