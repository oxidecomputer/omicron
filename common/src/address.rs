// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common IP addressing functionality.
//!
//! This addressing functionality is shared by both initialization services
//! and Nexus, who need to agree upon addressing schemes.

use crate::api::external::Ipv6Net;
use ipnetwork::Ipv6Network;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::{Ipv6Addr, SocketAddrV6};

pub const AZ_PREFIX: u8 = 48;
pub const RACK_PREFIX: u8 = 56;
pub const SLED_PREFIX: u8 = 64;

/// The amount of redundancy for DNS servers.
///
/// Must be less than MAX_DNS_REDUNDANCY.
pub const DNS_REDUNDANCY: usize = 1;
/// The maximum amount of redundancy for DNS servers.
///
/// This determines the number of addresses which are
/// reserved for DNS servers.
pub const MAX_DNS_REDUNDANCY: usize = 5;

pub const DNS_PORT: u16 = 53;
pub const DNS_SERVER_PORT: u16 = 5353;
pub const SLED_AGENT_PORT: u16 = 12345;

/// The port propolis-server listens on inside the propolis zone.
pub const PROPOLIS_PORT: u16 = 12400;
pub const COCKROACH_PORT: u16 = 32221;
pub const CLICKHOUSE_PORT: u16 = 8123;
pub const OXIMETER_PORT: u16 = 12223;

pub const NEXUS_INTERNAL_PORT: u16 = 12221;

// Anycast is a mechanism in which a single IP address is shared by multiple
// devices, and the destination is located based on routing distance.
//
// This is covered by RFC 4291 in much more detail:
// <https://datatracker.ietf.org/doc/html/rfc4291#section-2.6>
//
// Anycast addresses are always the "zeroeth" address within a subnet.  We
// always explicitly skip these addresses within our network.
const _ANYCAST_ADDRESS_INDEX: usize = 0;
const DNS_ADDRESS_INDEX: usize = 1;
const GZ_ADDRESS_INDEX: usize = 2;

/// The maximum number of addresses per sled reserved for RSS.
pub const RSS_RESERVED_ADDRESSES: u16 = 10;

/// Wraps an [`Ipv6Network`] with a compile-time prefix length.
#[derive(
    Debug, Clone, Copy, JsonSchema, Serialize, Deserialize, Hash, PartialEq, Eq,
)]
pub struct Ipv6Subnet<const N: u8> {
    net: Ipv6Net,
}

impl<const N: u8> Ipv6Subnet<N> {
    pub fn new(addr: Ipv6Addr) -> Self {
        // Create a network with the compile-time prefix length.
        let net = Ipv6Network::new(addr, N).unwrap();
        // Ensure the address is set to within-prefix only components.
        let net = Ipv6Network::new(net.network(), N).unwrap();
        Self { net: Ipv6Net(net) }
    }

    /// Returns the underlying network.
    pub fn net(&self) -> Ipv6Network {
        self.net.0
    }
}

/// Represents a subnet which may be used for contacting DNS services.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DnsSubnet {
    subnet: Ipv6Subnet<SLED_PREFIX>,
}

impl DnsSubnet {
    /// Returns the DNS server address within the subnet.
    ///
    /// This is the first address within the subnet.
    pub fn dns_address(&self) -> Ipv6Network {
        Ipv6Network::new(
            self.subnet.net().iter().nth(DNS_ADDRESS_INDEX).unwrap(),
            SLED_PREFIX,
        )
        .unwrap()
    }

    /// Returns the address which the Global Zone should create
    /// to be able to contact the DNS server.
    ///
    /// This is the second address within the subnet.
    pub fn gz_address(&self) -> Ipv6Network {
        Ipv6Network::new(
            self.subnet.net().iter().nth(GZ_ADDRESS_INDEX).unwrap(),
            SLED_PREFIX,
        )
        .unwrap()
    }
}

/// A wrapper around an IPv6 network, indicating it is a "reserved" rack
/// subnet which can be used for AZ-wide services.
#[derive(Debug, Clone)]
pub struct ReservedRackSubnet(pub Ipv6Subnet<RACK_PREFIX>);

impl ReservedRackSubnet {
    /// Returns the subnet for the reserved rack subnet.
    pub fn new(subnet: Ipv6Subnet<AZ_PREFIX>) -> Self {
        ReservedRackSubnet(Ipv6Subnet::<RACK_PREFIX>::new(subnet.net().ip()))
    }

    /// Returns the DNS addresses from this reserved rack subnet.
    ///
    /// These addresses will come from the first [`DNS_REDUNDANCY`] `/64s` of the
    /// [`RACK_PREFIX`] subnet.
    pub fn get_dns_subnets(&self) -> Vec<DnsSubnet> {
        (0..DNS_REDUNDANCY)
            .map(|idx| {
                let subnet =
                    get_64_subnet(self.0, u8::try_from(idx + 1).unwrap());
                DnsSubnet { subnet }
            })
            .collect()
    }
}

const SLED_AGENT_ADDRESS_INDEX: usize = 1;

/// Return the sled agent address for a subnet.
///
/// This address will come from the first address of the [`SLED_PREFIX`] subnet.
pub fn get_sled_address(sled_subnet: Ipv6Subnet<SLED_PREFIX>) -> SocketAddrV6 {
    let sled_agent_ip =
        sled_subnet.net().iter().nth(SLED_AGENT_ADDRESS_INDEX).unwrap();
    SocketAddrV6::new(sled_agent_ip, SLED_AGENT_PORT, 0, 0)
}

/// Returns a sled subnet within a rack subnet.
///
/// The subnet at index == 0 is used for rack-local services.
pub fn get_64_subnet(
    rack_subnet: Ipv6Subnet<RACK_PREFIX>,
    index: u8,
) -> Ipv6Subnet<SLED_PREFIX> {
    let mut rack_network = rack_subnet.net().network().octets();

    // To set bits distinguishing the /64 from the /56, we modify the 7th octet.
    rack_network[7] = index;
    Ipv6Subnet::<SLED_PREFIX>::new(Ipv6Addr::from(rack_network))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dns_subnets() {
        let subnet = Ipv6Subnet::<AZ_PREFIX>::new(
            "fd00:1122:3344:0100::".parse::<Ipv6Addr>().unwrap(),
        );
        let rack_subnet = ReservedRackSubnet::new(subnet);

        assert_eq!(
            //              Note that these bits (indicating the rack) are zero.
            //              vv
            "fd00:1122:3344:0000::/56".parse::<Ipv6Network>().unwrap(),
            rack_subnet.0.net(),
        );

        // Observe the first DNS subnet within this reserved rack subnet.
        let dns_subnets = rack_subnet.get_dns_subnets();
        assert_eq!(DNS_REDUNDANCY, dns_subnets.len());

        // The DNS address and GZ address should be only differing by one.
        assert_eq!(
            "fd00:1122:3344:0001::1/64".parse::<Ipv6Network>().unwrap(),
            dns_subnets[0].dns_address(),
        );
        assert_eq!(
            "fd00:1122:3344:0001::2/64".parse::<Ipv6Network>().unwrap(),
            dns_subnets[0].gz_address(),
        );
    }

    #[test]
    fn test_sled_address() {
        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(
            "fd00:1122:3344:0101::".parse::<Ipv6Addr>().unwrap(),
        );
        assert_eq!(
            "[fd00:1122:3344:0101::1]:12345".parse::<SocketAddrV6>().unwrap(),
            get_sled_address(subnet)
        );

        let subnet = Ipv6Subnet::<SLED_PREFIX>::new(
            "fd00:1122:3344:0308::".parse::<Ipv6Addr>().unwrap(),
        );
        assert_eq!(
            "[fd00:1122:3344:0308::1]:12345".parse::<SocketAddrV6>().unwrap(),
            get_sled_address(subnet)
        );
    }
}
