// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common IP addressing functionality.
//!
//! This addressing functionality is shared by both initialization services
//! and Nexus, who need to agree upon addressing schemes.

use ipnetwork::Ipv6Network;
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

/// Represents a subnet which may be used for contacting DNS services.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DnsSubnet {
    network: Ipv6Network,
}

impl DnsSubnet {
    /// Returns the DNS server address within the subnet.
    ///
    /// This is the first address within the subnet.
    pub fn dns_address(&self) -> Ipv6Network {
        let mut iter = self.network.iter();
        let _anycast_ip = iter.next().unwrap();
        let dns_ip = iter.next().unwrap();
        Ipv6Network::new(dns_ip, SLED_PREFIX).unwrap()
    }

    /// Returns the address which the Global Zone should create
    /// to be able to contact the DNS server.
    ///
    /// This is the second address within the subnet.
    pub fn gz_address(&self) -> Ipv6Network {
        let mut iter = self.network.iter();
        let _anycast_ip = iter.next().unwrap();
        let _dns_ip = iter.next().unwrap();
        Ipv6Network::new(iter.next().unwrap(), SLED_PREFIX).unwrap()
    }
}

/// A wrapper around an IPv6 network, indicating it is a "reserved" rack
/// subnet which can be used for AZ-wide services.
#[derive(Debug, Clone)]
pub struct ReservedRackSubnet(pub Ipv6Network);

impl ReservedRackSubnet {
    /// Returns the subnet for the reserved rack subnet.
    pub fn new(subnet: Ipv6Network) -> Self {
        let net = Ipv6Network::new(subnet.network(), AZ_PREFIX).unwrap();
        ReservedRackSubnet(
            Ipv6Network::new(net.network(), RACK_PREFIX).unwrap(),
        )
    }

    /// Given a particular rack subnet, return the DNS addresses.
    ///
    /// These addresses will come from the first [`DNS_REDUNDANCY`] `/64s` of the
    /// [`RACK_PREFIX`] subnet.
    pub fn get_dns_subnets(&self) -> Vec<DnsSubnet> {
        assert_eq!(self.0.prefix(), RACK_PREFIX);

        (0..DNS_REDUNDANCY)
            .map(|idx| {
                let network =
                    get_64_subnet(self.0, u8::try_from(idx + 1).unwrap());

                DnsSubnet { network }
            })
            .collect()
    }
}

/// Return the sled agent address for a subnet.
///
/// This address will come from the first address of the [`SLED_PREFIX`] subnet.
pub fn get_sled_address(sled_subnet: Ipv6Network) -> SocketAddrV6 {
    assert_eq!(sled_subnet.prefix(), SLED_PREFIX);

    let mut iter = sled_subnet.iter();
    let _anycast_ip = iter.next().unwrap();
    let sled_agent_ip = iter.next().unwrap();
    SocketAddrV6::new(sled_agent_ip, SLED_AGENT_PORT, 0, 0)
}

/// Returns a sled subnet within a rack subnet.
///
/// The subnet at index == 0 is used for rack-local services.
pub fn get_64_subnet(rack_subnet: Ipv6Network, index: u8) -> Ipv6Network {
    assert_eq!(rack_subnet.prefix(), RACK_PREFIX);

    let mut rack_network = rack_subnet.network().octets();

    // To set bits distinguishing the /64 from the /56, we modify the 7th octet.
    rack_network[7] = index;
    Ipv6Network::new(Ipv6Addr::from(rack_network), 64).unwrap()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_dns_subnets() {
        let subnet = "fd00:1122:3344:0100::/64".parse::<Ipv6Network>().unwrap();
        let rack_subnet = ReservedRackSubnet::new(subnet);

        assert_eq!(
            //              Note that these bits (indicating the rack) are zero.
            //              vv
            "fd00:1122:3344:0000::/56".parse::<Ipv6Network>().unwrap(),
            rack_subnet.0,
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
        let subnet = "fd00:1122:3344:0101::/64".parse::<Ipv6Network>().unwrap();
        assert_eq!(
            "[fd00:1122:3344:0101::1]:12345".parse::<SocketAddrV6>().unwrap(),
            get_sled_address(subnet)
        );

        let subnet = "fd00:1122:3344:0308::/64".parse::<Ipv6Network>().unwrap();
        assert_eq!(
            "[fd00:1122:3344:0308::1]:12345".parse::<SocketAddrV6>().unwrap(),
            get_sled_address(subnet)
        );
    }
}
