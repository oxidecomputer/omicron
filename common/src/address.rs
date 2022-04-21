// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common IP addressing functionality.
//!
//! This addressing functionality is shared by both initialization services
//! and Nexus, who need to agree upon addressing schemes.

use std::net::{Ipv6Addr, SocketAddrV6};
use serde::{Serialize, Deserialize};
use ipnetwork::Ipv6Network;

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
    pub fn dns_address(&self) -> SocketAddrV6 {
        let mut iter = self.network.iter();
        let _anycast_ip = iter.next().unwrap();
        let dns_ip = iter.next().unwrap();
        SocketAddrV6::new(dns_ip, DNS_SERVER_PORT, 0, 0)
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

/// Given a particular rack subnet, return the DNS addresses.
///
/// These addresses will come from the first [`DNS_REDUNDANCY`] `/64s` of the
/// [`RACK_PREFIX`] subnet.
pub fn get_dns_subnets(reserved_rack_subnet: Ipv6Network) -> Vec<DnsSubnet> {
    assert_eq!(reserved_rack_subnet.prefix(), RACK_PREFIX);

    let mut iter = reserved_rack_subnet.iter();
    let _anycast_ip = iter.next().unwrap();

    (0..DNS_REDUNDANCY).map(|idx| {
        let network = get_64_subnet(
            reserved_rack_subnet,
            u8::try_from(idx + 1).unwrap()
        );

        DnsSubnet {
            network
        }
    }).collect()
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
