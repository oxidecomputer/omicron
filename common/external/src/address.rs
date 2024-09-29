// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::net::{IpAddr, Ipv6Addr};

pub const AZ_PREFIX: u8 = 48;
pub const RACK_PREFIX: u8 = 56;
pub const SLED_PREFIX: u8 = 64;

pub const DNS_PORT: u16 = 53;

pub const INTERNAL_DNS_REDUNDANCY: usize = 3;
const DNS_ADDRESS_INDEX: usize = 1;

// Mask an address, given a subnet prefix length
const fn ipv6_prefix_masked<const N: u8>(addr: Ipv6Addr) -> Ipv6Addr {
    if let Some(mask) = u128::MAX.checked_shl((128 - N) as u32) {
        Ipv6Addr::from_bits(addr.to_bits() & mask)
    } else {
        panic!("invalid prefix length");
    }
}

/// Return the list of DNS servers for the rack, given any address in the AZ
/// subnet
//
// Note: This is effectively a clone of get_internal_dns_server_addresses in
// omicron_common::address.  The tests there should verify that this
// implementation is kept in sync.
pub fn get_internal_dns_server_addresses(addr: Ipv6Addr) -> Vec<IpAddr> {
    let az_net = ipv6_prefix_masked::<AZ_PREFIX>(addr);
    let reserved_rack_net = ipv6_prefix_masked::<RACK_PREFIX>(az_net);
    let sled_net = ipv6_prefix_masked::<SLED_PREFIX>(reserved_rack_net);

    (0..INTERNAL_DNS_REDUNDANCY)
        .map(|idx| {
            let mut dns_net = sled_net.octets();
            // sled index lives in 7th octet
            dns_net[7] = (idx + 1) as u8;
            // DNS is first address in that net
            dns_net[15] = DNS_ADDRESS_INDEX as u8;
            Ipv6Addr::from(dns_net).into()
        })
        .collect()
}
