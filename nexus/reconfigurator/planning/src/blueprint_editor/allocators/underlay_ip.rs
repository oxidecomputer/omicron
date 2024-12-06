// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use ipnet::IpAdd;
use omicron_common::address::get_sled_address;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::CP_SERVICES_RESERVED_ADDRESSES;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::SLED_RESERVED_ADDRESSES;
use std::net::Ipv6Addr;

#[derive(Debug, thiserror::Error)]
#[error("reserved IP {reserved} not in sled underlay range ({low}..={high})")]
pub struct SledUnderlayIpError {
    pub reserved: Ipv6Addr,
    pub low: Ipv6Addr,
    pub high: Ipv6Addr,
}

/// Very simple allocator for picking addresses from a sled's subnet
///
/// The current implementation takes the max address seen so far and uses the
/// next one.  This will never reuse old IPs.  That avoids a bunch of
/// operational issues.  It does mean we will eventually run out of IPs.  But we
/// do have a big space right now (2^16).
// This overlaps with the bump allocator that's used in RSS.  That one is not
// general enough to use here, though this one could potentially be used there.
#[derive(Debug)]
pub(crate) struct SledUnderlayIpAllocator {
    last: Ipv6Addr,
    maximum: Ipv6Addr,
}

impl SledUnderlayIpAllocator {
    /// Create a new allocator for the given sled subnet that reserves all the
    /// specified IPs.
    ///
    /// Fails if any of the specified IPs are not part of the sled subnet.
    pub fn new<I>(
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
        reserved_ips: I,
    ) -> Result<Self, SledUnderlayIpError>
    where
        I: Iterator<Item = Ipv6Addr>,
    {
        let sled_subnet_addr = sled_subnet.net().prefix();
        let minimum = sled_subnet_addr
            .saturating_add(u128::from(SLED_RESERVED_ADDRESSES));
        let maximum = sled_subnet_addr
            .saturating_add(u128::from(CP_SERVICES_RESERVED_ADDRESSES));
        assert!(maximum > minimum);

        // We shouldn't need to explicitly reserve the sled's global
        // zone and switch addresses because they should be out of our
        // range, but we do so just to be sure.
        let sled_gz_addr = *get_sled_address(sled_subnet).ip();
        assert!(sled_subnet.net().contains(sled_gz_addr));
        assert!(minimum > sled_gz_addr);
        let switch_zone_addr = get_switch_zone_address(sled_subnet);
        assert!(sled_subnet.net().contains(switch_zone_addr));
        assert!(minimum > switch_zone_addr);
        assert!(sled_subnet.net().contains(minimum));
        assert!(sled_subnet.net().contains(maximum));

        let mut last = minimum;
        for reserved_ip in reserved_ips {
            if reserved_ip < minimum || reserved_ip > maximum {
                return Err(SledUnderlayIpError {
                    reserved: reserved_ip,
                    low: minimum,
                    high: maximum,
                });
            }
            last = Ipv6Addr::max(last, reserved_ip);
        }
        assert!(minimum <= last);
        assert!(last <= maximum);

        Ok(Self { last, maximum })
    }

    /// Allocate an unused address from this allocator's range
    pub fn alloc(&mut self) -> Option<Ipv6Addr> {
        let next = self.last.saturating_add(1);
        if next == self.last {
            // We ran out of the entire IPv6 address space.
            return None;
        }

        if next >= self.maximum {
            // We ran out of our allotted range.
            return None;
        }

        self.last = next;
        Some(next)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic() {
        let sled_subnet = Ipv6Subnet::new("fd00::d0".parse().unwrap());
        let reserved: Vec<Ipv6Addr> = vec![
            "fd00::50".parse().unwrap(),
            "fd00::d3".parse().unwrap(),
            "fd00::d7".parse().unwrap(),
        ];

        let mut allocator =
            SledUnderlayIpAllocator::new(sled_subnet, reserved.iter().copied())
                .expect("created allocator");

        let mut allocated = Vec::new();
        for _ in 0..16 {
            let addr = allocator.alloc().expect("allocated IP");
            println!("allocated: {addr}");
            assert!(!reserved.contains(&addr));
            assert!(!allocated.contains(&addr));
            allocated.push(addr);
        }

        assert_eq!(
            allocated,
            [
                // Because fd00::d7 was reserved, everything up to it is also
                // skipped. It doesn't have to work that way, but it currently
                // does.
                "fd00::d8".parse::<Ipv6Addr>().unwrap(),
                "fd00::d9".parse().unwrap(),
                "fd00::da".parse().unwrap(),
                "fd00::db".parse().unwrap(),
                "fd00::dc".parse().unwrap(),
                "fd00::dd".parse().unwrap(),
                "fd00::de".parse().unwrap(),
                "fd00::df".parse().unwrap(),
                "fd00::e0".parse().unwrap(),
                "fd00::e1".parse().unwrap(),
                "fd00::e2".parse().unwrap(),
                "fd00::e3".parse().unwrap(),
                "fd00::e4".parse().unwrap(),
                "fd00::e5".parse().unwrap(),
                "fd00::e6".parse().unwrap(),
                "fd00::e7".parse().unwrap(),
            ]
            .to_vec()
        );
    }

    #[test]
    fn test_reject_out_of_range_reserved_ips() {
        let sled_subnet = Ipv6Subnet::new("fd00::d0".parse().unwrap());
        let reserved_low: Vec<Ipv6Addr> = vec![
            // ok
            "fd00::d3".parse().unwrap(),
            // too low; first 32 addrs are reserved
            "fd00::1f".parse().unwrap(),
        ];
        let err = SledUnderlayIpAllocator::new(
            sled_subnet,
            reserved_low.iter().copied(),
        )
        .expect_err("failed to create allocator");
        assert_eq!(err.reserved, reserved_low[1]);

        let reserved_high: Vec<Ipv6Addr> = vec![
            // ok
            "fd00::d3".parse().unwrap(),
            // too high; above sled /64
            "fd00:0000:0000:0001::1ff".parse().unwrap(),
        ];
        let err = SledUnderlayIpAllocator::new(
            sled_subnet,
            reserved_high.iter().copied(),
        )
        .expect_err("failed to create allocator");
        assert_eq!(err.reserved, reserved_high[1]);
    }
}
