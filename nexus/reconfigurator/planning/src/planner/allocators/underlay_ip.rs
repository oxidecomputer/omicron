// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Very simple allocator for picking addresses from a sled's subnet

use ipnet::IpAdd;
use omicron_common::address::get_sled_address;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::CP_SERVICES_RESERVED_ADDRESSES;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::SLED_RESERVED_ADDRESSES;
use std::net::Ipv6Addr;

/// Very simple allocator for picking addresses from a sled's subnet
///
/// The current implementation takes the max address seen so far and uses the
/// next one.  This will never reuse old IPs.  That avoids a bunch of
/// operational issues.  It does mean we will eventually run out of IPs.  But we
/// do have a big space right now (2^16).
// This overlaps with the bump allocator that's used in RSS.  That one is not
// general enough to use here, though this one could potentially be used there.
#[derive(Debug)]
pub(crate) struct UnderlayIpAllocator {
    last: Ipv6Addr,
    maximum: Ipv6Addr,
}

impl UnderlayIpAllocator {
    /// Make an allocator that allocates addresses from the range `(minimum,
    /// maximum)` (exclusive).
    pub fn new(minimum: Ipv6Addr, maximum: Ipv6Addr) -> Self {
        Self { last: minimum, maximum }
    }

    /// Make an allocator that allocates addresses from the given sled's subnet,
    /// reserving any IPs.
    pub fn for_sled(
        sled_subnet: Ipv6Subnet<SLED_PREFIX>,
        reserved: impl Iterator<Item = Ipv6Addr>,
    ) -> Self {
        let sled_subnet_addr = sled_subnet.net().prefix();
        let minimum = sled_subnet_addr
            .saturating_add(u128::from(SLED_RESERVED_ADDRESSES));
        let maximum = sled_subnet_addr
            .saturating_add(u128::from(CP_SERVICES_RESERVED_ADDRESSES));
        assert!(sled_subnet.net().contains(minimum));
        assert!(sled_subnet.net().contains(maximum));
        let mut ip_allocator = Self::new(minimum, maximum);

        // We shouldn't need to explicitly reserve the sled's global
        // zone and switch addresses because they should be out of our
        // range, but we do so just to be sure.
        let sled_gz_addr = *get_sled_address(sled_subnet).ip();
        assert!(sled_subnet.net().contains(sled_gz_addr));
        assert!(minimum > sled_gz_addr);
        assert!(maximum > sled_gz_addr);
        let switch_zone_addr = get_switch_zone_address(sled_subnet);
        assert!(sled_subnet.net().contains(switch_zone_addr));
        assert!(minimum > switch_zone_addr);
        assert!(maximum > switch_zone_addr);

        // Record each of the sled's zones' underlay addresses as
        // allocated.
        for ip in reserved {
            ip_allocator.reserve(ip);
        }

        ip_allocator
    }

    /// Mark the given address reserved so that it will never be returned by
    /// `alloc()`.
    ///
    /// The given address can be outside the range provided to
    /// `IpAllocator::new()`, in which case this reservation will be ignored.
    pub fn reserve(&mut self, addr: Ipv6Addr) {
        if addr < self.maximum && addr > self.last {
            self.last = addr;
        }
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
    use super::UnderlayIpAllocator;
    use std::collections::BTreeSet;
    use std::net::Ipv6Addr;

    #[test]
    fn test_basic() {
        let range_start: Ipv6Addr = "fd00::d0".parse().unwrap();
        let range_end: Ipv6Addr = "fd00::e8".parse().unwrap();
        let reserved: BTreeSet<Ipv6Addr> = [
            // These first two are deliberately out of range.
            "fd00::ff".parse().unwrap(),
            "fd00::c0".parse().unwrap(),
            "fd00::d3".parse().unwrap(),
            "fd00::d7".parse().unwrap(),
        ]
        .iter()
        .copied()
        .collect();

        let mut allocator = UnderlayIpAllocator::new(range_start, range_end);
        for r in &reserved {
            allocator.reserve(*r);
        }

        let mut allocated = BTreeSet::new();
        while let Some(addr) = allocator.alloc() {
            println!("allocated: {}", addr);
            assert!(!reserved.contains(&addr));
            assert!(!allocated.contains(&addr));
            allocated.insert(addr);
        }

        assert_eq!(
            allocated,
            [
                // Because d7 was reserved, everything up to it is also skipped.
                // It doesn't have to work that way, but it currently does.
                "fd00::d8".parse().unwrap(),
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
            .iter()
            .copied()
            .collect()
        );
    }
}
