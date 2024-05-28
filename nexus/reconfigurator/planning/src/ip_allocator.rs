// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Very simple allocator for picking addresses from a sled's subnet

use ipnet::IpAdd;
use std::net::Ipv6Addr;

/// Very simple allocator for picking addresses from a sled's subnet
///
/// The current implementation takes the max address seen so far and uses the
/// next one.  This will never reuse old IPs.  That avoids a bunch of
/// operational issues.  It does mean we will eventually run out of IPs.  But we
/// do have a big space right now (2^16).
// This overlaps with the bump allocator that's used in RSS.  That one is not
// general enough to use here, though this one could potentially be used there.
pub struct IpAllocator {
    last: Ipv6Addr,
    maximum: Ipv6Addr,
}

impl IpAllocator {
    /// Make an allocator that allocates addresses from the range `(minimum,
    /// maximum)` (exclusive).
    pub fn new(minimum: Ipv6Addr, maximum: Ipv6Addr) -> IpAllocator {
        IpAllocator { last: minimum, maximum }
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
    use super::IpAllocator;
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

        let mut allocator = IpAllocator::new(range_start, range_end);
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
