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
    subnet: ipnetwork::Ipv6Network,
    max_seen: Ipv6Addr,
}

impl IpAllocator {
    pub fn new(subnet: ipnetwork::Ipv6Network) -> IpAllocator {
        IpAllocator {
            subnet,
            max_seen: subnet
                .iter()
                .next()
                .expect("expected at least one address in the subnet"),
        }
    }

    /// Mark the given address reserved so that it will never be returned by
    /// `alloc()`
    pub fn reserve(&mut self, addr: Ipv6Addr) {
        assert!(
            self.subnet.contains(addr),
            "attempted to reserve IP {} which is outside \
            the current subnet {:?}",
            addr,
            self.subnet
        );
        if addr > self.max_seen {
            self.max_seen = addr;
        }
    }

    /// Allocate an unused address from this allocator's subnet
    pub fn alloc(&mut self) -> Option<Ipv6Addr> {
        let next = self.max_seen.saturating_add(1);
        if next == self.max_seen {
            // We ran out of the entire IPv6 address space.
            return None;
        }

        if !self.subnet.contains(next) {
            // We ran out of this subnet.
            return None;
        }

        self.max_seen = next;
        Some(next)
    }
}
