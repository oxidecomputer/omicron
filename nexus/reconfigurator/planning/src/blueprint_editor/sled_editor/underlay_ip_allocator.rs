// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use ipnet::IpAdd;
use nexus_sled_agent_shared::inventory::ZoneKind;
use omicron_common::address::get_sled_address;
use omicron_common::address::get_switch_zone_address;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::CP_SERVICES_RESERVED_ADDRESSES;
use omicron_common::address::SLED_PREFIX;
use omicron_common::address::SLED_RESERVED_ADDRESSES;
use std::net::Ipv6Addr;

#[derive(Debug, thiserror::Error)]
#[error(
    "{zone_kind:?} zone IP {ip} not in sled underlay range ({low}..={high})"
)]
pub struct SledUnderlayIpOutOfRange {
    pub zone_kind: ZoneKind,
    pub ip: Ipv6Addr,
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
    minimum: Ipv6Addr,
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
        in_use_zone_ips: I,
    ) -> Result<Self, SledUnderlayIpOutOfRange>
    where
        I: Iterator<Item = (ZoneKind, Ipv6Addr)>,
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

        let mut slf = Self { minimum, last: minimum, maximum };
        for (zone_kind, ip) in in_use_zone_ips {
            slf.mark_as_allocated(zone_kind, ip)?;
        }
        assert!(slf.minimum <= slf.last);
        assert!(slf.last <= slf.maximum);

        Ok(slf)
    }

    /// Mark an address as used.
    ///
    /// Marking an address that has already been handed out by this allocator
    /// (or could have been handed out by this allocator) is allowed and does
    /// nothing.
    ///
    /// # Errors
    ///
    /// Fails if `ip` is outside the subnet of this sled.
    pub fn mark_as_allocated(
        &mut self,
        zone_kind: ZoneKind,
        ip: Ipv6Addr,
    ) -> Result<(), SledUnderlayIpOutOfRange> {
        // We intentionally ignore any internal DNS underlay IPs; they live
        // outside the sled subnet and are allocated separately.
        if zone_kind == ZoneKind::InternalDns {
            return Ok(());
        }

        if ip < self.minimum || ip > self.maximum {
            return Err(SledUnderlayIpOutOfRange {
                zone_kind,
                ip,
                low: self.minimum,
                high: self.maximum,
            });
        }

        self.last = Ipv6Addr::max(self.last, ip);

        Ok(())
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
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn test_basic() {
        let sled_subnet = Ipv6Subnet::new("fd00::d0".parse().unwrap());
        let reserved: Vec<(ZoneKind, Ipv6Addr)> = vec![
            (ZoneKind::Nexus, "fd00::50".parse().unwrap()),
            (ZoneKind::Nexus, "fd00::d3".parse().unwrap()),
            (ZoneKind::Nexus, "fd00::d7".parse().unwrap()),
        ];
        let reserved_ips =
            reserved.iter().map(|(_, ip)| *ip).collect::<BTreeSet<_>>();

        let mut allocator =
            SledUnderlayIpAllocator::new(sled_subnet, reserved.iter().copied())
                .expect("created allocator");

        let mut allocated = Vec::new();
        for _ in 0..16 {
            let addr = allocator.alloc().expect("allocated IP");
            println!("allocated: {addr}");
            assert!(!reserved_ips.contains(&addr));
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
        let reserved_low: Vec<(ZoneKind, Ipv6Addr)> = vec![
            // ok
            (ZoneKind::Nexus, "fd00::d3".parse().unwrap()),
            // too low; first 32 addrs are reserved
            (ZoneKind::Nexus, "fd00::1f".parse().unwrap()),
        ];
        let err = SledUnderlayIpAllocator::new(
            sled_subnet,
            reserved_low.iter().copied(),
        )
        .expect_err("failed to create allocator");
        assert_eq!(err.ip, reserved_low[1].1);

        let reserved_high: Vec<(ZoneKind, Ipv6Addr)> = vec![
            // ok
            (ZoneKind::Nexus, "fd00::d3".parse().unwrap()),
            // too high; above sled /64
            (ZoneKind::Nexus, "fd00:0000:0000:0001::1ff".parse().unwrap()),
        ];
        let err = SledUnderlayIpAllocator::new(
            sled_subnet,
            reserved_high.iter().copied(),
        )
        .expect_err("failed to create allocator");
        assert_eq!(err.ip, reserved_high[1].1);
    }
}
