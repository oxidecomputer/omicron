// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Error;
use nexus_types::deployment::blueprint_zone_type::InternalDns;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::PlanningInput;
use omicron_common::address::DnsSubnet;
use omicron_common::address::ReservedRackSubnet;
use omicron_common::policy::MAX_DNS_REDUNDANCY;
use std::collections::BTreeSet;

/// Internal DNS zones are not allocated an address in the sled's subnet.
/// Instead, they get a /64 subnet of the "reserved" rack subnet (so that
/// it's routable with IPv6), and use the first address in that. There may
/// be at most `MAX_DNS_REDUNDANCY` subnets (and so servers) allocated.
/// This structure tracks which subnets are currently allocated.
#[derive(Debug)]
pub struct DnsSubnetAllocator {
    in_use: BTreeSet<DnsSubnet>,
}

impl DnsSubnetAllocator {
    pub fn new<'a>(
        parent_blueprint: &'a Blueprint,
        input: &'a PlanningInput,
    ) -> Result<Self, Error> {
        let in_use = parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|(_sled_id, zone_config)| match zone_config.zone_type {
                BlueprintZoneType::InternalDns(InternalDns {
                    dns_address,
                    ..
                }) => Some(DnsSubnet::from(*dns_address.ip())),
                _ => None,
            })
            .collect::<BTreeSet<DnsSubnet>>();

        let redundancy = input.target_internal_dns_zone_count();
        if redundancy > MAX_DNS_REDUNDANCY {
            return Err(Error::TooManyDnsServers);
        }

        Ok(Self { in_use })
    }

    /// Allocate the first available DNS subnet, or call a function to generate
    /// a default. The default is needed because we can't necessarily guess the
    /// correct reserved rack subnet (e.g., there might not be any internal DNS
    /// zones in the parent blueprint, though that would itself be odd), but we
    /// can derive it at runtime from the sled address.
    pub fn alloc_or_else<F>(&mut self, default: F) -> Result<DnsSubnet, Error>
    where
        F: FnOnce() -> DnsSubnet,
    {
        let new = if let Some(first) = self.in_use.first() {
            // Take the first available DNS subnet. We currently generate
            // all `MAX_DNS_REDUNDANCY` subnets and subtract any that are
            // in use; this is fine as long as that constant is small.
            let subnet = first.subnet();
            let subnets = ReservedRackSubnet::from(subnet).get_dns_subnets();
            let subnets = BTreeSet::from_iter(subnets);
            let mut avail = subnets.difference(&self.in_use);
            if let Some(first) = avail.next() {
                first.clone()
            } else {
                return Err(Error::NoAvailableDnsSubnets);
            }
        } else {
            default()
        };
        self.in_use.insert(new.clone());
        Ok(new)
    }

    #[cfg(test)]
    fn first(&self) -> Option<DnsSubnet> {
        self.in_use.first().cloned()
    }

    #[cfg(test)]
    fn last(&self) -> Option<DnsSubnet> {
        self.in_use.last().cloned()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.in_use.len()
    }

    #[cfg(test)]
    fn clear(&mut self) {
        self.in_use.clear()
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::blueprint_builder::test::verify_blueprint;
    use crate::example::ExampleSystem;
    use omicron_common::policy::{DNS_REDUNDANCY, MAX_DNS_REDUNDANCY};
    use omicron_test_utils::dev::test_setup_log;

    #[test]
    fn test_dns_subnet_allocator() {
        static TEST_NAME: &str = "test_dns_subnet_allocator";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system to create a blueprint and input.
        let example =
            ExampleSystem::new(&logctx.log, TEST_NAME, DNS_REDUNDANCY);
        let blueprint1 = &example.blueprint;
        verify_blueprint(blueprint1);

        // Create an allocator.
        let mut allocator = DnsSubnetAllocator::new(blueprint1, &example.input)
            .expect("can't create allocator");

        // Save the first & last allocated subnets.
        let first = allocator.first().expect("should be a first subnet");
        let last = allocator.last().expect("should be a last subnet");
        assert!(last > first, "first should come before last");

        // Allocate two new subnets.
        assert_eq!(MAX_DNS_REDUNDANCY - DNS_REDUNDANCY, 2);
        assert_eq!(
            allocator.len(),
            DNS_REDUNDANCY,
            "should be {DNS_REDUNDANCY} subnets allocated"
        );
        let new1 = allocator
            .alloc_or_else(|| panic!("shouldn't need a default"))
            .expect("failed to allocate a subnet");
        let new2 = allocator
            .alloc_or_else(|| panic!("shouldn't need a default"))
            .expect("failed to allocate a subnet");
        assert!(
            new1 > last,
            "newly allocated subnets should be after initial ones"
        );
        assert!(new2 > new1, "allocated subnets out of order");
        assert_ne!(new1, new2, "allocated duplicate subnets");
        assert_eq!(
            allocator.len(),
            MAX_DNS_REDUNDANCY,
            "should be {DNS_REDUNDANCY} subnets allocated"
        );
        allocator
            .alloc_or_else(|| panic!("shouldn't need a default"))
            .expect_err("no subnets available");

        // Clear and test default.
        allocator.clear();
        let default = allocator
            .alloc_or_else(|| new1.clone())
            .expect("failed to alloc default");
        assert_eq!(default, new1, "should default");

        // Test packing.
        allocator.clear();
        assert!(first < new2);
        assert_eq!(
            allocator
                .alloc_or_else(|| new2.clone())
                .expect("failed to alloc default"),
            new2,
            "should default"
        );
        assert_eq!(
            allocator
                .alloc_or_else(|| panic!("shouldn't need a default"))
                .expect("allocation failed"),
            first,
            "should be first subnet"
        );
        assert!(
            allocator
                .alloc_or_else(|| panic!("shouldn't need a default"))
                .expect("allocation failed")
                > first,
            "should be after first subnet"
        );

        // Done!
        logctx.cleanup_successful();
    }
}
