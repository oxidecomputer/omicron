// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::planner::blueprint_zones_editor::BlueprintZonesEditor;
use nexus_types::deployment::blueprint_zone_type::InternalDns;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use omicron_common::address::DnsSubnet;
use omicron_common::address::ReservedRackSubnet;
use std::collections::BTreeSet;

/// Internal DNS zones are not allocated an address in the sled's subnet.
/// Instead, they get a /64 subnet of the "reserved" rack subnet (so that
/// it's routable with IPv6), and use the first address in that. There may
/// be at most `INTERNAL_DNS_REDUNDANCY` subnets (and so servers)
/// allocated. This structure tracks which subnets are currently allocated.
#[derive(Debug)]
pub(crate) struct DnsSubnetAllocator {
    in_use: BTreeSet<DnsSubnet>,
}

impl DnsSubnetAllocator {
    pub fn new(zones_editor: &BlueprintZonesEditor) -> Self {
        let in_use = zones_editor
            .current_zones(BlueprintZoneFilter::ShouldBeRunning)
            .filter_map(|(_, zone_config)| match zone_config.zone_type {
                BlueprintZoneType::InternalDns(InternalDns {
                    dns_address,
                    ..
                }) => Some(DnsSubnet::from_addr(*dns_address.ip())),
                _ => None,
            })
            .collect::<BTreeSet<DnsSubnet>>();

        Self { in_use }
    }

    /// Allocate the first available DNS subnet, or call a function to generate
    /// a default. The default is needed because we can't necessarily guess the
    /// correct reserved rack subnet (e.g., there might not be any internal DNS
    /// zones in the parent blueprint, though that would itself be odd), but we
    /// can derive it at runtime from the sled address.
    pub fn alloc(
        &mut self,
        rack_subnet: ReservedRackSubnet,
    ) -> Option<DnsSubnet> {
        let new = if let Some(first) = self.in_use.first() {
            // Take the first available DNS subnet. We currently generate
            // all `INTERNAL_DNS_REDUNDANCY` subnets and subtract any
            // that are in use; this is fine as long as that constant is small.
            let subnets = BTreeSet::from_iter(
                ReservedRackSubnet::from_subnet(first.subnet())
                    .get_dns_subnets(),
            );
            let mut avail = subnets.difference(&self.in_use);
            if let Some(first) = avail.next() {
                *first
            } else {
                return None;
            }
        } else {
            rack_subnet.get_dns_subnet(1)
        };
        self.in_use.insert(new);
        Some(new)
    }

    #[cfg(test)]
    fn first(&self) -> Option<DnsSubnet> {
        self.in_use.first().copied()
    }

    #[cfg(test)]
    fn pop_first(&mut self) -> Option<DnsSubnet> {
        self.in_use.pop_first()
    }

    #[cfg(test)]
    fn last(&self) -> Option<DnsSubnet> {
        self.in_use.last().cloned()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.in_use.len()
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::blueprint_builder::test::verify_blueprint;
    use crate::example::ExampleSystemBuilder;
    use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
    use omicron_test_utils::dev::test_setup_log;

    #[test]
    fn test_dns_subnet_allocator() {
        static TEST_NAME: &str = "test_dns_subnet_allocator";
        let logctx = test_setup_log(TEST_NAME);

        // Our test below assumes `INTERNAL_DNS_REDUNDANCY` is greater than 1
        // (so we can test adding more); fail fast if that changes.
        assert!(INTERNAL_DNS_REDUNDANCY > 1);

        // Use our example system to create a blueprint and input.
        let (example, mut blueprint1) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .nsleds(INTERNAL_DNS_REDUNDANCY)
                .build();

        // `ExampleSystem` adds an internal DNS server to every sled. Manually
        // prune out all but the first of them to give us space to add more.
        for (_, zone_config) in blueprint1.blueprint_zones.iter_mut().skip(1) {
            zone_config.zones.retain(|z| !z.zone_type.is_internal_dns());
        }
        let npruned = blueprint1.blueprint_zones.len() - 1;
        assert!(npruned > 0);

        verify_blueprint(&blueprint1);

        // Create an allocator.
        let mut allocator = DnsSubnetAllocator::new(
            &BlueprintZonesEditor::new(blueprint1.blueprint_zones),
        );

        // There should be exactly one subnet allocated.
        assert_eq!(allocator.len(), 1, "should be 1 subnets allocated");
        let first = allocator.first().expect("should be a first subnet");
        let mut last = allocator.last().expect("should be a last subnet");
        assert_eq!(first, last);
        let rack_subnet = first.rack_subnet();

        // Allocate `npruned` new subnets.
        for _ in 0..npruned {
            let new = allocator.alloc(rack_subnet).expect("allocated a subnet");
            assert!(
                new > last,
                "newly allocated subnets should be after prior ones"
            );
            assert_eq!(
                new.rack_subnet(),
                last.rack_subnet(),
                "newly allocated subnets should be in the same rack subnet"
            );
            last = new;
        }
        assert_eq!(
            allocator.len(),
            INTERNAL_DNS_REDUNDANCY,
            "should be {INTERNAL_DNS_REDUNDANCY} subnets allocated"
        );
        assert!(allocator.alloc(rack_subnet).is_none());

        // Test packing.
        let first = allocator.pop_first().expect("should be a first subnet");
        let second = allocator.pop_first().expect("should be a second subnet");
        assert!(first < second, "first should be before second");
        assert_eq!(
            allocator.alloc(rack_subnet).expect("allocation failed"),
            first,
            "should get first subnet"
        );
        assert_eq!(
            allocator.alloc(rack_subnet).expect("allocation failed"),
            second,
            "should get second subnet"
        );
        assert!(allocator.alloc(rack_subnet).is_none());

        // Done!
        logctx.cleanup_successful();
    }
}
