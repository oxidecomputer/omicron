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
}
