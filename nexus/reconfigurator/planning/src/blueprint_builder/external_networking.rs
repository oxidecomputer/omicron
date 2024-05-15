// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Error;
use anyhow::bail;
use debug_ignore::DebugIgnore;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::PlanningInput;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV6_SUBNET;
use omicron_common::api::external::IpNet;
use omicron_common::api::external::MacAddr;
use std::collections::HashSet;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;

#[derive(Debug)]
pub(super) struct BuilderExternalNetworking<'a> {
    // These fields mirror how RSS chooses addresses for zone NICs.
    nexus_v4_ips: AvailableIterator<'static, Ipv4Addr>,
    nexus_v6_ips: AvailableIterator<'static, Ipv6Addr>,

    // Iterator of available external IPs for service zones
    available_external_ips: AvailableIterator<'a, IpAddr>,

    // Iterator of available MAC addresses in the system address range
    available_system_macs: AvailableIterator<'a, MacAddr>,
}

impl<'a> BuilderExternalNetworking<'a> {
    pub(super) fn new(
        parent_blueprint: &'a Blueprint,
        input: &'a PlanningInput,
    ) -> anyhow::Result<Self> {
        // Scan through the parent blueprint and build several sets of "used
        // resources". When adding new control plane zones to a sled, we may
        // need to allocate new resources to that zone. However, allocation at
        // this point is entirely optimistic and theoretical: our caller may
        // discard the blueprint we create without ever making it the new
        // target, or it might be an arbitrarily long time before it becomes
        // the target. We need to be able to make allocation decisions that we
        // expect the blueprint executor to be able to realize successfully if
        // and when we become the target, but we cannot _actually_ perform
        // resource allocation.
        //
        // To do this, we look at our parent blueprint's used resources, and
        // then choose new resources that aren't already in use (if possible;
        // if we need to allocate a new resource and the parent blueprint
        // appears to be using all the resources of that kind, our blueprint
        // generation will fail).
        //
        // For example, RSS assigns Nexus NIC IPs by stepping through a list of
        // addresses based on `NEXUS_OPTE_IPVx_SUBNET` (as in the iterators
        // below). We use the same list of addresses, but additionally need to
        // filter out the existing IPs for any Nexus instances that already
        // exist.
        //
        // Note that by building these iterators up front based on
        // `parent_blueprint`, we cannot reuse resources in a case where we
        // remove a zone that used a resource and then add another zone that
        // wants the same kind of resource. That is mostly okay, but there are
        // some cases in which we may have to do that -- particularly external
        // DNS zones, which tend to have a small number of fixed IPs. Solving
        // that is a TODO.
        //
        // Also note that currently, we don't perform any kind of garbage
        // collection on sleds and zones that no longer have any attached
        // resources. Once a sled or zone is marked expunged, it will always
        // stay in that state.
        // https://github.com/oxidecomputer/omicron/issues/5552 tracks
        // implementing this kind of garbage collection, and we should do it
        // very soon.

        let mut existing_nexus_v4_ips: HashSet<Ipv4Addr> = HashSet::new();
        let mut existing_nexus_v6_ips: HashSet<Ipv6Addr> = HashSet::new();
        let mut used_external_ips: HashSet<IpAddr> = HashSet::new();
        let mut used_macs: HashSet<MacAddr> = HashSet::new();

        for (_, z) in
            parent_blueprint.all_omicron_zones(BlueprintZoneFilter::All)
        {
            let zone_type = &z.zone_type;
            if let BlueprintZoneType::Nexus(nexus) = zone_type {
                match nexus.nic.ip {
                    IpAddr::V4(ip) => {
                        if !existing_nexus_v4_ips.insert(ip) {
                            bail!("duplicate Nexus NIC IP: {ip}");
                        }
                    }
                    IpAddr::V6(ip) => {
                        if !existing_nexus_v6_ips.insert(ip) {
                            bail!("duplicate Nexus NIC IP: {ip}");
                        }
                    }
                }
            }

            if let Some((external_ip, nic)) = zone_type.external_networking() {
                // For the test suite, ignore localhost.  It gets reused many
                // times and that's okay.  We don't expect to see localhost
                // outside the test suite.
                if !external_ip.ip().is_loopback()
                    && !used_external_ips.insert(external_ip.ip())
                {
                    bail!("duplicate external IP: {external_ip:?}");
                }

                if !used_macs.insert(nic.mac) {
                    bail!("duplicate service vNIC MAC: {}", nic.mac);
                }
            }
        }

        // TODO-performance Building these iterators as "walk through the list
        // and skip anything we've used already" is fine as long as we're
        // talking about a small number of resources (e.g., single-digit number
        // of Nexus instances), but wouldn't be ideal if we have many resources
        // we need to skip. We could do something smarter here based on the sets
        // of used resources we built above if needed.
        let nexus_v4_ips = AvailableIterator::new(
            NEXUS_OPTE_IPV4_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_nexus_v4_ips,
        );
        let nexus_v6_ips = AvailableIterator::new(
            NEXUS_OPTE_IPV6_SUBNET
                .0
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_nexus_v6_ips,
        );
        let available_external_ips = AvailableIterator::new(
            input.service_ip_pool_ranges().iter().flat_map(|r| r.iter()),
            used_external_ips,
        );
        let available_system_macs =
            AvailableIterator::new(MacAddr::iter_system(), used_macs);

        Ok(Self {
            nexus_v4_ips,
            nexus_v6_ips,
            available_external_ips,
            available_system_macs,
        })
    }

    pub(super) fn for_new_nexus(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, Error> {
        let external_ip = self
            .available_external_ips
            .next()
            .ok_or(Error::NoExternalServiceIpAvailable)?;
        let (nic_ip, nic_subnet) = match external_ip {
            IpAddr::V4(_) => (
                self.nexus_v4_ips
                    .next()
                    .ok_or(Error::ExhaustedNexusIps)?
                    .into(),
                IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
            ),
            IpAddr::V6(_) => (
                self.nexus_v6_ips
                    .next()
                    .ok_or(Error::ExhaustedNexusIps)?
                    .into(),
                IpNet::from(*NEXUS_OPTE_IPV6_SUBNET),
            ),
        };
        let nic_mac = self
            .available_system_macs
            .next()
            .ok_or(Error::NoSystemMacAddressAvailable)?;

        Ok(ExternalNetworkingChoice {
            external_ip,
            nic_ip,
            nic_subnet,
            nic_mac,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalNetworkingChoice {
    pub(super) external_ip: IpAddr,
    pub(super) nic_ip: IpAddr,
    pub(super) nic_subnet: IpNet,
    pub(super) nic_mac: MacAddr,
}

/// Combines a base iterator with an `in_use` set, filtering out any elements
/// that are in the "in_use" set.
///
/// This can be done with a chained `.filter` on the iterator, but
/// `AvailableIterator` also allows for inspection of the `in_use` set.
///
/// Note that this is a stateful iterator -- i.e. it implements `Iterator`, not
/// `IntoIterator`. That's what we currently need in the planner.
#[derive(Debug)]
struct AvailableIterator<'a, T> {
    base: DebugIgnore<Box<dyn Iterator<Item = T> + Send + 'a>>,
    in_use: HashSet<T>,
}

impl<'a, T: Hash + Eq> AvailableIterator<'a, T> {
    /// Creates a new `AvailableIterator` from a base iterator and a set of
    /// elements that are in use.
    fn new<I>(base: I, in_use: impl IntoIterator<Item = T>) -> Self
    where
        I: Iterator<Item = T> + Send + 'a,
    {
        let in_use = in_use.into_iter().collect();
        AvailableIterator { base: DebugIgnore(Box::new(base)), in_use }
    }
}

impl<T: Hash + Eq> Iterator for AvailableIterator<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.base.find(|item| !self.in_use.contains(item))
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use test_strategy::proptest;

    /// Test that `AvailableIterator` correctly filters out items that are in
    /// use.
    #[proptest]
    fn test_available_iterator(items: HashSet<(i32, bool)>) {
        let mut in_use_map = HashSet::new();
        let mut expected_available = Vec::new();
        let items: Vec<_> = items
            .into_iter()
            .map(|(item, in_use)| {
                if in_use {
                    in_use_map.insert(item);
                } else {
                    expected_available.push(item);
                }
                item
            })
            .collect();

        let available = AvailableIterator::new(items.into_iter(), in_use_map);
        let actual_available = available.collect::<Vec<_>>();

        assert_eq!(
            expected_available, actual_available,
            "available items match"
        );
    }
}
