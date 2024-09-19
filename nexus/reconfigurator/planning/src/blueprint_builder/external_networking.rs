// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::Error;
use anyhow::bail;
use debug_ignore::DebugIgnore;
use nexus_config::NUM_INITIAL_RESERVED_IP_ADDRESSES;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::OmicronZoneExternalIp;
use nexus_types::deployment::PlanningInput;
use nexus_types::inventory::SourceNatConfig;
use omicron_common::address::IpRange;
use omicron_common::address::DNS_OPTE_IPV4_SUBNET;
use omicron_common::address::DNS_OPTE_IPV6_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV4_SUBNET;
use omicron_common::address::NEXUS_OPTE_IPV6_SUBNET;
use omicron_common::address::NTP_OPTE_IPV4_SUBNET;
use omicron_common::address::NTP_OPTE_IPV6_SUBNET;
use omicron_common::address::NUM_SOURCE_NAT_PORTS;
use omicron_common::api::external::MacAddr;
use omicron_common::api::internal::shared::SourceNatConfigError;
use oxnet::IpNet;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::Ipv6Addr;
use strum::IntoEnumIterator as _;

#[derive(Debug)]
pub(super) struct BuilderExternalNetworking<'a> {
    // These fields mirror how RSS chooses addresses for zone NICs.
    boundary_ntp_v4_ips: AvailableIterator<'static, Ipv4Addr>,
    boundary_ntp_v6_ips: AvailableIterator<'static, Ipv6Addr>,
    nexus_v4_ips: AvailableIterator<'static, Ipv4Addr>,
    nexus_v6_ips: AvailableIterator<'static, Ipv6Addr>,
    external_dns_v4_ips: AvailableIterator<'static, Ipv4Addr>,
    external_dns_v6_ips: AvailableIterator<'static, Ipv6Addr>,

    // External DNS server addresses currently only come from RSS;
    // see https://github.com/oxidecomputer/omicron/issues/3732
    available_external_dns_ips: BTreeSet<IpAddr>,

    // Allocator for external IPs for service zones
    external_ip_alloc: ExternalIpAllocator<'a>,

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
        let mut existing_boundary_ntp_v4_ips: HashSet<Ipv4Addr> =
            HashSet::new();
        let mut existing_boundary_ntp_v6_ips: HashSet<Ipv6Addr> =
            HashSet::new();
        let mut existing_external_dns_v4_ips: HashSet<Ipv4Addr> =
            HashSet::new();
        let mut existing_external_dns_v6_ips: HashSet<Ipv6Addr> =
            HashSet::new();
        let mut external_ip_alloc =
            ExternalIpAllocator::new(input.service_ip_pool_ranges());
        let mut used_macs: HashSet<MacAddr> = HashSet::new();

        for (_, z) in parent_blueprint
            .all_omicron_zones(BlueprintZoneFilter::ShouldBeRunning)
        {
            let zone_type = &z.zone_type;
            match zone_type {
                BlueprintZoneType::BoundaryNtp(ntp) => match ntp.nic.ip {
                    IpAddr::V4(ip) => {
                        if !existing_boundary_ntp_v4_ips.insert(ip) {
                            bail!("duplicate Boundary NTP NIC IP: {ip}");
                        }
                    }
                    IpAddr::V6(ip) => {
                        if !existing_boundary_ntp_v6_ips.insert(ip) {
                            bail!("duplicate Boundary NTP NIC IP: {ip}");
                        }
                    }
                },
                BlueprintZoneType::Nexus(nexus) => match nexus.nic.ip {
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
                },
                BlueprintZoneType::ExternalDns(dns) => match dns.nic.ip {
                    IpAddr::V4(ip) => {
                        if !existing_external_dns_v4_ips.insert(ip) {
                            bail!("duplicate external DNS IP: {ip}");
                        }
                    }
                    IpAddr::V6(ip) => {
                        if !existing_external_dns_v6_ips.insert(ip) {
                            bail!("duplicate external DNS IP: {ip}");
                        }
                    }
                },
                _ => (),
            }

            if let Some((external_ip, nic)) = zone_type.external_networking() {
                // For the test suite, ignore localhost.  It gets reused many
                // times and that's okay.  We don't expect to see localhost
                // outside the test suite.
                if !external_ip.ip().is_loopback() {
                    external_ip_alloc.mark_ip_used(&external_ip)?;
                }

                if !used_macs.insert(nic.mac) {
                    bail!("duplicate service vNIC MAC: {}", nic.mac);
                }
            }
        }

        // Recycle the IP addresses of expunged external DNS zones.
        // TODO: Remove when external DNS addresses come from policy.
        let mut available_external_dns_ips: BTreeSet<IpAddr> = BTreeSet::new();
        for (_, zone) in
            parent_blueprint.all_omicron_zones(BlueprintZoneFilter::Expunged)
        {
            if let BlueprintZoneType::ExternalDns(dns) = &zone.zone_type {
                available_external_dns_ips.insert(dns.dns_address.addr.ip());
            }
        }

        // Check the planning input: there shouldn't be any external networking
        // resources in the database (the source of `input`) that we don't know
        // about from the parent blueprint.
        //
        // Logically this could be the first thing we do in this function, but
        // we have some tests that check error cases in the above block that
        // would also fail these checks (so reordering the checks would require
        // those tests to do more work to construct valid `input`s), and we
        // never expect this to fail in practice, so there's no use in "failing
        // fast".
        ensure_input_records_appear_in_parent_blueprint(
            parent_blueprint,
            input,
        )?;

        // TODO-performance Building these iterators as "walk through the list
        // and skip anything we've used already" is fine as long as we're
        // talking about a small number of resources (e.g., single-digit number
        // of Nexus instances), but wouldn't be ideal if we have many resources
        // we need to skip. We could do something smarter here based on the sets
        // of used resources we built above if needed.
        let nexus_v4_ips = AvailableIterator::new(
            NEXUS_OPTE_IPV4_SUBNET
                .addr_iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_nexus_v4_ips,
        );
        let nexus_v6_ips = AvailableIterator::new(
            NEXUS_OPTE_IPV6_SUBNET
                .iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_nexus_v6_ips,
        );
        let boundary_ntp_v4_ips = AvailableIterator::new(
            NTP_OPTE_IPV4_SUBNET
                .addr_iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_boundary_ntp_v4_ips,
        );
        let boundary_ntp_v6_ips = AvailableIterator::new(
            NTP_OPTE_IPV6_SUBNET.iter().skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_boundary_ntp_v6_ips,
        );
        let external_dns_v4_ips = AvailableIterator::new(
            DNS_OPTE_IPV4_SUBNET
                .addr_iter()
                .skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_external_dns_v4_ips,
        );
        let external_dns_v6_ips = AvailableIterator::new(
            DNS_OPTE_IPV6_SUBNET.iter().skip(NUM_INITIAL_RESERVED_IP_ADDRESSES),
            existing_external_dns_v6_ips,
        );
        let available_system_macs =
            AvailableIterator::new(MacAddr::iter_system(), used_macs);

        Ok(Self {
            boundary_ntp_v4_ips,
            boundary_ntp_v6_ips,
            nexus_v4_ips,
            nexus_v6_ips,
            external_dns_v4_ips,
            external_dns_v6_ips,
            available_external_dns_ips,
            external_ip_alloc,
            available_system_macs,
        })
    }

    pub(super) fn for_new_nexus(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, Error> {
        let external_ip = self.external_ip_alloc.claim_next_exclusive_ip()?;
        let (nic_ip, nic_subnet) = match external_ip {
            IpAddr::V4(_) => (
                self.nexus_v4_ips
                    .next()
                    .ok_or(Error::ExhaustedOpteIps { kind: ZoneKind::Nexus })?
                    .into(),
                IpNet::from(*NEXUS_OPTE_IPV4_SUBNET),
            ),
            IpAddr::V6(_) => (
                self.nexus_v6_ips
                    .next()
                    .ok_or(Error::ExhaustedOpteIps { kind: ZoneKind::Nexus })?
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

    pub(super) fn for_new_boundary_ntp(
        &mut self,
    ) -> Result<ExternalSnatNetworkingChoice, Error> {
        let snat_cfg = self.external_ip_alloc.claim_next_snat_ip()?;
        let (nic_ip, nic_subnet) = match snat_cfg.ip {
            IpAddr::V4(_) => (
                self.boundary_ntp_v4_ips
                    .next()
                    .ok_or(Error::ExhaustedOpteIps {
                        kind: ZoneKind::BoundaryNtp,
                    })?
                    .into(),
                IpNet::from(*NTP_OPTE_IPV4_SUBNET),
            ),
            IpAddr::V6(_) => (
                self.boundary_ntp_v6_ips
                    .next()
                    .ok_or(Error::ExhaustedOpteIps {
                        kind: ZoneKind::BoundaryNtp,
                    })?
                    .into(),
                IpNet::from(*NTP_OPTE_IPV6_SUBNET),
            ),
        };
        let nic_mac = self
            .available_system_macs
            .next()
            .ok_or(Error::NoSystemMacAddressAvailable)?;

        Ok(ExternalSnatNetworkingChoice {
            snat_cfg,
            nic_ip,
            nic_subnet,
            nic_mac,
        })
    }

    pub(super) fn for_new_external_dns(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, Error> {
        let external_ip = self
            .available_external_dns_ips
            .pop_first()
            .ok_or(Error::NoExternalDnsIpAvailable)?;

        let (nic_ip, nic_subnet) = match external_ip {
            IpAddr::V4(_) => (
                self.external_dns_v4_ips
                    .next()
                    .ok_or(Error::ExhaustedOpteIps {
                        kind: ZoneKind::ExternalDns,
                    })?
                    .into(),
                IpNet::from(*DNS_OPTE_IPV4_SUBNET),
            ),
            IpAddr::V6(_) => (
                self.external_dns_v6_ips
                    .next()
                    .ok_or(Error::ExhaustedOpteIps {
                        kind: ZoneKind::ExternalDns,
                    })?
                    .into(),
                IpNet::from(*DNS_OPTE_IPV6_SUBNET),
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

    /// Allow a test to manually add an external DNS address,
    /// which could otherwise only be added via RSS.
    ///
    /// TODO-cleanup: Remove when external DNS addresses are in the policy.
    #[cfg(test)]
    pub fn add_external_dns_ip(&mut self, addr: IpAddr) {
        assert!(
            self.available_external_dns_ips.insert(addr),
            "duplicate external DNS IP address"
        );
    }
}

// Helper to validate that the system hasn't gone off the rails. There should
// never be any external networking resources in the planning input (which is
// derived from the contents of CRDB) that we don't know about from the parent
// blueprint. It's possible a given planning iteration could see such a state
// there have been intermediate changes made by other Nexus instances; e.g.,
//
// 1. Nexus A generates a `PlanningInput` by reading from CRDB
// 2. Nexus B executes on a target blueprint that removes IPs/NICs from
//    CRDB
// 3. Nexus B regenerates a new blueprint and prunes the zone(s) associated
//    with the IPs/NICs from step 2
// 4. Nexus B makes this new blueprint the target
// 5. Nexus A attempts to run planning with its `PlanningInput` from step 1 but
//    the target blueprint from step 4; this will fail the following checks
//    because the input contains records that were removed in step 3
//
// We do not need to handle this class of error; it's a transient failure that
// will clear itself up when Nexus A repeats its planning loop from the top and
// generates a new `PlanningInput`.
//
// There may still be database records corresponding to _expunged_ zones, but
// that's okay: it just means we haven't yet realized a blueprint where those
// zones are expunged. And those should should still be in the blueprint (not
// pruned) until their database records are cleaned up.
//
// It's also possible that there may be networking records in the database
// assigned to zones that have been expunged, and our parent blueprint uses
// those same records for new zones. This is also fine and expected, and is a
// similar case to the previous paragraph: a zone with networking resources was
// expunged, the database doesn't realize it yet, but can still move forward and
// make planning decisions that reuse those resources for new zones.
fn ensure_input_records_appear_in_parent_blueprint(
    parent_blueprint: &Blueprint,
    input: &PlanningInput,
) -> anyhow::Result<()> {
    let mut all_macs: HashSet<MacAddr> = HashSet::new();
    let mut all_nexus_nic_ips: HashSet<IpAddr> = HashSet::new();
    let mut all_boundary_ntp_nic_ips: HashSet<IpAddr> = HashSet::new();
    let mut all_external_dns_nic_ips: HashSet<IpAddr> = HashSet::new();
    let mut all_external_ips: HashSet<OmicronZoneExternalIp> = HashSet::new();

    // Unlike the construction of the external IP allocator and existing IPs
    // constructed above in `BuilderExternalNetworking::new()`, we do not
    // check for duplicates here: we could very well see reuse of IPs
    // between expunged zones or between expunged -> running zones.
    for (_, z) in parent_blueprint.all_omicron_zones(BlueprintZoneFilter::All) {
        let zone_type = &z.zone_type;
        match zone_type {
            BlueprintZoneType::BoundaryNtp(ntp) => {
                all_boundary_ntp_nic_ips.insert(ntp.nic.ip);
            }
            BlueprintZoneType::Nexus(nexus) => {
                all_nexus_nic_ips.insert(nexus.nic.ip);
            }
            BlueprintZoneType::ExternalDns(dns) => {
                all_external_dns_nic_ips.insert(dns.nic.ip);
            }
            _ => (),
        }

        if let Some((external_ip, nic)) = zone_type.external_networking() {
            // As above, ignore localhost (used by the test suite).
            if !external_ip.ip().is_loopback() {
                all_external_ips.insert(external_ip);
            }
            all_macs.insert(nic.mac);
        }
    }
    for external_ip_entry in
        input.network_resources().omicron_zone_external_ips()
    {
        // As above, ignore localhost (used by the test suite).
        if external_ip_entry.ip.ip().is_loopback() {
            continue;
        }
        if !all_external_ips.contains(&external_ip_entry.ip) {
            bail!(
                "planning input contains unexpected external IP \
                 (IP not found in parent blueprint): {external_ip_entry:?}"
            );
        }
    }
    for nic_entry in input.network_resources().omicron_zone_nics() {
        if !all_macs.contains(&nic_entry.nic.mac) {
            bail!(
                "planning input contains unexpected NIC \
                 (MAC not found in parent blueprint): {nic_entry:?}"
            );
        }
        match nic_entry.nic.ip {
            IpAddr::V4(ip) if NEXUS_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_nexus_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V4(ip) if NTP_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_boundary_ntp_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V4(ip) if DNS_OPTE_IPV4_SUBNET.contains(ip) => {
                if !all_external_dns_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V6(ip) if NEXUS_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_nexus_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V6(ip) if NTP_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_boundary_ntp_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            IpAddr::V6(ip) if DNS_OPTE_IPV6_SUBNET.contains(ip) => {
                if !all_external_dns_nic_ips.contains(&ip.into()) {
                    bail!(
                        "planning input contains unexpected NIC \
                         (IP not found in parent blueprint): {nic_entry:?}"
                    );
                }
            }
            _ => {
                bail!(
                    "planning input contains unexpected NIC \
                    (IP not contained in known OPTE subnet): {nic_entry:?}"
                )
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalNetworkingChoice {
    pub(super) external_ip: IpAddr,
    pub(super) nic_ip: IpAddr,
    pub(super) nic_subnet: IpNet,
    pub(super) nic_mac: MacAddr,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExternalSnatNetworkingChoice {
    pub(super) snat_cfg: SourceNatConfig,
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

// External IPs come in two flavors, from an allocation point of view: IPs that
// require exclusive use, and SNAT IPs that can be shared amongst up to four
// services, because the port range is broken up into four 16384-sized chunks.
// This struct keeps track of both kinds of IPs used by blueprints, allowing
// allocation of either kind.
#[derive(Debug)]
struct ExternalIpAllocator<'a> {
    service_ip_pool_ips: DebugIgnore<Box<dyn Iterator<Item = IpAddr> + 'a>>,
    used_exclusive_ips: BTreeSet<IpAddr>,
    used_snat_ips: BTreeMap<IpAddr, BTreeSet<SnatPortRange>>,
}

impl<'a> ExternalIpAllocator<'a> {
    fn new(service_pool_ranges: &'a [IpRange]) -> Self {
        let service_ip_pool_ips =
            service_pool_ranges.iter().flat_map(|r| r.iter());
        Self {
            service_ip_pool_ips: DebugIgnore(Box::new(service_ip_pool_ips)),
            used_exclusive_ips: BTreeSet::new(),
            used_snat_ips: BTreeMap::new(),
        }
    }

    fn mark_ip_used(
        &mut self,
        external_ip: &OmicronZoneExternalIp,
    ) -> anyhow::Result<()> {
        match external_ip {
            OmicronZoneExternalIp::Floating(ip) => {
                let ip = ip.ip;
                if self.used_snat_ips.contains_key(&ip)
                    || !self.used_exclusive_ips.insert(ip)
                {
                    bail!("duplicate external IP: {external_ip:?}");
                }
            }
            OmicronZoneExternalIp::Snat(snat) => {
                let ip = snat.snat_cfg.ip;
                let port_range =
                    SnatPortRange::try_from(snat.snat_cfg.port_range_raw())?;
                if self.used_exclusive_ips.contains(&ip)
                    || !self
                        .used_snat_ips
                        .entry(ip)
                        .or_default()
                        .insert(port_range)
                {
                    bail!("duplicate external IP: {external_ip:?}");
                }
            }
        }
        Ok(())
    }

    // Returns `Ok(true)` if we contain this IP exactly, `Ok(false)` if we do
    // not contain this IP, and an error if we contain a matching IP address but
    // a mismatched exclusiveness / SNAT-ness.
    #[cfg(test)]
    fn contains(
        &self,
        external_ip: &OmicronZoneExternalIp,
    ) -> anyhow::Result<bool> {
        match external_ip {
            OmicronZoneExternalIp::Floating(ip) => {
                let ip = ip.ip;
                if self.used_snat_ips.contains_key(&ip) {
                    bail!(
                        "mismatched IP: {external_ip:?} also used as SNAT IP"
                    );
                }
                Ok(self.used_exclusive_ips.contains(&ip))
            }
            OmicronZoneExternalIp::Snat(snat) => {
                let ip = snat.snat_cfg.ip;
                let port_range =
                    SnatPortRange::try_from(snat.snat_cfg.port_range_raw())?;

                if self.used_exclusive_ips.contains(&ip) {
                    bail!(
                        "mismatched IP: {external_ip:?} also used as floating"
                    );
                }

                Ok(self
                    .used_snat_ips
                    .get(&ip)
                    .map(|port_ranges| port_ranges.contains(&port_range))
                    .unwrap_or(false))
            }
        }
    }

    fn claim_next_exclusive_ip(&mut self) -> Result<IpAddr, Error> {
        for ip in &mut *self.service_ip_pool_ips {
            if !self.used_snat_ips.contains_key(&ip)
                && self.used_exclusive_ips.insert(ip)
            {
                return Ok(ip);
            }
        }

        Err(Error::NoExternalServiceIpAvailable)
    }

    fn claim_next_snat_ip(&mut self) -> Result<SourceNatConfig, Error> {
        // Prefer reusing an existing SNAT IP, if we still have port ranges
        // available on that ip.
        for (ip, used_port_ranges) in self.used_snat_ips.iter_mut() {
            for port_range in SnatPortRange::iter() {
                if used_port_ranges.insert(port_range) {
                    return Ok(port_range.into_source_nat_config(*ip));
                }
            }
        }

        // No available port ranges left; allocate a new IP.
        for ip in &mut *self.service_ip_pool_ips {
            if self.used_exclusive_ips.contains(&ip) {
                continue;
            }
            match self.used_snat_ips.entry(ip) {
                // We already checked all occupied slots above; move on.
                Entry::Occupied(_) => continue,
                Entry::Vacant(slot) => {
                    let port_range = SnatPortRange::One;
                    slot.insert(BTreeSet::new()).insert(port_range);
                    return Ok(port_range.into_source_nat_config(ip));
                }
            }
        }

        Err(Error::NoExternalServiceIpAvailable)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, strum::EnumIter,
)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
enum SnatPortRange {
    One,
    Two,
    Three,
    Four,
}

impl SnatPortRange {
    fn into_source_nat_config(self, ip: IpAddr) -> SourceNatConfig {
        let first = match self {
            SnatPortRange::One => 0,
            SnatPortRange::Two => NUM_SOURCE_NAT_PORTS,
            SnatPortRange::Three => 2 * NUM_SOURCE_NAT_PORTS,
            SnatPortRange::Four => 3 * NUM_SOURCE_NAT_PORTS,
        };

        // Parens are important here to avoid overflow on the last range!
        let last = first + (NUM_SOURCE_NAT_PORTS - 1);

        // By construction our (first, last) pair is aligned, so we can unwrap
        // here. We'll use an explicit match to guard against `SourceNatConfig`
        // gaining other kinds of validation we're currently not aware of.
        match SourceNatConfig::new(ip, first, last) {
            Ok(cfg) => cfg,
            Err(SourceNatConfigError::UnalignedPortPair { .. }) => {
                unreachable!("port pair guaranteed aligned: {first}, {last}");
            }
        }
    }
}

impl TryFrom<(u16, u16)> for SnatPortRange {
    type Error = anyhow::Error;

    fn try_from((first, last): (u16, u16)) -> Result<Self, Self::Error> {
        // We assume there are exactly four SNAT port ranges; fail to compile if
        // that ever changes.
        static_assertions::const_assert_eq!(
            NUM_SOURCE_NAT_PORTS as u32 * 4,
            u16::MAX as u32 + 1
        );

        // Validate the pair is legal.
        if first % NUM_SOURCE_NAT_PORTS != 0
            || u32::from(first) + u32::from(NUM_SOURCE_NAT_PORTS) - 1
                != u32::from(last)
        {
            bail!("invalid SNAT port range: ({first}, {last})");
        }

        let value = match first / NUM_SOURCE_NAT_PORTS {
            0 => Self::One,
            1 => Self::Two,
            2 => Self::Three,
            3 => Self::Four,
            _ => unreachable!("incorrect arithmetic"),
        };

        Ok(value)
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use nexus_types::deployment::OmicronZoneExternalFloatingIp;
    use nexus_types::deployment::OmicronZoneExternalSnatIp;
    use omicron_uuid_kinds::ExternalIpUuid;
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

    #[proptest]
    fn test_external_ip_allocator(
        items: BTreeMap<IpAddr, (bool, BTreeSet<SnatPortRange>)>,
    ) {
        // Break up the proptest input into categories.
        let mut ip_pool_ranges = Vec::new();
        let mut used_exclusive = BTreeSet::new();
        let mut used_snat: BTreeMap<IpAddr, BTreeSet<SnatPortRange>> =
            BTreeMap::new();
        let mut expected_available_ips = BTreeSet::new();
        let mut expected_available_snat_ranges = BTreeSet::new();

        for (ip, (in_use, snat_ranges)) in items {
            ip_pool_ranges.push(IpRange::from(ip));
            if in_use {
                if snat_ranges.is_empty() {
                    used_exclusive.insert(ip);
                } else {
                    // Record any _unused_ port ranges associated with this IP,
                    // since we should be able to allocate them later.
                    for port_range in SnatPortRange::iter()
                        .filter(|r| !snat_ranges.contains(r))
                    {
                        expected_available_snat_ranges.insert((ip, port_range));
                    }
                    for snat_range in snat_ranges {
                        used_snat.entry(ip).or_default().insert(snat_range);
                    }
                }
            } else {
                expected_available_ips.insert(ip);
            }
        }

        // The API for ExternalIpAllocator is in terms of
        // `OmicronZoneExternalIp`, but it doesn't actually care about the IDs;
        // we'll generate random ones as needed.
        let as_floating = |ip| {
            OmicronZoneExternalIp::Floating(OmicronZoneExternalFloatingIp {
                id: ExternalIpUuid::new_v4(),
                ip,
            })
        };
        let as_snat = |ip, snat: &SnatPortRange| {
            OmicronZoneExternalIp::Snat(OmicronZoneExternalSnatIp {
                id: ExternalIpUuid::new_v4(),
                snat_cfg: snat.into_source_nat_config(ip),
            })
        };

        // Build up the allocator and mark all used IPs.
        let mut allocator = ExternalIpAllocator::new(&ip_pool_ranges);
        for &ip in &used_exclusive {
            allocator
                .mark_ip_used(&as_floating(ip))
                .expect("failed to mark floating ip as used");
        }
        for (&ip, snat_ranges) in &used_snat {
            for snat_range in snat_ranges {
                allocator
                    .mark_ip_used(&as_snat(ip, snat_range))
                    .expect("failed to mark floating ip as used");
            }
        }

        // Check that all used IPs return the expected value when the allocator
        // is asked if it already contains them: Ok(true) for exact matches, and
        // an error if we ask for containment of an IP with the wrong
        // exclusive/snat type.
        for &ip in &used_exclusive {
            assert!(
                allocator.contains(&as_floating(ip)).unwrap(),
                "missing ip {ip}"
            );
            for snat in SnatPortRange::iter() {
                assert!(
                    allocator.contains(&as_snat(ip, &snat)).is_err(),
                    "unexpected success for {ip}"
                );
            }
        }
        for (&ip, snat_ranges) in &used_snat {
            for snat_range in snat_ranges {
                assert!(
                    allocator.contains(&as_snat(ip, snat_range)).unwrap(),
                    "missing ip {ip}/{snat_range:?}"
                );
            }
            assert!(
                allocator.contains(&as_floating(ip)).is_err(),
                "unexpected success for {ip}"
            );
        }

        // Asking for a SNAT IP should prefer to reuse any of the existing SNAT
        // IPs that have unused ranges; we'll confirm this by allocating new
        // SNAT IPs until all the existing SNAT IP ranges are exhausted.
        while !expected_available_snat_ranges.is_empty() {
            let snat = allocator
                .claim_next_snat_ip()
                .expect("failed to get SNAT IPs with ranges still available");
            let port_range = SnatPortRange::try_from(snat.port_range_raw())
                .expect("illegal snat port range");
            assert!(
                expected_available_snat_ranges.remove(&(snat.ip, port_range)),
                "unexpected SNAT IP {snat:?}"
            );
        }

        // We now have a set of fully-unused IP addresses. We'll flip flop
        // between asking for an exclusive IP and a set of four SNAT IPs, until
        // all of these are exhausted.
        let mut claim_exclusive = true;
        while !expected_available_ips.is_empty() {
            if claim_exclusive {
                let ip = allocator
                    .claim_next_exclusive_ip()
                    .expect("failed to get exclusive IP");
                assert!(
                    expected_available_ips.remove(&ip),
                    "unexpected exclusive ip {ip}"
                );
            } else {
                let snat = allocator.claim_next_snat_ip().expect(
                    "failed to get SNAT IPs with ranges still available",
                );
                assert!(
                    expected_available_ips.remove(&snat.ip),
                    "unexpected SNAT ip {snat:?}"
                );

                // This first claim should give back the first port range.
                let port_range = SnatPortRange::One;
                assert_eq!(snat, port_range.into_source_nat_config(snat.ip));

                // Three subsequent claims should give out the three subsequent
                // port ranges for this same IP address, in order.
                let ip = snat.ip;
                for port_range in SnatPortRange::iter().skip(1) {
                    let snat = allocator.claim_next_snat_ip().expect(
                        "failed to get SNAT IPs with ranges still available",
                    );
                    assert_eq!(snat, port_range.into_source_nat_config(ip));
                }
            }
            claim_exclusive = !claim_exclusive;
        }
    }
}
