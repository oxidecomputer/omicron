// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blueprint planner resource allocation

use std::net::IpAddr;

use super::SledEditor;
use nexus_types::deployment::blueprint_zone_type::InternalDns;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneType;
use omicron_common::address::DnsSubnet;
use omicron_common::address::IpRange;
use omicron_common::address::ReservedRackSubnet;

mod external_networking;
mod internal_dns;

pub use self::external_networking::ExternalNetworkingError;
pub use self::internal_dns::NoAvailableDnsSubnets;

pub(crate) use self::external_networking::ExternalNetworkingChoice;
pub(crate) use self::external_networking::ExternalSnatNetworkingChoice;

use self::external_networking::ExternalNetworkingAllocator;
use self::internal_dns::InternalDnsSubnetAllocator;

#[derive(Debug, thiserror::Error)]
pub enum BlueprintResourceAllocatorInputError {
    #[error("failed to create external networking allocator")]
    ExternalNetworking(#[source] anyhow::Error),
}

#[derive(Debug)]
pub(crate) struct BlueprintResourceAllocator {
    external_networking: ExternalNetworkingAllocator,
    internal_dns: InternalDnsSubnetAllocator,
}

impl BlueprintResourceAllocator {
    pub fn new<'a, I>(
        all_sleds: I,
        service_ip_pool_ranges: Vec<IpRange>,
    ) -> Result<Self, BlueprintResourceAllocatorInputError>
    where
        I: Iterator<Item = &'a SledEditor> + Clone,
    {
        let internal_dns_subnets_in_use = all_sleds
            .clone()
            .flat_map(|editor| {
                editor
                    // We use `could_be_running` here instead of `in_service` to
                    // avoid reusing an internal DNS subnet from an
                    // expunged-but-possibly-still-running zone.
                    .zones(BlueprintZoneDisposition::could_be_running)
                    .filter_map(|z| match z.zone_type {
                        BlueprintZoneType::InternalDns(InternalDns {
                            dns_address,
                            ..
                        }) => Some(DnsSubnet::from_addr(*dns_address.ip())),
                        _ => None,
                    })
            })
            .collect();
        let internal_dns =
            InternalDnsSubnetAllocator::new(internal_dns_subnets_in_use);

        let external_networking = ExternalNetworkingAllocator::new(
            all_sleds.clone().flat_map(|editor| {
                editor.zones(BlueprintZoneDisposition::is_in_service)
            }),
            all_sleds.flat_map(|editor| {
                editor.zones(BlueprintZoneDisposition::is_expunged)
            }),
            service_ip_pool_ranges,
        )
        .map_err(BlueprintResourceAllocatorInputError::ExternalNetworking)?;

        Ok(Self { external_networking, internal_dns })
    }

    pub fn next_internal_dns_subnet(
        &mut self,
        rack_subnet: ReservedRackSubnet,
    ) -> Result<DnsSubnet, NoAvailableDnsSubnets> {
        self.internal_dns.alloc(rack_subnet)
    }

    pub(crate) fn next_external_ip_nexus(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, ExternalNetworkingError> {
        self.external_networking.for_new_nexus()
    }

    pub(crate) fn next_external_ip_external_dns(
        &mut self,
    ) -> Result<ExternalNetworkingChoice, ExternalNetworkingError> {
        self.external_networking.for_new_external_dns()
    }

    pub(crate) fn next_external_ip_boundary_ntp(
        &mut self,
    ) -> Result<ExternalSnatNetworkingChoice, ExternalNetworkingError> {
        self.external_networking.for_new_boundary_ntp()
    }

    /// Allow a test to manually add an external DNS address, which could
    /// ordinarily only come from RSS.
    ///
    /// TODO-cleanup: Remove when external DNS addresses are in the policy.
    // This can't be `#[cfg(test)]` because it's used by the `ExampleSystem`
    // helper (which itself is used by reconfigurator-cli and friends). We give
    // it a scary name instead.
    pub(crate) fn inject_untracked_external_dns_ip(
        &mut self,
        ip: IpAddr,
    ) -> Result<(), ExternalNetworkingError> {
        self.external_networking.add_external_dns_ip(ip)
    }
}
