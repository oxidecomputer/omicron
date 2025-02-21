// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blueprint planner resource allocation

use std::net::IpAddr;

use super::SledEditor;
use nexus_types::deployment::BlueprintZoneDisposition;
use omicron_common::address::DnsSubnet;
use omicron_common::address::IpRange;
use omicron_common::address::ReservedRackSubnet;
use omicron_uuid_kinds::SledUuid;

mod external_networking;
mod internal_dns;

pub use self::external_networking::ExternalNetworkingError;
pub use self::internal_dns::InternalDnsError;
pub use self::internal_dns::InternalDnsInputError;

pub(crate) use self::external_networking::ExternalNetworkingChoice;
pub(crate) use self::external_networking::ExternalSnatNetworkingChoice;

use self::external_networking::ExternalNetworkingAllocator;
use self::internal_dns::InternalDnsSubnetAllocator;

#[derive(Debug, thiserror::Error)]
pub enum BlueprintResourceAllocatorInputError {
    #[error(transparent)]
    InternalDns(#[from] InternalDnsInputError),
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
        target_internal_dns_redundancy: usize,
    ) -> Result<Self, BlueprintResourceAllocatorInputError>
    where
        I: Iterator<Item = (SledUuid, &'a SledEditor)> + Clone,
    {
        let internal_dns = InternalDnsSubnetAllocator::new(
            all_sleds.clone().flat_map(|(_, editor)| {
                editor.zones(BlueprintZoneDisposition::is_in_service)
            }),
            target_internal_dns_redundancy,
        )?;

        let external_networking = ExternalNetworkingAllocator::new(
            all_sleds.clone().flat_map(|(_, editor)| {
                editor.zones(BlueprintZoneDisposition::is_in_service)
            }),
            all_sleds.flat_map(|(_, editor)| {
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
    ) -> Result<DnsSubnet, InternalDnsError> {
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
