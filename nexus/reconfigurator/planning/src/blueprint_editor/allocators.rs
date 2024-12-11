// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blueprint planner resource allocation

use super::SledEditor;
use nexus_types::deployment::BlueprintZoneFilter;
use omicron_common::address::IpRange;
use omicron_uuid_kinds::SledUuid;

mod external_networking;
mod internal_dns;

pub use self::external_networking::ExternalNetworkingError;
pub use self::internal_dns::InternalDnsError;
pub use self::internal_dns::InternalDnsInputError;

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
                editor.zones(BlueprintZoneFilter::ShouldBeRunning)
            }),
            target_internal_dns_redundancy,
        )?;

        let external_networking = ExternalNetworkingAllocator::new(
            all_sleds.clone().flat_map(|(_, editor)| {
                editor.zones(BlueprintZoneFilter::ShouldBeRunning)
            }),
            all_sleds.flat_map(|(_, editor)| {
                editor.zones(BlueprintZoneFilter::Expunged)
            }),
            service_ip_pool_ranges,
        )
        .map_err(BlueprintResourceAllocatorInputError::ExternalNetworking)?;

        Ok(Self { external_networking, internal_dns })
    }
}
