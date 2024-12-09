// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blueprint planner resource allocation

use super::SledEditor;
use nexus_types::deployment::BlueprintZoneFilter;
use omicron_common::address::IpRange;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;

mod external_networking;
mod internal_dns;
mod underlay_ip;

pub use self::external_networking::ExternalNetworkingError;
pub use self::internal_dns::InternalDnsError;
pub use self::internal_dns::InternalDnsInputError;
pub use self::underlay_ip::SledUnderlayIpError;

use self::external_networking::ExternalNetworkingAllocator;
use self::internal_dns::InternalDnsSubnetAllocator;
use self::underlay_ip::SledUnderlayIpAllocator;

#[derive(Debug, thiserror::Error)]
pub enum BlueprintResourceAllocatorInputError {
    #[error(transparent)]
    InternalDns(#[from] InternalDnsInputError),
    #[error("failed to create underlay IP allocator for sled {sled_id}")]
    UnderlayIp {
        sled_id: SledUuid,
        #[source]
        err: SledUnderlayIpError,
    },
    #[error("failed to create external networking allocator")]
    ExternalNetworking(#[source] anyhow::Error),
}

#[derive(Debug)]
pub(crate) struct BlueprintResourceAllocator {
    underlay_ip: BTreeMap<SledUuid, SledUnderlayIpAllocator>,
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
        let underlay_ip = all_sleds
            .clone()
            .filter_map(|(sled_id, editor)| {
                // If the editor doesn't have a subnet, that means the sled has
                // been decommissioned, so we'll never try to allocate underlay
                // IPs for it anyway. (Attempting to do so will fail.)
                let subnet = editor.subnet()?;
                // We never reuse underlay IPs within a sled; mark all underlay
                // IPs for any zones as reserved, regardless of those zones'
                // dispositions.
                let reserved_ips = editor
                    .zones(BlueprintZoneFilter::All)
                    .map(|z| z.underlay_ip());
                let result =
                    match SledUnderlayIpAllocator::new(subnet, reserved_ips) {
                        Ok(allocator) => Ok((sled_id, allocator)),
                        Err(err) => Err(
                            BlueprintResourceAllocatorInputError::UnderlayIp {
                                sled_id,
                                err,
                            },
                        ),
                    };
                Some(result)
            })
            .collect::<Result<BTreeMap<SledUuid, SledUnderlayIpAllocator>, _>>(
            )?;

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

        Ok(Self { underlay_ip, external_networking, internal_dns })
    }
}
