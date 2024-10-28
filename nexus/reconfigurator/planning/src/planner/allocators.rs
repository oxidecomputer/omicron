// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::blueprint_zones_editor::BlueprintZonesEditor;
use external_networking::ExternalNetworkingAllocator;
use internal_dns::DnsSubnetAllocator;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::blueprint_zone_type;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::SledResources;
use nexus_types::inventory::ZpoolName;
use omicron_common::address::DnsSubnet;
use omicron_common::address::IpRange;
use omicron_common::address::Ipv6Subnet;
use omicron_common::address::ReservedRackSubnet;
use omicron_common::address::SLED_PREFIX;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::net::Ipv6Addr;
use underlay_ip::UnderlayIpAllocator;
use zpool::ZpoolAllocator;

mod external_networking;
mod internal_dns;
mod underlay_ip;
mod zpool;

pub use external_networking::ExternalNetworkingError;
pub use zpool::RunningZoneMissingZpool;

pub(crate) use external_networking::ExternalNetworkingChoice;
pub(crate) use external_networking::ExternalSnatNetworkingChoice;

#[derive(Debug, thiserror::Error)]
pub enum SledAllocationError {
    #[error(transparent)]
    RunningZoneMissingZpool(#[from] RunningZoneMissingZpool),
    #[error(transparent)]
    ExternalNetworkingError(#[from] ExternalNetworkingError),
    #[error("attempted to allocate resources for unknown sled: {sled_id}")]
    UnknownSled { sled_id: SledUuid },
    #[error("sled {sled_id}: no available underlay IP addresses")]
    NoAvailableUnderlayIp { sled_id: SledUuid },
    #[error(
        "sled {sled_id}: no available zpools for additional {kind:?} zones"
    )]
    NoAvailableZpool { sled_id: SledUuid, kind: ZoneKind },
    #[error("no reserved subnets available for DNS")]
    NoAvailableDnsSubnets,
}

#[derive(Debug)]
struct SingleSled {
    // This sled's subnet; we (potentially) use it when allocating an internal
    // DNS zone, where we need the rack subnet.
    subnet: Ipv6Subnet<SLED_PREFIX>,
    // Each internal DNS zone on a single sled needs a distinct
    // `gz_address_index`, which sled-agent uses to set up routing due to their
    // underlay address being distinct from the sled's subnet. Here we track
    // what indices are already in use, so we can generate "the next unused one"
    // when needed.
    used_internal_dns_gz_address_indices: BTreeSet<u32>,
    underlay_ip: UnderlayIpAllocator,
    zpool: ZpoolAllocator,
}

impl SingleSled {
    fn next_internal_dns_gz_address_index(&mut self) -> u32 {
        // In production, we expect any given sled to have 0 or 1 internal DNS
        // zones. In test environments, we might have as many as 5. Either way,
        // an O(n^2) loop here to find the next unused index is probably fine.
        for i in 0.. {
            if self.used_internal_dns_gz_address_indices.insert(i) {
                return i;
            }
        }
        unreachable!("more than u32::MAX internal DNS zones on one sled");
    }
}

#[derive(Debug)]
pub(super) struct InternalDnsAllocation {
    pub subnet: DnsSubnet,
    pub gz_address_index: u32,
}

#[derive(Debug)]
pub(super) struct SledAllocators<'a> {
    by_sled: BTreeMap<SledUuid, SingleSled>,
    external_networking: ExternalNetworkingAllocator<'a>,
    internal_dns: DnsSubnetAllocator,
}

impl<'a> SledAllocators<'a> {
    pub(super) fn new<'b>(
        service_ip_pool_ranges: &'a [IpRange],
        sled_resources: impl Iterator<Item = (SledUuid, &'b SledResources)>,
        zones_editor: &BlueprintZonesEditor,
    ) -> Result<Self, SledAllocationError> {
        let mut by_sled = BTreeMap::new();
        for (sled_id, sled_resources) in sled_resources {
            let subnet = sled_resources.subnet;
            let underlay_ip = UnderlayIpAllocator::for_sled(
                subnet,
                zones_editor
                    .current_sled_zones(sled_id, BlueprintZoneFilter::All)
                    .map(|z| z.underlay_ip()),
            );
            let zpool =
                ZpoolAllocator::new(sled_id, sled_resources, zones_editor)?;
            let used_internal_dns_gz_address_indices = zones_editor
                .current_sled_zones(
                    sled_id,
                    BlueprintZoneFilter::ShouldBeRunning,
                )
                .filter_map(|z| match z.zone_type {
                    BlueprintZoneType::InternalDns(
                        blueprint_zone_type::InternalDns {
                            gz_address_index,
                            ..
                        },
                    ) => Some(gz_address_index),
                    _ => None,
                })
                .collect();

            by_sled.insert(
                sled_id,
                SingleSled {
                    subnet,
                    used_internal_dns_gz_address_indices,
                    underlay_ip,
                    zpool,
                },
            );
        }
        let external_networking = ExternalNetworkingAllocator::new(
            service_ip_pool_ranges,
            zones_editor,
        )?;
        let internal_dns = DnsSubnetAllocator::new(zones_editor);
        Ok(Self { by_sled, external_networking, internal_dns })
    }

    pub(super) fn external_networking(
        &mut self,
    ) -> &mut ExternalNetworkingAllocator<'a> {
        &mut self.external_networking
    }

    fn get_sled_mut(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<&mut SingleSled, SledAllocationError> {
        self.by_sled
            .get_mut(&sled_id)
            .ok_or(SledAllocationError::UnknownSled { sled_id })
    }

    pub(super) fn alloc_underlay_ip(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<Ipv6Addr, SledAllocationError> {
        self.get_sled_mut(sled_id)?
            .underlay_ip
            .alloc()
            .ok_or(SledAllocationError::NoAvailableUnderlayIp { sled_id })
    }

    pub(super) fn alloc_zpool(
        &mut self,
        sled_id: SledUuid,
        zone_kind: ZoneKind,
    ) -> Result<ZpoolName, SledAllocationError> {
        self.get_sled_mut(sled_id)?.zpool.alloc(zone_kind).ok_or(
            SledAllocationError::NoAvailableZpool { sled_id, kind: zone_kind },
        )
    }

    pub(super) fn alloc_internal_dns_subnet(
        &mut self,
        sled_id: SledUuid,
    ) -> Result<InternalDnsAllocation, SledAllocationError> {
        let sled = self.get_sled_mut(sled_id)?;
        let rack_subnet = ReservedRackSubnet::from_subnet(sled.subnet);
        let gz_address_index = sled.next_internal_dns_gz_address_index();
        let subnet = self
            .internal_dns
            .alloc(rack_subnet)
            .ok_or(SledAllocationError::NoAvailableDnsSubnets)?;
        Ok(InternalDnsAllocation { subnet, gz_address_index })
    }
}
