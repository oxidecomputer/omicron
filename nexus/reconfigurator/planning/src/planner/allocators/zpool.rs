// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::planner::blueprint_zones_editor::BlueprintZonesEditor;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::SledResources;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::inventory::ZpoolName;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
#[error(
    "planning error: sled {sled_id} has running zone {zone_id} ({zone_kind:?}) \
     with a durable dataset on nonexistent or not-in-service zpool: {zpool_id}"
)]
pub struct RunningZoneMissingZpool {
    pub sled_id: SledUuid,
    pub zone_id: OmicronZoneUuid,
    pub zone_kind: ZoneKind,
    pub zpool_id: ZpoolUuid,
}

#[derive(Debug)]
pub(crate) struct ZpoolAllocator {
    in_service_zpools: Vec<ZpoolUuid>,
    has_durable_datasets: BTreeMap<ZoneKind, BTreeSet<usize>>,
}

impl ZpoolAllocator {
    pub fn new(
        sled_id: SledUuid,
        sled_resources: &SledResources,
        zones_editor: &BlueprintZonesEditor,
    ) -> Result<Self, RunningZoneMissingZpool> {
        let in_service_zpools = sled_resources
            .all_zpools(ZpoolFilter::InService)
            .collect::<Vec<_>>();

        let mut has_durable_datasets: BTreeMap<ZoneKind, BTreeSet<usize>> =
            BTreeMap::new();

        for zone_config in zones_editor
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
        {
            if let Some(zpool) = zone_config.zone_type.durable_zpool() {
                let kind = zone_config.zone_type.kind();
                let pos = in_service_zpools
                    .iter()
                    .position(|&zpool_id| zpool_id == zpool.id())
                    .ok_or_else(|| {
                        // We have a running zone whose durable dataset is on a
                        // zpool that is not in service on this sled; this is a
                        // fatal planning error. This could happen if a disk has
                        // been expunged, but we should have already expunged
                        // the zone if that's the case (zone expungement is the
                        // first thing the planner does, before it instantiates
                        // a resource allocator).
                        RunningZoneMissingZpool {
                            sled_id,
                            zone_id: zone_config.id,
                            zone_kind: zone_config.zone_type.kind(),
                            zpool_id: zpool.id(),
                        }
                    })?;
                has_durable_datasets.entry(kind).or_default().insert(pos);
            }
        }

        Ok(Self { in_service_zpools, has_durable_datasets })
    }

    /// Selects a zpool for this zone type.
    ///
    /// This zpool may be used for either durable storage or transient
    /// storage - the usage is up to the caller.
    ///
    /// If `zone_kind` already exists on `sled_id`, it is prevented
    /// from using the same zpool as existing zones with the same kind.
    pub fn alloc(
        &mut self,
        zone_kind: ZoneKind,
    ) -> Option<ZpoolName> {
        let already_used =
            self.has_durable_datasets.entry(zone_kind).or_default();
        for (i, &zpool_id) in self.in_service_zpools.iter().enumerate() {
            // Skip this zpool if it already contains a zone of this kind.
            if already_used.insert(i) {
                return Some(ZpoolName::new_external(zpool_id));
            }
        }
        None
    }
}
