// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use indexmap::map::Entry;
use indexmap::IndexMap;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZonesConfig;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::ZpoolUuid;

#[derive(Debug, thiserror::Error)]
pub enum ZonesEditError {
    #[error(
        "tried to add duplicate zone ID {id} (kinds: {kind1:?}, {kind2:?})"
    )]
    AddDuplicateZoneId { id: OmicronZoneUuid, kind1: ZoneKind, kind2: ZoneKind },
    #[error("tried to expunge nonexistent zone {id}")]
    ExpungeNonexistentZone { id: OmicronZoneUuid },
}

#[derive(Debug, thiserror::Error)]
#[error(
    "invalid blueprint input: duplicate zone ID {id} \
     (kinds: {kind1:?}, {kind2:?})"
)]
pub struct DuplicateZoneId {
    pub id: OmicronZoneUuid,
    pub kind1: ZoneKind,
    pub kind2: ZoneKind,
}

#[derive(Debug)]
pub(super) struct ZonesEditor {
    generation: Generation,
    zones: IndexMap<OmicronZoneUuid, BlueprintZoneConfig>,
    counts: EditCounts,
}

impl ZonesEditor {
    pub fn empty() -> Self {
        Self {
            generation: Generation::new(),
            zones: IndexMap::new(),
            counts: EditCounts::zeroes(),
        }
    }

    pub fn finalize(self) -> (BlueprintZonesConfig, EditCounts) {
        let mut generation = self.generation;
        if self.counts.has_nonzero_counts() {
            generation = generation.next();
        }
        (
            BlueprintZonesConfig {
                generation,
                zones: self.zones.into_values().collect(),
            },
            self.counts,
        )
    }

    pub fn edit_counts(&self) -> EditCounts {
        self.counts
    }

    pub fn zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        self.zones
            .values()
            .filter(move |config| config.disposition.matches(filter))
    }

    pub fn add_zone(
        &mut self,
        zone: BlueprintZoneConfig,
    ) -> Result<(), ZonesEditError> {
        match self.zones.entry(zone.id) {
            Entry::Vacant(slot) => {
                slot.insert(zone);
                self.counts.added += 1;
                Ok(())
            }
            Entry::Occupied(prev) => {
                // We shouldn't be trying to add zones that already exist --
                // something went wrong in the planner logic.
                Err(ZonesEditError::AddDuplicateZoneId {
                    id: zone.id,
                    kind1: zone.zone_type.kind(),
                    kind2: prev.get().zone_type.kind(),
                })
            }
        }
    }

    pub fn expunge(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<&BlueprintZoneConfig, ZonesEditError> {
        let config = self.zones.get_mut(zone_id).ok_or_else(|| {
            ZonesEditError::ExpungeNonexistentZone { id: *zone_id }
        })?;

        Self::expunge_impl(config, &mut self.counts);

        Ok(&*config)
    }

    fn expunge_impl(config: &mut BlueprintZoneConfig, counts: &mut EditCounts) {
        match config.disposition {
            BlueprintZoneDisposition::InService
            | BlueprintZoneDisposition::Quiesced => {
                config.disposition = BlueprintZoneDisposition::Expunged;
                counts.expunged += 1;
            }
            BlueprintZoneDisposition::Expunged => {
                // expunge is idempotent; do nothing
            }
        }
    }

    pub fn expunge_all_on_zpool(&mut self, zpool: &ZpoolUuid) {
        for config in self.zones.values_mut() {
            // Expunge this zone if its filesystem or durable dataset are on
            // this zpool. (If it has both, they should be on the _same_ zpool,
            // but that's not strictly required by this method - we'll expunge a
            // zone that depends on this zpool in any way.)
            let fs_is_on_zpool = config
                .filesystem_pool
                .as_ref()
                .map_or(false, |pool| pool.id() == *zpool);
            let dd_is_on_zpool = config
                .zone_type
                .durable_zpool()
                .map_or(false, |pool| pool.id() == *zpool);
            if fs_is_on_zpool || dd_is_on_zpool {
                Self::expunge_impl(config, &mut self.counts);
            }
        }
    }
}

impl TryFrom<BlueprintZonesConfig> for ZonesEditor {
    type Error = DuplicateZoneId;

    fn try_from(config: BlueprintZonesConfig) -> Result<Self, Self::Error> {
        let mut zones = IndexMap::with_capacity(config.zones.len());
        for zone in config.zones {
            match zones.entry(zone.id) {
                Entry::Vacant(slot) => {
                    slot.insert(zone);
                }
                Entry::Occupied(prev) => {
                    return Err(DuplicateZoneId {
                        id: zone.id,
                        kind1: zone.zone_type.kind(),
                        kind2: prev.get().zone_type.kind(),
                    });
                }
            }
        }
        Ok(Self {
            generation: config.generation,
            zones,
            counts: EditCounts::zeroes(),
        })
    }
}
