// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use illumos_utils::zpool::ZpoolName;
use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::id_map::Entry;
use nexus_types::deployment::id_map::IdMap;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
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
    #[error("tried to mark a nonexistent zone as ready for cleanup: {id}")]
    MarkNonexistentZoneReadyForCleanup { id: OmicronZoneUuid },
    #[error("tried to mark a non-expunged zone as ready for cleanup: {id}")]
    MarkNonExpungedZoneReadyForCleanup { id: OmicronZoneUuid },
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
    zones: IdMap<BlueprintZoneConfig>,
    counts: EditCounts,
}

impl ZonesEditor {
    pub fn empty() -> Self {
        Self {
            generation: Generation::new(),
            zones: IdMap::new(),
            counts: EditCounts::zeroes(),
        }
    }

    pub fn finalize(self) -> (BlueprintZonesConfig, EditCounts) {
        let mut generation = self.generation;
        if self.counts.has_nonzero_counts() {
            generation = generation.next();
        }
        let config = BlueprintZonesConfig { generation, zones: self.zones };
        (config, self.counts)
    }

    pub fn edit_counts(&self) -> EditCounts {
        self.counts
    }

    pub fn zones<F>(
        &self,
        mut filter: F,
    ) -> impl Iterator<Item = &BlueprintZoneConfig>
    where
        F: FnMut(BlueprintZoneDisposition) -> bool,
    {
        self.zones.iter().filter(move |config| filter(config.disposition))
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

    /// Temporary method to backfill `filesystem_pool` properties for existing
    /// zones.
    ///
    /// # Panics
    ///
    /// Panics if called with a nonexistent zone_id. This is not meant to be a
    /// general-purpose method and should be removed entirely once all deployed
    /// systems have this property backfilled successfully.
    pub(super) fn backfill_filesystem_pool(
        &mut self,
        zone_id: OmicronZoneUuid,
        filesystem_pool: ZpoolName,
    ) {
        let Some(mut zone) = self.zones.get_mut(&zone_id) else {
            panic!(
                "backfill_filesystem_pool called with \
                 nonexistent zone id {zone_id}"
            );
        };

        if zone.filesystem_pool.as_ref() != Some(&filesystem_pool) {
            zone.filesystem_pool = Some(filesystem_pool);
            self.counts.updated += 1;
        }
    }

    /// Expunge a zone, returning `true` if the zone was expunged and `false` if
    /// the zone was already expunged, along with the updated zone config.
    pub fn expunge(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<(bool, &BlueprintZoneConfig), ZonesEditError> {
        let mut config = self.zones.get_mut(zone_id).ok_or_else(|| {
            ZonesEditError::ExpungeNonexistentZone { id: *zone_id }
        })?;

        let did_expunge =
            Self::expunge_impl(&mut config, &mut self.counts, self.generation);

        Ok((did_expunge, config.into_ref()))
    }

    /// Set an expunged zone's `ready_for_cleanup` flag to true.
    ///
    /// Unlike most edit operations, this (alone) will not result in an
    /// increased generation when `finalize()` is called: this flag is produced
    /// and consumed inside the Reconfigurator system, and is not included in
    /// the generation-guarded config send to sled-agents.
    ///
    /// # Errors
    ///
    /// Fails if this zone ID does not exist or is not already in the expunged
    /// disposition.
    pub fn mark_expunged_zone_ready_for_cleanup(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<bool, ZonesEditError> {
        let mut config = self.zones.get_mut(zone_id).ok_or_else(|| {
            ZonesEditError::MarkNonexistentZoneReadyForCleanup { id: *zone_id }
        })?;

        match &mut config.disposition {
            BlueprintZoneDisposition::InService => {
                Err(ZonesEditError::MarkNonExpungedZoneReadyForCleanup {
                    id: *zone_id,
                })
            }
            BlueprintZoneDisposition::Expunged {
                ready_for_cleanup, ..
            } => {
                let did_mark_ready = !*ready_for_cleanup;
                *ready_for_cleanup = true;
                Ok(did_mark_ready)
            }
        }
    }

    fn expunge_impl(
        config: &mut BlueprintZoneConfig,
        counts: &mut EditCounts,
        current_generation: Generation,
    ) -> bool {
        match config.disposition {
            BlueprintZoneDisposition::InService => {
                config.disposition = BlueprintZoneDisposition::Expunged {
                    as_of_generation: current_generation.next(),
                    ready_for_cleanup: false,
                };
                counts.expunged += 1;
                true
            }
            BlueprintZoneDisposition::Expunged { .. } => {
                // expunge is idempotent; do nothing
                false
            }
        }
    }

    pub fn expunge_all_on_zpool(&mut self, zpool: &ZpoolUuid) -> usize {
        let mut nexpunged = 0;
        for mut config in self.zones.iter_mut() {
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
                if Self::expunge_impl(
                    &mut config,
                    &mut self.counts,
                    self.generation,
                ) {
                    nexpunged += 1;
                }
            }
        }
        nexpunged
    }
}

impl From<BlueprintZonesConfig> for ZonesEditor {
    fn from(config: BlueprintZonesConfig) -> Self {
        Self {
            generation: config.generation,
            zones: config.zones,
            counts: EditCounts::zeroes(),
        }
    }
}
