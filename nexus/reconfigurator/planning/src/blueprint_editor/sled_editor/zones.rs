// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::blueprint_builder::EditCounts;
use iddqd::IdOrdMap;
use iddqd::id_ord_map::Entry;
use nexus_types::deployment::BlueprintExpungedZoneAccessReason;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneImageSource;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::ZpoolUuid;
use sled_agent_types::inventory::ZoneKind;

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
    #[error(
        "tried to set image source for nonexistent zone {id} to {image_source:?}"
    )]
    SetImageSourceForNonexistentZone {
        id: OmicronZoneUuid,
        image_source: BlueprintZoneImageSource,
    },
}

#[derive(Debug)]
pub(super) struct ZonesEditor {
    incoming_sled_agent_generation: Generation,
    zones: IdOrdMap<BlueprintZoneConfig>,
    counts: EditCounts,
}

impl ZonesEditor {
    pub fn new(
        incoming_sled_agent_generation: Generation,
        zones: IdOrdMap<BlueprintZoneConfig>,
    ) -> Self {
        Self {
            incoming_sled_agent_generation,
            zones,
            counts: EditCounts::zeroes(),
        }
    }

    pub fn empty() -> Self {
        Self {
            incoming_sled_agent_generation: Generation::new(),
            zones: IdOrdMap::new(),
            counts: EditCounts::zeroes(),
        }
    }

    pub fn finalize(self) -> (IdOrdMap<BlueprintZoneConfig>, EditCounts) {
        (self.zones, self.counts)
    }

    pub fn edit_counts(&self) -> EditCounts {
        self.counts
    }

    /// Iterate over all in-service zones on this sled.
    pub fn in_service_zones(
        &self,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        // Danger note: this call has no danger of accessing expunged zones,
        // because we're filtering to in-service.
        self.danger_all_zones(BlueprintZoneDisposition::is_in_service)
    }

    /// Iterate over all zones on this sled that could be running; this includes
    /// in-service zones and expunged zones that are not yet ready for cleanup
    /// (because we haven't confirmed via inventory that they've been shut
    /// down).
    pub fn could_be_running_zones(
        &self,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        // Danger note: this may access expunged zones, but only if they're not
        // yet ready for cleanup. We don't need the caller to provide a `reason`
        // for that - we only need to track access reasons for cleanup purposes,
        // which only acts on ready-to-clean-up expunged zones.
        self.danger_all_zones(BlueprintZoneDisposition::could_be_running)
    }

    /// Iterate over the expunged zones on this sled.
    ///
    /// Like `Blueprint::expunged_zones()`, callers must specify a
    /// [`BlueprintExpungedZoneAccessReason`]. This allows us to statically
    /// track all uses of expunged zones, each of which we must account for in
    /// the planner's logic to permanently prune expunged zones from the
    /// blueprint.
    pub fn expunged_zones(
        &self,
        _reason: BlueprintExpungedZoneAccessReason,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        // Danger note: we will definitely access expunged zones, but the caller
        // has provided a known `_reason`.
        self.danger_all_zones(BlueprintZoneDisposition::is_expunged)
    }

    /// Iterate over all zones on this sled, regardless of whether they're
    /// in-service or expunged.
    ///
    /// Like [`Self::expunged_zones()`], callers are required to specify a
    /// reason to access expunged zones.
    ///
    /// The set of zones returned by this method is equivalent to the set of
    /// zones returned by chaining together calls to `Self::in_service_zones()`
    /// and `Self::expunged_zones(reason)`, but only iterates over the zones
    /// once.
    pub fn all_in_service_and_expunged_zones(
        &self,
        _reason: BlueprintExpungedZoneAccessReason,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        // Danger note: this call will definitely access expunged zones, but we
        // know the caller has provided a known reason to do so.
        self.danger_all_zones(BlueprintZoneDisposition::any)
    }

    /// Iterate over the [`BlueprintZoneConfig`] instances in the blueprint
    /// that match the provided filter, along with the associated sled id.
    ///
    /// This method is prefixed with `danger_` and is private because it allows
    /// the caller to potentially act on expunged zones without providing a
    /// reason for doing so. It should only be called by `in_service_zones()`
    /// and the helper methods that require callers to specify a
    /// [`BlueprintExpungedZoneAccessReason`] defined above.
    fn danger_all_zones<F>(
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

    /// Expunge a zone, returning `true` if the zone was expunged and `false` if
    /// the zone was already expunged, along with the updated zone config.
    pub fn expunge(
        &mut self,
        zone_id: &OmicronZoneUuid,
    ) -> Result<(bool, &BlueprintZoneConfig), ZonesEditError> {
        let mut config = self.zones.get_mut(zone_id).ok_or_else(|| {
            ZonesEditError::ExpungeNonexistentZone { id: *zone_id }
        })?;

        let did_expunge = Self::expunge_impl(
            &mut config,
            &mut self.counts,
            self.incoming_sled_agent_generation,
        );

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

    /// Set the image source for a zone, returning the old image source.
    pub fn set_zone_image_source(
        &mut self,
        zone_id: &OmicronZoneUuid,
        image_source: BlueprintZoneImageSource,
    ) -> Result<BlueprintZoneImageSource, ZonesEditError> {
        let mut config = self.zones.get_mut(zone_id).ok_or_else(|| {
            ZonesEditError::SetImageSourceForNonexistentZone {
                id: *zone_id,
                image_source: image_source.clone(),
            }
        })?;

        let old_image_source = config.image_source.clone();
        if old_image_source != image_source {
            self.counts.updated += 1;
        }
        config.image_source = image_source;

        Ok(old_image_source)
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
            let fs_is_on_zpool = config.filesystem_pool.id() == *zpool;
            let dd_is_on_zpool = config
                .zone_type
                .durable_zpool()
                .map_or(false, |pool| pool.id() == *zpool);
            if fs_is_on_zpool || dd_is_on_zpool {
                if Self::expunge_impl(
                    &mut config,
                    &mut self.counts,
                    self.incoming_sled_agent_generation,
                ) {
                    nexpunged += 1;
                }
            }
        }
        nexpunged
    }
}
