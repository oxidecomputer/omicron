// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Editing Omicron zones for the blueprint planner

use nexus_sled_agent_shared::inventory::ZoneKind;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::BlueprintZoneType;
use nexus_types::deployment::BlueprintZonesConfig;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
pub enum EditZonesError {
    #[error(
        "attempted to add duplicate zone ID ({zone_id}) to sled {sled_id}: \
         new zone {new_kind:?} conflicts with old zone {old_kind:?}"
    )]
    AddDuplicateZoneId {
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
        new_kind: ZoneKind,
        old_kind: ZoneKind,
    },
    #[error("attempted to expunge unknown zone on sled {sled_id}: {zone_id}")]
    ExpungeUnknownZone { sled_id: SledUuid, zone_id: OmicronZoneUuid },
    #[error(
        "attempted to expunge zone {zone_id} (can only expunge unchanged zones)"
    )]
    ExpungeModifiedZone { zone_id: OmicronZoneUuid },
    #[error("sled {sled_id} has multiple internal NTP zones: {id1}, {id2}")]
    MultipleInternalNtpZones {
        sled_id: SledUuid,
        id1: OmicronZoneUuid,
        id2: OmicronZoneUuid,
    },
}

#[derive(Debug)]
pub(super) struct BlueprintZonesEditor {
    zones: BTreeMap<SledUuid, BlueprintZonesConfig>,
    changed_zones: BTreeMap<SledUuid, BTreeSet<OmicronZoneUuid>>,
}

impl BlueprintZonesEditor {
    pub fn new(zones: BTreeMap<SledUuid, BlueprintZonesConfig>) -> Self {
        Self { zones, changed_zones: BTreeMap::new() }
    }

    /// Iterates over the list of sled IDs for which we have zones.
    ///
    /// This may include decommissioned sleds.
    pub fn sled_ids_with_zones(&self) -> impl Iterator<Item = SledUuid> + '_ {
        self.zones.keys().copied()
    }

    /// Produces an owned map of zones for the sleds recorded in this blueprint
    /// plus any newly-added sleds
    pub fn into_zones_map(
        mut self,
        added_sled_ids: impl Iterator<Item = SledUuid>,
    ) -> BTreeMap<SledUuid, BlueprintZonesConfig> {
        for sled_id in added_sled_ids {
            self.zones.entry(sled_id).or_insert_with(|| BlueprintZonesConfig {
                generation: Generation::new(),
                zones: vec![],
            });
        }

        for (sled_id, _) in self.changed_zones {
            let zones_config = self.zones
                .get_mut(&sled_id)
                .expect("changed_zones keys are always present in zones");
            zones_config.generation = zones_config.generation.next();
        }

        self.zones
    }

    pub fn current_zones(
        &self,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = (SledUuid, &BlueprintZoneConfig)> {
        self.zones.iter().flat_map(move |(sled_id, zones_config)| {
            zones_config.zones.iter().filter_map(move |z| {
                z.disposition.matches(filter).then_some((*sled_id, z))
            })
        })
    }

    pub fn current_sled_zones(
        &self,
        sled_id: SledUuid,
        filter: BlueprintZoneFilter,
    ) -> impl Iterator<Item = &BlueprintZoneConfig> {
        use itertools::Either;

        match self.zones.get(&sled_id) {
            Some(zones_config) => Either::Left(
                zones_config
                    .zones
                    .iter()
                    .filter(move |z| z.disposition.matches(filter)),
            ),
            None => Either::Right(std::iter::empty()),
        }
    }

    /// If the specified sled has an internal NTP zone that passes the
    /// `BlueprintZoneFilter::ShouldBeRunning` filter, it is returned.
    ///
    /// # Errors
    ///
    /// Fails if this sled contains multiple internal NTP zones, which indicates
    /// a logic error in the planner.
    pub fn current_sled_running_internal_ntp_zone(
        &self,
        sled_id: SledUuid,
    ) -> Result<Option<&BlueprintZoneConfig>, EditZonesError> {
        let mut internal_ntp_zone_iter = self
            .current_sled_zones(sled_id, BlueprintZoneFilter::ShouldBeRunning)
            .filter(|config| {
                matches!(config.zone_type, BlueprintZoneType::InternalNtp(_))
            });

        let Some(internal_ntp_zone) = internal_ntp_zone_iter.next() else {
            return Ok(None);
        };

        // We should have at most one internal NTP zone.
        if let Some(another_ntp_zone) = internal_ntp_zone_iter.next() {
            return Err(EditZonesError::MultipleInternalNtpZones {
                sled_id,
                id1: internal_ntp_zone.id,
                id2: another_ntp_zone.id,
            });
        }

        Ok(Some(internal_ntp_zone))
    }

    fn zones_config_mut(
        &mut self,
        sled_id: SledUuid,
    ) -> &mut BlueprintZonesConfig {
        self.zones.entry(sled_id).or_insert_with(|| BlueprintZonesConfig {
            generation: Generation::new(),
            zones: Vec::new(),
        })
    }

    pub fn add_zone(
        &mut self,
        sled_id: SledUuid,
        zone: BlueprintZoneConfig,
    ) -> Result<(), EditZonesError> {
        let zones_config = self.zones_config_mut(sled_id);

        // Ensure we're not adding a new zone with an ID that collides with an
        // existing zone ID.
        if let Some(old) = zones_config.zones.iter().find(|z| z.id == zone.id) {
            return Err(EditZonesError::AddDuplicateZoneId {
                sled_id,
                zone_id: zone.id,
                new_kind: zone.zone_type.kind(),
                old_kind: old.zone_type.kind(),
            });
        }

        let new_zone_id = zone.id;
        zones_config.zones.push(zone);
        self.record_changed_zone(sled_id, new_zone_id);

        Ok(())
    }

    pub fn expunge_zone(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
    ) -> Result<(), EditZonesError> {
        // We shouldn't be trying to expunge zones that have also been changed
        // in this blueprint -- something went wrong in the planner logic.
        if let Some(changed_zones) = self.changed_zones.get(&sled_id) {
            if changed_zones.contains(&zone_id) {
                return Err(EditZonesError::ExpungeModifiedZone { zone_id });
            }
        }

        // If the zone is already expunged, we have nothing to do.
        let zones_config = self.zones_config_mut(sled_id);
        let zone =
            zones_config.zones.iter_mut().find(|z| z.id == zone_id).ok_or(
                EditZonesError::ExpungeUnknownZone { sled_id, zone_id },
            )?;
        if zone.disposition == BlueprintZoneDisposition::Expunged {
            return Ok(());
        }

        zone.disposition = BlueprintZoneDisposition::Expunged;
        self.record_changed_zone(sled_id, zone_id);

        Ok(())
    }

    fn record_changed_zone(
        &mut self,
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
    ) {
        self.changed_zones.entry(sled_id).or_default().insert(zone_id);
    }
}
