// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! One stop shop for planner logic for expunging sleds, zones, disks, datasets and
//! anything else that needs expungement.

use crate::blueprint_builder::Error;
use crate::blueprint_builder::Operation;
use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::DiskFilter;
use nexus_types::external_api::views::SledPolicy;
// TODO: put this type in this module
use crate::planner::ZoneExpungeReason;
use anyhow::anyhow;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledDetails;
use nexus_types::deployment::SledFilter;
use nexus_types::inventory::Collection;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use std::collections::BTreeMap;

use crate::blueprint_builder;
use crate::blueprint_editor::SledEditor;

/// Keep track of everything that needs expungement in a single plan.
pub struct Expunger<'a> {
    log: slog::Logger,
    input: &'a PlanningInput,
    parent_blueprint: &'a Blueprint,
    latest_inventory: &'a Collection,
    sled_editors: &'a mut BTreeMap<SledUuid, SledEditor>,
}

impl<'a> Expunger<'a> {
    /// Create a new expunger that utilizes the sled editors to expunge
    /// resources that require it.
    ///
    /// There are several rules here, based on a hierarchy of resources.
    /// We make sure we uphold all the rules during any expungement.
    ///
    /// RULES:
    ///  Expunge sled - expunge disks, datasets, zones on that sled
    ///  Expunge disk - expunge datasets on that disk and any corresponding
    ///                 zones using those datasets
    ///  Expunge zone - expunge any datasets used by those zones
    pub fn new(
        log: slog::Logger,
        input: &'a PlanningInput,
        parent_blueprint: &'a Blueprint,
        latest_inventory: &'a Collection,
        sled_editors: &'a mut BTreeMap<SledUuid, SledEditor>,
    ) -> Expunger<'a> {
        Expunger {
            log,
            input,
            parent_blueprint,
            latest_inventory,
            sled_editors,
        }
    }

    /// Expunge all resources on commissioned sleds
    pub fn expunge_resources_on_all_commissioned_sleds(
        &mut self,
    ) -> Result<(), blueprint_builder::Error> {
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::Commissioned)
        {
            // Is the sled expunged via policy?
            //
            // If so we have to remove all resources.
            if sled_details.policy == SledPolicy::Expunged {
                self.remove_all_resources_for_sled(sled_id, sled_details)?;
            }

            // TODO: Expunge zones, disks, datasets for reasons other than sled
            // expungement
        }

        Ok(())
    }

    fn remove_all_resources_for_sled(
        &mut self,
        sled_id: SledUuid,
        sled_details: &SledDetails,
    ) -> Result<Vec<ExpungeOperation>, Error> {
        let log = self.log.new(o!(
            "sled_id" => sled_id.to_string(),
        ));
        info!(self.log, "Expunging all resources for sled {sled_id}");
        let editor = self.sled_editors.get_mut(&sled_id).ok_or_else(|| {
            Error::Planner(anyhow!(
                "tried to expunge zones for unknown sled {sled_id}"
            ))
        })?;

        // All zones that are running must be marked expunged
        //
        // We need the intermediate collection so we can call
        // `editor.expunge_zone` below without the borrow checker complaining.
        // Should we instead provide an editor method to expunge all zones
        // that should be running?
        let zones_to_expunge: Vec<_> = editor
            .zones(BlueprintZoneFilter::ShouldBeRunning)
            .map(|zone_config| zone_config.id)
            .collect();

        let mut operations = Vec::with_capacity(zones_to_expunge.len());

        // Expunge all in service
        for zone_id in zones_to_expunge {
            // We can't do this inside the loop due to the borrow checker.
            editor
                .expunge_zone(&zone_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;
            operations.push(ExpungeOperation::Zone {
                sled_id,
                zone_id,
                reason: ZoneExpungeReason::SledExpunged,
            });
        }

        // Expunge all disks on the sled. This will also expunge any datasets
        // not associated with zones, as those would have been expunged above.
        // TODO: What to do about tracking datset operations?
        for (_, sled_disk) in sled_details.resources.all_disks(DiskFilter::All)
        {
            let disk_id = sled_disk.disk_id;
            editor
                .expunge_disk(&disk_id)
                .map_err(|err| Error::SledEditError { sled_id, err })?;

            // This is slightly different from zones above, as we'll end up
            // tracking disks that may already have their disposition set to
            // expunged.
            operations.push(ExpungeOperation::Disk { sled_id, disk_id });
        }

        Ok(operations)
    }
}

pub enum ExpungeOperation {
    Zone {
        sled_id: SledUuid,
        zone_id: OmicronZoneUuid,
        reason: ZoneExpungeReason,
    },
    // TODO: reason
    Disk {
        sled_id: SledUuid,
        disk_id: PhysicalDiskUuid,
    },
    // TODO: reason
    Dataset {
        sled_id: SledUuid,
        dataset_id: DatasetUuid,
    },
}
