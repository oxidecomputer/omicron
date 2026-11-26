// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Compares the zones in a blueprint with the zones that sled agents report in
//! inventory.

use nexus_types::deployment::BlueprintZoneConfig;
use nexus_types::deployment::BlueprintZoneImageSource;
use nexus_types::inventory::Collection;
use omicron_uuid_kinds::OmicronZoneUuid;
use sled_agent_types::inventory::ConfigReconcilerInventoryResult;
use sled_agent_types::inventory::OmicronZoneImageSource;
use sled_agent_types::inventory::ZoneKind;
use std::collections::BTreeMap;

/// Returns the given blueprint zones that are not yet reflected in the latest
/// reconciliation result in inventory
///
/// Callers are responsible for choosing which zones to check.  The planner
/// checks the in-service zones on commissioned sleds.
pub fn zones_not_yet_propagated_to_inventory<'a>(
    blueprint_zones: impl IntoIterator<Item = &'a BlueprintZoneConfig>,
    inventory: &'a Collection,
) -> Vec<ZonePropagationIncomplete<'a>> {
    let inventory_zones = inventory
        .all_reconciled_omicron_zones()
        .map(|(z, sa_result)| (z.id, (&z.image_source, sa_result)))
        .collect::<BTreeMap<_, _>>();

    blueprint_zones
        .into_iter()
        .filter_map(|zone| {
            let bp_image_source =
                OmicronZoneImageSource::from(zone.image_source.clone());
            let reason = match inventory_zones.get(&zone.id) {
                Some((
                    inv_image_source,
                    ConfigReconcilerInventoryResult::Ok,
                )) if *inv_image_source == &bp_image_source => {
                    // The inventory and blueprint image sources match -- this
                    // means that the zone is up-to-date.
                    None
                }
                Some((
                    inv_image_source,
                    ConfigReconcilerInventoryResult::Ok,
                )) => {
                    // The inventory and blueprint image sources differ.
                    Some(ZonePropagationStatus::ImageSourceMismatch {
                        bp_image_source: &zone.image_source,
                        inv_image_source,
                    })
                }
                Some((
                    inv_image_source,
                    ConfigReconcilerInventoryResult::Err { message },
                )) => {
                    // The inventory reports this zone but there was an error
                    // reconciling it (most likely an error starting the zone).
                    Some(ZonePropagationStatus::ReconciliationError {
                        bp_image_source: &zone.image_source,
                        inv_image_source,
                        message,
                    })
                }
                None => {
                    // The blueprint has a zone that inventory does not have.
                    Some(ZonePropagationStatus::MissingInInventory {
                        bp_image_source: &zone.image_source,
                    })
                }
            };
            reason.map(|reason| ZonePropagationIncomplete {
                zone_id: zone.id,
                zone_kind: zone.kind(),
                reason,
            })
        })
        .collect()
}

/// Describes how a zone's status differs between the blueprint and the sled
/// inventory
#[derive(Debug)]
pub struct ZonePropagationIncomplete<'a> {
    pub zone_id: OmicronZoneUuid,
    pub zone_kind: ZoneKind,
    pub reason: ZonePropagationStatus<'a>,
}

/// Why a blueprint zone is not yet reflected in inventory
#[derive(Debug)]
pub enum ZonePropagationStatus<'a> {
    /// The current blueprint and the sled inventory disagree about the image
    /// source for a zone.
    ///
    /// This can mean that the sled inventory is out-of-date, or that a
    /// different blueprint has been applied.
    ImageSourceMismatch {
        bp_image_source: &'a BlueprintZoneImageSource,
        inv_image_source: &'a OmicronZoneImageSource,
    },
    /// Although this zone appears in the blueprint, it does not exist on the
    /// sled's inventory.
    MissingInInventory { bp_image_source: &'a BlueprintZoneImageSource },
    /// The last reconciliation attempt for this zone failed.
    ReconciliationError {
        bp_image_source: &'a BlueprintZoneImageSource,
        inv_image_source: &'a OmicronZoneImageSource,
        message: &'a str,
    },
}
