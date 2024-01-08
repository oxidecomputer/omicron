// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Error;
use crate::blueprint_builder::SledInfo;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::inventory::Collection;
use slog::{info, Logger};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use uuid::Uuid;

pub struct Planner<'a> {
    log: Logger,
    parent_blueprint: &'a Blueprint,
    collection: &'a Collection,
    sleds: &'a BTreeMap<Uuid, SledInfo>,
    blueprint: BlueprintBuilder<'a>,
}

impl<'a> Planner<'a> {
    pub fn new_based_on(
        log: Logger,
        parent_blueprint: &'a Blueprint,
        collection: &'a Collection,
        sleds: &'a BTreeMap<Uuid, SledInfo>,
        zones_in_service: BTreeSet<Uuid>,
        creator: &str,
        reason: &str,
    ) -> Planner<'a> {
        let blueprint = BlueprintBuilder::new_based_on(
            parent_blueprint,
            sleds,
            zones_in_service,
            creator,
            reason,
        );
        Planner { log, parent_blueprint, collection, sleds, blueprint }
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        self.do_plan()?;
        Ok(self.blueprint.build())
    }

    fn do_plan(&mut self) -> Result<(), Error> {
        // The only thing this planner currently knows how to do is add services
        // to a sled that's missing them.  So let's see if we're in that case.
        for (sled_id, sled_info) in self.sleds {
            let sled_zones = self.parent_blueprint.omicron_zones.get(sled_id);

            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            let has_ntp = sled_zones
                .map(|found_zones| {
                    found_zones.zones.iter().any(|z| {
                        matches!(
                            z.zone_type,
                            OmicronZoneType::BoundaryNtp { .. }
                                | OmicronZoneType::InternalNtp { .. }
                        )
                    })
                })
                .unwrap_or(false);
            if !has_ntp {
                // XXX-dap maybe this should be
                // builder.sled_ensure_zone_internal_ntp() and it just does
                // nothing if it finds one?  That would eliminate the risk that
                // the builder is used improperly.
                info!(
                    &self.log,
                    "found sled missing NTP zone (will add one)";
                    "sled_id" => ?sled_id
                );
                self.blueprint.sled_add_zone_internal_ntp(*sled_id)?;
                // XXX-dap should we be willing to do make changes to multiple
                // sleds at a time?  If not, we should `break` here.  Here's
                // an example where if we did `break`, then two Nexuses that
                // were iterating over the sleds in different orders might
                // choose to work on two different sleds.
                continue;
            }

            // We have an NTP zone.  If time is not synchronized on this sled,
            // there's nothing more we can do right now.
            // XXX-dap add the timesync status to the sled agent inventory
            // In the meantime, we could make an API call to the sled agent to
            // see if time is synchronized.  Ugh but then we'd be async.
            // XXX-dap could we safely assume that if we have any other zones,
            // then we don't have to worry about timesync?  Related question:
            // what would happen if the sled was sync'd, _did_ have a bunch of
            // zones on it, then bounced, then we attempted to PUT even the same
            // set of zones to it before time sync had finished?  would sled
            // agent reject that request?  Or would it try to start some zones
            // before timesync had happened?  It seems like it should reject
            // that request.  And in that case, we _could_ safely assume here
            // that time was synchronized if we had any other zones?
            //
            // And actually, if the sled agent rejects requests to ensure
            // non-NTP zones before time sync has happened, can we just forget
            // about this consideration altogether, knowing that our attempt to
            // execute this blueprint will fail until timesync happens, at which
            // point it will succeed?
            // XXX-dap as I write this I realize that blueprints need to be
            // predicated on a previous (parent) blueprint, and they can only
            // become the current "target" if their parent is the current
            // target.  Otherwise: consider the case that we start with a sled
            // agent at generation 4, then make a blueprint with generation 5,
            // then start executing that blueprint, then somebody else makes a
            // different blueprint with generation 5, and then we start
            // executing _that_ blueprint?  but it's a different generation 5 --
            // disaster.  This implies that aside from the initial blueprint, a
            // blueprint A is dependent on some "parent" blueprint, and A
            // becomes invalid if some other blueprint B becomes the target
            // before A does.
            // A consequence of this is that you can never go "back" to an
            // earlier blueprint, since going back to an earlier state requires
            // writing a _new_ generation of everything that's _equivalent_ to
            // that earlier state.  And as long as that's the case, I wonder if
            // there's a lot less point to the idea that you can just have
            // blueprints floating around in the system because they'd be
            // frequently invalidated.  Maybe that's still useful though but the
            // key operation on them isn't "set the target to this blueprint"
            // but "make a new blueprint equivalent to this one and then make
            // that the target.

            // XXX-dap Anyway, at this point, assume the clock is synchronized.

            // Every zpool on the sled should have a Crucible zone on it.
            for zpool_name in &sled_info.zpools {
                let has_crucible_on_this_pool = sled_zones
                    .map(|found_zones| {
                        found_zones.zones.iter().any(|z| {
                            matches!(
                                &z.zone_type,
                                OmicronZoneType::Crucible { dataset, .. }
                                if dataset.pool_name == *zpool_name
                            )
                        })
                    })
                    .unwrap_or(false);
                if !has_crucible_on_this_pool {
                    self.blueprint
                        .sled_add_zone_crucible(*sled_id, zpool_name.clone())?;
                }
            }
        }

        Ok(())
    }
}
