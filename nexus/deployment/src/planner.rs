// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Error;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::OmicronZoneType;
use nexus_types::deployment::Policy;
use slog::{info, Logger};

pub struct Planner<'a> {
    log: Logger,
    parent_blueprint: &'a Blueprint,
    policy: &'a Policy,
    blueprint: BlueprintBuilder<'a>,
}

impl<'a> Planner<'a> {
    pub fn new_based_on(
        log: Logger,
        parent_blueprint: &'a Blueprint,
        policy: &'a Policy,
        creator: &str,
    ) -> Planner<'a> {
        let blueprint =
            BlueprintBuilder::new_based_on(parent_blueprint, policy, creator);
        Planner { log, parent_blueprint, policy, blueprint }
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        self.do_plan()?;
        Ok(self.blueprint.build())
    }

    fn do_plan(&mut self) -> Result<(), Error> {
        // The only thing this planner currently knows how to do is add services
        // to a sled that's missing them.  So let's see if we're in that case.
        for (sled_id, sled_info) in &self.policy.sleds {
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
                self.blueprint
                    .comment(&format!("sled {}: add NTP zone", sled_id));
                self.blueprint.sled_add_zone_internal_ntp(*sled_id)?;

                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                continue;
            }

            // Every zpool on the sled should have a Crucible zone on it.
            let mut ncrucibles_added = 0;
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
                    info!(
                        &self.log,
                        "found sled zpool missing Crucible zone (will add one)";
                        "sled_id" => ?sled_id,
                        "zpool_name" => ?zpool_name,
                    );
                    ncrucibles_added += 1;
                    self.blueprint
                        .sled_add_zone_crucible(*sled_id, zpool_name.clone())?;
                }
            }

            if ncrucibles_added > 0 {
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                // (Yes, it's currently the last thing in the loop, but being
                // explicit here means we won't forget to do this when more code
                // is added below.)
                self.blueprint.comment(&format!("sled {}: add zones", sled_id));
            }
        }

        Ok(())
    }
}
