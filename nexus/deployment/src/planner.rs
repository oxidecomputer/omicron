// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Error;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::Policy;
use slog::{info, Logger};

pub struct Planner<'a> {
    log: Logger,
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
        Planner { log, policy, blueprint }
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        self.do_plan()?;
        Ok(self.blueprint.build())
    }

    fn do_plan(&mut self) -> Result<(), Error> {
        // The only thing this planner currently knows how to do is add services
        // to a sled that's missing them.  So let's see if we're in that case.

        // Internal DNS is a prerequisite for bringing up all other zones.  At
        // this point, we assume that internal DNS (as a service) is already
        // functioning.  At some point, this function will have to grow the
        // ability to determine whether more internal DNS zones need to be
        // added and where they should go.  And the blueprint builder will need
        // to grow the ability to provision one.

        for (sled_id, sled_info) in &self.policy.sleds {
            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            if self.blueprint.sled_ensure_zone_internal_ntp(*sled_id)? {
                info!(
                    &self.log,
                    "found sled missing NTP zone (will add one)";
                    "sled_id" => ?sled_id
                );
                self.blueprint
                    .comment(&format!("sled {}: add NTP zone", sled_id));
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                continue;
            }

            // Every zpool on the sled should have a Crucible zone on it.
            let mut ncrucibles_added = 0;
            for zpool_name in &sled_info.zpools {
                if self
                    .blueprint
                    .sled_ensure_zone_crucible(*sled_id, zpool_name.clone())?
                {
                    info!(
                        &self.log,
                        "found sled zpool missing Crucible zone (will add one)";
                        "sled_id" => ?sled_id,
                        "zpool_name" => ?zpool_name,
                    );
                    ncrucibles_added += 1;
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

#[cfg(test)]
mod test {
    use super::Planner;
    use crate::blueprint_builder::test::example;
    use crate::blueprint_builder::test::policy_add_sled;
    use crate::blueprint_builder::BlueprintBuilder;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use sled_agent_client::types::OmicronZoneType;

    /// Runs through a basic sequence of blueprints for adding a sled
    #[test]
    fn test_basic_add_sled() {
        let logctx = test_setup_log("planner_basic_add_sled");

        // Use our example inventory collection.
        let (collection, mut policy) = example();

        // Build the initial blueprint.  We don't bother verifying it here
        // because there's a separate test for that.
        let blueprint1 = BlueprintBuilder::build_initial_from_collection(
            &collection,
            &policy,
            "the_test",
        )
        .expect("failed to create initial blueprint");

        // Now run the planner.  It should do nothing because our initial
        // system didn't have any issues that the planner currently knows how to
        // fix.
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &policy,
            "no-op?",
        )
        .plan()
        .expect("failed to plan");

        let diff = blueprint1.diff(&blueprint2);
        println!("1 -> 2 (expected no changes):\n{}", diff);
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        // Now add a new sled.
        let new_sled_id =
            "7097f5b3-5896-4fff-bd97-63a9a69563a9".parse().unwrap();
        let _ = policy_add_sled(&mut policy, new_sled_id);

        // Check that the first step is to add an NTP zone
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &policy,
            "test: add NTP?",
        )
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff(&blueprint3);
        println!("2 -> 3 (expect new NTP zone on new sled):\n{}", diff,);
        let sleds = diff.sleds_added().collect::<Vec<_>>();
        let (sled_id, sled_zones) = sleds[0];
        // We have defined elsewhere that the first generation contains no
        // zones.  So the first one with zones must be newer.  See
        // OMICRON_ZONES_CONFIG_INITIAL_GENERATION.
        assert!(sled_zones.generation > Generation::new());
        assert_eq!(sled_id, new_sled_id);
        assert_eq!(sled_zones.zones.len(), 1);
        assert!(matches!(
            sled_zones.zones[0].zone_type,
            OmicronZoneType::InternalNtp { .. }
        ));
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        // Check that the next step is to add Crucible zones
        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &policy,
            "test: add Crucible zones?",
        )
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff(&blueprint4);
        println!("3 -> 4 (expect Crucible zones):\n{}", diff);
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        let sleds = diff.sleds_changed().collect::<Vec<_>>();
        assert_eq!(sleds.len(), 1);
        let (sled_id, sled_changes) = &sleds[0];
        assert_eq!(
            sled_changes.generation_after,
            sled_changes.generation_before.next()
        );
        assert_eq!(*sled_id, new_sled_id);
        assert_eq!(sled_changes.zones_removed().count(), 0);
        assert_eq!(sled_changes.zones_changed().count(), 0);
        let zones = sled_changes.zones_added().collect::<Vec<_>>();
        assert_eq!(zones.len(), 3);
        for zone in &zones {
            let OmicronZoneType::Crucible { .. } = zone.zone_type else {
                panic!("unexpectedly added a non-Crucible zone");
            };
        }

        // Check that there are no more steps
        let blueprint5 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint4,
            &policy,
            "test: no-op?",
        )
        .plan()
        .expect("failed to plan");

        let diff = blueprint4.diff(&blueprint5);
        println!("4 -> 5 (expect no changes):\n{}", diff);
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        logctx.cleanup_successful();
    }
}
