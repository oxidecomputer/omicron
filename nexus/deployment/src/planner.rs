// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Ensure;
use crate::blueprint_builder::Error;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::Policy;
use nexus_types::inventory::Collection;
use slog::{info, Logger};

pub struct Planner<'a> {
    log: Logger,
    policy: &'a Policy,
    blueprint: BlueprintBuilder<'a>,
    // latest inventory collection
    //
    // We must be very careful when using this during planning.  The real-world
    // state may have changed since this inventory was collected.  Planning
    // choices should not depend on this still being totally accurate.
    //
    // If we do start depending on specific criteria (e.g., it contains
    // information about all sleds that we expect), we should verify that up
    // front and update callers to ensure that it's true.
    inventory: &'a Collection,
}

impl<'a> Planner<'a> {
    pub fn new_based_on(
        log: Logger,
        parent_blueprint: &'a Blueprint,
        policy: &'a Policy,
        creator: &str,
        // NOTE: Right now, we just assume that this is the latest inventory
        // collection.  See the comment on the corresponding field in `Planner`.
        inventory: &'a Collection,
    ) -> anyhow::Result<Planner<'a>> {
        let blueprint =
            BlueprintBuilder::new_based_on(parent_blueprint, policy, creator)?;
        Ok(Planner { log, policy, blueprint, inventory })
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
            if self.blueprint.sled_ensure_zone_ntp(*sled_id)? == Ensure::Added {
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

            // Now we've established that the current blueprint _says_ there's
            // an NTP zone on this system.  But we must wait for it to actually
            // be there before we can proceed to add anything else.  Otherwise,
            // we may wind up trying to provision this zone at the same time as
            // other zones, and Sled Agent will reject requests to provision
            // other zones before the clock is synchronized.
            //
            // Note that it's always possible that the NTP zone was added after
            // this inventory was collected (in which case we'll erroneously
            // choose to bail out here, but we'll pick it up again next time
            // we're invoked).  It's conceivable that the NTP zone was removed
            // after this inventory was collected (in which case we'd be making
            // a wrong decision here).  However, we don't ever do this today.
            // If we were to do something like that (maybe as part of upgrading
            // the NTP zone or switching between an internal NTP vs. boundary
            // NTP zone), we'll need to be careful how we do it to avoid a
            // problem here.
            let has_ntp_inventory = self
                .inventory
                .omicron_zones
                .get(&sled_id)
                .map(|sled_zones| {
                    sled_zones.zones.zones.iter().any(|z| z.zone_type.is_ntp())
                })
                .unwrap_or(false);
            if !has_ntp_inventory {
                info!(
                    &self.log,
                    "parent blueprint contains NTP zone, but it's not in \
                    inventory yet";
                    "sled_id" => ?sled_id,
                );
                continue;
            }

            // Every zpool on the sled should have a Crucible zone on it.
            let mut ncrucibles_added = 0;
            for zpool_name in &sled_info.zpools {
                if self
                    .blueprint
                    .sled_ensure_zone_crucible(*sled_id, zpool_name.clone())?
                    == Ensure::Added
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

            // TODO XXX To test Nexus zone addition, attempt to ensure every
            // sled has a Nexus zone. This is NOT a sensible policy, and should
            // be removed before this lands on `main`.
            //
            // Adding a new Nexus currently requires the parent blueprint to
            // have had at least one Nexus. By trying to ensure a Nexus on every
            // sled, that implicitly requires every parent_blueprint we accept
            // to have at least one Nexus in order to successfully call this
            // method. That should be true in production and is currently true
            // for all tests, but might be overly restrictive? We could change
            // this to "if at least one Nexus exists, add a Nexus on every
            // sled", but that seems like overkill for what should be a very
            // short-lived policy.
            //
            // Ensuring a Nexus zone exists will also fail if there are no
            // external IP addresses left. We should take that into account with
            // whatever real logic we put here, too.
            let nexus_added =
                match self.blueprint.sled_ensure_zone_nexus(*sled_id)? {
                    Ensure::Added => true,
                    Ensure::NotNeeded => false,
                };

            if ncrucibles_added > 0 || nexus_added {
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                // (Yes, it's currently the last thing in the loop, but being
                // explicit here means we won't forget to do this when more code
                // is added below.)
                self.blueprint.comment(&format!("sled {}: add zones", sled_id));
                continue;
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
    use nexus_inventory::now_db_precision;
    use nexus_types::inventory::OmicronZoneType;
    use nexus_types::inventory::OmicronZonesFound;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;

    /// Runs through a basic sequence of blueprints for adding a sled
    #[test]
    fn test_basic_add_sled() {
        let logctx = test_setup_log("planner_basic_add_sled");

        // Use our example inventory collection.
        let (mut collection, mut policy) = example();

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
            &collection,
        )
        .expect("failed to create planner")
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
            &collection,
        )
        .expect("failed to create planner")
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff(&blueprint3);
        println!("2 -> 3 (expect new NTP zone on new sled):\n{}", diff);
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

        // Check that with no change in inventory, the planner makes no changes.
        // It needs to wait for inventory to reflect the new NTP zone before
        // proceeding.
        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &policy,
            "test: add nothing more",
            &collection,
        )
        .expect("failed to create planner")
        .plan()
        .expect("failed to plan");
        let diff = blueprint3.diff(&blueprint4);
        println!("3 -> 4 (expected no changes):\n{}", diff);
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        // Now update the inventory to have the requested NTP zone.
        assert!(collection
            .omicron_zones
            .insert(
                new_sled_id,
                OmicronZonesFound {
                    time_collected: now_db_precision(),
                    source: String::from("test suite"),
                    sled_id: new_sled_id,
                    zones: blueprint4
                        .omicron_zones
                        .get(&new_sled_id)
                        .cloned()
                        .expect("blueprint should contain zones for new sled"),
                }
            )
            .is_none());

        // Check that the next step is to add Crucible zones
        let blueprint5 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &policy,
            "test: add Crucible zones?",
            &collection,
        )
        .expect("failed to create planner")
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff(&blueprint5);
        println!("3 -> 5 (expect 3 Crucible and 1 Nexus zones):\n{}", diff);
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
        let mut num_crucible_added = 0;
        let mut num_nexus_added = 0;
        for zone in &zones {
            match zone.zone_type {
                OmicronZoneType::Crucible { .. } => {
                    num_crucible_added += 1;
                }
                OmicronZoneType::Nexus { .. } => {
                    num_nexus_added += 1;
                }
                _ => panic!("unexpectedly added a non-Crucible zone: {zone:?}"),
            }
        }
        assert_eq!(num_crucible_added, 3);
        assert_eq!(num_nexus_added, 1);

        // Check that there are no more steps.
        //
        // This also implicitly checks that the new Nexus zone added in
        // blueprint4 did not reuse any resources from the existing Nexus zones
        // (otherwise creation of the planner would fail due to an invalid
        // parent blueprint).
        let blueprint6 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint5,
            &policy,
            "test: no-op?",
            &collection,
        )
        .expect("failed to create planner")
        .plan()
        .expect("failed to plan");

        let diff = blueprint5.diff(&blueprint6);
        println!("5 -> 6 (expect no changes):\n{}", diff);
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);

        logctx.cleanup_successful();
    }
}
