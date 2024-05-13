// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for generating Blueprints
//!
//! See crate-level documentation for details.

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::Ensure;
use crate::blueprint_builder::EnsureMultiple;
use crate::blueprint_builder::Error;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneDisposition;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::deployment::ZpoolFilter;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use omicron_uuid_kinds::SledUuid;
use slog::error;
use slog::{info, warn, Logger};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::Hash;

pub struct Planner<'a> {
    log: Logger,
    input: &'a PlanningInput,
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
        input: &'a PlanningInput,
        creator: &str,
        // NOTE: Right now, we just assume that this is the latest inventory
        // collection.  See the comment on the corresponding field in `Planner`.
        inventory: &'a Collection,
    ) -> anyhow::Result<Planner<'a>> {
        let blueprint = BlueprintBuilder::new_based_on(
            &log,
            parent_blueprint,
            input,
            creator,
        )?;
        Ok(Planner { log, input, blueprint, inventory })
    }

    /// Within tests, set a seeded RNG for deterministic results.
    ///
    /// This will ensure that tests that use this builder will produce the same
    /// results each time they are run.
    pub fn with_rng_seed<H: Hash>(mut self, seed: H) -> Self {
        // This is an owned builder because it is almost never going to be
        // conditional.
        self.blueprint.set_rng_seed(seed);
        self
    }

    pub fn plan(mut self) -> Result<Blueprint, Error> {
        self.do_plan()?;
        Ok(self.blueprint.build())
    }

    fn do_plan(&mut self) -> Result<(), Error> {
        // We perform planning in two loops: the first one turns expunged sleds
        // into expunged zones, and the second one adds services.

        self.do_plan_expunge()?;
        self.do_plan_add()?;
        self.do_plan_decommission()?;

        Ok(())
    }

    fn do_plan_decommission(&mut self) -> Result<(), Error> {
        // Check for any sleds that are currently commissioned but can be
        // decommissioned. Our gates for decommissioning are:
        //
        // 1. The policy indicates the sled has been removed (i.e., the policy
        //    is "expunged"; we may have other policies that satisfy this
        //    requirement in the future).
        // 2. All zones associated with the sled have been marked expunged.
        // 3. There are no instances assigned to this sled. This is blocked by
        //    omicron#4872, so today we omit this check entirely, as any sled
        //    that could be otherwise decommissioned that still has instances
        //    assigned to it needs support intervention for cleanup.
        // 4. All disks associated with the sled have been marked expunged. This
        //    happens implicitly when a sled is expunged, so is covered by our
        //    first check.
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::Commissioned)
        {
            // Check 1: look for sleds that are expunged.
            match (sled_details.policy, sled_details.state) {
                // If the sled is still in service, don't decommission it.
                (SledPolicy::InService { .. }, _) => continue,
                // If the sled is already decommissioned it... why is it showing
                // up when we ask for commissioned sleds? Warn, but don't try to
                // decommission it again.
                (SledPolicy::Expunged, SledState::Decommissioned) => {
                    error!(
                        self.log,
                        "decommissioned sled returned by \
                         SledFilter::Commissioned";
                        "sled_id" => %sled_id,
                    );
                    continue;
                }
                // The sled is expunged but not yet decommissioned; fall through
                // to check the rest of the criteria.
                (SledPolicy::Expunged, SledState::Active) => (),
            }

            // Check 2: have all this sled's zones been expunged? It's possible
            // we ourselves have made this change, which is fine.
            let all_zones_expunged =
                self.blueprint.current_sled_zones(sled_id).all(|zone| {
                    zone.disposition == BlueprintZoneDisposition::Expunged
                });

            // Check 3: Are there any instances assigned to this sled? See
            // comment above; while we wait for omicron#4872, we just assume
            // there are no instances running.
            let num_instances_assigned = 0;

            if all_zones_expunged && num_instances_assigned == 0 {
                self.blueprint
                    .set_sled_state(sled_id, SledState::Decommissioned);
            }
        }

        Ok(())
    }

    fn do_plan_expunge(&mut self) -> Result<(), Error> {
        let mut commissioned_sled_ids = BTreeSet::new();

        // Remove services from sleds marked expunged. We use
        // `SledFilter::Commissioned` and have a custom `needs_zone_expungement`
        // function that allows us to produce better errors.
        for (sled_id, sled_details) in
            self.input.all_sleds(SledFilter::Commissioned)
        {
            commissioned_sled_ids.insert(sled_id);

            // Does this sled need zone expungement based on the details?
            let Some(reason) =
                needs_zone_expungement(sled_details.state, sled_details.policy)
            else {
                continue;
            };

            // Perform the expungement.
            self.blueprint.expunge_all_zones_for_sled(sled_id, reason)?;
        }

        // Check for any decommissioned sleds (i.e., sleds for which our
        // blueprint has zones, but are not in the input sled list). Any zones
        // for decommissioned sleds must have already be expunged for
        // decommissioning to have happened; fail if we find non-expunged zones
        // associated with a decommissioned sled.
        for sled_id in self.blueprint.sled_ids_with_zones() {
            if !commissioned_sled_ids.contains(&sled_id) {
                let num_zones = self
                    .blueprint
                    .current_sled_zones(sled_id)
                    .filter(|zone| {
                        zone.disposition != BlueprintZoneDisposition::Expunged
                    })
                    .count();
                if num_zones > 0 {
                    return Err(
                        Error::DecommissionedSledWithNonExpungedZones {
                            sled_id,
                            num_zones,
                        },
                    );
                }
            }
        }

        Ok(())
    }

    fn do_plan_add(&mut self) -> Result<(), Error> {
        // Internal DNS is a prerequisite for bringing up all other zones.  At
        // this point, we assume that internal DNS (as a service) is already
        // functioning.  At some point, this function will have to grow the
        // ability to determine whether more internal DNS zones need to be
        // added and where they should go.  And the blueprint builder will need
        // to grow the ability to provision one.

        // After we make our initial pass through the sleds below to check for
        // zones every sled should have (NTP, Crucible), we'll start making
        // decisions about placing other service zones. We need to _exclude_ any
        // sleds for which we just added an NTP zone, as we won't be able to add
        // additional services to them until that NTP zone has been brought up.
        //
        // We will not mark sleds getting Crucible zones as ineligible; other
        // control plane service zones starting concurrently with Crucible zones
        // is fine.
        let mut sleds_waiting_for_ntp_zones = BTreeSet::new();

        for (sled_id, sled_resources) in
            self.input.all_sled_resources(SledFilter::InService)
        {
            // First, we need to ensure that sleds are using their expected
            // disks. This is necessary before we can allocate any zones.
            if self.blueprint.sled_ensure_disks(sled_id, &sled_resources)?
                == Ensure::Added
            {
                info!(
                    &self.log,
                    "altered physical disks";
                    "sled_id" => %sled_id
                );
                self.blueprint
                    .comment(&format!("sled {}: altered disks", sled_id));

                // Note that this doesn't actually need to short-circuit the
                // rest of the blueprint planning, as long as during execution
                // we send this request first.
            }

            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            if self.blueprint.sled_ensure_zone_ntp(sled_id)? == Ensure::Added {
                info!(
                    &self.log,
                    "found sled missing NTP zone (will add one)";
                    "sled_id" => %sled_id
                );
                self.blueprint
                    .comment(&format!("sled {}: add NTP zone", sled_id));
                // Don't make any other changes to this sled.  However, this
                // change is compatible with any other changes to other sleds,
                // so we can "continue" here rather than "break".
                sleds_waiting_for_ntp_zones.insert(sled_id);
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
                    "sled_id" => %sled_id,
                );
                continue;
            }

            // Every provisionable zpool on the sled should have a Crucible zone on it.
            let mut ncrucibles_added = 0;
            for zpool_id in sled_resources.all_zpools(ZpoolFilter::InService) {
                if self
                    .blueprint
                    .sled_ensure_zone_crucible(sled_id, *zpool_id)?
                    == Ensure::Added
                {
                    info!(
                        &self.log,
                        "found sled zpool missing Crucible zone (will add one)";
                        "sled_id" => ?sled_id,
                        "zpool_id" => ?zpool_id,
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
                continue;
            }
        }

        self.ensure_correct_number_of_nexus_zones(
            &sleds_waiting_for_ntp_zones,
        )?;

        Ok(())
    }

    fn ensure_correct_number_of_nexus_zones(
        &mut self,
        sleds_waiting_for_ntp_zone: &BTreeSet<SledUuid>,
    ) -> Result<(), Error> {
        // Count the number of Nexus zones on all in-service sleds. This will
        // include sleds that are in service but not eligible for new services,
        // but will not include sleds that have been expunged or decommissioned.
        let mut num_total_nexus = 0;
        for sled_id in self.input.all_sled_ids(SledFilter::InService) {
            let num_nexus = self.blueprint.sled_num_nexus_zones(sled_id);
            num_total_nexus += num_nexus;
        }

        // TODO-correctness What should we do if we have _too many_ Nexus
        // instances? For now, just log it the number of zones any time we have
        // at least the minimum number.
        let nexus_to_add = self
            .input
            .target_nexus_zone_count()
            .saturating_sub(num_total_nexus);
        if nexus_to_add == 0 {
            info!(
                self.log, "sufficient Nexus zones exist in plan";
                "desired_count" => self.input.target_nexus_zone_count(),
                "current_count" => num_total_nexus,
            );
            return Ok(());
        }

        // Now bin all the sleds which are eligible choices for a new Nexus zone
        // by their current Nexus zone count. Skip sleds with a policy/state
        // that should be eligible for Nexus but that don't yet have an NTP
        // zone.
        let mut sleds_by_num_nexus: BTreeMap<usize, Vec<SledUuid>> =
            BTreeMap::new();
        for sled_id in self
            .input
            .all_sled_ids(SledFilter::Discretionary)
            .filter(|sled_id| !sleds_waiting_for_ntp_zone.contains(sled_id))
        {
            let num_nexus = self.blueprint.sled_num_nexus_zones(sled_id);
            sleds_by_num_nexus.entry(num_nexus).or_default().push(sled_id);
        }

        // Ensure we have at least one sled on which we can add Nexus zones. If
        // we don't, we have nothing else to do. This isn't a hard error,
        // because we might be waiting for NTP on all eligible sleds (although
        // it would be weird, since we're presumably running from within Nexus
        // on some sled).
        if sleds_by_num_nexus.is_empty() {
            warn!(self.log, "want to add Nexus zones, but no eligible sleds");
            return Ok(());
        }

        // Build a map of sled -> new nexus zone count.
        let mut sleds_to_change: BTreeMap<SledUuid, usize> = BTreeMap::new();

        'outer: for _ in 0..nexus_to_add {
            // `sleds_by_num_nexus` is sorted by key already, and we want to
            // pick from the lowest-numbered bin. We can just loop over its
            // keys, expecting to stop on the first iteration, with the only
            // exception being when we've removed all the sleds from a bin.
            for (&num_nexus, sleds) in sleds_by_num_nexus.iter_mut() {
                // `sleds` contains all sleds with the minimum number of Nexus
                // zones. Pick one arbitrarily but deterministically.
                let Some(sled_id) = sleds.pop() else {
                    // We already drained this bin; move on.
                    continue;
                };

                // This insert might overwrite an old value for this sled (e.g.,
                // in the "we have 1 sled and need to add many Nexus instances
                // to it" case). That's fine.
                sleds_to_change.insert(sled_id, num_nexus + 1);

                // Put this sled back in our map, but now with one more Nexus.
                sleds_by_num_nexus
                    .entry(num_nexus + 1)
                    .or_default()
                    .push(sled_id);

                continue 'outer;
            }

            // This should be unreachable: it's only possible if we fail to find
            // a nonempty vec in `sleds_by_num_nexus`, and we checked above that
            // `sleds_by_num_nexus` is not empty.
            unreachable!("logic error finding sleds for Nexus");
        }

        // For each sled we need to change, actually do so.
        let mut total_added = 0;
        for (sled_id, new_nexus_count) in sleds_to_change {
            match self
                .blueprint
                .sled_ensure_zone_multiple_nexus(sled_id, new_nexus_count)?
            {
                EnsureMultiple::Added(n) => {
                    info!(
                        self.log, "will add {n} Nexus zone(s) to sled";
                        "sled_id" => %sled_id,
                    );
                    total_added += n;
                }
                // This is only possible if we asked the sled to ensure the same
                // number of zones it already has, but that's impossible based
                // on the way we built up `sleds_to_change`.
                EnsureMultiple::NotNeeded => unreachable!(
                    "sled on which we added Nexus zones did not add any"
                ),
            }
        }

        // Double check that we didn't make any arithmetic mistakes. If we've
        // arrived here, we think we've added the number of Nexus zones we
        // needed to.
        assert_eq!(
            total_added, nexus_to_add,
            "internal error counting Nexus zones"
        );

        Ok(())
    }
}

/// Returns `Some(reason)` if the sled needs its zones to be expunged,
/// based on the policy and state.
fn needs_zone_expungement(
    state: SledState,
    policy: SledPolicy,
) -> Option<ZoneExpungeReason> {
    match state {
        SledState::Active => {}
        SledState::Decommissioned => {
            // A decommissioned sled that still has resources attached to it is
            // an illegal state, but representable. If we see a sled in this
            // state, we should still expunge all zones in it, but parent code
            // should warn on it.
            return Some(ZoneExpungeReason::SledDecommissioned { policy });
        }
    }

    match policy {
        SledPolicy::InService { .. } => None,
        SledPolicy::Expunged => Some(ZoneExpungeReason::SledExpunged),
    }
}

/// The reason a sled's zones need to be expunged.
///
/// This is used only for introspection and logging -- it's not part of the
/// logical flow.
#[derive(Copy, Clone, Debug)]
pub(crate) enum ZoneExpungeReason {
    SledDecommissioned { policy: SledPolicy },
    SledExpunged,
}

#[cfg(test)]
mod test {
    use super::Planner;
    use crate::blueprint_builder::test::verify_blueprint;
    use crate::blueprint_builder::test::DEFAULT_N_SLEDS;
    use crate::example::example;
    use crate::example::ExampleSystem;
    use crate::system::SledBuilder;
    use chrono::NaiveDateTime;
    use chrono::TimeZone;
    use chrono::Utc;
    use expectorate::assert_contents;
    use nexus_inventory::now_db_precision;
    use nexus_types::deployment::blueprint_zone_type;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::deployment::BlueprintZoneType;
    use nexus_types::deployment::DiffSledModified;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledProvisionPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::OmicronZonesFound;
    use omicron_common::api::external::Generation;
    use omicron_common::disk::DiskIdentity;
    use omicron_test_utils::dev::test_setup_log;
    use omicron_uuid_kinds::PhysicalDiskUuid;
    use omicron_uuid_kinds::ZpoolUuid;
    use std::collections::HashMap;

    /// Runs through a basic sequence of blueprints for adding a sled
    #[test]
    fn test_basic_add_sled() {
        static TEST_NAME: &str = "planner_basic_add_sled";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system.
        let mut example =
            ExampleSystem::new(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);
        let blueprint1 = &example.blueprint;
        verify_blueprint(blueprint1);

        // Now run the planner.  It should do nothing because our initial
        // system didn't have any issues that the planner currently knows how to
        // fix.
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            blueprint1,
            &example.input,
            "no-op?",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(blueprint1).unwrap();
        println!("1 -> 2 (expected no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);
        verify_blueprint(&blueprint2);

        // Now add a new sled.
        let new_sled_id = example.sled_rng.next();
        let _ =
            example.system.sled(SledBuilder::new().id(new_sled_id)).unwrap();
        let input = example.system.to_planning_input_builder().unwrap().build();

        // Check that the first step is to add an NTP zone
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test: add NTP?",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp3"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff_since_blueprint(&blueprint2).unwrap();
        println!(
            "2 -> 3 (expect new NTP zone on new sled):\n{}",
            diff.display()
        );
        assert_contents(
            "tests/output/planner_basic_add_sled_2_3.txt",
            &diff.display().to_string(),
        );
        let sleds = diff.sleds_added().collect::<Vec<_>>();
        let (sled_id, sled_zones) = sleds[0];
        // We have defined elsewhere that the first generation contains no
        // zones.  So the first one with zones must be newer.  See
        // OmicronZonesConfig::INITIAL_GENERATION.
        assert!(sled_zones.generation > Generation::new());
        assert_eq!(sled_id, new_sled_id);
        assert_eq!(sled_zones.zones.len(), 1);
        assert!(matches!(
            sled_zones.zones[0].zone_type,
            BlueprintZoneType::InternalNtp(_),
        ));
        assert_eq!(diff.sleds_removed().len(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);
        verify_blueprint(&blueprint3);

        // Check that with no change in inventory, the planner makes no changes.
        // It needs to wait for inventory to reflect the new NTP zone before
        // proceeding.
        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &input,
            "test: add nothing more",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp4"))
        .plan()
        .expect("failed to plan");
        let diff = blueprint4.diff_since_blueprint(&blueprint3).unwrap();
        println!("3 -> 4 (expected no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);
        verify_blueprint(&blueprint4);

        // Now update the inventory to have the requested NTP zone.
        let mut collection = example.collection.clone();
        assert!(collection
            .omicron_zones
            .insert(
                new_sled_id,
                OmicronZonesFound {
                    time_collected: now_db_precision(),
                    source: String::from("test suite"),
                    sled_id: new_sled_id,
                    zones: blueprint4
                        .blueprint_zones
                        .get(&new_sled_id)
                        .expect("blueprint should contain zones for new sled")
                        .to_omicron_zones_config(
                            BlueprintZoneFilter::ShouldBeRunning
                        )
                }
            )
            .is_none());

        // Check that the next step is to add Crucible zones
        let blueprint5 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            &input,
            "test: add Crucible zones?",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp5"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint5.diff_since_blueprint(&blueprint3).unwrap();
        println!("3 -> 5 (expect Crucible zones):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_basic_add_sled_3_5.txt",
            &diff.display().to_string(),
        );
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        let sleds = diff.sleds_modified().collect::<Vec<_>>();
        assert_eq!(sleds.len(), 1);
        let (sled_id, sled_changes) = &sleds[0];
        assert_eq!(
            sled_changes.generation_after,
            sled_changes.generation_before.next()
        );
        assert_eq!(*sled_id, new_sled_id);
        assert_eq!(sled_changes.zones_removed().len(), 0);
        assert_eq!(sled_changes.zones_modified().count(), 0);
        let zones = sled_changes.zones_added().collect::<Vec<_>>();
        assert_eq!(zones.len(), 10);
        for zone in &zones {
            if !zone.zone_type.is_crucible() {
                panic!("unexpectedly added a non-Crucible zone: {zone:?}");
            }
        }
        verify_blueprint(&blueprint5);

        // Check that there are no more steps.
        let blueprint6 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint5,
            &input,
            "test: no-op?",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp6"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint6.diff_since_blueprint(&blueprint5).unwrap();
        println!("5 -> 6 (expect no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);
        verify_blueprint(&blueprint6);

        logctx.cleanup_successful();
    }

    /// Check that the planner will add more Nexus zones to a single sled, if
    /// needed
    #[test]
    fn test_add_multiple_nexus_to_one_sled() {
        static TEST_NAME: &str = "planner_add_multiple_nexus_to_one_sled";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point, but strip it down to just
        // one sled.
        let (sled_id, blueprint1, collection, input) = {
            let (mut collection, input, mut blueprint) =
                example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

            // Pick one sled ID to keep and remove the rest.
            let mut builder = input.into_builder();
            let keep_sled_id =
                builder.sleds().keys().next().copied().expect("no sleds");
            builder.sleds_mut().retain(|&k, _v| keep_sled_id == k);
            collection.sled_agents.retain(|&k, _v| keep_sled_id == k);
            collection.omicron_zones.retain(|&k, _v| keep_sled_id == k);

            assert_eq!(collection.sled_agents.len(), 1);
            assert_eq!(collection.omicron_zones.len(), 1);
            blueprint.blueprint_zones.retain(|k, _v| keep_sled_id == *k);

            (keep_sled_id, blueprint, collection, builder.build())
        };

        // This blueprint should only have 1 Nexus instance on the one sled we
        // kept.
        assert_eq!(blueprint1.blueprint_zones.len(), 1);
        assert_eq!(
            blueprint1
                .blueprint_zones
                .get(&sled_id)
                .expect("missing kept sled")
                .zones
                .iter()
                .filter(|z| z.zone_type.is_nexus())
                .count(),
            1
        );

        // Now run the planner.  It should add additional Nexus instances to the
        // one sled we have.
        let mut builder = input.into_builder();
        builder.policy_mut().target_nexus_zone_count = 5;
        let input = builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1).unwrap();
        println!("1 -> 2 (added additional Nexus zones):\n{}", diff.display());
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        let mut sleds = diff.sleds_modified().collect::<Vec<_>>();
        assert_eq!(sleds.len(), 1);
        let (changed_sled_id, sled_changes) = sleds.pop().unwrap();
        // TODO-cleanup use `TypedUuid` everywhere
        assert_eq!(changed_sled_id, sled_id);
        assert_eq!(sled_changes.zones_removed().len(), 0);
        assert_eq!(sled_changes.zones_modified().count(), 0);
        let zones = sled_changes.zones_added().collect::<Vec<_>>();
        assert_eq!(zones.len(), input.target_nexus_zone_count() - 1);
        for zone in &zones {
            if !zone.zone_type.is_nexus() {
                panic!("unexpectedly added a non-Nexus zone: {zone:?}");
            }
        }

        logctx.cleanup_successful();
    }

    /// Check that the planner will spread additional Nexus zones out across
    /// sleds as it adds them
    #[test]
    fn test_spread_additional_nexus_zones_across_sleds() {
        static TEST_NAME: &str =
            "planner_spread_additional_nexus_zones_across_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // This blueprint should only have 3 Nexus zones: one on each sled.
        assert_eq!(blueprint1.blueprint_zones.len(), 3);
        for sled_config in blueprint1.blueprint_zones.values() {
            assert_eq!(
                sled_config
                    .zones
                    .iter()
                    .filter(|z| z.zone_type.is_nexus())
                    .count(),
                1
            );
        }

        // Now run the planner with a high number of target Nexus zones.
        let mut builder = input.into_builder();
        builder.policy_mut().target_nexus_zone_count = 14;
        let input = builder.build();
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1).unwrap();
        println!("1 -> 2 (added additional Nexus zones):\n{}", diff.display());
        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        let sleds = diff.sleds_modified().collect::<Vec<_>>();

        // All 3 sleds should get additional Nexus zones. We expect a total of
        // 11 new Nexus zones, which should be spread evenly across the three
        // sleds (two should get 4 and one should get 3).
        assert_eq!(sleds.len(), 3);
        let mut total_new_nexus_zones = 0;
        for (sled_id, sled_changes) in sleds {
            assert_eq!(sled_changes.zones_removed().len(), 0);
            assert_eq!(sled_changes.zones_modified().count(), 0);
            let zones = sled_changes.zones_added().collect::<Vec<_>>();
            match zones.len() {
                n @ (3 | 4) => {
                    total_new_nexus_zones += n;
                }
                n => {
                    panic!("unexpected number of zones added to {sled_id}: {n}")
                }
            }
            for zone in &zones {
                if !zone.zone_type.is_nexus() {
                    panic!("unexpectedly added a non-Nexus zone: {zone:?}");
                }
            }
        }
        assert_eq!(total_new_nexus_zones, 11);

        logctx.cleanup_successful();
    }

    #[test]
    fn test_crucible_allocation_skips_nonprovisionable_disks() {
        static TEST_NAME: &str =
            "planner_crucible_allocation_skips_nonprovisionable_disks";
        let logctx = test_setup_log(TEST_NAME);

        // Create an example system with a single sled
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, 1);

        let mut builder = input.into_builder();

        // Avoid churning on the quantity of Nexus zones - we're okay staying at
        // one.
        builder.policy_mut().target_nexus_zone_count = 1;

        let new_sled_disk = |policy| nexus_types::deployment::SledDisk {
            disk_identity: DiskIdentity {
                vendor: "test-vendor".to_string(),
                serial: "test-serial".to_string(),
                model: "test-model".to_string(),
            },
            disk_id: PhysicalDiskUuid::new_v4(),
            policy,
            state: nexus_types::external_api::views::PhysicalDiskState::Active,
        };

        let (_, sled_details) = builder.sleds_mut().iter_mut().next().unwrap();

        // Inject some new disks into the input.
        //
        // These counts are arbitrary, as long as they're non-zero
        // for the sake of the test.

        const NEW_IN_SERVICE_DISKS: usize = 2;
        const NEW_EXPUNGED_DISKS: usize = 1;

        for _ in 0..NEW_IN_SERVICE_DISKS {
            sled_details.resources.zpools.insert(
                ZpoolUuid::new_v4(),
                new_sled_disk(nexus_types::external_api::views::PhysicalDiskPolicy::InService),
            );
        }
        for _ in 0..NEW_EXPUNGED_DISKS {
            sled_details.resources.zpools.insert(
                ZpoolUuid::new_v4(),
                new_sled_disk(nexus_types::external_api::views::PhysicalDiskPolicy::Expunged),
            );
        }

        let input = builder.build();

        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test: some new disks",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_since_blueprint(&blueprint1).unwrap();
        println!("1 -> 2 (some new disks, one expunged):\n{}", diff.display());
        let mut modified_sleds = diff.sleds_modified();
        assert_eq!(modified_sleds.len(), 1);
        let (_, diff_modified) = modified_sleds.next().unwrap();

        // We should be adding a Crucible zone for each new in-service disk.
        assert_eq!(diff_modified.zones_added().count(), NEW_IN_SERVICE_DISKS);
        assert_eq!(diff_modified.zones_removed().len(), 0);

        logctx.cleanup_successful();
    }

    /// Check that the planner will skip non-provisionable sleds when allocating
    /// extra Nexus zones
    #[test]
    fn test_nexus_allocation_skips_nonprovisionable_sleds() {
        static TEST_NAME: &str =
            "planner_nexus_allocation_skips_nonprovisionable_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        //
        // Request two extra sleds here so we test non-provisionable, expunged,
        // and decommissioned sleds. (When we add more kinds of
        // non-provisionable states in the future, we'll have to add more
        // sleds.)
        let (collection, input, mut blueprint1) =
            example(&logctx.log, TEST_NAME, 5);

        // This blueprint should only have 5 Nexus zones: one on each sled.
        assert_eq!(blueprint1.blueprint_zones.len(), 5);
        for sled_config in blueprint1.blueprint_zones.values() {
            assert_eq!(
                sled_config
                    .zones
                    .iter()
                    .filter(|z| z.zone_type.is_nexus())
                    .count(),
                1
            );
        }

        // Arbitrarily choose some of the sleds and mark them non-provisionable
        // in various ways.
        let mut builder = input.into_builder();
        let mut sleds_iter = builder.sleds_mut().iter_mut();

        let nonprovisionable_sled_id = {
            let (sled_id, details) = sleds_iter.next().expect("no sleds");
            details.policy = SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            };
            *sled_id
        };
        println!("1 -> 2: marked non-provisionable {nonprovisionable_sled_id}");
        let expunged_sled_id = {
            let (sled_id, details) = sleds_iter.next().expect("no sleds");
            details.policy = SledPolicy::Expunged;
            *sled_id
        };
        println!("1 -> 2: expunged {expunged_sled_id}");
        let decommissioned_sled_id = {
            let (sled_id, details) = sleds_iter.next().expect("no sleds");
            details.state = SledState::Decommissioned;

            // Decommissioned sleds can only occur if their zones have been
            // expunged, so lie and pretend like that already happened
            // (otherwise the planner will rightfully fail to generate a new
            // blueprint, because we're feeding it invalid inputs).
            for zone in
                &mut blueprint1.blueprint_zones.get_mut(sled_id).unwrap().zones
            {
                zone.disposition = BlueprintZoneDisposition::Expunged;
            }

            *sled_id
        };
        println!("1 -> 2: decommissioned {decommissioned_sled_id}");

        // Now run the planner with a high number of target Nexus zones. The
        // number (9) is chosen such that:
        //
        // * we start with 5 sleds with 1 Nexus each
        // * we take two sleds out of service (one expunged, one
        //   decommissioned), so we're down to 3 in-service Nexuses: we need to
        //   add 6 to get to the new policy target of 9
        // * of the remaining 3 sleds, only 2 are eligible for provisioning
        // * each of those 2 sleds should get exactly 3 new Nexuses
        builder.policy_mut().target_nexus_zone_count = 9;
        let input = builder.build();
        let mut blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        // Define a time_created for consistent output across runs.
        blueprint2.time_created =
            Utc.from_utc_datetime(&NaiveDateTime::UNIX_EPOCH);

        assert_contents(
            "tests/output/planner_nonprovisionable_bp2.txt",
            &blueprint2.display().to_string(),
        );

        let diff = blueprint2.diff_since_blueprint(&blueprint1).unwrap();
        println!(
            "1 -> 2 (added additional Nexus zones, take 2 sleds out of service):\n{}",
            diff.display()
        );
        assert_contents(
            "tests/output/planner_nonprovisionable_1_2.txt",
            &diff.display().to_string(),
        );

        // The expunged and decommissioned sleds should have had all zones be
        // marked as expunged. (Not removed! Just marked as expunged.)
        //
        // Note that at this point we're neither removing zones from the
        // blueprint nor marking sleds as decommissioned -- we still need to do
        // cleanup, and we aren't performing garbage collection on zones or
        // sleds at the moment.

        assert_eq!(diff.sleds_added().len(), 0);
        assert_eq!(diff.sleds_removed().len(), 0);
        let mut sleds = diff.sleds_modified().collect::<HashMap<_, _>>();

        let expunged_modified = sleds.remove(&expunged_sled_id).unwrap();
        assert_all_zones_expunged(&expunged_modified, "expunged sled");

        // Only 2 of the 3 remaining sleds (not the non-provisionable sled)
        // should get additional Nexus zones. We expect a total of 6 new Nexus
        // zones, which should be split evenly between the two sleds, while the
        // non-provisionable sled should be unchanged.
        assert_eq!(sleds.len(), 2);
        let mut total_new_nexus_zones = 0;
        for (sled_id, sled_changes) in sleds {
            assert!(sled_id != nonprovisionable_sled_id);
            assert!(sled_id != expunged_sled_id);
            assert!(sled_id != decommissioned_sled_id);
            assert_eq!(sled_changes.zones_removed().len(), 0);
            assert_eq!(sled_changes.zones_modified().count(), 0);
            let zones = sled_changes.zones_added().collect::<Vec<_>>();
            for zone in &zones {
                let BlueprintZoneType::Nexus(_) = zone.zone_type else {
                    panic!("unexpectedly added a non-Crucible zone: {zone:?}");
                };
            }
            if zones.len() == 3 {
                total_new_nexus_zones += 3;
            } else {
                panic!(
                    "unexpected number of zones added to {sled_id}: {}",
                    zones.len()
                );
            }
        }
        assert_eq!(total_new_nexus_zones, 6);

        // ---

        // Also poke at some of the config by hand; we'll use this to test out
        // diff output. This isn't a real blueprint, just one that we're
        // creating to test diff output.
        //
        // Some of the things we're testing here:
        //
        // * modifying zones
        // * removing zones
        // * removing sleds
        // * for modified sleds' zone config generation, both a bump and the
        //   generation staying the same (the latter should produce a warning)
        let mut blueprint2a = blueprint2.clone();

        enum NextCrucibleMutate {
            Modify,
            Remove,
            Done,
        }
        let mut next = NextCrucibleMutate::Modify;

        // Leave the non-provisionable sled's generation alone.
        let zones = &mut blueprint2a
            .blueprint_zones
            .get_mut(&nonprovisionable_sled_id)
            .unwrap()
            .zones;

        zones.retain_mut(|zone| {
            if let BlueprintZoneType::Nexus(blueprint_zone_type::Nexus {
                internal_address,
                ..
            }) = &mut zone.zone_type
            {
                // Change the internal address.
                let mut segments = internal_address.ip().segments();
                segments[0] = segments[0].wrapping_add(1);
                internal_address.set_ip(segments.into());
                true
            } else if let BlueprintZoneType::Crucible(_) = zone.zone_type {
                match next {
                    NextCrucibleMutate::Modify => {
                        zone.disposition = BlueprintZoneDisposition::Quiesced;
                        next = NextCrucibleMutate::Remove;
                        true
                    }
                    NextCrucibleMutate::Remove => {
                        next = NextCrucibleMutate::Done;
                        false
                    }
                    NextCrucibleMutate::Done => true,
                }
            } else if let BlueprintZoneType::InternalNtp(_) =
                &mut zone.zone_type
            {
                // Change the underlay IP.
                let mut segments = zone.underlay_address.segments();
                segments[0] += 1;
                zone.underlay_address = segments.into();
                true
            } else {
                true
            }
        });

        let expunged_zones =
            blueprint2a.blueprint_zones.get_mut(&expunged_sled_id).unwrap();
        expunged_zones.zones.clear();
        expunged_zones.generation = expunged_zones.generation.next();

        blueprint2a.blueprint_zones.remove(&decommissioned_sled_id);

        blueprint2a.external_dns_version =
            blueprint2a.external_dns_version.next();

        let diff = blueprint2a.diff_since_blueprint(&blueprint2).unwrap();
        println!("2 -> 2a (manually modified zones):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_nonprovisionable_2_2a.txt",
            &diff.display().to_string(),
        );

        // ---

        logctx.cleanup_successful();
    }

    fn assert_all_zones_expunged(modified: &DiffSledModified, desc: &str) {
        assert_eq!(
            modified.generation_before.next(),
            modified.generation_after,
            "for {desc}, generation should have been bumped"
        );

        assert_eq!(
            modified.zones_added().count(),
            0,
            "for {desc}, no zones should have been added to blueprint"
        );

        // A zone disposition going to expunged *does not* mean that the
        // zone is actually removed, i.e. `zones_removed` is still 0. Any
        // zone removal will be part of some future garbage collection
        // process that isn't currently defined.

        assert_eq!(
            modified.zones_removed().len(),
            0,
            "for {desc}, no zones should have been removed from blueprint"
        );

        // Run through all the common zones and ensure that all of them
        // have been marked expunged.
        for zone in modified.zones_modified() {
            assert_eq!(
                zone.zone_after.disposition,
                BlueprintZoneDisposition::Expunged,
                "for {desc}, zone {} should have been marked expunged",
                zone.zone_after.id
            );
        }
    }

    #[test]
    fn planner_decommissions_sleds() {
        static TEST_NAME: &str = "planner_decommissions_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example system as a starting point.
        let (collection, input, blueprint1) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // Expunge one of the sleds.
        let mut builder = input.into_builder();
        let expunged_sled_id = {
            let mut iter = builder.sleds_mut().iter_mut();
            let (sled_id, details) = iter.next().expect("at least one sled");
            details.policy = SledPolicy::Expunged;
            *sled_id
        };

        let input = builder.build();
        let mut blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            &input,
            "test_blueprint2",
            &collection,
        )
        .expect("created planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        // Define a time_created for consistent output across runs.
        blueprint2.time_created =
            Utc.from_utc_datetime(&NaiveDateTime::UNIX_EPOCH);

        assert_contents(
            "tests/output/planner_decommissions_sleds_bp2.txt",
            &blueprint2.display().to_string(),
        );
        let diff = blueprint2.diff_since_blueprint(&blueprint1).unwrap();
        println!("1 -> 2 (expunged {expunged_sled_id}):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_decommissions_sleds_1_2.txt",
            &diff.display().to_string(),
        );

        // All the zones of the expunged sled should be expunged, and the sled
        // itself should be decommissioned.
        assert!(blueprint2.blueprint_zones[&expunged_sled_id]
            .are_all_zones_expunged());
        assert_eq!(
            blueprint2.sled_state[&expunged_sled_id],
            SledState::Decommissioned
        );

        // Remove the now-decommissioned sled from the planning input.
        let mut builder = input.into_builder();
        builder.sleds_mut().remove(&expunged_sled_id);
        let input = builder.build();

        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            &input,
            "test_blueprint3",
            &collection,
        )
        .expect("created planner")
        .with_rng_seed((TEST_NAME, "bp3"))
        .plan()
        .expect("failed to plan");

        // There should be no changes to the blueprint; we don't yet garbage
        // collect zones, so we should still have the sled's expunged zones
        // (even though the sled itself is no longer present in the list of
        // commissioned sleds).
        let diff = blueprint3.diff_since_blueprint(&blueprint2).unwrap();
        println!(
            "2 -> 3 (decommissioned {expunged_sled_id}):\n{}",
            diff.display()
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_modified().count(), 0);
        assert_eq!(diff.sleds_unchanged().count(), DEFAULT_N_SLEDS);

        logctx.cleanup_successful();
    }
}
