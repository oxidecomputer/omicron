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
use nexus_types::deployment::Policy;
use nexus_types::external_api::views::SledState;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Generation;
use slog::{info, warn, Logger};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::hash::Hash;
use uuid::Uuid;

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
        internal_dns_version: Generation,
        external_dns_version: Generation,
        policy: &'a Policy,
        creator: &str,
        // NOTE: Right now, we just assume that this is the latest inventory
        // collection.  See the comment on the corresponding field in `Planner`.
        inventory: &'a Collection,
    ) -> anyhow::Result<Planner<'a>> {
        let blueprint = BlueprintBuilder::new_based_on(
            &log,
            parent_blueprint,
            internal_dns_version,
            external_dns_version,
            policy,
            creator,
        )?;
        Ok(Planner { log, policy, blueprint, inventory })
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
        // The only thing this planner currently knows how to do is add services
        // to a sled that's missing them.  So let's see if we're in that case.

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
        let mut sleds_ineligible_for_services = BTreeSet::new();

        for (sled_id, sled_info) in &self.policy.sleds {
            // Decommissioned sleds don't get any services. (This is an
            // explicit match so that when more states are added, this fails to
            // compile.)
            match sled_info.state {
                SledState::Decommissioned => {
                    sleds_ineligible_for_services.insert(*sled_id);
                    continue;
                }
                SledState::Active => {}
            }

            // Check for an NTP zone.  Every sled should have one.  If it's not
            // there, all we can do is provision that one zone.  We have to wait
            // for that to succeed and synchronize the clock before we can
            // provision anything else.
            if self.blueprint.sled_ensure_zone_ntp(*sled_id)? == Ensure::Added {
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
                sleds_ineligible_for_services.insert(*sled_id);
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
                .get(sled_id)
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

        // We've now placed all the services that should always exist on all
        // sleds. Before moving on to make decisions about placing services that
        // are _not_ present on all sleds, check the provision state of all our
        // sleds so we can avoid any non-provisionable sleds under the
        // assumption that there is something amiss with them.
        sleds_ineligible_for_services.extend(
            self.policy.sleds.iter().filter_map(|(sled_id, sled_info)| {
                (!sled_info.is_eligible_for_discretionary_services())
                    .then_some(*sled_id)
            }),
        );

        self.ensure_correct_number_of_nexus_zones(
            &sleds_ineligible_for_services,
        )?;

        Ok(())
    }

    fn ensure_correct_number_of_nexus_zones(
        &mut self,
        sleds_ineligible_for_services: &BTreeSet<Uuid>,
    ) -> Result<(), Error> {
        // Bin every sled by the number of Nexus zones it currently has while
        // counting the total number of Nexus zones.
        let mut num_total_nexus = 0;
        let mut sleds_by_num_nexus: BTreeMap<usize, Vec<Uuid>> =
            BTreeMap::new();
        for &sled_id in self.policy.sleds.keys() {
            let num_nexus = self.blueprint.sled_num_nexus_zones(sled_id);
            num_total_nexus += num_nexus;

            // Only bin this sled if we're allowed to use it. If we have a sled
            // we're not allowed to use that's already running a Nexus (seems
            // fishy!), we counted its Nexus above but will ignore it here.
            if !sleds_ineligible_for_services.contains(&sled_id) {
                sleds_by_num_nexus.entry(num_nexus).or_default().push(sled_id);
            }
        }

        // TODO-correctness What should we do if we have _too many_ Nexus
        // instances? For now, just log it the number of zones any time we have
        // at least the minimum number.
        let nexus_to_add =
            self.policy.target_nexus_zone_count.saturating_sub(num_total_nexus);
        if nexus_to_add == 0 {
            info!(
                self.log, "sufficient Nexus zones exist in plan";
                "desired_count" => self.policy.target_nexus_zone_count,
                "current_count" => num_total_nexus,
            );
            return Ok(());
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
        let mut sleds_to_change: BTreeMap<Uuid, usize> = BTreeMap::new();

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

#[cfg(test)]
mod test {
    use super::Planner;
    use crate::blueprint_builder::test::verify_blueprint;
    use crate::blueprint_builder::test::DEFAULT_N_SLEDS;
    use crate::blueprint_builder::BlueprintBuilder;
    use crate::example::example;
    use crate::example::ExampleSystem;
    use crate::system::SledBuilder;
    use expectorate::assert_contents;
    use nexus_inventory::now_db_precision;
    use nexus_types::deployment::BlueprintZoneFilter;
    use nexus_types::external_api::views::SledPolicy;
    use nexus_types::external_api::views::SledProvisionPolicy;
    use nexus_types::external_api::views::SledState;
    use nexus_types::inventory::OmicronZoneType;
    use nexus_types::inventory::OmicronZonesFound;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;

    /// Runs through a basic sequence of blueprints for adding a sled
    #[test]
    fn test_basic_add_sled() {
        static TEST_NAME: &str = "planner_basic_add_sled";
        let logctx = test_setup_log(TEST_NAME);

        // For our purposes, we don't care about the DNS generations.
        let internal_dns_version = Generation::new();
        let external_dns_version = Generation::new();

        // Use our example inventory collection.
        let mut example =
            ExampleSystem::new(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // Build the initial blueprint.  We don't bother verifying it here
        // because there's a separate test for that.
        let blueprint1 =
            BlueprintBuilder::build_initial_from_collection_seeded(
                &example.collection,
                internal_dns_version,
                external_dns_version,
                &example.policy,
                "the_test",
                (TEST_NAME, "bp1"),
            )
            .expect("failed to create initial blueprint");
        verify_blueprint(&blueprint1);

        // Now run the planner.  It should do nothing because our initial
        // system didn't have any issues that the planner currently knows how to
        // fix.
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            internal_dns_version,
            external_dns_version,
            &example.policy,
            "no-op?",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint1.diff_sleds(&blueprint2);
        println!("1 -> 2 (expected no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);
        verify_blueprint(&blueprint2);

        // Now add a new sled.
        let new_sled_id = example.sled_rng.next_uuid();
        let _ =
            example.system.sled(SledBuilder::new().id(new_sled_id)).unwrap();
        let policy = example.system.to_policy().unwrap();

        // Check that the first step is to add an NTP zone
        let blueprint3 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint2,
            internal_dns_version,
            external_dns_version,
            &policy,
            "test: add NTP?",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp3"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint2.diff_sleds(&blueprint3);
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
            sled_zones.zones[0].config.zone_type,
            OmicronZoneType::InternalNtp { .. }
        ));
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);
        verify_blueprint(&blueprint3);

        // Check that with no change in inventory, the planner makes no changes.
        // It needs to wait for inventory to reflect the new NTP zone before
        // proceeding.
        let blueprint4 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            internal_dns_version,
            external_dns_version,
            &policy,
            "test: add nothing more",
            &example.collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp4"))
        .plan()
        .expect("failed to plan");
        let diff = blueprint3.diff_sleds(&blueprint4);
        println!("3 -> 4 (expected no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);
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
                            BlueprintZoneFilter::SledAgentPut
                        )
                }
            )
            .is_none());

        // Check that the next step is to add Crucible zones
        let blueprint5 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint3,
            internal_dns_version,
            external_dns_version,
            &policy,
            "test: add Crucible zones?",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp5"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint3.diff_sleds(&blueprint5);
        println!("3 -> 5 (expect Crucible zones):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_basic_add_sled_3_5.txt",
            &diff.display().to_string(),
        );
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
        assert_eq!(zones.len(), 10);
        for zone in &zones {
            if !zone.config.zone_type.is_crucible() {
                panic!("unexpectedly added a non-Crucible zone: {zone:?}");
            }
        }
        verify_blueprint(&blueprint5);

        // Check that there are no more steps.
        let blueprint6 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint5,
            internal_dns_version,
            external_dns_version,
            &policy,
            "test: no-op?",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp6"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint5.diff_sleds(&blueprint6);
        println!("5 -> 6 (expect no changes):\n{}", diff.display());
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        assert_eq!(diff.sleds_changed().count(), 0);
        verify_blueprint(&blueprint6);

        logctx.cleanup_successful();
    }

    /// Check that the planner will add more Nexus zones to a single sled, if
    /// needed
    #[test]
    fn test_add_multiple_nexus_to_one_sled() {
        static TEST_NAME: &str = "planner_add_multiple_nexus_to_one_sled";
        let logctx = test_setup_log(TEST_NAME);

        // For our purposes, we don't care about the DNS generations.
        let internal_dns_version = Generation::new();
        let external_dns_version = Generation::new();

        // Use our example inventory collection as a starting point, but strip
        // it down to just one sled.
        let (sled_id, collection, mut policy) = {
            let (mut collection, mut policy) =
                example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

            // Pick one sled ID to keep and remove the rest.
            let keep_sled_id =
                policy.sleds.keys().next().copied().expect("no sleds");
            policy.sleds.retain(|&k, _v| keep_sled_id == k);
            collection.sled_agents.retain(|&k, _v| keep_sled_id == k);
            collection.omicron_zones.retain(|&k, _v| keep_sled_id == k);

            assert_eq!(collection.sled_agents.len(), 1);
            assert_eq!(collection.omicron_zones.len(), 1);

            (keep_sled_id, collection, policy)
        };

        // Build the initial blueprint.
        let blueprint1 =
            BlueprintBuilder::build_initial_from_collection_seeded(
                &collection,
                internal_dns_version,
                external_dns_version,
                &policy,
                "the_test",
                (TEST_NAME, "bp1"),
            )
            .expect("failed to create initial blueprint");

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
                .filter(|z| z.config.zone_type.is_nexus())
                .count(),
            1
        );

        // Now run the planner.  It should add additional Nexus instances to the
        // one sled we have.
        policy.target_nexus_zone_count = 5;
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            internal_dns_version,
            external_dns_version,
            &policy,
            "add more Nexus",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint1.diff_sleds(&blueprint2);
        println!("1 -> 2 (added additional Nexus zones):\n{}", diff.display());
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        let mut sleds = diff.sleds_changed().collect::<Vec<_>>();
        assert_eq!(sleds.len(), 1);
        let (changed_sled_id, sled_changes) = sleds.pop().unwrap();
        assert_eq!(changed_sled_id, sled_id);
        assert_eq!(sled_changes.zones_removed().count(), 0);
        assert_eq!(sled_changes.zones_changed().count(), 0);
        let zones = sled_changes.zones_added().collect::<Vec<_>>();
        assert_eq!(zones.len(), policy.target_nexus_zone_count - 1);
        for zone in &zones {
            if !zone.config.zone_type.is_nexus() {
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

        // Use our example inventory collection as a starting point.
        let (collection, mut policy) =
            example(&logctx.log, TEST_NAME, DEFAULT_N_SLEDS);

        // Build the initial blueprint.
        let blueprint1 =
            BlueprintBuilder::build_initial_from_collection_seeded(
                &collection,
                Generation::new(),
                Generation::new(),
                &policy,
                "the_test",
                (TEST_NAME, "bp1"),
            )
            .expect("failed to create initial blueprint");

        // This blueprint should only have 3 Nexus zones: one on each sled.
        assert_eq!(blueprint1.blueprint_zones.len(), 3);
        for sled_config in blueprint1.blueprint_zones.values() {
            assert_eq!(
                sled_config
                    .zones
                    .iter()
                    .filter(|z| z.config.zone_type.is_nexus())
                    .count(),
                1
            );
        }

        // Now run the planner with a high number of target Nexus zones.
        policy.target_nexus_zone_count = 14;
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            Generation::new(),
            Generation::new(),
            &policy,
            "add more Nexus",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint1.diff_sleds(&blueprint2);
        println!("1 -> 2 (added additional Nexus zones):\n{}", diff.display());
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        let sleds = diff.sleds_changed().collect::<Vec<_>>();

        // All 3 sleds should get additional Nexus zones. We expect a total of
        // 11 new Nexus zones, which should be spread evenly across the three
        // sleds (two should get 4 and one should get 3).
        assert_eq!(sleds.len(), 3);
        let mut total_new_nexus_zones = 0;
        for (sled_id, sled_changes) in sleds {
            assert_eq!(sled_changes.zones_removed().count(), 0);
            assert_eq!(sled_changes.zones_changed().count(), 0);
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
                if !zone.config.zone_type.is_nexus() {
                    panic!("unexpectedly added a non-Nexus zone: {zone:?}");
                }
            }
        }
        assert_eq!(total_new_nexus_zones, 11);

        logctx.cleanup_successful();
    }

    /// Check that the planner will skip non-provisionable sleds when allocating
    /// extra Nexus zones
    #[test]
    fn test_nexus_allocation_skips_nonprovisionable_sleds() {
        static TEST_NAME: &str =
            "planner_nexus_allocation_skips_nonprovisionable_sleds";
        let logctx = test_setup_log(TEST_NAME);

        // Use our example inventory collection as a starting point.
        //
        // Request two extra sleds here so we test non-provisionable, expunged,
        // and decommissioned sleds. (When we add more kinds of
        // non-provisionable states in the future, we'll have to add more
        // sleds.)
        let (collection, mut policy) = example(&logctx.log, TEST_NAME, 5);

        // Build the initial blueprint.
        let blueprint1 =
            BlueprintBuilder::build_initial_from_collection_seeded(
                &collection,
                Generation::new(),
                Generation::new(),
                &policy,
                "the_test",
                (TEST_NAME, "bp1"),
            )
            .expect("failed to create initial blueprint");

        // This blueprint should only have 5 Nexus zones: one on each sled.
        assert_eq!(blueprint1.blueprint_zones.len(), 5);
        for sled_config in blueprint1.blueprint_zones.values() {
            assert_eq!(
                sled_config
                    .zones
                    .iter()
                    .filter(|z| z.config.zone_type.is_nexus())
                    .count(),
                1
            );
        }

        // Arbitrarily choose some of the sleds and mark them non-provisionable
        // in various ways.
        let mut sleds_iter = policy.sleds.iter_mut();

        let nonprovisionable_sled_id = {
            let (sled_id, resources) = sleds_iter.next().expect("no sleds");
            resources.policy = SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            };
            *sled_id
        };
        let expunged_sled_id = {
            let (sled_id, resources) = sleds_iter.next().expect("no sleds");
            resources.policy = SledPolicy::Expunged;
            *sled_id
        };
        let decommissioned_sled_id = {
            let (sled_id, resources) = sleds_iter.next().expect("no sleds");
            resources.state = SledState::Decommissioned;
            *sled_id
        };

        // Now run the planner with a high number of target Nexus zones. The
        // number (16) is chosen such that:
        //
        // * we start with 5 sleds
        // * we need to add 11 Nexus zones
        // * there are two sleds eligible for provisioning
        // * => 5 or 6 new Nexus zones per sled
        //
        // When the planner gets smarter about removing zones from expunged
        // and/or removed sleds, we'll have to adjust this number.
        policy.target_nexus_zone_count = 16;
        let blueprint2 = Planner::new_based_on(
            logctx.log.clone(),
            &blueprint1,
            Generation::new(),
            Generation::new(),
            &policy,
            "add more Nexus",
            &collection,
        )
        .expect("failed to create planner")
        .with_rng_seed((TEST_NAME, "bp2"))
        .plan()
        .expect("failed to plan");

        let diff = blueprint1.diff_sleds(&blueprint2);
        println!("1 -> 2 (added additional Nexus zones):\n{}", diff.display());
        assert_contents(
            "tests/output/planner_nonprovisionable_1_2.txt",
            &diff.display().to_string(),
        );
        assert_eq!(diff.sleds_added().count(), 0);
        assert_eq!(diff.sleds_removed().count(), 0);
        let sleds = diff.sleds_changed().collect::<Vec<_>>();

        // Only 2 of the 3 sleds should get additional Nexus zones. We expect a
        // total of 12 new Nexus zones, which should be spread evenly across the
        // two sleds (one gets 6 and the other gets 5), while the
        // non-provisionable sled should be unchanged.
        assert_eq!(sleds.len(), 2);
        let mut total_new_nexus_zones = 0;
        for (sled_id, sled_changes) in sleds {
            assert!(sled_id != nonprovisionable_sled_id);
            assert!(sled_id != expunged_sled_id);
            assert!(sled_id != decommissioned_sled_id);
            assert_eq!(sled_changes.zones_removed().count(), 0);
            assert_eq!(sled_changes.zones_changed().count(), 0);
            let zones = sled_changes.zones_added().collect::<Vec<_>>();
            match zones.len() {
                n @ (5 | 6) => {
                    total_new_nexus_zones += n;
                }
                n => {
                    panic!("unexpected number of zones added to {sled_id}: {n}")
                }
            }
            for zone in &zones {
                let OmicronZoneType::Nexus { .. } = zone.config.zone_type
                else {
                    panic!("unexpectedly added a non-Crucible zone: {zone:?}");
                };
            }
        }
        assert_eq!(total_new_nexus_zones, 11);

        logctx.cleanup_successful();
    }
}
