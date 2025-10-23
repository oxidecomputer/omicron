// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Re-assign sagas from expunged Nexus zones

use nexus_db_model::SecId;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_types::deployment::{Blueprint, BlueprintZoneDisposition};
use omicron_common::api::external::Error;
use omicron_uuid_kinds::{GenericUuid, OmicronZoneUuid};
use slog::{debug, info, warn};

/// For each expunged Nexus zone in the same generation as the current Nexus,
/// re-assign sagas owned by that Nexus to the specified nexus (`nexus_id`).
///
/// Reassigning sagas in this way is how we ensure that that sagas complete even
/// when the Nexus that was running them fails permanently (leading to
/// expungement).
///
/// Reassignment of sagas assigned to any expunged Nexus nodes is a prerequisite
/// for Nexus handoff (during upgrade).  That's because in general, Nexus is
/// tightly coupled to saga implementations and it's not safe for different
/// versions of a Nexus to operate on the same saga (even at different times).
/// For more on this, see RFD 289.  As a result, we finish all sagas prior to
/// handing off from one version to the next.
///
/// Strictly speaking, it should not be required to limit re-assignment to Nexus
/// instances of the same generation.  As mentioned above, sagas do not survive
/// across generations, so the only sagas that exist ought to be from the same
/// generation that Nexus is running.  This is a belt-and-suspenders check put
/// in place because the impact of getting this wrong (running a saga created by
/// an older version) could be pretty bad.
///
/// We could use the Nexus image for this rather than the generation.
/// In practice, these should be equivalent here because we bump the Nexus
/// generation on deployed systems when (and only when) there's a new Nexus
/// image.  But handoff works in terms of generations -- that's the abstraction
/// we've created for "a group of Nexus instances that speak the same database
/// version and can exchange sagas".  So that's what we use here.
pub(crate) async fn reassign_sagas_from_expunged(
    opctx: &OpContext,
    datastore: &DataStore,
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Result<bool, Error> {
    let log = &opctx.log;

    let nexus_zone_ids = find_expunged_same_generation(blueprint, nexus_id)?;
    debug!(
        log,
        "re-assign sagas: found expunged Nexus instances with matching generation";
        "nexus_zone_ids" => ?nexus_zone_ids
    );

    let result =
        datastore.sagas_reassign_sec(opctx, &nexus_zone_ids, nexus_id).await;

    match result {
        Ok(count) => {
            info!(log, "re-assigned sagas";
                "nexus_zone_ids" => ?nexus_zone_ids,
                "count" => count,
            );

            Ok(count != 0)
        }
        Err(error) => {
            warn!(log, "failed to re-assign sagas";
                "nexus_zone_ids" => ?nexus_zone_ids,
                &error,
            );

            Err(error)
        }
    }
}

/// Returns the list of Nexus ids for expunged (and ready-to-cleanup) Nexus
/// zones in the same generation as the given Nexus id
fn find_expunged_same_generation(
    blueprint: &Blueprint,
    nexus_id: SecId,
) -> Result<Vec<SecId>, Error> {
    let nexus_zone_id = OmicronZoneUuid::from_untyped_uuid(nexus_id.0);
    let active_nexus_generation =
        blueprint.find_generation_for_self(nexus_zone_id)?;
    Ok(blueprint
        .all_nexus_zones(BlueprintZoneDisposition::is_ready_for_cleanup)
        .filter_map(|(_sled_id, zone_config, nexus_config)| {
            (nexus_config.nexus_generation == active_nexus_generation)
                .then_some(zone_config.id)
        })
        .map(|id| SecId(id.into_untyped_uuid()))
        .collect())
}

#[cfg(test)]
mod test {
    use super::*;
    use nexus_reconfigurator_planning::blueprint_builder::BlueprintBuilder;
    use nexus_reconfigurator_planning::example::ExampleSystemBuilder;
    use nexus_reconfigurator_planning::planner::PlannerRng;
    use nexus_types::deployment::BlueprintSource;
    use omicron_common::api::external::Generation;
    use omicron_test_utils::dev::test_setup_log;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    #[test]
    fn test_find_expunged_same_generation() {
        const TEST_NAME: &str = "test_find_expunged_same_generation";

        let logctx = test_setup_log(TEST_NAME);
        let log = &logctx.log;

        // To do an exhaustive test of `find_expunged_same_generation()`, we
        // want some expunged zones and some non-expunged zones in each of two
        // different generations.

        // Frst, create a basic blueprint with several Nexus zones in generation 1.
        let (example, blueprint1) =
            ExampleSystemBuilder::new(log, TEST_NAME).nexus_count(4).build();
        let g1 = Generation::new();
        let g1_nexus_ids: Vec<_> = blueprint1
            .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
            .map(|(sled_id, zone_config, nexus_config)| {
                assert_eq!(nexus_config.nexus_generation, g1);
                (sled_id, zone_config.id, zone_config.image_source.clone())
            })
            .collect();

        // Expunge two of these Nexus zones and mark them ready for cleanup
        // immediately.
        let (g1_expunge_ids, g1_keep_ids) = g1_nexus_ids.split_at(2);
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint1,
            &example.input,
            &example.collection,
            "test suite",
            PlannerRng::from_entropy(),
        )
        .expect("new blueprint builder");

        for (sled_id, expunge_id, _image_source) in g1_expunge_ids {
            builder
                .sled_expunge_zone(*sled_id, *expunge_id)
                .expect("expunge zone");
            builder
                .sled_mark_expunged_zone_ready_for_cleanup(
                    *sled_id,
                    *expunge_id,
                )
                .expect("mark zone for cleanup");
        }

        // Create the same number of Nexus zones in the next generation.
        // We'll use the same images.
        let g2 = g1.next();
        for (sled_id, _zone_id, image_source) in &g1_nexus_ids {
            builder
                .sled_add_zone_nexus(*sled_id, image_source.clone(), g2)
                .expect("add Nexus zone");
        }

        let blueprint2 = builder.build(BlueprintSource::Test);
        let g2_nexus_ids: Vec<_> = blueprint2
            .all_nexus_zones(BlueprintZoneDisposition::is_in_service)
            .filter_map(|(sled_id, zone_config, nexus_config)| {
                (nexus_config.nexus_generation == g2)
                    .then_some((sled_id, zone_config.id))
            })
            .collect();

        // Now expunge a few of those, too.  This time, only the first is going
        // to be marked ready for cleanup.
        let (g2_expunge_ids, g2_keep_ids) = g2_nexus_ids.split_at(2);
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &blueprint2,
            &example.input,
            &example.collection,
            "test suite",
            PlannerRng::from_entropy(),
        )
        .expect("new blueprint builder");

        let (sled_id, g2_expunged_cleaned_up) = g2_expunge_ids[0];
        builder
            .sled_expunge_zone(sled_id, g2_expunged_cleaned_up)
            .expect("expunge zone");
        builder
            .sled_mark_expunged_zone_ready_for_cleanup(
                g2_expunge_ids[0].0,
                g2_expunged_cleaned_up,
            )
            .expect("mark zone for cleanup");

        let (sled_id, g2_expunged_not_cleaned_up) = g2_expunge_ids[1];
        builder
            .sled_expunge_zone(sled_id, g2_expunged_not_cleaned_up)
            .expect("expunge zone");

        let blueprint3 = builder.build(BlueprintSource::Test);

        // Finally, we have:
        //
        // - g1_keep_ids:    two in-service Nexus zones in generation 1
        // - g1_expunge_ids: two expunged Nexus zones in generation 1,
        //                   both cleaned up
        // - g2_keep_ids:    two in-service Nexus zones in generation 2
        // - g2_expunge_ids: expunged Nexus zones in generation 2,
        //                   only the first of which is ready for cleanup
        //
        // Now we can exhaustively test various cases.

        // For the in-service zones in generation 1, we should find the expunged
        // zones in generation 1.
        let g1_matched: BTreeSet<SecId> = g1_expunge_ids
            .into_iter()
            .map(|(_sled_id, zone_id, _image_source)| {
                SecId(zone_id.into_untyped_uuid())
            })
            .collect();
        for (_sled_id, zone_id, _image_source) in g1_keep_ids {
            let matched = find_expunged_same_generation(
                &blueprint3,
                SecId(zone_id.into_untyped_uuid()),
            )
            .unwrap();
            assert_eq!(
                matched.into_iter().collect::<BTreeSet<_>>(),
                g1_matched
            );
        }

        // It should be impossible in a real system for the
        // expunged-and-ready-to-cleanup zones to execute this function.  Being
        // ready to cleanup means we know they're gone.  So there's nothing to
        // test here.

        // For the in-service zones in generation 2, we should find the
        // expunged-and-ready-for-cleanup zone in generation 2.
        let g2_matched = SecId(g2_expunged_cleaned_up.into_untyped_uuid());
        for (_sled_id, zone_id) in g2_keep_ids {
            let matched = find_expunged_same_generation(
                &blueprint3,
                SecId(zone_id.into_untyped_uuid()),
            )
            .unwrap();
            assert_eq!(matched.len(), 1);
            assert_eq!(matched[0], g2_matched);
        }

        // It is possible for the expunged and not-yet-ready-for-cleanup zone in
        // generation 2 to wind up calling this function.  It should not find
        // itself!
        let matched = find_expunged_same_generation(
            &blueprint3,
            SecId(g2_expunged_not_cleaned_up.into_untyped_uuid()),
        )
        .unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0], g2_matched);

        // Test the sole error case: if we cannot figure out which generation we
        // were given.
        let error =
            find_expunged_same_generation(&blueprint3, SecId(Uuid::new_v4()))
                .expect_err("made-up Nexus should not exist");
        assert!(matches!(error, Error::InternalError { internal_message }
            if internal_message.contains("did not find Nexus")));

        logctx.cleanup_successful();
    }
}
