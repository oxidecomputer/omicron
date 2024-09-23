// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use crate::blueprint_builder::BlueprintBuilder;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::inventory::Collection;
use omicron_uuid_kinds::SledKind;
use typed_rng::TypedUuidRng;

pub struct ExampleSystem {
    pub system: SystemDescription,
    pub input: PlanningInput,
    pub collection: Collection,
    pub blueprint: Blueprint,
    // If we add more types of RNGs than just sleds here, we'll need to
    // expand this to be similar to BlueprintBuilderRng where a root RNG
    // creates sub-RNGs.
    //
    // This is currently only used for tests, so it looks unused in normal
    // builds.  But in the future it could be used by other consumers, too.
    #[allow(dead_code)]
    pub(crate) sled_rng: TypedUuidRng<SledKind>,
}

impl ExampleSystem {
    pub fn new(
        log: &slog::Logger,
        test_name: &str,
        nsleds: usize,
    ) -> ExampleSystem {
        let mut system = SystemDescription::new();
        let mut sled_rng = TypedUuidRng::from_seed(test_name, "ExampleSystem");
        let sled_ids: Vec<_> = (0..nsleds).map(|_| sled_rng.next()).collect();
        for sled_id in &sled_ids {
            let _ = system.sled(SledBuilder::new().id(*sled_id)).unwrap();
        }

        let mut input_builder = system
            .to_planning_input_builder()
            .expect("failed to make planning input builder");
        let base_input = input_builder.clone().build();

        // Start with an empty blueprint containing only our sleds, no zones.
        let initial_blueprint = BlueprintBuilder::build_empty_with_sleds_seeded(
            base_input.all_sled_ids(SledFilter::Commissioned),
            "test suite",
            (test_name, "ExampleSystem initial"),
        );

        // Start with an empty collection
        let collection = system
            .to_collection_builder()
            .expect("failed to build collection")
            .build();

        // Now make a blueprint and collection with some zones on each sled.
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            &base_input,
            &collection,
            "test suite",
        )
        .unwrap();
        builder.set_rng_seed((test_name, "ExampleSystem make_zones"));
        for (sled_id, sled_resources) in
            base_input.all_sled_resources(SledFilter::Commissioned)
        {
            let _ = builder.sled_ensure_zone_ntp(sled_id).unwrap();
            let _ = builder
                .sled_ensure_zone_multiple_nexus_with_config(
                    sled_id,
                    1,
                    false,
                    vec![],
                )
                .unwrap();
            let _ = builder
                .sled_ensure_zone_multiple_internal_dns(sled_id, 1)
                .unwrap();
            let _ = builder.sled_ensure_disks(sled_id, sled_resources).unwrap();
            for pool_name in sled_resources.zpools.keys() {
                let _ = builder
                    .sled_ensure_zone_crucible(sled_id, *pool_name)
                    .unwrap();
            }
            builder.sled_ensure_datasets(sled_id, &sled_resources).unwrap();
        }

        let blueprint = builder.build();
        let mut builder =
            system.to_collection_builder().expect("failed to build collection");
        builder.set_rng_seed((test_name, "ExampleSystem collection"));

        for (sled_id, zones) in &blueprint.blueprint_zones {
            builder
                .found_sled_omicron_zones(
                    "fake sled agent",
                    *sled_id,
                    zones.to_omicron_zones_config(
                        BlueprintZoneFilter::ShouldBeRunning,
                    ),
                )
                .unwrap();
        }

        // Ensure that our "input" contains the datasets we would have
        // provisioned.
        //
        // This mimics them existing within the database.
        let input_sleds = input_builder.sleds_mut();
        for (sled_id, bp_datasets_config) in &blueprint.blueprint_datasets {
            let sled = input_sleds.get_mut(sled_id).unwrap();
            for (_, bp_dataset) in &bp_datasets_config.datasets {
                let (_, datasets) = sled
                    .resources
                    .zpools
                    .get_mut(&bp_dataset.pool.id())
                    .unwrap();
                let bp_config: omicron_common::disk::DatasetConfig =
                    bp_dataset.clone().try_into().unwrap();
                if !datasets.contains(&bp_config) {
                    datasets.push(bp_config);
                }
            }
        }

        ExampleSystem {
            system,
            input: input_builder.build(),
            collection: builder.build(),
            blueprint,
            sled_rng,
        }
    }
}

/// Returns a collection, planning input, and blueprint describing a pretty
/// simple system.
///
/// The test name is used as the RNG seed.
///
/// `n_sleds` is the number of sleds supported. Currently, this value can
/// be anywhere between 0 and 5. (More can be added in the future if
/// necessary.)
pub fn example(
    log: &slog::Logger,
    test_name: &str,
    nsleds: usize,
) -> (Collection, PlanningInput, Blueprint) {
    let example = ExampleSystem::new(log, test_name, nsleds);
    (example.collection, example.input, example.blueprint)
}
