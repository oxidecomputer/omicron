// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use crate::blueprint_builder::BlueprintBuilder;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::OmicronZoneNic;
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
            base_input.all_sled_ids(SledFilter::All),
            "test suite",
            (test_name, "ExampleSystem initial"),
        );

        // Now make a blueprint and collection with some zones on each sled.
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            &base_input,
            "test suite",
        )
        .unwrap();
        builder.set_rng_seed((test_name, "ExampleSystem make_zones"));
        for (sled_id, sled_resources) in
            base_input.all_sled_resources(SledFilter::All)
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
            for pool_name in sled_resources.zpools.keys() {
                let _ = builder
                    .sled_ensure_zone_crucible(sled_id, *pool_name)
                    .unwrap();
            }
        }

        let blueprint = builder.build();
        let mut builder =
            system.to_collection_builder().expect("failed to build collection");
        builder.set_rng_seed((test_name, "ExampleSystem collection"));

        for sled_id in blueprint.sleds() {
            let Some(zones) = blueprint.blueprint_zones.get(&sled_id) else {
                continue;
            };
            for zone in zones.zones.iter() {
                let service_id = zone.id;
                if let Some((external_ip, nic)) =
                    zone.zone_type.external_networking()
                {
                    input_builder
                        .add_omicron_zone_external_ip(service_id, external_ip)
                        .expect("failed to add Omicron zone external IP");
                    input_builder
                        .add_omicron_zone_nic(
                            service_id,
                            OmicronZoneNic {
                                id: nic.id,
                                mac: nic.mac,
                                ip: nic.ip,
                                slot: nic.slot,
                                primary: nic.primary,
                            },
                        )
                        .expect("failed to add Omicron zone NIC");
                }
            }
            builder
                .found_sled_omicron_zones(
                    "fake sled agent",
                    sled_id,
                    zones.to_omicron_zones_config(
                        BlueprintZoneFilter::ShouldBeRunning,
                    ),
                )
                .unwrap();
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
