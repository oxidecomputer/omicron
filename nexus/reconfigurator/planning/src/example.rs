// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use crate::blueprint_builder::BlueprintBuilder;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::Policy;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Generation;
use sled_agent_client::types::OmicronZonesConfig;
use uuid::Uuid;

pub struct ExampleSystem {
    pub system: SystemDescription,
    pub policy: Policy,
    pub collection: Collection,
    pub blueprint: Blueprint,
}

impl ExampleSystem {
    pub fn new(log: &slog::Logger, nsleds: usize) -> ExampleSystem {
        let mut system = SystemDescription::new();
        let sled_ids: Vec<_> = (0..nsleds).map(|_| Uuid::new_v4()).collect();
        for sled_id in &sled_ids {
            let _ = system.sled(SledBuilder::new().id(*sled_id)).unwrap();
        }

        let policy = system.to_policy().expect("failed to make policy");
        let mut inventory_builder =
            system.to_collection_builder().expect("failed to build collection");

        // For each sled, have it report 0 zones in the initial inventory.
        // This will enable us to build a blueprint from the initial
        // inventory, which we can then use to build new blueprints.
        for sled_id in &sled_ids {
            inventory_builder
                .found_sled_omicron_zones(
                    "fake sled agent",
                    *sled_id,
                    OmicronZonesConfig {
                        generation: Generation::new(),
                        zones: vec![],
                    },
                )
                .expect("recording Omicron zones");
        }

        let empty_zone_inventory = inventory_builder.build();
        let initial_blueprint =
            BlueprintBuilder::build_initial_from_collection(
                &empty_zone_inventory,
                Generation::new(),
                Generation::new(),
                &policy,
                "test suite",
            )
            .unwrap();

        // Now make a blueprint and collection with some zones on each sled.
        let mut builder = BlueprintBuilder::new_based_on(
            &log,
            &initial_blueprint,
            Generation::new(),
            Generation::new(),
            &policy,
            "test suite",
        )
        .unwrap();
        for (sled_id, sled_resources) in &policy.sleds {
            let _ = builder.sled_ensure_zone_ntp(*sled_id).unwrap();
            let _ = builder
                .sled_ensure_zone_multiple_nexus_with_config(
                    *sled_id,
                    1,
                    false,
                    vec![],
                )
                .unwrap();
            for pool_name in &sled_resources.zpools {
                let _ = builder
                    .sled_ensure_zone_crucible(*sled_id, pool_name.clone())
                    .unwrap();
            }
        }

        let blueprint = builder.build();
        let mut builder =
            system.to_collection_builder().expect("failed to build collection");

        for sled_id in blueprint.sleds() {
            let Some(zones) = blueprint.omicron_zones.get(&sled_id) else {
                continue;
            };
            builder
                .found_sled_omicron_zones(
                    "fake sled agent",
                    sled_id,
                    zones.clone(),
                )
                .unwrap();
        }

        ExampleSystem { system, policy, collection: builder.build(), blueprint }
    }
}

/// Returns a collection and policy describing a pretty simple system.
///
/// `n_sleds` is the number of sleds supported. Currently, this value can
/// be anywhere between 0 and 5. (More can be added in the future if
/// necessary.)
pub fn example(log: &slog::Logger, nsleds: usize) -> (Collection, Policy) {
    let example = ExampleSystem::new(log, nsleds);
    (example.collection, example.policy)
}
