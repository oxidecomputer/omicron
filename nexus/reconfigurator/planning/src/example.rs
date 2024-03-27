// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use crate::blueprint_builder::BlueprintBuilder;
use crate::blueprint_builder::UuidRng;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::ExternalIp;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::ServiceNetworkInterface;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::ServiceKind;
use omicron_uuid_kinds::TypedUuid;
use sled_agent_client::types::OmicronZonesConfig;
use std::collections::BTreeMap;
use uuid::Uuid;

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
    pub(crate) sled_rng: UuidRng,
}

impl ExampleSystem {
    pub fn new(
        log: &slog::Logger,
        test_name: &str,
        nsleds: usize,
    ) -> ExampleSystem {
        let mut system = SystemDescription::new();
        let mut sled_rng = UuidRng::from_seed(test_name, "ExampleSystem");
        let sled_ids: Vec<_> =
            (0..nsleds).map(|_| sled_rng.next_uuid()).collect();
        for sled_id in &sled_ids {
            let _ = system.sled(SledBuilder::new().id(*sled_id)).unwrap();
        }

        let policy = system.to_policy().expect("failed to make policy");
        let mut inventory_builder =
            system.to_collection_builder().expect("failed to build collection");
        let mut input = PlanningInput {
            policy,
            service_external_ips: BTreeMap::new(),
            service_nics: BTreeMap::new(),
        };

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
            BlueprintBuilder::build_initial_from_collection_seeded(
                &empty_zone_inventory,
                Generation::new(),
                Generation::new(),
                &input.policy,
                "test suite",
                (test_name, "ExampleSystem initial"),
            )
            .unwrap();

        // Now make a blueprint and collection with some zones on each sled.
        let mut builder = BlueprintBuilder::new_based_on(
            log,
            &initial_blueprint,
            Generation::new(),
            Generation::new(),
            &input,
            "test suite",
        )
        .unwrap();
        builder.set_rng_seed((test_name, "ExampleSystem make_zones"));
        for (sled_id, sled_resources) in &input.policy.sleds {
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
            let Some(zones) = blueprint.blueprint_zones.get(&sled_id) else {
                continue;
            };
            for zone in zones.zones.iter().map(|z| &z.config) {
                let service_id =
                    TypedUuid::<ServiceKind>::from_untyped_uuid(zone.id);
                if let Ok(Some(ip)) = zone.zone_type.external_ip() {
                    input.service_external_ips.insert(
                        service_id,
                        ExternalIp { id: Uuid::new_v4(), ip: ip.into() },
                    );
                }
                if let Some(nic) = zone.zone_type.service_vnic() {
                    input.service_nics.insert(
                        service_id,
                        ServiceNetworkInterface {
                            id: nic.id,
                            mac: nic.mac,
                            ip: nic.ip.into(),
                            slot: nic.slot,
                            primary: nic.primary,
                        },
                    );
                }
            }
            builder
                .found_sled_omicron_zones(
                    "fake sled agent",
                    sled_id,
                    zones.to_omicron_zones_config(
                        BlueprintZoneFilter::SledAgentPut,
                    ),
                )
                .unwrap();
        }

        ExampleSystem {
            system,
            input,
            collection: builder.build(),
            blueprint,
            sled_rng,
        }
    }
}

/// Returns a collection and planning input describing a pretty simple system.
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
) -> (Collection, PlanningInput) {
    let example = ExampleSystem::new(log, test_name, nsleds);
    (example.collection, example.input)
}
