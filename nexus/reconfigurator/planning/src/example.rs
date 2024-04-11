// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use crate::blueprint_builder::BlueprintBuilder;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::ExternalIp;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::ServiceNetworkInterface;
use nexus_types::deployment::SledFilter;
use nexus_types::inventory::Collection;
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::TypedUuid;
use sled_agent_client::types::OmicronZonesConfig;
use typed_rng::TypedUuidRng;
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
        let mut inventory_builder =
            system.to_collection_builder().expect("failed to build collection");
        let base_input = input_builder.clone().build();

        // For each sled, have it report 0 zones in the initial inventory.
        // This will enable us to build a blueprint from the initial
        // inventory, which we can then use to build new blueprints.
        for sled_id in &sled_ids {
            inventory_builder
                .found_sled_omicron_zones(
                    "fake sled agent",
                    // TODO-cleanup use `TypedUuid` everywhere
                    sled_id.into_untyped_uuid(),
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
                base_input.all_sled_ids(SledFilter::All),
                "test suite",
                (test_name, "ExampleSystem initial"),
            )
            .unwrap();

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
                    .sled_ensure_zone_crucible(sled_id, pool_name.clone())
                    .unwrap();
            }
        }

        let blueprint = builder.build();
        let mut builder =
            system.to_collection_builder().expect("failed to build collection");
        builder.set_rng_seed((test_name, "ExampleSystem collection"));

        for sled_id in blueprint.sleds() {
            // TODO-cleanup use `TypedUuid` everywhere
            let Some(zones) =
                blueprint.blueprint_zones.get(sled_id.as_untyped_uuid())
            else {
                continue;
            };
            for zone in zones.zones.iter().map(|z| &z.config) {
                let service_id =
                    TypedUuid::<OmicronZoneKind>::from_untyped_uuid(zone.id);
                if let Ok(Some(ip)) = zone.zone_type.external_ip() {
                    input_builder
                        .add_omicron_zone_external_ip(
                            service_id,
                            ExternalIp { id: Uuid::new_v4(), ip: ip.into() },
                        )
                        .expect("failed to add Omicron zone external IP");
                }
                if let Some(nic) = zone.zone_type.service_vnic() {
                    input_builder
                        .add_omicron_zone_nic(
                            service_id,
                            ServiceNetworkInterface {
                                id: nic.id,
                                mac: nic.mac,
                                ip: nic.ip.into(),
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
                    // TODO-cleanup use `TypedUuid` everywhere
                    sled_id.into_untyped_uuid(),
                    zones.to_omicron_zones_config(
                        BlueprintZoneFilter::SledAgentPut,
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
