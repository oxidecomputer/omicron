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
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::VnicUuid;
use typed_rng::TypedUuidRng;

pub struct ExampleSystem {
    pub system: SystemDescription,
    pub input: PlanningInput,
    pub collection: Collection,
    // If we add more types of RNGs than just sleds here, we'll need to
    // expand this to be similar to BlueprintBuilderRng where a root RNG
    // creates sub-RNGs.
    //
    // This is currently only used for tests, so it looks unused in normal
    // builds.  But in the future it could be used by other consumers, too.
    #[allow(dead_code)]
    pub(crate) sled_rng: TypedUuidRng<SledKind>,
}

/// Returns a collection, planning input, and blueprint describing a pretty
/// simple system.
///
/// The test name is used as the RNG seed.
pub fn example(
    log: &slog::Logger,
    test_name: &str,
) -> (Collection, PlanningInput, Blueprint) {
    let (example, blueprint) =
        ExampleSystemBuilder::new(log, test_name).build();
    (example.collection, example.input, blueprint)
}

/// A builder for the example system.
#[derive(Debug, Clone)]
pub struct ExampleSystemBuilder {
    log: slog::Logger,
    test_name: String,
    nsleds: usize,
    no_zones: bool,
    no_disks: bool,
    no_disks_in_blueprint: bool,
}

impl ExampleSystemBuilder {
    /// The default number of sleds in the example system.
    pub const DEFAULT_N_SLEDS: usize = 3;

    pub fn new(log: &slog::Logger, test_name: &str) -> Self {
        Self {
            log: log.new(slog::o!("component" => "ExampleSystem", "test_name" => test_name.to_string())),
            test_name: test_name.to_string(),
            nsleds: Self::DEFAULT_N_SLEDS,
            no_zones: false,
            no_disks: false,
            no_disks_in_blueprint: false,
        }
    }

    /// Set the number of sleds in the example system.
    ///
    /// Currently, this value can be anywhere between 0 and 5. (More can be
    /// added in the future if necessary.)
    pub fn nsleds(mut self, nsleds: usize) -> Self {
        self.nsleds = nsleds;
        self
    }

    /// Do not create any zones in the example system.
    pub fn no_zones(mut self) -> Self {
        self.no_zones = true;
        self
    }

    /// Do not create any disks in the example system.
    pub fn no_disks(mut self) -> Self {
        self.no_disks = true;
        self
    }

    /// Do not add any disks to the returned blueprint.
    ///
    /// [`Self::no_disks`] implies this: if no disks are created, then the
    /// blueprint won't have any disks.
    pub fn no_disks_in_blueprint(mut self) -> Self {
        self.no_disks_in_blueprint = true;
        self
    }

    /// Create a new example system with the given modifications.
    ///
    /// Return the system, and the initial blueprint that matches it.
    pub fn build(&self) -> (ExampleSystem, Blueprint) {
        slog::info!(
            &self.log,
            "Creating example system";
            "nsleds" => self.nsleds,
            "no_zones" => self.no_zones,
            "no_disks" => self.no_disks,
            "no_disks_in_blueprint" => self.no_disks_in_blueprint,
        );

        let mut system = SystemDescription::new();
        let mut sled_rng =
            TypedUuidRng::from_seed(&self.test_name, "ExampleSystem");
        let sled_ids: Vec<_> =
            (0..self.nsleds).map(|_| sled_rng.next()).collect();
        let npools =
            if self.no_disks { 0 } else { SledBuilder::DEFAULT_NPOOLS };

        for sled_id in &sled_ids {
            let _ = system
                .sled(SledBuilder::new().id(*sled_id).npools(npools))
                .unwrap();
        }

        let mut input_builder = system
            .to_planning_input_builder()
            .expect("failed to make planning input builder");
        let base_input = input_builder.clone().build();

        // Start with an empty blueprint containing only our sleds, no zones.
        let initial_blueprint = BlueprintBuilder::build_empty_with_sleds_seeded(
            base_input.all_sled_ids(SledFilter::Commissioned),
            "test suite",
            (&self.test_name, "ExampleSystem initial"),
        );

        // Start with an empty collection
        let collection = system
            .to_collection_builder()
            .expect("failed to build collection")
            .build();

        // Now make a blueprint and collection with some zones on each sled.
        let mut builder = BlueprintBuilder::new_based_on(
            &self.log,
            &initial_blueprint,
            &base_input,
            &collection,
            "test suite",
        )
        .unwrap();
        builder.set_rng_seed((&self.test_name, "ExampleSystem make_zones"));
        for (i, (sled_id, sled_resources)) in
            base_input.all_sled_resources(SledFilter::Commissioned).enumerate()
        {
            if !self.no_zones {
                let _ = builder.sled_ensure_zone_ntp(sled_id).unwrap();
                let _ = builder
                    .sled_ensure_zone_multiple_nexus_with_config(
                        sled_id,
                        1,
                        false,
                        vec![],
                    )
                    .unwrap();
                if i < INTERNAL_DNS_REDUNDANCY {
                    let _ = builder
                        .sled_ensure_zone_multiple_internal_dns(sled_id, 1)
                        .unwrap();
                }
            }
            if !self.no_disks_in_blueprint {
                let _ =
                    builder.sled_ensure_disks(sled_id, sled_resources).unwrap();
            }
            if !self.no_zones {
                for pool_name in sled_resources.zpools.keys() {
                    let _ = builder
                        .sled_ensure_zone_crucible(sled_id, *pool_name)
                        .unwrap();
                }
            }
        }

        let blueprint = builder.build();
        let mut builder =
            system.to_collection_builder().expect("failed to build collection");
        builder.set_rng_seed((&self.test_name, "ExampleSystem collection"));

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
                                // TODO-cleanup use `TypedUuid` everywhere
                                id: VnicUuid::from_untyped_uuid(nic.id),
                                mac: nic.mac,
                                ip: nic.ip,
                                slot: nic.slot,
                                primary: nic.primary,
                            },
                        )
                        .expect("failed to add Omicron zone NIC");
                }
            }
        }

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

        // Previously ExampleSystem itself used to carry around the blueprint,
        // but logically the blueprint evolves separately from the system -- so
        // it's returned as a separate value.
        let example = ExampleSystem {
            system,
            input: input_builder.build(),
            collection: builder.build(),
            sled_rng,
        };
        (example, blueprint)
    }
}
