// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use std::fmt;
use std::hash::Hash;
use std::net::IpAddr;
use std::net::Ipv4Addr;

use crate::blueprint_builder::BlueprintBuilder;
use crate::planner::rng::PlannerRng;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use nexus_inventory::CollectionBuilderRng;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::OmicronZoneNic;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::inventory::Collection;
use omicron_common::policy::CRUCIBLE_PANTRY_REDUNDANCY;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::VnicUuid;
use typed_rng::TypedUuidRng;

/// Stateful PRNG for generating simulated systems.
///
/// When generating a succession of simulated systems, this stateful PRNG allows
/// for reproducible generation of those systems after setting an initial seed.
/// The PRNGs are structured in tree form as much as possible, so that (for
/// example) if one part of the system decides to change how many sleds are in
/// the system, it can do so without affecting other UUIDs.
///
/// We have a number of existing tests that manually set seeds for individual
/// RNG instances. The old-style seeds have been kept around for backwards
/// compatibility. Newer tests should use this struct to generate their RNGs
/// instead, since it conveniently tracks generation numbers for each seed.
#[derive(Clone, Debug)]
pub struct SimRngState {
    seed: String,
    // Generation numbers for each RNG type.
    system_rng_gen: u64,
    collection_rng_gen: u64,
    planner_rng_gen: u64,
    // TODO: Currently, sled IDs are used to generate UUIDs to mutate the
    // system. This should be replaced in the future with a richer simulation
    // framework.
    sled_id_rng_gen: u64,
}

impl SimRngState {
    pub fn from_seed(seed: &str) -> Self {
        Self {
            seed: seed.to_string(),
            system_rng_gen: 0,
            collection_rng_gen: 0,
            planner_rng_gen: 0,
            sled_id_rng_gen: 0,
        }
    }

    pub fn seed(&self) -> &str {
        &self.seed
    }

    pub fn next_system_rng(&mut self) -> ExampleSystemRng {
        // Different behavior for the first system_rng_gen is a bit weird, but
        // it retains backwards compatibility with existing tests -- it means
        // that generated UUIDs particularly in fixtures don't change.
        self.system_rng_gen += 1;
        if self.system_rng_gen == 1 {
            ExampleSystemRng::from_seed(self.seed.as_str())
        } else {
            ExampleSystemRng::from_seed((
                self.seed.as_str(),
                self.system_rng_gen,
            ))
        }
    }

    pub fn next_collection_rng(&mut self) -> CollectionBuilderRng {
        self.collection_rng_gen += 1;
        // We don't need to pass in extra bits unique to collections, because
        // `CollectionBuilderRng` adds its own.
        let seed = (self.seed.as_str(), self.collection_rng_gen);
        CollectionBuilderRng::from_seed(seed)
    }

    pub fn next_planner_rng(&mut self) -> PlannerRng {
        self.planner_rng_gen += 1;
        // We don't need to pass in extra bits unique to the planner, because
        // `PlannerRng` adds its own.
        PlannerRng::from_seed((self.seed.as_str(), self.planner_rng_gen))
    }

    pub fn next_sled_id_rng(&mut self) -> TypedUuidRng<SledKind> {
        self.sled_id_rng_gen += 1;
        TypedUuidRng::from_seed(
            self.seed.as_str(),
            ("sled-id-rng", self.sled_id_rng_gen),
        )
    }
}

#[derive(Debug, Clone)]
pub struct ExampleSystemRng {
    seed: String,
    sled_rng: TypedUuidRng<SledKind>,
    collection_rng: CollectionBuilderRng,
    // ExampleSystem instances generate two blueprints: create RNGs for both.
    blueprint1_rng: PlannerRng,
    blueprint2_rng: PlannerRng,
}

impl ExampleSystemRng {
    pub fn from_seed<H: Hash + fmt::Debug>(seed: H) -> Self {
        // This is merely "ExampleSystem" for backwards compatibility with
        // existing test fixtures.
        let sled_rng = TypedUuidRng::from_seed(&seed, "ExampleSystem");
        // We choose to make our own collection and blueprint RNGs rather than
        // passing them in via `SimRngState`. This means that `SimRngState` is
        // influenced as little as possible by the specifics of how
        // `ExampleSystem` instances are generated, and RNG stability is
        // maintained.
        let collection_rng = CollectionBuilderRng::from_seed((
            &seed,
            "ExampleSystem collection",
        ));
        let blueprint1_rng =
            PlannerRng::from_seed((&seed, "ExampleSystem initial"));
        let blueprint2_rng =
            PlannerRng::from_seed((&seed, "ExampleSystem make_zones"));
        Self {
            seed: format!("{:?}", seed),
            sled_rng,
            collection_rng,
            blueprint1_rng,
            blueprint2_rng,
        }
    }
}

/// An example generated system, along with a consistent planning input and
/// collection.
///
/// The components of this struct are generated together and match each other.
/// The planning input and collection represent database input and inventory
/// that would be collected from a system matching the system description.
#[derive(Clone, Debug)]
pub struct ExampleSystem {
    pub system: SystemDescription,
    pub input: PlanningInput,
    pub collection: Collection,
    /// The initial blueprint that was used to describe the system. This
    /// blueprint has sleds but no zones.
    pub initial_blueprint: Blueprint,
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
    rng: ExampleSystemRng,
    // TODO: Store a Policy struct instead of these fields:
    // https://github.com/oxidecomputer/omicron/issues/6803
    nsleds: usize,
    ndisks_per_sled: u8,
    // None means nsleds
    nexus_count: Option<ZoneCount>,
    internal_dns_count: ZoneCount,
    external_dns_count: ZoneCount,
    crucible_pantry_count: ZoneCount,
    create_zones: bool,
    create_disks_in_blueprint: bool,
}

impl ExampleSystemBuilder {
    /// The default number of sleds in the example system.
    pub const DEFAULT_N_SLEDS: usize = 3;

    /// The default number of external DNS instances in the example system.
    ///
    /// The default value is picked for backwards compatibility -- we may wish
    /// to revisit it in the future.
    pub const DEFAULT_EXTERNAL_DNS_COUNT: usize = 0;

    pub fn new(log: &slog::Logger, test_name: &str) -> Self {
        let rng = ExampleSystemRng::from_seed(test_name);
        Self::new_with_rng(log, rng)
    }

    pub fn new_with_rng(log: &slog::Logger, rng: ExampleSystemRng) -> Self {
        Self {
            log: log.new(slog::o!(
                "component" => "ExampleSystem",
                "rng_seed" => rng.seed.clone(),
            )),
            rng,
            nsleds: Self::DEFAULT_N_SLEDS,
            ndisks_per_sled: SledBuilder::DEFAULT_NPOOLS,
            nexus_count: None,
            internal_dns_count: ZoneCount(INTERNAL_DNS_REDUNDANCY),
            external_dns_count: ZoneCount(Self::DEFAULT_EXTERNAL_DNS_COUNT),
            crucible_pantry_count: ZoneCount(CRUCIBLE_PANTRY_REDUNDANCY),
            create_zones: true,
            create_disks_in_blueprint: true,
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

    /// Set the number of disks per sled in the example system.
    ///
    /// The default value is [`SledBuilder::DEFAULT_NPOOLS`]. A value of 0 is
    /// permitted.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn ndisks_per_sled(mut self, ndisks_per_sled: u8) -> Self {
        self.ndisks_per_sled = ndisks_per_sled;
        self
    }

    /// Set the number of Nexus instances in the example system.
    ///
    /// The default value is the same as the number of sleds (i.e. one Nexus
    /// instance per sled). A value of 0 is permitted.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn nexus_count(mut self, nexus_count: usize) -> Self {
        self.nexus_count = Some(ZoneCount(nexus_count));
        self
    }

    /// Set the number of internal DNS instances in the example system.
    ///
    /// The default value is [`INTERNAL_DNS_REDUNDANCY`]. A value anywhere
    /// between 0 and [`INTERNAL_DNS_REDUNDANCY`], inclusive, is permitted.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn internal_dns_count(
        mut self,
        internal_dns_count: usize,
    ) -> anyhow::Result<Self> {
        if internal_dns_count > INTERNAL_DNS_REDUNDANCY {
            anyhow::bail!(
                "internal_dns_count {} is greater than INTERNAL_DNS_REDUNDANCY {}",
                internal_dns_count,
                INTERNAL_DNS_REDUNDANCY,
            );
        }
        self.internal_dns_count = ZoneCount(internal_dns_count);
        Ok(self)
    }

    /// Set the number of external DNS instances in the example system.
    ///
    /// The default value is [`Self::DEFAULT_EXTERNAL_DNS_COUNT`]. A value
    /// anywhere between 0 and 30, inclusive, is permitted. (The limit of 30 is
    /// primarily to simplify the implementation.)
    ///
    /// Each DNS server is assigned an address in the 10.x.x.x range.
    pub fn external_dns_count(
        mut self,
        external_dns_count: usize,
    ) -> anyhow::Result<Self> {
        if external_dns_count > 30 {
            anyhow::bail!(
                "external_dns_count {} is greater than 30",
                external_dns_count,
            );
        }
        self.external_dns_count = ZoneCount(external_dns_count);
        Ok(self)
    }

    /// Set the number of Crucible pantry instances in the example system.
    ///
    /// If [`Self::create_zones`] is set to `false`, this is ignored.
    pub fn crucible_pantry_count(
        mut self,
        crucible_pantry_count: usize,
    ) -> Self {
        self.crucible_pantry_count = ZoneCount(crucible_pantry_count);
        self
    }

    /// Create zones in the example system.
    ///
    /// The default is `true`.
    pub fn create_zones(mut self, create_zones: bool) -> Self {
        self.create_zones = create_zones;
        self
    }

    /// Create disks in the blueprint.
    ///
    /// The default is `true`.
    ///
    /// If [`Self::ndisks_per_sled`] is set to 0, then this is implied: if no
    /// disks are created, then the blueprint won't have any disks.
    pub fn create_disks_in_blueprint(mut self, create: bool) -> Self {
        self.create_disks_in_blueprint = create;
        self
    }

    fn get_nexus_zones(&self) -> ZoneCount {
        self.nexus_count.unwrap_or(ZoneCount(self.nsleds))
    }

    pub fn get_internal_dns_zones(&self) -> usize {
        self.internal_dns_count.0
    }

    pub fn get_external_dns_zones(&self) -> usize {
        self.external_dns_count.0
    }

    /// Create a new example system with the given modifications.
    ///
    /// Return the system, and the initial blueprint that matches it.
    pub fn build(&self) -> (ExampleSystem, Blueprint) {
        let nexus_count = self.get_nexus_zones();

        slog::debug!(
            &self.log,
            "Creating example system";
            "nsleds" => self.nsleds,
            "ndisks_per_sled" => self.ndisks_per_sled,
            "nexus_count" => nexus_count.0,
            "internal_dns_count" => self.internal_dns_count.0,
            "external_dns_count" => self.external_dns_count.0,
            "crucible_pantry_count" => self.crucible_pantry_count.0,
            "create_zones" => self.create_zones,
            "create_disks_in_blueprint" => self.create_disks_in_blueprint,
        );

        let mut rng = self.rng.clone();

        let mut system = SystemDescription::new();
        // Update the system's target counts with the counts. (Note that
        // there's no external DNS count.)
        system
            .target_nexus_zone_count(nexus_count.0)
            .target_internal_dns_zone_count(self.internal_dns_count.0)
            .target_crucible_pantry_zone_count(self.crucible_pantry_count.0);
        let sled_ids: Vec<_> =
            (0..self.nsleds).map(|_| rng.sled_rng.next()).collect();

        for sled_id in &sled_ids {
            let _ = system
                .sled(
                    SledBuilder::new()
                        .id(*sled_id)
                        .npools(self.ndisks_per_sled),
                )
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
            rng.blueprint1_rng,
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
        builder.set_rng(rng.blueprint2_rng);

        // Add as many external IPs as is necessary for external DNS zones. We
        // pick addresses in the TEST-NET-2 (RFC 5737) range.
        for i in 0..self.external_dns_count.0 {
            builder
                .inject_untracked_external_dns_ip(IpAddr::V4(Ipv4Addr::new(
                    198,
                    51,
                    100,
                    (i + 1)
                        .try_into()
                        .expect("external_dns_count is always <= 30"),
                )))
                .expect(
                    "this shouldn't error because provided external IPs \
                     are all unique",
                );
        }

        for (i, (sled_id, sled_details)) in
            base_input.all_sleds(SledFilter::Commissioned).enumerate()
        {
            if self.create_disks_in_blueprint {
                let _ = builder
                    .sled_add_disks(sled_id, &sled_details.resources)
                    .unwrap();
            }
            if self.create_zones {
                let _ = builder.sled_ensure_zone_ntp(sled_id).unwrap();
                for _ in 0..nexus_count.on(i, self.nsleds) {
                    builder
                        .sled_add_zone_nexus_with_config(sled_id, false, vec![])
                        .unwrap();
                }
                if i == 0 {
                    builder.sled_add_zone_clickhouse(sled_id).unwrap();
                }
                for _ in 0..self.internal_dns_count.on(i, self.nsleds) {
                    builder.sled_add_zone_internal_dns(sled_id).unwrap();
                }
                for _ in 0..self.external_dns_count.on(i, self.nsleds) {
                    builder.sled_add_zone_external_dns(sled_id).unwrap();
                }
                for _ in 0..self.crucible_pantry_count.on(i, self.nsleds) {
                    builder.sled_add_zone_crucible_pantry(sled_id).unwrap();
                }
            }
            if self.create_zones {
                for pool_name in sled_details.resources.zpools.keys() {
                    let _ = builder
                        .sled_ensure_zone_crucible(sled_id, *pool_name)
                        .unwrap();
                }
            }
            builder.sled_ensure_zone_datasets(sled_id).unwrap();
        }

        let blueprint = builder.build();
        for sled_cfg in blueprint.sleds.values() {
            for zone in sled_cfg.zones.iter() {
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

        for (sled_id, sled_cfg) in &blueprint.sleds {
            let sled_cfg = sled_cfg.clone().into_in_service_sled_config();
            system.sled_set_omicron_config(*sled_id, sled_cfg).unwrap();
        }

        // We just ensured that a handful of datasets should exist in
        // the blueprint, but they don't yet exist in the SystemDescription.
        //
        // Go back and add them so that the blueprint is consistent with
        // inventory.
        for (sled_id, sled_cfg) in &blueprint.sleds {
            let sled = system.get_sled_mut(*sled_id).unwrap();

            for dataset_config in sled_cfg.datasets.iter() {
                let config = dataset_config.clone().try_into().unwrap();
                sled.add_synthetic_dataset(config);
            }
        }

        let mut builder =
            system.to_collection_builder().expect("failed to build collection");
        builder.set_rng(rng.collection_rng);

        // The blueprint evolves separately from the system -- so it's returned
        // as a separate value.
        let example = ExampleSystem {
            system,
            input: input_builder.build(),
            collection: builder.build(),
            initial_blueprint,
        };
        (example, blueprint)
    }
}

// A little wrapper to try and avoid having an `on` function which takes 3
// usize parameters.
#[derive(Clone, Copy, Debug)]
struct ZoneCount(pub usize);

impl ZoneCount {
    fn on(self, sled_id: usize, total_sleds: usize) -> usize {
        // Spread instances out as evenly as possible. If there are 5 sleds and 3
        // instances, we want to spread them out as 2, 2, 1.
        let div = self.0 / total_sleds;
        let rem = self.0 % total_sleds;
        div + if sled_id < rem { 1 } else { 0 }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use nexus_sled_agent_shared::inventory::{OmicronZoneConfig, ZoneKind};
    use nexus_types::deployment::BlueprintZoneConfig;
    use nexus_types::deployment::BlueprintZoneDisposition;
    use omicron_test_utils::dev::test_setup_log;

    use super::*;

    #[test]
    fn instances_on_examples() {
        assert_eq!(ZoneCount(3).on(0, 5), 1);
        assert_eq!(ZoneCount(3).on(1, 5), 1);
        assert_eq!(ZoneCount(3).on(2, 5), 1);
        assert_eq!(ZoneCount(3).on(3, 5), 0);
        assert_eq!(ZoneCount(3).on(4, 5), 0);

        assert_eq!(ZoneCount(5).on(0, 5), 1);
        assert_eq!(ZoneCount(5).on(1, 5), 1);
        assert_eq!(ZoneCount(5).on(2, 5), 1);
        assert_eq!(ZoneCount(5).on(3, 5), 1);
        assert_eq!(ZoneCount(5).on(4, 5), 1);

        assert_eq!(ZoneCount(7).on(0, 5), 2);
        assert_eq!(ZoneCount(7).on(1, 5), 2);
        assert_eq!(ZoneCount(7).on(2, 5), 1);
        assert_eq!(ZoneCount(6).on(3, 5), 1);
        assert_eq!(ZoneCount(6).on(4, 5), 1);
    }

    #[test]
    fn builder_zone_counts() {
        static TEST_NAME: &str = "example_builder_zone_counts";
        let logctx = test_setup_log(TEST_NAME);

        let (example, mut blueprint) =
            ExampleSystemBuilder::new(&logctx.log, TEST_NAME)
                .nsleds(5)
                .nexus_count(6)
                .crucible_pantry_count(5)
                .internal_dns_count(2)
                .unwrap()
                .external_dns_count(10)
                .unwrap()
                .build();

        // Define a time_created for consistent output across runs.
        blueprint.time_created = DateTime::<Utc>::UNIX_EPOCH;

        expectorate::assert_contents(
            "tests/output/example_builder_zone_counts_blueprint.txt",
            &blueprint.display().to_string(),
        );

        // Check that the system's target counts are set correctly.
        assert_eq!(example.system.get_target_nexus_zone_count(), 6);
        assert_eq!(example.system.get_target_internal_dns_zone_count(), 2);
        assert_eq!(example.system.get_target_crucible_pantry_zone_count(), 5);

        // Check that the right number of zones are present in both the
        // blueprint and in the collection.
        let nexus_zones = blueprint_zones_of_kind(&blueprint, ZoneKind::Nexus);
        assert_eq!(
            nexus_zones.len(),
            6,
            "expected 6 Nexus zones in blueprint, got {}: {:#?}",
            nexus_zones.len(),
            nexus_zones,
        );
        let nexus_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::Nexus,
        );
        assert_eq!(
            nexus_zones.len(),
            6,
            "expected 6 Nexus zones in collection, got {}: {:#?}",
            nexus_zones.len(),
            nexus_zones,
        );

        let internal_dns_zones =
            blueprint_zones_of_kind(&blueprint, ZoneKind::InternalDns);
        assert_eq!(
            internal_dns_zones.len(),
            2,
            "expected 2 internal DNS zones in blueprint, got {}: {:#?}",
            internal_dns_zones.len(),
            internal_dns_zones,
        );
        let internal_dns_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::InternalDns,
        );
        assert_eq!(
            internal_dns_zones.len(),
            2,
            "expected 2 internal DNS zones in collection, got {}: {:#?}",
            internal_dns_zones.len(),
            internal_dns_zones,
        );

        let external_dns_zones =
            blueprint_zones_of_kind(&blueprint, ZoneKind::ExternalDns);
        assert_eq!(
            external_dns_zones.len(),
            10,
            "expected 10 external DNS zones in blueprint, got {}: {:#?}",
            external_dns_zones.len(),
            external_dns_zones,
        );
        let external_dns_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::ExternalDns,
        );
        assert_eq!(
            external_dns_zones.len(),
            10,
            "expected 10 external DNS zones in collection, got {}: {:#?}",
            external_dns_zones.len(),
            external_dns_zones,
        );

        let crucible_pantry_zones =
            blueprint_zones_of_kind(&blueprint, ZoneKind::CruciblePantry);
        assert_eq!(
            crucible_pantry_zones.len(),
            5,
            "expected 5 Crucible pantry zones in blueprint, got {}: {:#?}",
            crucible_pantry_zones.len(),
            crucible_pantry_zones,
        );
        let crucible_pantry_zones = collection_ledgered_zones_of_kind(
            &example.collection,
            ZoneKind::CruciblePantry,
        );
        assert_eq!(
            crucible_pantry_zones.len(),
            5,
            "expected 5 Crucible pantry zones in collection, got {}: {:#?}",
            crucible_pantry_zones.len(),
            crucible_pantry_zones,
        );

        logctx.cleanup_successful();
    }

    fn blueprint_zones_of_kind(
        blueprint: &Blueprint,
        kind: ZoneKind,
    ) -> Vec<&BlueprintZoneConfig> {
        blueprint
            .all_omicron_zones(BlueprintZoneDisposition::any)
            .filter_map(|(_, zone)| {
                (zone.zone_type.kind() == kind).then_some(zone)
            })
            .collect()
    }

    fn collection_ledgered_zones_of_kind(
        collection: &Collection,
        kind: ZoneKind,
    ) -> Vec<&OmicronZoneConfig> {
        collection
            .all_ledgered_omicron_zones()
            .filter(|zone| zone.zone_type.kind() == kind)
            .collect()
    }
}
