// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Example blueprints

use std::sync::OnceLock;

use crate::blueprint_builder::BlueprintBuilder;
use crate::system::SledBuilder;
use crate::system::SystemDescription;
use nexus_sled_agent_shared::inventory::OmicronZonesConfig;
use nexus_types::deployment::Blueprint;
use nexus_types::deployment::BlueprintZoneFilter;
use nexus_types::deployment::PlanningInput;
use nexus_types::deployment::SledFilter;
use nexus_types::inventory::Collection;
use omicron_common::policy::INTERNAL_DNS_REDUNDANCY;
use omicron_uuid_kinds::CollectionKind;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::SledKind;
use omicron_uuid_kinds::SledUuid;
use typed_rng::TypedUuidRng;

/// Returns a simple example system and an initial blueprint for it.
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
) -> (ExampleSystem, Blueprint) {
    let mut example = ExampleSystem::new(log, test_name, nsleds);
    let base_input = example.planning_input().clone();

    // Start with an empty blueprint containing only our sleds, no zones.
    let initial_blueprint = BlueprintBuilder::build_empty_with_sleds_seeded(
        base_input.all_sled_ids(SledFilter::Commissioned),
        "test suite",
        (test_name, "ExampleSystem initial"),
    );

    // Start with an empty collection.
    let collection = example.collection();

    // Now make a blueprint and collection with some zones on each sled.
    let mut builder = BlueprintBuilder::new_based_on(
        log,
        &initial_blueprint,
        &base_input,
        collection,
        "test suite",
    )
    .unwrap();
    builder.set_rng_seed((test_name, "ExampleSystem make_zones"));
    for (i, (sled_id, sled_resources)) in
        base_input.all_sled_resources(SledFilter::Commissioned).enumerate()
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
        if i < INTERNAL_DNS_REDUNDANCY {
            let _ = builder
                .sled_ensure_zone_multiple_internal_dns(sled_id, 1)
                .unwrap();
        }
        let _ = builder.sled_ensure_disks(sled_id, sled_resources).unwrap();
        for pool_name in sled_resources.zpools.keys() {
            let _ =
                builder.sled_ensure_zone_crucible(sled_id, *pool_name).unwrap();
        }
    }

    let blueprint = builder.build();

    // Seed the sleds in the system with the corresponding zones from the
    // blueprint.
    let system_mut = example.system_mut();
    for (sled_id, zones) in &blueprint.blueprint_zones {
        system_mut
            .sled_set_omicron_zones(
                *sled_id,
                zones.to_omicron_zones_config(
                    BlueprintZoneFilter::ShouldBeRunning,
                ),
            )
            .expect(
                "we should only attempt to add zones \
                 to sleds that exist",
            );
    }

    (example, blueprint)
}

pub struct ExampleSystem {
    log: slog::Logger,
    system: SystemDescription,
    // The input and collection. These are pure functions of `system`, and are
    // invalidated if `system` changes.
    derived: OnceLock<ExampleSystemDerived>,
    // If we add more types of RNGs than just sleds here, we'll need to
    // expand this to be similar to BlueprintBuilderRng where a root RNG
    // creates sub-RNGs.
    //
    // This is currently only used for tests, so it looks unused in normal
    // builds.  But in the future it could be used by other consumers, too.
    sled_rng: TypedUuidRng<SledKind>,
    // Each collection needs to get its own ID that's deterministic in tests.
    // Each time `derived` is invalidated, `collection_rng` is called to
    // generate the next collection UUID.
    collection_rng: TypedUuidRng<CollectionKind>,
    next_collection_id: CollectionUuid,
}

impl ExampleSystem {
    fn new(
        log: &slog::Logger,
        test_name: &str,
        nsleds: usize,
    ) -> ExampleSystem {
        let log = log.new(slog::o!("component" => "ExampleSystem", "test_name" => test_name.to_string()));
        let mut system = SystemDescription::new();
        let mut sled_rng = TypedUuidRng::from_seed(test_name, "ExampleSystem");

        let sled_ids: Vec<_> = (0..nsleds).map(|_| sled_rng.next()).collect();
        for sled_id in &sled_ids {
            // We can't add Omicron zones to the sleds until we've set up the
            // first blueprint, so we'll add them below.
            let _ = system.sled(SledBuilder::new().id(*sled_id)).unwrap();
        }

        let mut collection_rng =
            TypedUuidRng::from_seed(test_name, "ExampleSystem collection");
        let next_collection_id = collection_rng.next();

        Self {
            log,
            system,
            derived: OnceLock::new(),
            sled_rng,
            collection_rng,
            next_collection_id,
        }
    }

    /// Returns a reference to the system description.
    pub fn system(&self) -> &SystemDescription {
        &self.system
    }

    /// Returns a mutable reference to the system description, invalidating
    /// internal caches.
    pub fn system_mut(&mut self) -> &mut SystemDescription {
        self.invalidate_caches();
        &mut self.system
    }

    /// Adds a new empty sled to the system description, invalidating internal
    /// caches.
    ///
    /// Returns the new sled's ID.
    pub fn add_empty_sled(&mut self) -> SledUuid {
        self.add_sled_with_builder(|sled| sled)
    }

    /// Adds a new sled to the system description, invalidating internal
    /// caches.
    ///
    /// The callback can be used to customize the sled via a [`SledBuilder`]
    /// before adding it.
    ///
    /// Returns the new sled's ID.
    pub fn add_sled_with_builder<F>(&mut self, f: F) -> SledUuid
    where
        F: FnOnce(SledBuilder) -> SledBuilder,
    {
        self.invalidate_caches();

        let sled_id = self.sled_rng.next();
        let sled = SledBuilder::new().id(sled_id);
        let sled = f(sled);
        let _ = self.system.sled(sled).expect("new sled added successfully");
        sled_id
    }

    /// Sets the Omicron zones for a sled, invalidating internal caches.
    pub fn sled_set_omicron_zones(
        &mut self,
        sled_id: SledUuid,
        zones: OmicronZonesConfig,
    ) -> anyhow::Result<()> {
        self.invalidate_caches();
        self.system.sled_set_omicron_zones(sled_id, zones)?;

        Ok(())
    }

    /// Returns the planning input for the system.
    ///
    /// This is consistent with the system description, and is regenerated if
    /// [`Self::system_mut`] is called.
    pub fn planning_input(&self) -> &PlanningInput {
        &self.get_or_init_derived().input
    }

    /// Returns an inventory collection representing the system.
    ///
    /// This is consistent with the system description, and is regenerated if
    /// [`Self::system_mut`] is called. Some tests may wish to simulate
    /// situations where inventory collection lags a bit -- in those cases, it
    /// is recommended that you clone the collection.
    pub fn collection(&self) -> &Collection {
        &self.get_or_init_derived().collection
    }

    fn get_or_init_derived(&self) -> &ExampleSystemDerived {
        self.derived.get_or_init(|| {
            let input_builder = self
                .system
                .to_planning_input_builder()
                .expect("failed to make planning input builder");
            let input = input_builder.build();

            let collection_builder = self
                .system
                .to_collection_builder()
                .expect("failed to build collection");
            let mut collection = collection_builder.build();
            // This is a somewhat gross hack.
            collection.id = self.next_collection_id;

            ExampleSystemDerived { input, collection }
        })
    }

    // This must be called whenever the system description changes.
    fn invalidate_caches(&mut self) {
        // If the derived cache wasn't created, then we don't need to
        // invalidate anything (and in particular we don't need to bump the
        // collection ID). Note that this is a `&mut self` method, which means
        // that we're statically guaranteed that nothing else is attempting to
        // initialize derived.
        if self.derived.get().is_none() {
            return;
        }
        self.derived = OnceLock::new();
        self.next_collection_id = self.collection_rng.next();
        slog::debug!(
            self.log,
            "invalidated derived cache";
            "next_collection_id" => %self.next_collection_id,
        );
    }
}

struct ExampleSystemDerived {
    input: PlanningInput,
    collection: Collection,
}
