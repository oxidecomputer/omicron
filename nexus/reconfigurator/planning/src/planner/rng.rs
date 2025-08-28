// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! RNG for blueprint planning to allow reproducibility (particularly for
//! tests).

use omicron_uuid_kinds::BlueprintKind;
use omicron_uuid_kinds::BlueprintUuid;
use omicron_uuid_kinds::DatasetKind;
use omicron_uuid_kinds::DatasetUuid;
use omicron_uuid_kinds::ExternalIpKind;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::SledUuid;
use rand::SeedableRng as _;
use rand::rngs::StdRng;
use std::collections::BTreeMap;
use std::hash::Hash;
use typed_rng::TypedUuidRng;
use typed_rng::UuidRng;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct PlannerRng {
    parent: StdRng,
    // Have separate RNGs for the different kinds of UUIDs we might add,
    // generated from the main RNG. This is so that e.g. adding a new network
    // interface doesn't alter the blueprint or sled UUID.
    //
    // In the future, when we switch to typed UUIDs, each of these will be
    // associated with a specific `TypedUuidKind`.
    blueprint_rng: TypedUuidRng<BlueprintKind>,
    // Each sled gets its own set of RNGs to avoid test changes to one sled
    // (e.g., adding an extra zone) perturbing all subsequent zone/dataset/etc.
    // IDs on other sleds.
    sled_rngs: BTreeMap<SledUuid, SledPlannerRng>,
    clickhouse_rng: UuidRng,
}

impl PlannerRng {
    pub fn from_entropy() -> Self {
        Self::new_from_parent(StdRng::from_os_rng())
    }

    pub fn from_seed<H: Hash>(seed: H) -> Self {
        // Important to add some more bytes here, so that builders with the
        // same seed but different purposes don't end up with the same UUIDs.
        const SEED_EXTRA: &str = "blueprint-builder";
        Self::new_from_parent(typed_rng::from_seed(seed, SEED_EXTRA))
    }

    pub fn new_from_parent(mut parent: StdRng) -> Self {
        let blueprint_rng =
            TypedUuidRng::from_parent_rng(&mut parent, "blueprint");
        let clickhouse_rng =
            UuidRng::from_parent_rng(&mut parent, "clickhouse");

        Self {
            parent,
            blueprint_rng,
            sled_rngs: BTreeMap::new(),
            clickhouse_rng,
        }
    }

    pub fn sled_rng(&mut self, sled_id: SledUuid) -> &mut SledPlannerRng {
        self.sled_rngs
            .entry(sled_id)
            .or_insert_with(|| SledPlannerRng::new(sled_id, &mut self.parent))
    }

    pub fn next_blueprint(&mut self) -> BlueprintUuid {
        self.blueprint_rng.next()
    }

    pub fn next_clickhouse(&mut self) -> Uuid {
        self.clickhouse_rng.next()
    }
}

#[derive(Clone, Debug)]
pub struct SledPlannerRng {
    zone_rng: TypedUuidRng<OmicronZoneKind>,
    dataset_rng: TypedUuidRng<DatasetKind>,
    network_interface_rng: UuidRng,
    external_ip_rng: TypedUuidRng<ExternalIpKind>,
}

impl SledPlannerRng {
    fn new(sled_id: SledUuid, parent: &mut StdRng) -> Self {
        let zone_rng = TypedUuidRng::from_parent_rng(parent, (sled_id, "zone"));
        let dataset_rng =
            TypedUuidRng::from_parent_rng(parent, (sled_id, "dataset"));
        let network_interface_rng =
            UuidRng::from_parent_rng(parent, (sled_id, "network_interface"));
        let external_ip_rng =
            TypedUuidRng::from_parent_rng(parent, (sled_id, "external_ip"));

        Self { zone_rng, dataset_rng, network_interface_rng, external_ip_rng }
    }

    pub fn next_zone(&mut self) -> OmicronZoneUuid {
        self.zone_rng.next()
    }

    pub fn next_dataset(&mut self) -> DatasetUuid {
        self.dataset_rng.next()
    }

    pub fn next_network_interface(&mut self) -> Uuid {
        self.network_interface_rng.next()
    }

    pub fn next_external_ip(&mut self) -> ExternalIpUuid {
        self.external_ip_rng.next()
    }
}
