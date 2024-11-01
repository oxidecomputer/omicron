// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! RNG for blueprint planning to allow reproducibility (particularly for
//! tests).

use omicron_uuid_kinds::ExternalIpKind;
use omicron_uuid_kinds::ExternalIpUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::hash::Hash;
use typed_rng::TypedUuidRng;
use typed_rng::UuidRng;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct PlannerRng {
    // Have separate RNGs for the different kinds of UUIDs we might add,
    // generated from the main RNG. This is so that e.g. adding a new network
    // interface doesn't alter the blueprint or sled UUID.
    //
    // In the future, when we switch to typed UUIDs, each of these will be
    // associated with a specific `TypedUuidKind`.
    blueprint_rng: UuidRng,
    zone_rng: TypedUuidRng<OmicronZoneKind>,
    network_interface_rng: UuidRng,
    external_ip_rng: TypedUuidRng<ExternalIpKind>,
}

impl PlannerRng {
    pub fn new() -> Self {
        Self::new_from_parent(StdRng::from_entropy())
    }

    pub fn new_from_parent(mut parent: StdRng) -> Self {
        let blueprint_rng = UuidRng::from_parent_rng(&mut parent, "blueprint");
        let zone_rng = TypedUuidRng::from_parent_rng(&mut parent, "zone");
        let network_interface_rng =
            UuidRng::from_parent_rng(&mut parent, "network_interface");
        let external_ip_rng =
            TypedUuidRng::from_parent_rng(&mut parent, "external_ip");

        Self { blueprint_rng, zone_rng, network_interface_rng, external_ip_rng }
    }

    pub fn set_seed<H: Hash>(&mut self, seed: H) {
        // Important to add some more bytes here, so that builders with the
        // same seed but different purposes don't end up with the same UUIDs.
        const SEED_EXTRA: &str = "blueprint-builder";
        *self = Self::new_from_parent(typed_rng::from_seed(seed, SEED_EXTRA));
    }

    pub fn next_blueprint(&mut self) -> Uuid {
        self.blueprint_rng.next()
    }

    pub fn next_zone(&mut self) -> OmicronZoneUuid {
        self.zone_rng.next()
    }

    pub fn next_network_interface(&mut self) -> Uuid {
        self.network_interface_rng.next()
    }

    pub fn next_external_ip(&mut self) -> ExternalIpUuid {
        self.external_ip_rng.next()
    }
}
