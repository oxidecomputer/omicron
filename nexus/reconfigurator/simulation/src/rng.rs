// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Versioned random number generation for the simulator.

use nexus_inventory::CollectionBuilderRng;
use nexus_reconfigurator_planning::{
    blueprint_builder::BlueprintBuilderRng,
    example::{ExampleRngState, ExampleSystemRng},
};
use omicron_uuid_kinds::SledUuid;

/// Versioned random number generator for the simulator.
///
/// The simulator is designed to be as deterministic as possible, so that
/// simulations can be replayed and compared. To that end, this RNG is
/// versioned.
#[derive(Clone, Debug)]
pub struct SimRng {
    // ExampleRngState is cheap to clone (just a string and a bunch of
    // integers), so there's no need for Arc.
    state: ExampleRngState,
}

impl SimRng {
    /// Create a new RNG.
    pub fn from_entropy() -> Self {
        let seed = seed_from_entropy();
        let state = ExampleRngState::from_seed(&seed);
        Self { state }
    }

    pub fn from_seed(seed: String) -> Self {
        let state = ExampleRngState::from_seed(&seed);
        Self { state }
    }

    /// Obtain the current seed.
    pub fn seed(&self) -> &str {
        self.state.seed()
    }

    /// Set a new seed for the RNG, resetting internal state.
    pub fn set_seed(&mut self, seed: String) {
        self.state = ExampleRngState::from_seed(&seed);
    }

    /// Get the next example system RNG.
    #[inline]
    pub fn next_example_rng(&mut self) -> ExampleSystemRng {
        self.state.next_system_rng()
    }

    /// Get the next collection RNG.
    #[inline]
    pub fn next_collection_rng(&mut self) -> CollectionBuilderRng {
        self.state.next_collection_rng()
    }

    /// Get the next blueprint RNG.
    #[inline]
    pub fn next_blueprint_rng(&mut self) -> BlueprintBuilderRng {
        self.state.next_blueprint_rng()
    }

    /// Get the next sled ID.
    #[inline]
    #[must_use]
    pub fn next_sled_id(&mut self) -> SledUuid {
        self.state.next_sled_id_rng().next()
    }

    /// Reset internal state while keeping the same seed.
    ///
    /// The RNGs are stateful, so it can be useful to reset them back to their
    /// initial state.
    ///
    /// In general, it only makes sense to call this as part of a system wipe.
    /// If it is called outside of a system wipe, then duplicate IDs might be
    /// generated.
    pub fn reset_state(&mut self) {
        self.state = ExampleRngState::from_seed(self.state.seed());
    }

    /// Regenerate a new seed for the RNG, resetting internal state.
    ///
    /// The seed is returned, and the caller is expected to display it to the
    /// user.
    #[must_use]
    pub fn regenerate_seed(&mut self) -> String {
        let seed = seed_from_entropy();
        self.set_seed(seed.clone());
        seed
    }
}

pub(crate) fn seed_from_entropy() -> String {
    petname::petname(3, "-")
        .expect("non-zero length requested => cannot be empty")
}
