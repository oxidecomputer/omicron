// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Versioned random number generation for the simulator.

use std::fmt;

use nexus_inventory::CollectionBuilderRng;
use nexus_reconfigurator_planning::{
    example::{ExampleSystemRng, SimRngState},
    planner::PlannerRng,
};
use omicron_uuid_kinds::SledUuid;

/// Versioned random number generator for the simulator.
///
/// The simulator is designed to be as deterministic as possible, so that
/// simulations can be replayed and compared. To that end, this RNG is
/// versioned.
#[derive(Clone, Debug)]
pub struct SimRng {
    // SimRngState is cheap to clone (just a string and a bunch of integers), so
    // there's no need for Arc.
    state: SimRngState,
}

impl SimRng {
    /// Create a new RNG.
    pub fn from_entropy() -> Self {
        let seed = seed_from_entropy();
        let state = SimRngState::from_seed(&seed);
        Self { state }
    }

    pub fn from_seed(seed: String) -> Self {
        let state = SimRngState::from_seed(&seed);
        Self { state }
    }

    /// Obtain the current seed.
    pub fn seed(&self) -> &str {
        self.state.seed()
    }

    pub(crate) fn to_mut(&self) -> SimRngBuilder {
        SimRngBuilder { rng: self.clone(), log: Vec::new() }
    }
}

/// A [`SimRng`] that can be changed to create new states.
///
/// Returned by [`SimStateBuilder::rng_mut`](crate::SimStateBuilder::rng_mut).
#[derive(Clone, Debug)]
pub struct SimRngBuilder {
    rng: SimRng,
    log: Vec<SimRngLogEntry>,
}

impl SimRngBuilder {
    /// Obtain the current seed.
    pub fn seed(&self) -> &str {
        self.rng.seed()
    }

    /// Set a new seed for the RNG, resetting internal state.
    pub fn set_seed(&mut self, seed: String) {
        self.rng = SimRng::from_seed(seed.clone());
        self.log.push(SimRngLogEntry::SetSeed(seed));
    }

    /// Reset internal state while keeping the same seed.
    ///
    /// RNGs are stateful, so it can be useful to reset them back to their
    /// initial state.
    ///
    /// In general, it only makes sense to call this as part of a system wipe.
    /// If it is called outside of a system wipe, then duplicate IDs might be
    /// generated.
    pub fn reset_state(&mut self) {
        let existing_seed = self.rng.seed().to_owned();
        self.rng = SimRng::from_seed(existing_seed.clone());
    }

    /// Regenerate a new seed for the RNG from entropy (not from the existing
    /// seed!), resetting internal state.
    ///
    /// The seed is returned, and the caller may wish to log it.
    #[must_use = "consider logging or displaying the new seed"]
    pub fn regenerate_seed_from_entropy(&mut self) -> String {
        let seed = seed_from_entropy();
        self.rng = SimRng::from_seed(seed.clone());
        self.log.push(SimRngLogEntry::RegenerateSeedFromEntropy(seed.clone()));
        seed
    }

    /// Get the next example system RNG.
    pub fn next_example_rng(&mut self) -> ExampleSystemRng {
        self.log.push(SimRngLogEntry::NextExampleRng);
        self.rng.state.next_system_rng()
    }

    /// Get the next collection RNG.
    pub fn next_collection_rng(&mut self) -> CollectionBuilderRng {
        self.log.push(SimRngLogEntry::NextCollectionRng);
        self.rng.state.next_collection_rng()
    }

    /// Get the next blueprint RNG.
    pub fn next_planner_rng(&mut self) -> PlannerRng {
        self.log.push(SimRngLogEntry::NextPlannerRng);
        self.rng.state.next_planner_rng()
    }

    /// Get the next sled ID.
    #[must_use]
    pub fn next_sled_id(&mut self) -> SledUuid {
        let id = self.rng.state.next_sled_id_rng().next();
        self.log.push(SimRngLogEntry::NextSledId(id));
        id
    }

    pub(crate) fn into_parts(self) -> (SimRng, Vec<SimRngLogEntry>) {
        (self.rng, self.log)
    }
}

#[derive(Clone, Debug)]
pub enum SimRngLogEntry {
    SetSeed(String),
    ResetState { existing_seed: String },
    RegenerateSeedFromEntropy(String),
    NextExampleRng,
    NextCollectionRng,
    NextPlannerRng,
    NextSledId(SledUuid),
}

impl fmt::Display for SimRngLogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SimRngLogEntry::SetSeed(seed) => {
                write!(f, "set seed: {}", seed)
            }
            SimRngLogEntry::ResetState { existing_seed } => {
                write!(f, "reset state (existing seed: {})", existing_seed)
            }
            SimRngLogEntry::RegenerateSeedFromEntropy(seed) => {
                write!(f, "regenerate seed from entropy: {}", seed)
            }
            SimRngLogEntry::NextExampleRng => {
                write!(f, "next example rng")
            }
            SimRngLogEntry::NextCollectionRng => {
                write!(f, "next collection rng")
            }
            SimRngLogEntry::NextPlannerRng => {
                write!(f, "next planner rng")
            }
            SimRngLogEntry::NextSledId(id) => {
                write!(f, "next sled id: {}", id)
            }
        }
    }
}

pub(crate) fn seed_from_entropy() -> String {
    // Each of the word lists petname uses are drawn from a pool of roughly
    // 1000 words, so 3 words gives us around 30 bits of entropy. That should
    // hopefully be enough to explore the entire state space. But if necessary
    // we could also increase the length or expand the word lists (petname has
    // much bigger ones too).
    petname::petname(3, "-")
        .expect("non-zero length requested => cannot be empty")
}
