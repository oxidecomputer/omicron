// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A store of successive reconfigurator states: the main entrypoint for
//! reconfigurator simulation.

use std::{collections::HashMap, sync::Arc};

use indexmap::IndexSet;
use omicron_uuid_kinds::{
    ReconfiguratorSimStateKind, ReconfiguratorSimStateUuid,
};
use typed_rng::TypedUuidRng;

use crate::{
    SimState,
    errors::{StateIdPrefixError, StateMatch},
    seed_from_entropy,
};

/// A store to track reconfigurator states: the main entrypoint for
/// reconfigurator simulation.
///
/// This store has key-based storage for systems and their states, and allows
/// for append-only storage of new states.
///
/// # Implementation notes
///
/// We currently index by UUIDs, but we could index by the hash of the contents
/// and make it a Merkle tree just as well. (We'd have to hook up canonical
/// hashing etc; it's a bunch of work but not too difficult). If there's a
/// compelling reason to use Merkle trees, we should switch over to that.
///
/// Currently, all data within is stored via `Arc` instances to enable cheap
/// cloning. In the future, it would be interesting to store data at the top
/// level, and only store references to it within each `SimState`. (The
/// references could be UUIDs, Rust `&` references, or content-addressed
/// hashes.) This does add some complexity, though, and there isn't a clear
/// benefit at the moment. For more, see the comment within the definition of
/// `system.rs`'s `SimSystem`.
#[derive(Clone, Debug)]
pub struct Simulator {
    log: slog::Logger,
    // The set of terminal nodes in the tree -- all states are reachable from
    // one or more of these.
    //
    // Similar to the list of Git branches or Jujutsu/Mercurial heads.
    //
    // In the future, it would be interesting to store a chain of every set of
    // heads over time, similar to `jj op log`. That would let us implement undo
    // and restore operations.
    heads: IndexSet<ReconfiguratorSimStateUuid>,
    states: HashMap<ReconfiguratorSimStateUuid, Arc<SimState>>,
    // This state corresponds to `ROOT_ID`.
    //
    // Storing it in the Arc is extremely important! `SimStateBuilder` stores a
    // pointer to the root state to ensure that there's no attempt to compare
    // "unrelated histories". The Simulator struct itself can both be cloned and
    // moved in memory, but because `root_state` is never changed, it always
    // points to the same memory address.
    root_state: Arc<SimState>,
    // Top-level (unversioned) RNG.
    sim_uuid_rng: TypedUuidRng<ReconfiguratorSimStateKind>,
}

impl Simulator {
    /// The root ID of the store.
    ///
    /// This is always defined to be the nil UUID, and if queried will always
    /// have a state associated with it.
    pub const ROOT_ID: ReconfiguratorSimStateUuid =
        ReconfiguratorSimStateUuid::nil();

    /// Create a new simulator with the given initial seed.
    pub fn new(log: &slog::Logger, seed: Option<String>) -> Self {
        let seed = seed.unwrap_or_else(|| seed_from_entropy());
        Self::new_inner(log, seed)
    }

    fn new_inner(log: &slog::Logger, seed: String) -> Self {
        let log = log.new(slog::o!("component" => "SimStore"));
        // The ReconfiguratorSimStateUuid type used to be ReconfiguratorSimUuid.
        // Retain the old name in the seed for generated ID compatibility.
        let sim_uuid_rng =
            TypedUuidRng::from_seed(&seed, "ReconfiguratorSimUuid");
        let root_state = SimState::new_root(seed);
        Self {
            log,
            heads: IndexSet::new(),
            states: HashMap::new(),
            root_state,
            sim_uuid_rng,
        }
    }

    /// Get the initial RNG seed.
    ///
    /// Versioned configurations start with this seed, though they may choose
    /// to change it as they go along.
    pub fn initial_seed(&self) -> &str {
        &self.root_state.rng().seed()
    }

    /// Get the current heads of the store.
    #[inline]
    pub fn heads(&self) -> &IndexSet<ReconfiguratorSimStateUuid> {
        &self.heads
    }

    /// Get the state for the given UUID.
    pub fn get_state(
        &self,
        id: ReconfiguratorSimStateUuid,
    ) -> Option<&SimState> {
        if id == Self::ROOT_ID {
            return Some(&self.root_state);
        }
        Some(&**self.states.get(&id)?)
    }

    /// Get the root state.
    ///
    /// This is equivalent to
    /// [`Self::get_state`]`(`[`Self::ROOT_ID`]`).unwrap()`.
    pub fn root_state(&self) -> &SimState {
        &self.root_state
    }

    /// Get a state by UUID prefix.
    ///
    /// Returns the unique state ID that matches the given prefix.
    /// Returns an error if zero or multiple states match the prefix.
    pub fn get_state_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<ReconfiguratorSimStateUuid, StateIdPrefixError> {
        let mut matching_ids = Vec::new();

        if Self::ROOT_ID.to_string().starts_with(prefix) {
            matching_ids.push(Self::ROOT_ID);
        }

        for id in self.states.keys() {
            if id.to_string().starts_with(prefix) {
                matching_ids.push(*id);
            }
        }

        match matching_ids.len() {
            0 => Err(StateIdPrefixError::NoMatch(prefix.to_string())),
            1 => Ok(matching_ids[0]),
            n => {
                // Sort for deterministic output.
                matching_ids.sort();

                let matches = matching_ids
                    .iter()
                    .map(|id| {
                        let state = self
                            .get_state(*id)
                            .expect("matching ID should have a state");
                        StateMatch {
                            id: *id,
                            generation: state.generation(),
                            description: state.description().to_string(),
                        }
                    })
                    .collect();

                Err(StateIdPrefixError::Ambiguous {
                    prefix: prefix.to_string(),
                    count: n,
                    matches,
                })
            }
        }
    }

    #[inline]
    pub(crate) fn next_sim_uuid(&mut self) -> ReconfiguratorSimStateUuid {
        self.sim_uuid_rng.next()
    }

    // Invariant: the ID should be not present in the store, having been
    // generated by next_sim_uuid.
    pub(crate) fn add_state(&mut self, state: Arc<SimState>) {
        let id = state.id();
        let parent = state.parent();
        if self.states.insert(id, state).is_some() {
            panic!("ID {id} should be unique and generated by the store");
        }

        // Remove the parent if it exists as a head, and in any case add the
        // new one. Unlike in source control we don't have a concept of
        // "merges" here, so there's exactly one parent that may need to be
        // removed.
        if let Some(parent) = parent {
            self.heads.shift_remove(&parent);
        }
        self.heads.insert(id);

        slog::debug!(
            self.log,
            "committed new state";
            "id" => %id,
            "parent" => ?parent,
        );
    }
}
