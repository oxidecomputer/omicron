// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! The state of the simulated system. [`SimState`] is an immutable snapshot;
//! states are never edited in place, so users can hang onto them and use them
//! for before/after comparisons in tests. [`SimStateBuilder`] enables
//! functional-style mutations, producing new state snapshots from old ones.

use std::ops::Deref;

use chrono::{DateTime, Utc};

/// The whole simulated world, as an immutable value.
#[derive(Clone, Debug)]
pub struct SimState {
    now: DateTime<Utc>,
    // TODO(#10209): Add more state here to support the full set of things the
    // DEs and FM system care about; see https://726.rfd.oxide.computer.
}

impl SimState {
    /// The initial time for a new simulation.
    pub const EPOCH: DateTime<Utc> = DateTime::UNIX_EPOCH;

    /// Constructs an initial empty state.
    pub fn new() -> Self {
        Self { now: Self::EPOCH }
    }

    /// Returns the current simulated time.
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

/// Provides functional-style mutation for [`SimState`], allowing callers to
/// safely make changes to a copy of some given base state without affecting
/// the original.
///
/// This only provides the most basic primitive operations necessary to mutate
/// the simulated state; higher-level operations are defined in their respective
/// modules.
#[derive(Clone, Debug)]
pub struct SimStateBuilder {
    state: SimState,
}

impl SimStateBuilder {
    /// Constructs a new builder initialized from [`SimState::new`].
    pub fn new() -> Self {
        Self { state: SimState::new() }
    }

    /// Constructs a new builder from a copy of `state`.
    pub fn from_state(state: &SimState) -> Self {
        Self { state: state.clone() }
    }

    /// Returns the state.
    pub fn build(self) -> SimState {
        self.state
    }

    /// Primitive setter for the simulated clock. For the "advance" operation,
    /// see [`time::advance`](crate::time::advance).
    pub fn set_now(&mut self, now: DateTime<Utc>) {
        self.state.now = now;
    }
}

// We implement `Deref` on the builder for convenient syntactic sugar, so users
// can transparently call accessor methods on the underlying `SimState`. Yes,
// this violates the Rust API guideline on `Deref` (see
// https://rust-lang.github.io/api-guidelines/predictability#c-deref), but we
// don't generally follow that guideline in Omicron.
impl Deref for SimStateBuilder {
    type Target = SimState;

    fn deref(&self) -> &SimState {
        &self.state
    }
}
