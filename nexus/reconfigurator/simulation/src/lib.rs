// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulation of reconfigurator states.
//!
//! `nexus-reconfigurator-simulation` consists of facilities for:
//!
//! * Simulating successive system states in the face of reconfiguration
//!   events.
//! * Tracking a history of states, allowing for states to be rewound and
//!   new branches explored.
//!
//! `nexus-reconfigurator-simulation` is intended to be used in:
//!
//! * The reconfigurator CLI for interactive exploration.
//! * Example-based tests.
//! * More complex randomized (chaos) testing scenarios, such as randomly
//!   generating failure conditions.
//!
//! # Usage
//!
//! The main entrypoint of the library is the [`Simulator`] type. This
//! simulator type stores a tree of states in a UUID-indexed store. This is
//! similar to Git and other source control systems, except we don't use a
//! Merkle tree for now. But we could in the future if there's a compelling
//! reason to do so.
//!
//! Each state is captured in a [`SimState`], and consists of:
//!
//! * The ID of the state.
//! * The ID of the parent state.
//! * Metadata about the state, including the generation number, a
//!   description, and a log of changes made in that state.
//! * The contents of the state, which are:
//!   * The system itself, as a [`SimSystem`].
//!   * Configuration and policy knobs, as a [`SimConfig`].
//!   * The RNG state, as a [`SimRng`].
//!
//! Mutating states is done by calling [`SimState::to_mut`], which returns a
//! [`SimStateBuilder`]. Once changes are made, the state can be committed back
//! to the system with [`SimStateBuilder::commit`].
//!
//! ## Determinism
//!
//! `nexus-reconfigurator-simulation` is structured to be fully deterministic,
//! so that simulations can be replayed. Internally, it uses a seeded RNG, and
//! the only source of non-determinism is the seed for the RNG.

mod config;
pub mod errors;
mod rng;
mod sim;
mod state;
mod system;
mod utils;
mod zone_images;

pub use config::*;
pub use rng::*;
pub use sim::*;
pub use state::*;
pub use system::*;
pub use zone_images::*;
