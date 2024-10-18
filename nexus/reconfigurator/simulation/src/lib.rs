// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simulation of reconfigurator states.
//!
//! This library contains facilities to track and simulate successive system
//! states in the face of reconfiguration events. The library uses an operation
//! log internally to make it possible to rewind to previous states.

mod config;
pub mod errors;
mod policy;
mod rng;
mod sim;
mod state;
mod system;

pub use config::*;
pub use rng::*;
pub use sim::*;
pub use state::*;
pub use system::*;
