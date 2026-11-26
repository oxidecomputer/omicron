// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Fault Management simulator core library.
//!
//! This crate provides the core data types and operations for running fault
//! management in a simulated environment. It's designed to be used both in unit
//! tests and as the basis for the simulator CLI in `dev-tools/fm-sim`.

mod error;
mod state;
pub mod time;

pub use error::Error;
pub use state::{SimState, SimStateBuilder};
