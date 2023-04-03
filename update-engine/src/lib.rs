// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! A framework to declare a series of steps to perform updates with.
//!
//! The general data flow of the engine is as a series of steps that is executed
//! in a linear fashion. The closest analogue is to GitHub Actions.
//!
//! # Features
//!
//! 1. Declare and run a series of steps in order.
//! 2. Generate implementation-specific metadata for step execution, progress,
//!    and completion.
//! 3. Use borrowed data within steps, so long as it outlives the engine itself.
//! 4. Share data between steps.
//! 5. Receive a stream of serializable events that also implements
//!    `JsonSchema`.
//!
//! # Examples
//!
//! For a full-fledged example including showing progress bars corresponding to
//! steps, see the `examples` directory.
//!
//! # Future work
//!
//! 1. Support nested steps: steps that can generate other steps in a
//!    hierarchical fashion.
//! 2. Receive an event stream from a source and turn it into nested events in
//!    another source.

mod engine;
mod errors;
pub mod events;
mod spec;
#[cfg(test)]
mod test_utils;

pub use engine::*;
pub use spec::*;
