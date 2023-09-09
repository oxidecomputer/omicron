// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

//! A framework to declare a series of steps to perform updates with.
//!
//! The general data flow of the engine is as a series of steps that is executed
//! in a linear fashion, meant as a one-shot workflow. The closest analogue is
//! to GitHub Actions.
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
//! # Differences from distributed sagas
//!
//! At a high level, this engine is not entirely dissimilar from distributed
//! sagas, as implemented in [Steno](https://github.com/oxidecomputer/steno) and
//! elsewhere. However, there are some key differences that make this unique and
//! optimized for linear, one-shot flows:
//!
//! 1. This engine is not designed to be distributed. Instead, it is designed to
//!    start and complete within a single process. This has advantages, namely
//!    that steps can borrow from the stack and transfer non-serializable state
//!    across nodes.
//! 2. The engine is currently a linear list of operations and not a general
//!    DAG. The closest analogue is the linear series of steps that GitHub
//!    Actions runs. This can change in the future to a generic DAG, but this is
//!    simple for now since linearity is all we need.
//! 3. There's no notion of undos. Instead, steps are expected to keep retrying
//!    autonomously until they succeed.
//! 4. The update engine API comes with serializable progress and error
//!    reporting built in -- as a user of the API, you receive an event stream
//!    that you can process and/or send over a network request. In future work
//!    you'd also be able to nest steps. (This is a planned future improvement
//!    to Steno, but doesn't yet exist as of this writing.)
//!
//! # Future work
//!
//! 1. Support nested engines: steps that can generate other steps in a
//!    hierarchical fashion.
//! 2. Receive an event stream from a source and turn it into nested events in
//!    another source.

mod buffer;
mod context;
pub mod display;
mod engine;
pub mod errors;
pub mod events;
mod macros;
mod spec;
#[cfg(test)]
mod test_utils;

pub use buffer::*;
pub use context::*;
pub use engine::*;
pub use spec::*;
