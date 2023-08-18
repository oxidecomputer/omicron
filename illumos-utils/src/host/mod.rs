// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to either access or emulate a host system

mod byte_queue;
mod error;
mod executor;
mod input;
mod output;

pub const PFEXEC: &str = "/usr/bin/pfexec";

pub use error::ExecutionError;
pub use executor::{
    command_to_string, BoxedExecutor, CommandSequence, FakeExecutor,
    HostExecutor,
};
pub use input::Input;
pub use output::{Output, OutputExt};
