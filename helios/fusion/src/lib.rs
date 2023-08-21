// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interfaces used to interact with the underlying host system.

mod error;
mod executor;
mod input;
mod output;

pub use error::*;
pub use executor::*;
pub use input::*;
pub use output::*;

pub const PFEXEC: &str = "/usr/bin/pfexec";
