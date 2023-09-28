// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Local storage abstraction for use by sled-agent

pub(crate) mod dataset;
pub(crate) mod disk;
pub(crate) mod dump_setup;
pub mod error;
pub(crate) mod pool;
pub mod state;
