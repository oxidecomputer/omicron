// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron component inventory
//! XXX-dap TODO-doc
//!
//! This is currently inside Nexus, but it's expected to have few dependencies
//! on parts of Nexus (beyond the database crates) and could conceivably be put
//! into other components.

mod builder;
mod collector;

pub use collector::Collector;
