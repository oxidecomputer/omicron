// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared types for the OpenAPI manager.
//!
//! API trait crates can depend on this crate to get access to interfaces
//! exposed by the OpenAPI manager.

mod validation;

pub use validation::*;
