// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Common types used for Nexus configuration.
//!
//! The root structure is [`NexusConfig`].

mod nexus_config;
mod postgres_config;

pub use nexus_config::*;
pub use postgres_config::*;
