// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types for network setup required to bring up the control plane.

pub use sled_agent_types_migrations::latest::early_networking::*;

// Re-export back_compat module for bootstore serialization
pub use sled_agent_types_migrations::v1::early_networking::back_compat;
