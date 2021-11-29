// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Response types for the bootstrap agent

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Sent between bootstrap agents to establish trust quorum.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareResponse {
    // TODO-completeness: format TBD; currently opaque.
    pub shared_secret: Vec<u8>,
}
