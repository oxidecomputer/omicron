// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request body types for the bootstrap agent

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Identity signed by local RoT and Oxide certificate chain.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareRequest {
    // TODO-completeness: format TBD; currently opaque.
    pub identity: Vec<u8>,
}
