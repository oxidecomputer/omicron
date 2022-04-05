// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Request body types for the bootstrap agent

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use omicron_common::api::external::Ipv6Net;

/// Identity signed by local RoT and Oxide certificate chain.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareRequest {
    // TODO-completeness: format TBD; currently opaque.
    pub identity: Vec<u8>,
}

/// Configuration information for launching a Sled Agent.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentRequest {
    /// ID of the Sled to be initialized.
    pub uuid: Uuid,

    /// Portion of the IP space to be managed by the Sled Agnet.
    pub ip: Ipv6Net,
}
