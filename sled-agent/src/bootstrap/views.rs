// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Response types for the bootstrap agent

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Sent between bootstrap agents to establish trust quorum.
// Note: We intentionally do not derive `Debug` on this type, to avoid
// accidentally debug-logging the secret share.
#[derive(Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ShareResponse {
    // TODO-completeness: format TBD; currently opaque.
    pub shared_secret: Vec<u8>,
}

/// Describes the Sled Agent running on the device.
#[derive(Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SledAgentResponse {
    pub id: Uuid,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum Response {
    SledAgentResponse(SledAgentResponse),
    ShareResponse(ShareResponse),
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct ResponseEnvelope {
    pub version: u32,
    pub response: Result<Response, String>,
}
