// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Response types for the bootstrap agent

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Describes the Sled Agent running on the device.
#[derive(Serialize, Deserialize, PartialEq)]
pub struct SledAgentResponse {
    pub id: Uuid,
}

#[derive(Serialize, Deserialize, PartialEq)]
// Note: We intentionally do not derive `Debug` on this type, to avoid
// accidentally debug-logging the secret share.
pub enum Response {
    SledAgentResponse(SledAgentResponse),
}

#[derive(Serialize, Deserialize, PartialEq)]
pub struct ResponseEnvelope {
    pub version: u32,
    pub response: Result<Response, String>,
}
