//! APIs exposed by the bootstrap agent

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Identity signed by local RoT and Oxide certificate chain.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareRequest {
    // TODO-completeness: format TBD; currently opaque.
    pub identity: Vec<u8>,
}

/// Sent between bootstrap agents to establish trust quorum.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareResponse {
    // TODO-completeness: format TBD; currently opaque.
    pub shared_secret: Vec<u8>,
}
