//! Response types for the bootstrap agent

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Sent between bootstrap agents to establish trust quorum.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareResponse {
    // TODO-completeness: format TBD; currently opaque.
    pub shared_secret: Vec<u8>,
}
