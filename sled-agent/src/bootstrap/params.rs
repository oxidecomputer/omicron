//! Request body types for the bootstrap agent

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Identity signed by local RoT and Oxide certificate chain.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ShareRequest {
    // TODO-completeness: format TBD; currently opaque.
    pub identity: Vec<u8>,
}
