use omicron_common::api::internal::nexus::DiskRuntimeState;
use omicron_common::api::internal::sled_agent::DiskStateRequested;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Sent from to a sled agent to establish the runtime state of a Disk
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DiskEnsureBody {
    /// Last runtime state of the Disk known to Nexus (used if the agent has
    /// never seen this Disk before).
    pub initial_runtime: DiskRuntimeState,
    /// requested runtime state of the Disk
    pub target: DiskStateRequested,
}
