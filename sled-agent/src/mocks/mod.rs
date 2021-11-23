//! Mock structures for testing.

use mockall::mock;
use nexus_client::types::{InstanceRuntimeState, SledAgentStartupInfo};
use omicron_common::api::external::Error;
use slog::Logger;
use uuid::Uuid;

mock! {
    pub NexusClient {
        pub fn new(server_addr: &str, log: Logger) -> Self;
        pub async fn cpapi_sled_agents_post(
            &self,
            id: &Uuid,
            info: &SledAgentStartupInfo,
        ) -> Result<(), Error>;
        pub async fn cpapi_instances_put(
            &self,
            id: &Uuid,
            new_runtime_state: &InstanceRuntimeState,
        ) -> Result<(), Error>;
    }
}
