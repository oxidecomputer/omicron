//! Mock structures for testing.

use mockall::mock;
use omicron_common::api::ApiError;
use omicron_common::api::ApiInstanceRuntimeState;
use omicron_common::api::ApiSledAgentStartupInfo;
use slog::Logger;
use std::net::SocketAddr;
use uuid::Uuid;

mock! {
    pub NexusClient {
        pub fn new(server_addr: SocketAddr, log: Logger) -> Self;
        pub async fn notify_sled_agent_online(
            &self,
            id: Uuid,
            info: ApiSledAgentStartupInfo,
        ) -> Result<(), ApiError>;
        pub async fn notify_instance_updated(
            &self,
            id: &Uuid,
            new_runtime_state: &ApiInstanceRuntimeState,
        ) -> Result<(), ApiError>;
    }
}
