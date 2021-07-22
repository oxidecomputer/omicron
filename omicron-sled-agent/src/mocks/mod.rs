//! Mock structures for testing.

use mockall::mock;
use omicron_common::api::external::Error;
use omicron_common::api::internal::nexus::InstanceRuntimeState;
use omicron_common::api::internal::nexus::SledAgentStartupInfo;
use slog::Logger;
use std::net::SocketAddr;
use uuid::Uuid;

mock! {
    pub NexusClient {
        pub fn new(server_addr: SocketAddr, log: Logger) -> Self;
        pub async fn notify_sled_agent_online(
            &self,
            id: Uuid,
            info: SledAgentStartupInfo,
        ) -> Result<(), Error>;
        pub async fn notify_instance_updated(
            &self,
            id: &Uuid,
            new_runtime_state: &InstanceRuntimeState,
        ) -> Result<(), Error>;
    }
}
