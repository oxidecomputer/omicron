//! Mock structures for testing.

use anyhow::Error;
use mockall::mock;
use omicron_common::nexus_client::types::{
    DatasetPostRequest, DatasetPostResponse, InstanceRuntimeState,
    SledAgentStartupInfo, ZpoolPostRequest, ZpoolPostResponse,
};
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
        pub async fn zpool_post(
            &self,
            zpool_id: &Uuid,
            sled_id: &Uuid,
            info: &ZpoolPostRequest,
        ) -> Result<ZpoolPostResponse, Error>;
        pub async fn dataset_post(
            &self,
            dataset_id: &Uuid,
            zpool_id: &Uuid,
            info: &DatasetPostRequest,
        ) -> Result<DatasetPostResponse, Error>;
    }
}
