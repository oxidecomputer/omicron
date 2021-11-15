//! Mock structures for testing.

use anyhow::Error;
use mockall::mock;
use omicron_common::nexus_client::types::{
    DatasetPutRequest, DatasetPutResponse, InstanceRuntimeState,
    SledAgentStartupInfo, ZpoolPutRequest, ZpoolPutResponse,
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
        pub async fn zpool_put(
            &self,
            zpool_id: &Uuid,
            sled_id: &Uuid,
            info: &ZpoolPutRequest,
        ) -> Result<ZpoolPutResponse, Error>;
        pub async fn dataset_put(
            &self,
            dataset_id: &Uuid,
            zpool_id: &Uuid,
            info: &DatasetPutRequest,
        ) -> Result<DatasetPutResponse, Error>;
    }
}
