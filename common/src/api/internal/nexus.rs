//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, InstanceCpuCount, InstanceState,
};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// Runtime state of the Disk, which includes its attach state and some minimal
/// metadata
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DiskRuntimeState {
    /// runtime state of the Disk
    pub disk_state: DiskState,
    /// generation number for this state
    pub gen: Generation,
    /// timestamp for this information
    pub time_updated: DateTime<Utc>,
}

/// Runtime state of the Instance, including the actual running state and minimal
/// metadata
///
/// This state is owned by the sled agent running that Instance.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeState {
    /// runtime state of the Instance
    pub run_state: InstanceState,
    /// which sled is running this Instance
    pub sled_uuid: Uuid,
    /// number of CPUs allocated for this Instance
    pub ncpus: InstanceCpuCount,
    /// memory allocated for this Instance
    pub memory: ByteCount,
    /// RFC1035-compliant hostname for the Instance.
    // TODO-cleanup different type?
    pub hostname: String,
    /// generation number for this state
    pub gen: Generation,
    /// timestamp for this information
    pub time_updated: DateTime<Utc>,
}

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentStartupInfo {
    /// the address of the sled agent's API endpoint
    pub sa_address: SocketAddr,
}

/// Describes a dataset within a pool.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DatasetInfo {
    /// Unique identifier for the dataset.
    pub id: Uuid,
}

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentPoolInfo {
    /// Unique identifier for the pool.
    pub id: Uuid,
    /// Total size of the pool.
    pub size: ByteCount,
    // TODO: We could include any other data from `ZpoolInfo` we want,
    // such as "allocated/free" space and pool health?
    /// Description of datasets within the pool.
    pub datasets: Vec<DatasetInfo>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct AllocationInfo {
    /// A minimum reservation size for a filesystem.
    /// Refer to ZFS native properties for more detail.
    pub reservation: ByteCount,
    /// A maximum quota on filesystem usage.
    /// Refer to ZFS native properties for more detail.
    pub quota: ByteCount,
}

/// Allocation strategy for pools.
// TODO: This could be useful for indicating quotas, or
// for Nexus instructing the Sled Agent "what to format, and where".
//
// For now, the Sled Agent is a bit more proactive about allocation
// decisions - see the "storage manager" section of the Sled Agent for
// more details. Nexus, in response, merely advises minimums/maximums
// for dataset sizes.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentPoolAllocation {
    /// Mapping of Dataset UUID to allocation properties to set.
    pub allocations: HashMap<Uuid, AllocationInfo>,
}

// Oximeter producer/collector objects.

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ProducerEndpoint {
    pub id: Uuid,
    pub address: SocketAddr,
    pub base_route: String,
    pub interval: Duration,
}

impl ProducerEndpoint {
    /**
     * Return the route that can be used to request metric data.
     */
    pub fn collection_route(&self) -> String {
        format!("{}/{}", &self.base_route, &self.id)
    }
}

/// Message used to notify Nexus that this oximeter instance is up and running.
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterInfo {
    /// The ID for this oximeter instance.
    pub collector_id: Uuid,

    /// The address on which this oximeter instance listens for requests
    pub address: SocketAddr,
}
