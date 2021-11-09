//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, InstanceCpuCount, InstanceState,
};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
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

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct ZpoolPostRequest {
    /// Total size of the pool.
    pub size: ByteCount,
    // TODO: We could include any other data from `ZpoolInfo` we want,
    // such as "allocated/free" space and pool health?
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct ZpoolPostResponse {}

/// Describes the purpose of the dataset.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, Copy)]
pub enum DatasetFlavor {
    Crucible,
    Cockroach,
    Clickhouse,
}

impl fmt::Display for DatasetFlavor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use DatasetFlavor::*;
        let s = match self {
            Crucible => "crucible",
            Cockroach => "cockroach",
            Clickhouse => "clickhouse",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for DatasetFlavor {
    type Err = crate::api::external::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use DatasetFlavor::*;
        match s {
            "crucible" => Ok(Crucible),
            "cockroach" => Ok(Cockroach),
            "clickhouse" => Ok(Clickhouse),
            _ => Err(Self::Err::InternalError {
                internal_message: format!("Unknown dataset flavor: {}", s),
            }),
        }
    }
}

/// Describes a dataset within a pool.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct DatasetPostRequest {
    /// Address on which a service is responding to requests for the
    /// dataset.
    pub address: SocketAddr,

    /// Type of dataset being inserted.
    pub flavor: DatasetFlavor,
}

/// Describes which ZFS properties should be set for a particular allocated
/// dataset.
// TODO: This could be useful for indicating quotas, or
// for Nexus instructing the Sled Agent "what to format, and where".
//
// For now, the Sled Agent is a bit more proactive about allocation
// decisions - see the "storage manager" section of the Sled Agent for
// more details. Nexus, in response, merely advises minimums/maximums
// for dataset sizes.
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct DatasetPostResponse {
    /// A minimum reservation size for a filesystem.
    /// Refer to ZFS native properties for more detail.
    pub reservation: Option<ByteCount>,
    /// A maximum quota on filesystem usage.
    /// Refer to ZFS native properties for more detail.
    pub quota: Option<ByteCount>,
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
