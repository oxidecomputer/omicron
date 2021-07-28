//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, IdentityMetadata, InstanceCpuCount,
    InstanceState,
};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

pub struct Rack {
    pub identity: IdentityMetadata,
}

pub struct Sled {
    pub identity: IdentityMetadata,
    pub service_address: SocketAddr,
}

/// A collection of associated resources.
pub struct Project {
    /// common identifying metadata.
    pub identity: IdentityMetadata,
}

/// A Disk (network block device).
#[derive(Clone, Debug)]
pub struct Disk {
    /// common identifying metadata.
    pub identity: IdentityMetadata,
    /// id for the project containing this Disk
    pub project_id: Uuid,
    /// id for the snapshot from which this Disk was created (None means a blank
    /// disk)
    pub create_snapshot_id: Option<Uuid>,
    /// size of the Disk
    pub size: ByteCount,
    /// runtime state of the Disk
    pub runtime: DiskRuntimeState,
}

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

/// An Instance (VM).
#[derive(Clone, Debug)]
pub struct Instance {
    /// common identifying metadata
    pub identity: IdentityMetadata,

    /// id for the project containing this Instance
    pub project_id: Uuid,

    /// number of CPUs allocated for this Instance
    pub ncpus: InstanceCpuCount,
    /// memory allocated for this Instance
    pub memory: ByteCount,
    /// RFC1035-compliant hostname for the Instance.
    // TODO-cleanup different type?
    pub hostname: String,

    /// state owned by the data plane
    pub runtime: InstanceRuntimeState,
    // TODO-completeness: add disks, network, tags, metrics
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
    /// generation number for this state
    pub gen: Generation,
    /// timestamp for this information
    pub time_updated: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct VPC {
    /** common identifying metadata */
    pub identity: IdentityMetadata,
    /** id for the project containing this VPC */
    pub project_id: Uuid,
}

/// Sent by a sled agent on startup to Nexus to request further instruction
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct SledAgentStartupInfo {
    /// the address of the sled agent's API endpoint
    pub sa_address: SocketAddr,
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

/// An assignment of an Oximeter instance to a metric producer for collection.
// TODO: Goes to storage
#[derive(Debug, Clone, Copy, JsonSchema, Serialize, Deserialize)]
pub struct OximeterAssignment {
    pub oximeter_id: Uuid,
    pub producer_id: Uuid,
}
