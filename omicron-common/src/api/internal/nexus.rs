//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount,
    DiskState,
    Generation,
    IdentityMetadata,
    InstanceCpuCount,
    InstanceState,
};
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
