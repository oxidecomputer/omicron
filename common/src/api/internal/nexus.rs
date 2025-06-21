// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, Hostname, InstanceCpuCount, Vni,
};
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::DownstairsRegionKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsRepairKind;
use omicron_uuid_kinds::UpstairsSessionKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Certificate {
    pub cert: String,
    pub key: String,
}

impl std::fmt::Debug for Certificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certificate")
            .field("cert", &self.cert)
            .field("key", &"<redacted>")
            .finish()
    }
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

/// The "static" properties of an instance: information about the instance that
/// doesn't change while the instance is running.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceProperties {
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    /// RFC1035-compliant hostname for the instance.
    pub hostname: Hostname,
}

/// One of the states that a VMM can be in.
#[derive(
    Copy, Clone, Debug, Deserialize, Serialize, JsonSchema, Eq, PartialEq,
)]
#[serde(rename_all = "snake_case")]
pub enum VmmState {
    /// The VMM is initializing and has not started running guest CPUs yet.
    Starting,
    /// The VMM has finished initializing and may be running guest CPUs.
    Running,
    /// The VMM is shutting down.
    Stopping,
    /// The VMM's guest has stopped, and the guest will not run again, but the
    /// VMM process may not have released all of its resources yet.
    Stopped,
    /// The VMM is being restarted or its guest OS is rebooting.
    Rebooting,
    /// The VMM is part of a live migration.
    Migrating,
    /// The VMM process reported an internal failure.
    Failed,
    /// The VMM process has been destroyed and its resources have been released.
    Destroyed,
}

impl VmmState {
    /// States in which the VMM no longer exists and must be cleaned up.
    pub const TERMINAL_STATES: &'static [Self] =
        &[Self::Failed, Self::Destroyed];

    /// Returns `true` if this VMM is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        Self::TERMINAL_STATES.contains(self)
    }
}

/// The dynamic runtime properties of an individual VMM process.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VmmRuntimeState {
    /// The last state reported by this VMM.
    pub state: VmmState,
    /// The generation number for this VMM's state.
    pub gen: Generation,
    /// Timestamp for the VMM's state.
    pub time_updated: DateTime<Utc>,
}

/// A wrapper type containing a sled's total knowledge of the state of a VMM.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SledVmmState {
    /// The most recent state of the sled's VMM process.
    pub vmm_state: VmmRuntimeState,

    /// The current state of any inbound migration to this VMM.
    pub migration_in: Option<MigrationRuntimeState>,

    /// The state of any outbound migration from this VMM.
    pub migration_out: Option<MigrationRuntimeState>,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Migrations<'state> {
    pub migration_in: Option<&'state MigrationRuntimeState>,
    pub migration_out: Option<&'state MigrationRuntimeState>,
}

impl Migrations<'_> {
    pub fn empty() -> Self {
        Self { migration_in: None, migration_out: None }
    }
}

impl SledVmmState {
    pub fn migrations(&self) -> Migrations<'_> {
        Migrations {
            migration_in: self.migration_in.as_ref(),
            migration_out: self.migration_out.as_ref(),
        }
    }
}

/// An update from a sled regarding the state of a migration, indicating the
/// role of the VMM whose migration state was updated.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MigrationRuntimeState {
    pub migration_id: Uuid,
    pub state: MigrationState,
    pub gen: Generation,

    /// Timestamp for the migration state update.
    pub time_updated: DateTime<Utc>,
}

/// The state of an instance's live migration.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationState {
    /// The migration has not started for this VMM.
    #[default]
    Pending,
    /// The migration is in progress.
    InProgress,
    /// The migration has failed.
    Failed,
    /// The migration has completed.
    Completed,
}

impl MigrationState {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
    /// Returns `true` if this migration state means that the migration is no
    /// longer in progress (it has either succeeded or failed).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, MigrationState::Completed | MigrationState::Failed)
    }
}

impl fmt::Display for MigrationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

// Oximeter producer/collector objects.

/// The kind of metric producer this is.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerKind {
    /// The producer is a sled-agent.
    SledAgent,
    /// The producer is an Omicron-managed service.
    Service,
    /// The producer is a Propolis VMM managing a guest instance.
    Instance,
    /// The producer is a management gateway service.
    ManagementGateway,
}

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
pub struct ProducerEndpoint {
    /// A unique ID for this producer.
    pub id: Uuid,
    /// The kind of producer.
    pub kind: ProducerKind,
    /// The IP address and port at which `oximeter` can collect metrics from the
    /// producer.
    pub address: SocketAddr,
    /// The interval on which `oximeter` should collect metrics.
    pub interval: Duration,
}

/// Response to a successful producer registration.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
pub struct ProducerRegistrationResponse {
    /// Period within which producers must renew their lease.
    ///
    /// Producers are required to periodically re-register with Nexus, to ensure
    /// that they are still collected from by `oximeter`.
    pub lease_duration: Duration,
}

/// A `HostIdentifier` represents either an IP host or network (v4 or v6),
/// or an entire VPC (identified by its VNI). It is used in firewall rule
/// host filters.
#[derive(
    Clone, Debug, Deserialize, Serialize, Eq, PartialEq, Hash, JsonSchema,
)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum HostIdentifier {
    Ip(oxnet::IpNet),
    Vpc(Vni),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum UpstairsRepairType {
    Live,
    Reconciliation,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct DownstairsUnderRepair {
    pub region_uuid: TypedUuid<DownstairsRegionKind>,
    pub target_addr: std::net::SocketAddrV6,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct RepairStartInfo {
    pub time: DateTime<Utc>,
    pub session_id: TypedUuid<UpstairsSessionKind>,
    pub repair_id: TypedUuid<UpstairsRepairKind>,
    pub repair_type: UpstairsRepairType,
    pub repairs: Vec<DownstairsUnderRepair>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct RepairFinishInfo {
    pub time: DateTime<Utc>,
    pub session_id: TypedUuid<UpstairsSessionKind>,
    pub repair_id: TypedUuid<UpstairsRepairKind>,
    pub repair_type: UpstairsRepairType,
    pub repairs: Vec<DownstairsUnderRepair>,
    pub aborted: bool,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct RepairProgress {
    pub time: DateTime<Utc>,
    pub current_item: i64,
    pub total_items: i64,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DownstairsClientStopRequestReason {
    Replacing,
    Disabled,
    FailedReconcile,
    IOError,
    BadNegotiationOrder,
    Incompatible,
    FailedLiveRepair,
    TooManyOutstandingJobs,
    Deactivated,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct DownstairsClientStopRequest {
    pub time: DateTime<Utc>,
    pub reason: DownstairsClientStopRequestReason,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DownstairsClientStoppedReason {
    ConnectionTimeout,
    ConnectionFailed,
    Timeout,
    WriteFailed,
    ReadFailed,
    RequestedStop,
    Finished,
    QueueClosed,
    ReceiveTaskCancelled,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub struct DownstairsClientStopped {
    pub time: DateTime<Utc>,
    pub reason: DownstairsClientStoppedReason,
}
