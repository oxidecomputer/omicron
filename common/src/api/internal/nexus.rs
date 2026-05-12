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
    #[serde(rename = "gen")]
    pub generation: Generation,
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
