// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, Hostname, InstanceCpuCount,
    SemverVersion, Vni,
};
use chrono::{DateTime, Utc};
use omicron_uuid_kinds::DownstairsRegionKind;
use omicron_uuid_kinds::TypedUuid;
use omicron_uuid_kinds::UpstairsRepairKind;
use omicron_uuid_kinds::UpstairsSessionKind;
use parse_display::{Display, FromStr};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use strum::{EnumIter, IntoEnumIterator};
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

/// The "static" properties of an instance: information about the instance that
/// doesn't change while the instance is running.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceProperties {
    pub ncpus: InstanceCpuCount,
    pub memory: ByteCount,
    /// RFC1035-compliant hostname for the instance.
    pub hostname: Hostname,
}

/// The dynamic runtime properties of an instance: its current VMM ID (if any),
/// migration information (if any), and the instance state to report if there is
/// no active VMM.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeState {
    /// The instance's currently active VMM ID.
    pub propolis_id: Option<Uuid>,
    /// If a migration is active, the ID of the target VMM.
    pub dst_propolis_id: Option<Uuid>,
    /// If a migration is active, the ID of that migration.
    pub migration_id: Option<Uuid>,
    /// Generation number for this state.
    pub gen: Generation,
    /// Timestamp for this information.
    pub time_updated: DateTime<Utc>,
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

/// A wrapper type containing a sled's total knowledge of the state of a
/// specific VMM and the instance it incarnates.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SledInstanceState {
    /// The sled's conception of the state of the instance.
    pub instance_state: InstanceRuntimeState,

    /// The ID of the VMM whose state is being reported.
    pub propolis_id: Uuid,

    /// The most recent state of the sled's VMM process.
    pub vmm_state: VmmRuntimeState,

    /// The current state of any in-progress migration for this instance, as
    /// understood by this sled.
    pub migration_state: Option<MigrationRuntimeState>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct MigrationRuntimeState {
    pub migration_id: Uuid,

    pub state: MigrationState,
}

/// The state of an instance's live migration.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationState {
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
            Self::InProgress => "in_progress",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
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
}

/// Information announced by a metric server, used so that clients can contact it and collect
/// available metric data from it.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, PartialEq)]
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

/// An identifier for a single update artifact.
#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Deserialize,
    Serialize,
    JsonSchema,
)]
pub struct UpdateArtifactId {
    /// The artifact's name.
    pub name: String,

    /// The artifact's version.
    pub version: SemverVersion,

    /// The kind of update artifact this is.
    pub kind: KnownArtifactKind,
}

// Adding a new KnownArtifactKind
// ===============================
//
// Adding a new update artifact kind is a tricky process. To do so:
//
// 1. Add it here.
//
// 2. Add the new kind to <repo root>/{nexus-client,sled-agent-client}/lib.rs.
//    The mapping from `UpdateArtifactKind::*` to `types::UpdateArtifactKind::*`
//    must be left as a `todo!()` for now; `types::UpdateArtifactKind` will not
//    be updated with the new variant until step 5 below.
//
// 3. Add it to the sql database schema under (CREATE TYPE
//    omicron.public.update_artifact_kind).
//
//    TODO: After omicron ships this would likely involve a DB migration.
//
// 4. Add the new kind and the mapping to its `update_artifact_kind` to
//    <repo root>/nexus/db-model/src/update_artifact.rs
//
// 5. Regenerate the OpenAPI specs for nexus and sled-agent:
//
//    ```
//    EXPECTORATE=overwrite cargo nextest run -p omicron-nexus -p omicron-sled-agent openapi
//    ```
//
// 6. Return to <repo root>/{nexus-client,sled-agent-client}/lib.rs from step 2
//    and replace the `todo!()`s with the new `types::UpdateArtifactKind::*`
//    variant.
//
// See https://github.com/oxidecomputer/omicron/pull/2300 as an example.
//
// NOTE: KnownArtifactKind has to be in snake_case due to openapi-lint requirements.

/// Kinds of update artifacts, as used by Nexus to determine what updates are available and by
/// sled-agent to determine how to apply an update when asked.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Display,
    FromStr,
    Deserialize,
    Serialize,
    JsonSchema,
    EnumIter,
)]
#[display(style = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum KnownArtifactKind {
    // Sled Artifacts
    GimletSp,
    GimletRot,
    Host,
    Trampoline,
    ControlPlane,

    // PSC Artifacts
    PscSp,
    PscRot,

    // Switch Artifacts
    SwitchSp,
    SwitchRot,
}

impl KnownArtifactKind {
    /// Returns an iterator over all the variants in this struct.
    ///
    /// This is provided as a helper so dependent packages don't have to pull in
    /// strum explicitly.
    pub fn iter() -> KnownArtifactKindIter {
        <Self as IntoEnumIterator>::iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_artifact_kind_roundtrip() {
        for kind in KnownArtifactKind::iter() {
            let as_string = kind.to_string();
            let kind2 = as_string.parse::<KnownArtifactKind>().unwrap_or_else(
                |error| panic!("error parsing kind {as_string}: {error}"),
            );
            assert_eq!(kind, kind2);
        }
    }
}

/// A `HostIdentifier` represents either an IP host or network (v4 or v6),
/// or an entire VPC (identified by its VNI). It is used in firewall rule
/// host filters.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, JsonSchema)]
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
