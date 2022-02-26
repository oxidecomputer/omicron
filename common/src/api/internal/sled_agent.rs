// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs exposed by Sled Agent.

use crate::api::{external, internal};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Result as FormatResult};
use std::net::SocketAddr;
use uuid::Uuid;

/// Describes the instance hardware.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct InstanceHardware {
    pub runtime: internal::nexus::InstanceRuntimeState,
    pub nics: Vec<external::NetworkInterface>,
}

/// Sent to a sled agent to establish the runtime state of an Instance
#[derive(Serialize, Deserialize, JsonSchema)]
pub struct InstanceEnsureBody {
    /// Last runtime state of the Instance known to Nexus (used if the agent
    /// has never seen this Instance before).
    pub initial: InstanceHardware,
    /// requested runtime state of the Instance
    pub target: InstanceRuntimeStateRequested,
    /// If we're migrating this instance, the details needed to drive the migration
    pub migrate: Option<InstanceMigrateParams>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceMigrateParams {
    pub src_propolis_uuid: Uuid,
    pub src_propolis_addr: SocketAddr,
}

/// Requestable running state of an Instance.
///
/// A subset of [`external::InstanceState`].
#[derive(
    Copy,
    Clone,
    Debug,
    Deserialize,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
    JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum InstanceStateRequested {
    Running,
    Stopped,
    // Issues a reset command to the instance, such that it should
    // stop and then immediately become running.
    Reboot,
    Migrating,
    Destroyed,
}

impl Display for InstanceStateRequested {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        write!(f, "{}", self.label())
    }
}

impl InstanceStateRequested {
    fn label(&self) -> &str {
        match self {
            InstanceStateRequested::Running => "running",
            InstanceStateRequested::Stopped => "stopped",
            InstanceStateRequested::Reboot => "reboot",
            InstanceStateRequested::Migrating => "migrating",
            InstanceStateRequested::Destroyed => "destroyed",
        }
    }

    /// Returns true if the state represents a stopped Instance.
    pub fn is_stopped(&self) -> bool {
        match self {
            InstanceStateRequested::Running => false,
            InstanceStateRequested::Stopped => true,
            InstanceStateRequested::Reboot => false,
            InstanceStateRequested::Migrating => false,
            InstanceStateRequested::Destroyed => true,
        }
    }
}

/// Instance runtime state to update for a migration.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeStateMigrateParams {
    pub migration_id: Uuid,
    pub dst_propolis_id: Uuid,
}

/// Used to request an Instance state change from a sled agent
///
/// Right now, it's only the run state and migration id that can
/// be changed, though we might want to support changing properties
/// like "ncpus" here.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceRuntimeStateRequested {
    pub run_state: InstanceStateRequested,
    pub migration_params: Option<InstanceRuntimeStateMigrateParams>,
}

/// The type of a dataset, and an axuiliary information necessary
/// to successfully launch a zone managing the associated data.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatasetKind {
    CockroachDb {
        /// The addresses of all nodes within the cluster.
        all_addresses: Vec<SocketAddr>,
    },
    Crucible,
    Clickhouse,
}

impl From<DatasetKind> for internal::nexus::DatasetKind {
    fn from(d: DatasetKind) -> Self {
        use DatasetKind::*;
        match d {
            CockroachDb { .. } => internal::nexus::DatasetKind::Cockroach,
            Crucible { .. } => internal::nexus::DatasetKind::Crucible,
            Clickhouse { .. } => internal::nexus::DatasetKind::Clickhouse,
        }
    }
}

/// Used to request a new partition kind exists within a zpool.
///
/// Many partition types are associated with services that will be
/// instantiated when the partition is detected.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct DatasetEnsureBody {
    // The name (and UUID) of the Zpool which we are inserting into.
    pub zpool_uuid: Uuid,
    // The type of the filesystem.
    pub partition_kind: DatasetKind,
    // The address on which the zone will listen for requests.
    pub address: SocketAddr,
    // TODO: We could insert a UUID here, if we want that to be set by the
    // caller explicitly? Currently, the lack of a UUID implies that
    // "at most one partition type" exists within a zpool.
}

#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
pub struct ServiceRequest {
    // The name of the service to be created.
    pub name: String,
    // The addresses on which the service should listen for requests.
    pub addresses: Vec<SocketAddr>,
}

/// Used to request that the Sled initialize certain services on initialization.
///
/// This may be used to record that certain sleds are responsible for
/// launching services which may not be associated with a partition, such
/// as Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct ServiceEnsureBody {
    pub services: Vec<ServiceRequest>,
}
