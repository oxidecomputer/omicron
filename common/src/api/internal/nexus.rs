// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, InstanceCpuCount, InstanceState,
};
use chrono::{DateTime, Utc};
use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
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
    pub sled_id: Uuid,
    /// which propolis-server is running this Instance
    pub propolis_id: Uuid,
    /// the target propolis-server during a migration of this Instance
    pub dst_propolis_id: Option<Uuid>,
    /// address of propolis-server running this Instance
    pub propolis_addr: Option<SocketAddr>,
    /// migration id (if one in process)
    pub migration_id: Option<Uuid>,
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
    /// Return the route that can be used to request metric data.
    pub fn collection_route(&self) -> String {
        format!("{}/{}", &self.base_route, &self.id)
    }
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
    pub version: String,

    /// The kind of update artifact this is.
    pub kind: UpdateArtifactKind,
}

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
    Deserialize,
    Serialize,
    JsonSchema,
)]
#[display(style = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum UpdateArtifactKind {
    Zone,
}
