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

/// Description of a single update artifact.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct UpdateArtifact {
    pub name: String,
    pub version: i64,
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

/// Forwarded to a sled agent to request the contents of an Instance's serial console.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InstanceSerialConsoleRequest {
    /// Character index in the serial buffer from which to read, counting the bytes output since
    /// instance start. If this is not provided, `most_recent` must be provided, and if this *is*
    /// provided, `most_recent` must *not* be provided.
    pub from_start: Option<u32>,
    /// Character index in the serial buffer from which to read, counting *backward* from the most
    /// recently buffered data retrieved from the instance. (See note on `from_start` about mutual
    /// exclusivity)
    pub most_recent: Option<u32>,
    /// Maximum number of bytes of buffered serial console contents to return. If the requested
    /// range runs to the end of the available buffer, the data returned will be shorter than
    /// `max_bytes`.
    pub max_bytes: Option<u32>,
}

/// Contents of an Instance's serial console buffer.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InstanceSerialConsoleData {
    /// The bytes starting from the requested offset up to either the end of the buffer or the
    /// request's `max_bytes`. Provided as a u8 array rather than a string, as it may not be UTF-8.
    pub data: Vec<u8>,
    /// The absolute offset since boot (suitable for use as `byte_offset` in a subsequent request)
    /// of the last byte returned in `data`.
    pub last_byte_offset: u32,
}
