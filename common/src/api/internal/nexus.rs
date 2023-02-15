// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, InstanceCpuCount, InstanceState,
};
use chrono::{DateTime, Utc};
use parse_display::{Display, FromStr};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use strum::EnumIter;
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
    pub kind: KnownArtifactKind,
}

// Adding a new KnownArtifactKind
// ===============================
//
// Adding a new update artifact kind is a tricky process. To do so:
//
// 1. Add it here.
//
// 2. Add it by hand to <repo root>/openapi/{nexus-internal.json,sled_agent.json}
//    (search for `"KnownArtifactKind"` with double-quotes).
//
//    TODO: is there a better way to do this? Rain couldn't figure out how to run
//    the command to regenerate the API: the nexus build kept failing. There
//    might be a circular dependency here, which is worrisome.
//
// 3. Add the new kind to <repo root>/{nexus-client,sled-agent-client}/lib.rs.
//
// 4. Add it to <repo root>/common/src/sql/dbinit.sql under (CREATE TYPE
//    omicron.public.update_artifact_kind).
//
//    TODO: After omicron ships this would likely involve a DB migration.
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

#[cfg(test)]
mod tests {
    use super::*;
    use strum::IntoEnumIterator;

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
