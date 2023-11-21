// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs exposed by Nexus.

use crate::api::external::{
    ByteCount, DiskState, Generation, InstanceCpuCount, InstanceState, IpNet,
    SemverVersion, Vni,
};
use chrono::{DateTime, Utc};
use parse_display::{Display, FromStr};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
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
    // TODO-cleanup different type?
    pub hostname: String,
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

/// The dynamic runtime properties of an individual VMM process.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct VmmRuntimeState {
    /// The last state reported by this VMM.
    pub state: InstanceState,
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
    pub kind: Option<ProducerKind>,
    /// The IP address and port at which `oximeter` can collect metrics from the
    /// producer.
    pub address: SocketAddr,
    /// The API base route from which `oximeter` can collect metrics.
    ///
    /// The full route is `{base_route}/{id}`.
    pub base_route: String,
    /// The interval on which `oximeter` should collect metrics.
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
    Ip(IpNet),
    Vpc(Vni),
}
