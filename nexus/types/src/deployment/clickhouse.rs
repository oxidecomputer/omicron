// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used in blueprints related to clickhouse configuration

use clickhouse_admin_types::{KeeperId, ServerId};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

/// A mechanism used by the `BlueprintBuilder` to update clickhouse server and
/// keeper ids
///
/// Also stores whether any zones have been expunged so that we can bump the
/// generation in the `ClickhouseClusterConfig` even if no new servers have
/// been added.
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct ClickhouseIdAllocator {
    /// Have any zones been expunged?
    zones_expunged: bool,

    /// Clickhouse Server ids must be unique and are handed out monotonically. Keep track
    /// of the last used one.
    max_used_server_id: ServerId,
    /// CLickhouse Keeper ids must be unique and are handed out monotonically. Keep track
    /// of the last used one.
    max_used_keeper_id: KeeperId,
}

impl ClickhouseIdAllocator {
    pub fn new(
        max_used_server_id: ServerId,
        max_used_keeper_id: KeeperId,
    ) -> ClickhouseIdAllocator {
        ClickhouseIdAllocator {
            zones_expunged: false,
            max_used_server_id,
            max_used_keeper_id,
        }
    }

    pub fn expunge_zone(&mut self) {
        self.zones_expunged = true;
    }

    pub fn next_server_id(&mut self) -> ServerId {
        self.max_used_server_id += 1.into();
        self.max_used_server_id
    }
    pub fn next_keeper_id(&mut self) -> KeeperId {
        self.max_used_keeper_id += 1.into();
        self.max_used_keeper_id
    }

    pub fn max_used_server_id(&self) -> ServerId {
        self.max_used_server_id
    }
    pub fn max_used_keeper_id(&self) -> KeeperId {
        self.max_used_keeper_id
    }
}

impl From<&ClickhouseClusterConfig> for ClickhouseIdAllocator {
    fn from(value: &ClickhouseClusterConfig) -> Self {
        ClickhouseIdAllocator::new(
            value.max_used_server_id,
            value.max_used_keeper_id,
        )
    }
}

/// Global configuration for all clickhouse servers (replicas) and keepers
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct ClickhouseClusterConfig {
    /// The last update to the clickhouse cluster configuration
    ///
    /// This is used by `clickhouse-admin` in the clickhouse server and keeper
    /// zones to discard old configurations.
    pub generation: Generation,
    /// Clickhouse Server ids must be unique and are handed out monotonically. Keep track
    /// of the last used one.
    pub max_used_server_id: ServerId,
    /// CLickhouse Keeper ids must be unique and are handed out monotonically. Keep track
    /// of the last used one.
    pub max_used_keeper_id: KeeperId,
    /// An arbitrary name for the Clickhouse cluster shared by all nodes
    pub cluster_name: String,
    /// An arbitrary string shared by all nodes used at runtime to determine whether
    /// nodes are part of the same cluster.
    pub cluster_secret: String,

    /// The desired state of the clickhouse keeper cluster
    ///
    /// We decouple deployment of zones that should contain clickhouse keeper
    /// processes from actually starting or stopping those processes, adding or
    /// removing them to/from the keeper cluster, and reconfiguring other keeper and
    /// clickhouse server nodes to reflect the new configuration.
    ///
    /// As part of this decoupling, we keep track of the intended zone
    /// deployment in the blueprint, but that is not enough to track the desired
    /// state of the keeper cluster. We are only allowed to add or remove one
    /// keeper node at a time, and therefore we must track the desired state of
    /// the keeper cluster which may change multiple times until the keepers in
    /// the cluster match the deployed zones. An example may help:
    ///
    ///   1. We start with 3 keeper nodes in 3 deployed keeper zones and need to
    ///      add two to reach our desired policy of 5 keepers
    ///   2. The planner adds 2 new keeper zones to the blueprint
    ///   3. The planner will also add **one** new keeper process that matches one
    ///      of the deployed zones to the desired keeper cluster.
    ///   4. The executor will start the new keeper process, attempt to add it
    ///      to the keeper cluster by pushing configuration updates to the other
    ///      keepers, and then updating the clickhouse server configurations to know
    ///      about the new keeper.
    ///   5. If the keeper is successfully added, as reflected in inventory, then
    ///      steps 3 and 4 above will be retried for the next keeper process.
    ///   6. If the keeper addition to the cluster has failed, as reflected in
    ///      inventory, then the planner will create a new desired state that
    ///      expunges the keeper zone for the failed keeper, and adds a new
    ///      keeper zone to replace it. The process will then repeat with the
    ///      new zone. This is necessary because we must uniquely identify each
    ///      keeper process with an integer id, and once we allocate a process
    ///      to a zone we don't want to change the mapping. While changing the
    ///      mapping *may* be fine, we play it safe and just try to add a node
    ///      with a new id to the keeper cluster so that keeper reconfiguration
    ///      never gets stuck.
    ///
    /// Note that because we must discover the `KeeperId` from the
    /// `BlueprintZoneType` of the omicron zone as stored in the blueprint,
    /// we cannot remove the expunged zones from the blueprint until we have
    /// also successfully removed the keepers for those zones from the keeper
    /// cluster.
    pub zones_with_keepers: BTreeSet<OmicronZoneUuid>,
}

impl ClickhouseClusterConfig {
    pub fn new(cluster_name: String) -> ClickhouseClusterConfig {
        ClickhouseClusterConfig {
            generation: Generation::new(),
            max_used_server_id: 0.into(),
            max_used_keeper_id: 0.into(),
            cluster_name,
            cluster_secret: Uuid::new_v4().to_string(),
            zones_with_keepers: BTreeSet::new(),
        }
    }

    /// If new IDs have been allocated or any zones expunged, then update the
    /// internal state and return true, otherwise return false.
    pub fn update_configuration(
        &mut self,
        allocator: &ClickhouseIdAllocator,
    ) -> bool {
        let mut updated = allocator.zones_expunged;
        if self.max_used_server_id < allocator.max_used_server_id() {
            self.max_used_server_id = allocator.max_used_server_id();
            updated = true;
        }
        if self.max_used_keeper_id < allocator.max_used_keeper_id() {
            self.max_used_keeper_id = allocator.max_used_keeper_id();
            updated = true;
        }
        if updated {
            self.generation = self.generation.next();
        }
        updated
    }

    pub fn has_configuration_changed(
        &self,
        allocator: &ClickhouseIdAllocator,
    ) -> bool {
        allocator.zones_expunged
            || self.max_used_server_id != allocator.max_used_server_id()
            || self.max_used_keeper_id != allocator.max_used_keeper_id()
    }
}
