// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used in blueprints related to clickhouse configuration

use clickhouse_admin_types::{KeeperId, ServerId};
use omicron_common::api::external::Generation;
use omicron_uuid_kinds::OmicronZoneUuid;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Global configuration for all clickhouse servers (replicas) and keepers
#[derive(Clone, Debug, Eq, PartialEq, JsonSchema, Deserialize, Serialize)]
pub struct ClickhouseClusterConfig {
    /// The last update to the clickhouse cluster configuration
    ///
    /// This is used by `clickhouse-admin` in the clickhouse server and keeper
    /// zones to discard old configurations.
    pub generation: Generation,
    /// Clickhouse Server IDs must be unique and are handed out monotonically.
    /// Keep track of the last used one.
    pub max_used_server_id: ServerId,
    /// CLickhouse Keeper ids must be unique and are handed out monotonically.
    /// Keep track of the last used one.
    pub max_used_keeper_id: KeeperId,
    /// An arbitrary name for the Clickhouse cluster shared by all nodes
    pub cluster_name: String,
    /// An arbitrary string shared by all nodes used at runtime to determine
    /// whether nodes are part of the same cluster.
    pub cluster_secret: String,

    /// This is used as a marker to tell if the raft configuration
    /// in a new inventory collection is newer than the last collection.
    //
    /// This serves as a surrogate for the log index of the last committed
    /// configuration, which clickhouse keeper doesn't expose.
    ///
    /// This is necesssary because during inventory collection we poll
    /// multiple keeper nodes, and each returns their local knowledge of the
    /// configuration. But we may reach different nodes in different attempts,
    /// and some nodes in a following attempt may reflect stale configuration.
    /// Due to timing, we can always query old information. That is just normal
    /// polling. However, we never want to use old configuration if we have
    /// already seen and acted on newer configuration.
    pub highest_seen_keeper_leader_committed_log_index: u64,

    /// The desired state of the clickhouse keeper cluster
    ///
    /// We decouple deployment of zones that should contain clickhouse keeper
    /// processes from actually starting or stopping those processes, adding or
    /// removing them to/from the keeper cluster, and reconfiguring other keeper
    /// and clickhouse server nodes to reflect the new configuration.
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
    ///   3. The planner will also add **one** new keeper process that matches
    ///      one of the deployed zones to the desired keeper cluster.
    ///   4. The executor will start the new keeper process, attempt to add it
    ///      to the keeper cluster by pushing configuration updates to the other
    ///      keepers, and then updating the clickhouse server configurations to
    ///      know about the new keeper.
    ///   5. If the keeper is successfully added, as reflected in inventory, then
    ///      steps 3 and 4 above will be retried for the next keeper process.
    ///   6. If the keeper is not successfully added by the executor it will
    ///      continue to retry indefinitely.
    ///   7. If the zone is expunged while the planner is has it as part of its
    ///      desired state, and the executor is trying to add it, the keeper
    ///      will be removed from the desired state in the next blueprint. If it
    ///      has been added by an executor in the meantime it will be removed on
    ///      the next iteration of an executor.
    pub keepers: BTreeMap<OmicronZoneUuid, KeeperId>,

    /// The desired state of clickhouse server processes on the rack
    ///
    /// Clickhouse servers do not have the same limitations as keepers
    /// and can be deployed all at once.
    pub servers: BTreeMap<OmicronZoneUuid, ServerId>,
}

impl ClickhouseClusterConfig {
    pub fn new(cluster_name: String) -> ClickhouseClusterConfig {
        ClickhouseClusterConfig {
            generation: Generation::new(),
            max_used_server_id: 0.into(),
            max_used_keeper_id: 0.into(),
            cluster_name,
            cluster_secret: Uuid::new_v4().to_string(),
            highest_seen_keeper_leader_committed_log_index: 0,
            keepers: BTreeMap::new(),
            servers: BTreeMap::new(),
        }
    }

    // Has the configuration changed such that the executor needs to update
    // servers or keepers in the clickhouse zones?
    //
    // We specifically ignore `highest_seen_keeper_leader_committed_log_index`
    // because that should be regularly changing without the actual keeper
    // membership actually changing. We don't want to push on each committed
    // raft log commit.
    pub fn needs_generation_bump(
        &self,
        parent: &ClickhouseClusterConfig,
    ) -> bool {
        self.keepers != parent.keepers || self.servers != parent.servers
    }
}
