// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types used in blueprints related to clickhouse configuration

use clickhouse_admin_types::{KeeperId, ServerId};
use omicron_common::api::external::Generation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
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
}

impl ClickhouseClusterConfig {
    pub fn new(cluster_name: String) -> ClickhouseClusterConfig {
        ClickhouseClusterConfig {
            generation: Generation::new(),
            max_used_server_id: 0.into(),
            max_used_keeper_id: 0.into(),
            cluster_name,
            cluster_secret: Uuid::new_v4().to_string(),
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
