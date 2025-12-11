// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Debug/chicken-switch types for Sled Agent API v1.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Chicken switch for orphaned dataset destruction.
///
/// This type is used in both GET and PUT operations (in versions 1-2).
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ChickenSwitchDestroyOrphanedDatasets {
    /// If true, sled-agent will attempt to destroy durable ZFS datasets that it
    /// believes were associated with now-expunged Omicron zones.
    pub destroy_orphans: bool,
}
