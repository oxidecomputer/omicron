// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared types for Sled Agent API v1.
//!
//! This module contains types used in both requests and responses.
//!
//! Per RFD 619, high-level types are defined in the earliest version they
//! appear in. These types are used directly by the API crate with fixed
//! identifiers.

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
