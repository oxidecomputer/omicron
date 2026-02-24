// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Asset identity types for version INITIAL.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Identity-related metadata that's included in "asset" public API objects
/// (which generally have no name or description)
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
pub struct AssetIdentityMetadata {
    /// unique, immutable, system-controlled identifier for each resource
    pub id: Uuid,
    /// timestamp when this resource was created
    pub time_created: DateTime<Utc>,
    /// timestamp when this resource was last modified
    pub time_modified: DateTime<Utc>,
}
