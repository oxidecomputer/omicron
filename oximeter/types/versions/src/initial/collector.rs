// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Collector-related types for the Oximeter collector.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, Serialize)]
pub struct CollectorInfo {
    /// The collector's UUID.
    pub id: Uuid,
    /// Last time we refreshed our producer list with Nexus.
    pub last_refresh: Option<DateTime<Utc>>,
}
