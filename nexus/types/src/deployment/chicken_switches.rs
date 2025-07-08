// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Runtime configuration for reconfigurator
//!
use chrono::{DateTime, TimeZone, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct ReconfiguratorChickenSwitchesParam {
    pub version: u32,
    pub planner_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconfiguratorChickenSwitches {
    pub version: u32,
    pub planner_enabled: bool,
    pub time_modified: DateTime<Utc>,
}

impl Default for ReconfiguratorChickenSwitches {
    fn default() -> Self {
        Self {
            version: 0,
            planner_enabled: false,
            time_modified: Utc.with_ymd_and_hms(1970, 1, 1, 0, 1, 1).unwrap(),
        }
    }
}
