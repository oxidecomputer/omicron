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
    pub planner_switches: PlannerChickenSwitches,
}

impl Default for ReconfiguratorChickenSwitchesParam {
    fn default() -> Self {
        Self {
            // The first supported version is 1.
            version: 1,
            planner_enabled: false,
            planner_switches: PlannerChickenSwitches::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReconfiguratorChickenSwitches {
    pub version: u32,
    pub planner_enabled: bool,
    pub planner_switches: PlannerChickenSwitches,
    pub time_modified: DateTime<Utc>,
}

impl Default for ReconfiguratorChickenSwitches {
    fn default() -> Self {
        // Use the default values from `ReconfiguratorChickenSwitchesParam`.
        let ReconfiguratorChickenSwitchesParam {
            version,
            planner_enabled,
            planner_switches,
        } = ReconfiguratorChickenSwitchesParam::default();
        Self {
            version,
            planner_enabled,
            planner_switches,
            time_modified: Utc.with_ymd_and_hms(1970, 1, 1, 0, 1, 1).unwrap(),
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema,
)]
pub struct PlannerChickenSwitches {
    /// Whether to add zones even if a mupdate override is present.
    ///
    /// Once Nexus-driven update is active on a customer system, we must not add
    /// new zones while the system is recovering from a MUPdate. But that would
    /// require customers to upload a TUF repo before adding a new sled, even
    /// though Nexus-driven update is not active (as of r16).
    ///
    /// This switch, which is currently on by default, allows us to add zones
    /// even if we've detected a recent MUPdate on the system. We will want to
    /// turn it off as part of enabling Nexus-driven update.
    pub add_zones_with_mupdate_override: bool,
}

impl PlannerChickenSwitches {
    /// Returns the default value for a `SystemDescription`, e.g. in
    /// reconfigurator-cli.
    pub fn default_for_system_description() -> Self {
        // In reconfigurator-cli we set this to false to ensure tests run
        // against the desired configuration for r17.
        Self { add_zones_with_mupdate_override: false }
    }
}

impl Default for PlannerChickenSwitches {
    fn default() -> Self {
        // On customer systems for now, we don't block zone additions on mupdate
        // overrides being present.
        Self { add_zones_with_mupdate_override: true }
    }
}
