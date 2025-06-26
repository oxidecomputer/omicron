// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing runtime configuration for reconfigurator

use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::reconfigurator_chicken_switches;
use nexus_types::deployment;

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = reconfigurator_chicken_switches)]
pub struct ReconfiguratorChickenSwitches {
    pub version: SqlU32,
    pub planner_enabled: bool,
    pub time_modified: DateTime<Utc>,
}

impl ReconfiguratorChickenSwitches {
    pub fn new(version: u32, planner_enabled: bool) -> Self {
        Self {
            version: version.into(),
            planner_enabled,
            time_modified: Utc::now(),
        }
    }
}

impl From<deployment::ReconfiguratorChickenSwitches>
    for ReconfiguratorChickenSwitches
{
    fn from(value: deployment::ReconfiguratorChickenSwitches) -> Self {
        Self {
            version: value.version.into(),
            planner_enabled: value.planner_enabled,
            time_modified: value.time_modified,
        }
    }
}

impl From<ReconfiguratorChickenSwitches>
    for deployment::ReconfiguratorChickenSwitches
{
    fn from(value: ReconfiguratorChickenSwitches) -> Self {
        Self {
            version: value.version.into(),
            planner_enabled: value.planner_enabled,
            time_modified: value.time_modified,
        }
    }
}
