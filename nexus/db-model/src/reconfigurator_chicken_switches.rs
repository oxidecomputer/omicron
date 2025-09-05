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
    pub add_zones_with_mupdate_override: bool,
}

impl From<deployment::ReconfiguratorConfigView>
    for ReconfiguratorChickenSwitches
{
    fn from(value: deployment::ReconfiguratorConfigView) -> Self {
        Self {
            version: value.version.into(),
            planner_enabled: value.switches.planner_enabled,
            time_modified: value.time_modified,
            add_zones_with_mupdate_override: value
                .switches
                .planner_switches
                .add_zones_with_mupdate_override,
        }
    }
}

impl From<ReconfiguratorChickenSwitches>
    for deployment::ReconfiguratorConfigView
{
    fn from(value: ReconfiguratorChickenSwitches) -> Self {
        Self {
            version: value.version.into(),
            switches: deployment::ReconfiguratorConfig {
                planner_enabled: value.planner_enabled,
                planner_switches: deployment::PlannerConfig {
                    add_zones_with_mupdate_override: value
                        .add_zones_with_mupdate_override,
                },
            },
            time_modified: value.time_modified,
        }
    }
}
