// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing runtime configuration for reconfigurator

use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::reconfigurator_config;
use nexus_types::deployment;

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = reconfigurator_config)]
pub struct ReconfiguratorConfig {
    pub version: SqlU32,
    pub planner_enabled: bool,
    pub time_modified: DateTime<Utc>,
    pub add_zones_with_mupdate_override: bool,
    pub tuf_repo_pruner_enabled: bool,
}

impl From<deployment::ReconfiguratorConfigView> for ReconfiguratorConfig {
    fn from(value: deployment::ReconfiguratorConfigView) -> Self {
        Self {
            version: value.version.into(),
            planner_enabled: value.config.planner_enabled,
            time_modified: value.time_modified,
            add_zones_with_mupdate_override: value
                .config
                .planner_config
                .add_zones_with_mupdate_override,
            tuf_repo_pruner_enabled: value.config.tuf_repo_pruner_enabled,
        }
    }
}

impl From<ReconfiguratorConfig> for deployment::ReconfiguratorConfigView {
    fn from(value: ReconfiguratorConfig) -> Self {
        Self {
            version: value.version.into(),
            config: deployment::ReconfiguratorConfig {
                planner_enabled: value.planner_enabled,
                planner_config: deployment::PlannerConfig {
                    add_zones_with_mupdate_override: value
                        .add_zones_with_mupdate_override,
                },
                tuf_repo_pruner_enabled: value.tuf_repo_pruner_enabled,
            },
            time_modified: value.time_modified,
        }
    }
}
