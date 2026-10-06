// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types representing runtime configuration for reconfigurator

use crate::{SqlU32, impl_enum_type};
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::reconfigurator_config;
use nexus_types::deployment;

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = reconfigurator_config)]
pub struct ReconfiguratorConfig {
    pub version: SqlU32,
    pub planner_enabled: bool,
    pub time_modified: DateTime<Utc>,
    pub tuf_repo_pruner_enabled: bool,
    pub disruption_policy: DbReconfiguratorDisruptionPolicy,
    pub blueprint_pruner_enabled: bool,
    pub blueprint_pruner_nkeep: SqlU32,
    pub sled_update_reboot_policy: DbSledUpdateRebootPolicy,
}

impl From<deployment::ReconfiguratorConfigView> for ReconfiguratorConfig {
    fn from(value: deployment::ReconfiguratorConfigView) -> Self {
        let planner_config = value.config.planner_config;
        Self {
            version: value.version.into(),
            planner_enabled: value.config.planner_enabled,
            time_modified: value.time_modified,
            tuf_repo_pruner_enabled: value.config.tuf_repo_pruner_enabled,
            disruption_policy: planner_config.disruption_policy.into(),
            blueprint_pruner_enabled: value.config.blueprint_pruner_enabled,
            blueprint_pruner_nkeep: value.config.blueprint_pruner_nkeep.into(),
            sled_update_reboot_policy: planner_config
                .sled_update_reboot_policy
                .into(),
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
                    disruption_policy: value.disruption_policy.into(),
                    sled_update_reboot_policy: value
                        .sled_update_reboot_policy
                        .into(),
                },
                tuf_repo_pruner_enabled: value.tuf_repo_pruner_enabled,
                blueprint_pruner_enabled: value.blueprint_pruner_enabled,
                blueprint_pruner_nkeep: value.blueprint_pruner_nkeep.into(),
            },
            time_modified: value.time_modified,
        }
    }
}

impl_enum_type!(
    ReconfiguratorDisruptionPolicyEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
    )]
    pub enum DbReconfiguratorDisruptionPolicy;

    Terminate => b"terminate"
    MigrateOrTerminate => b"migrate_or_terminate"
    MigrateOnly => b"migrate_only"
);

impl From<DbReconfiguratorDisruptionPolicy>
    for deployment::ReconfiguratorDisruptionPolicy
{
    fn from(value: DbReconfiguratorDisruptionPolicy) -> Self {
        match value {
            DbReconfiguratorDisruptionPolicy::Terminate => {
                deployment::ReconfiguratorDisruptionPolicy::Terminate
            }
            DbReconfiguratorDisruptionPolicy::MigrateOrTerminate => {
                deployment::ReconfiguratorDisruptionPolicy::MigrateOrTerminate
            }
            DbReconfiguratorDisruptionPolicy::MigrateOnly => {
                deployment::ReconfiguratorDisruptionPolicy::MigrateOnly
            }
        }
    }
}

impl From<deployment::ReconfiguratorDisruptionPolicy>
    for DbReconfiguratorDisruptionPolicy
{
    fn from(value: deployment::ReconfiguratorDisruptionPolicy) -> Self {
        match value {
            deployment::ReconfiguratorDisruptionPolicy::Terminate => {
                DbReconfiguratorDisruptionPolicy::Terminate
            }
            deployment::ReconfiguratorDisruptionPolicy::MigrateOrTerminate => {
                DbReconfiguratorDisruptionPolicy::MigrateOrTerminate
            }
            deployment::ReconfiguratorDisruptionPolicy::MigrateOnly => {
                DbReconfiguratorDisruptionPolicy::MigrateOnly
            }
        }
    }
}

impl_enum_type!(
    SledUpdateRebootPolicyEnum:

    #[derive(
        Copy,
        Clone,
        Debug,
        PartialEq,
        AsExpression,
        FromSqlRow,
    )]
    pub enum DbSledUpdateRebootPolicy;

    ImmediateNoEvacuation => b"immediate_no_evacuation"
    Evacuate => b"evacuate"
);

impl From<DbSledUpdateRebootPolicy> for deployment::SledUpdateRebootPolicy {
    fn from(value: DbSledUpdateRebootPolicy) -> Self {
        match value {
            DbSledUpdateRebootPolicy::ImmediateNoEvacuation => {
                deployment::SledUpdateRebootPolicy::ImmediateNoEvacuation
            }
            DbSledUpdateRebootPolicy::Evacuate => {
                deployment::SledUpdateRebootPolicy::Evacuate
            }
        }
    }
}

impl From<deployment::SledUpdateRebootPolicy> for DbSledUpdateRebootPolicy {
    fn from(value: deployment::SledUpdateRebootPolicy) -> Self {
        match value {
            deployment::SledUpdateRebootPolicy::ImmediateNoEvacuation => {
                DbSledUpdateRebootPolicy::ImmediateNoEvacuation
            }
            deployment::SledUpdateRebootPolicy::Evacuate => {
                DbSledUpdateRebootPolicy::Evacuate
            }
        }
    }
}
