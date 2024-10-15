// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a clickhouse deployment policy

use super::impl_enum_type;
use crate::SqlU32;
use crate::{schema::clickhouse_policy, SqlU8};
use chrono::{DateTime, Utc};
use nexus_types::deployment;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "clickhouse_mode", schema = "public"))]
    pub struct ClickhouseModeEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = ClickhouseModeEnum)]
    pub enum DbClickhouseMode;

    // Enum values
    SingleNodeOnly => b"single_node_only"
    ClusterOnly => b"cluster_only"
    Both => b"both"
);

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = clickhouse_policy)]
pub struct ClickhousePolicy {
    pub version: SqlU32,
    pub clickhouse_mode: DbClickhouseMode,
    pub clickhouse_cluster_target_servers: SqlU8,
    pub clickhouse_cluster_target_keepers: SqlU8,
    pub time_created: DateTime<Utc>,
}

impl From<&deployment::ClickhouseMode> for DbClickhouseMode {
    fn from(value: &deployment::ClickhouseMode) -> Self {
        match value {
            deployment::ClickhouseMode::SingleNodeOnly => {
                DbClickhouseMode::SingleNodeOnly
            }
            deployment::ClickhouseMode::ClusterOnly { .. } => {
                DbClickhouseMode::ClusterOnly
            }
            deployment::ClickhouseMode::Both { .. } => DbClickhouseMode::Both,
        }
    }
}

impl From<ClickhousePolicy> for deployment::ClickhousePolicy {
    fn from(value: ClickhousePolicy) -> Self {
        let mode = match value.clickhouse_mode {
            DbClickhouseMode::SingleNodeOnly => {
                deployment::ClickhouseMode::SingleNodeOnly
            }
            DbClickhouseMode::ClusterOnly => {
                deployment::ClickhouseMode::ClusterOnly {
                    target_servers: value.clickhouse_cluster_target_servers.0,
                    target_keepers: value.clickhouse_cluster_target_keepers.0,
                }
            }
            DbClickhouseMode::Both => deployment::ClickhouseMode::Both {
                target_servers: value.clickhouse_cluster_target_servers.0,
                target_keepers: value.clickhouse_cluster_target_keepers.0,
            },
        };

        deployment::ClickhousePolicy {
            version: value.version.0,
            mode,
            time_created: value.time_created,
        }
    }
}

impl From<deployment::ClickhousePolicy> for ClickhousePolicy {
    fn from(value: deployment::ClickhousePolicy) -> Self {
        match value.mode {
            deployment::ClickhouseMode::SingleNodeOnly => ClickhousePolicy {
                version: value.version.into(),
                clickhouse_mode: DbClickhouseMode::SingleNodeOnly,
                clickhouse_cluster_target_servers: 0.into(),
                clickhouse_cluster_target_keepers: 0.into(),
                time_created: value.time_created,
            },
            deployment::ClickhouseMode::ClusterOnly {
                target_servers,
                target_keepers,
            } => ClickhousePolicy {
                version: value.version.into(),
                clickhouse_mode: DbClickhouseMode::ClusterOnly,
                clickhouse_cluster_target_servers: target_servers.into(),
                clickhouse_cluster_target_keepers: target_keepers.into(),
                time_created: value.time_created,
            },
            deployment::ClickhouseMode::Both {
                target_servers,
                target_keepers,
            } => ClickhousePolicy {
                version: value.version.into(),
                clickhouse_mode: DbClickhouseMode::Both,
                clickhouse_cluster_target_servers: target_servers.into(),
                clickhouse_cluster_target_keepers: target_keepers.into(),
                time_created: value.time_created,
            },
        }
    }
}
