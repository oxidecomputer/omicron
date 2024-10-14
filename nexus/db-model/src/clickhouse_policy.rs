// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of a clickhouse deployment policy

use super::impl_enum_type;
use crate::SqlU32;
use crate::{schema::clickhouse_policy, SqlU8};
use chrono::{DateTime, Utc};
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
