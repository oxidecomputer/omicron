// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SqlU32;
use crate::{schema::clickhouse_policy, SqlU8};
use chrono::{DateTime, Utc};

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = clickhouse_policy)]
pub struct ClickhousePolicy {
    pub version: SqlU32,
    pub clickhouse_cluster_enabled: bool,
    pub clickhouse_single_node_enabled: bool,
    pub clickhouse_cluster_target_servers: SqlU8,
    pub clickhouse_cluster_target_keepers: SqlU8,
    pub time_created: DateTime<Utc>,
}
