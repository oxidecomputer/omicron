// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::schema::reconfigurator_policy;
use crate::SqlU32;
use chrono::{DateTime, Utc};

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = reconfigurator_policy)]
pub struct ReconfiguratorPolicy {
    pub version: SqlU32,
    pub clickhouse_cluster_enabled: bool,
    pub clickhouse_single_node_enabled: bool,
    pub time_created: DateTime<Utc>,
}
