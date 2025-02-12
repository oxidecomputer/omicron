// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of an oximeter reads policy

use super::impl_enum_type;
use crate::schema::oximeter_reads_policy;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_types::deployment;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "oximeter_reads_mode", schema = "public"))]
    pub struct OximeterReadsModeEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = OximeterReadsModeEnum)]
    pub enum DbOximeterReadsMode;

    // Enum values
    SingleNode => b"single_node"
    Cluster => b"cluster"
);

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = oximeter_reads_policy)]
pub struct OximeterReadsPolicy {
    pub version: SqlU32,
    pub oximeter_reads_mode: DbOximeterReadsMode,
    pub time_created: DateTime<Utc>,
}

impl From<&deployment::OximeterReadsMode> for DbOximeterReadsMode {
    fn from(value: &deployment::OximeterReadsMode) -> Self {
        match value {
            deployment::OximeterReadsMode::SingleNode => {
                DbOximeterReadsMode::SingleNode
            }
            deployment::OximeterReadsMode::Cluster => {
                DbOximeterReadsMode::Cluster
            }
        }
    }
}

impl From<OximeterReadsPolicy> for deployment::OximeterReadsPolicy {
    fn from(value: OximeterReadsPolicy) -> Self {
        let mode = match value.oximeter_reads_mode {
            DbOximeterReadsMode::SingleNode => {
                deployment::OximeterReadsMode::SingleNode
            }
            DbOximeterReadsMode::Cluster => {
                deployment::OximeterReadsMode::Cluster
            }
        };

        deployment::OximeterReadsPolicy {
            version: value.version.0,
            mode,
            time_created: value.time_created,
        }
    }
}

impl From<deployment::OximeterReadsPolicy> for OximeterReadsPolicy {
    fn from(value: deployment::OximeterReadsPolicy) -> Self {
        match value.mode {
            deployment::OximeterReadsMode::SingleNode => OximeterReadsPolicy {
                version: value.version.into(),
                oximeter_reads_mode: DbOximeterReadsMode::SingleNode,
                time_created: value.time_created,
            },
            deployment::OximeterReadsMode::Cluster => OximeterReadsPolicy {
                version: value.version.into(),
                oximeter_reads_mode: DbOximeterReadsMode::Cluster,
                time_created: value.time_created,
            },
        }
    }
}
