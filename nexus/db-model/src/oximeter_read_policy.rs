// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Database representation of an oximeter read policy

use super::impl_enum_type;
use crate::SqlU32;
use chrono::{DateTime, Utc};
use nexus_db_schema::schema::oximeter_read_policy;
use nexus_types::deployment;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    OximeterReadModeEnum:

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    pub enum DbOximeterReadMode;

    // Enum values
    SingleNode => b"single_node"
    Cluster => b"cluster"
);

impl From<DbOximeterReadMode> for nexus_types::deployment::OximeterReadMode {
    fn from(value: DbOximeterReadMode) -> Self {
        match value {
            DbOximeterReadMode::SingleNode => {
                nexus_types::deployment::OximeterReadMode::SingleNode
            }
            DbOximeterReadMode::Cluster => {
                nexus_types::deployment::OximeterReadMode::Cluster
            }
        }
    }
}

#[derive(Queryable, Clone, Debug, Selectable, Insertable)]
#[diesel(table_name = oximeter_read_policy)]
pub struct OximeterReadPolicy {
    pub version: SqlU32,
    pub oximeter_read_mode: DbOximeterReadMode,
    pub time_created: DateTime<Utc>,
}

impl From<&deployment::OximeterReadMode> for DbOximeterReadMode {
    fn from(value: &deployment::OximeterReadMode) -> Self {
        match value {
            deployment::OximeterReadMode::SingleNode => {
                DbOximeterReadMode::SingleNode
            }
            deployment::OximeterReadMode::Cluster => {
                DbOximeterReadMode::Cluster
            }
        }
    }
}

impl From<OximeterReadPolicy> for deployment::OximeterReadPolicy {
    fn from(value: OximeterReadPolicy) -> Self {
        let mode = match value.oximeter_read_mode {
            DbOximeterReadMode::SingleNode => {
                deployment::OximeterReadMode::SingleNode
            }
            DbOximeterReadMode::Cluster => {
                deployment::OximeterReadMode::Cluster
            }
        };

        deployment::OximeterReadPolicy {
            version: value.version.0,
            mode,
            time_created: value.time_created,
        }
    }
}

impl From<deployment::OximeterReadPolicy> for OximeterReadPolicy {
    fn from(value: deployment::OximeterReadPolicy) -> Self {
        match value.mode {
            deployment::OximeterReadMode::SingleNode => OximeterReadPolicy {
                version: value.version.into(),
                oximeter_read_mode: DbOximeterReadMode::SingleNode,
                time_created: value.time_created,
            },
            deployment::OximeterReadMode::Cluster => OximeterReadPolicy {
                version: value.version.into(),
                oximeter_read_mode: DbOximeterReadMode::Cluster,
                time_created: value.time_created,
            },
        }
    }
}
