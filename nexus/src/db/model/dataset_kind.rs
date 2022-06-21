// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use crate::internal_api;
use omicron_common::address::{CLICKHOUSE_PORT, COCKROACH_PORT, CRUCIBLE_PORT};
use serde::{Deserialize, Serialize};
use std::io::Write;

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "dataset_kind"))]
    pub struct DatasetKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = DatasetKindEnum)]
    pub enum DatasetKind;

    // Enum values
    Crucible => b"crucible"
    Cockroach => b"cockroach"
    Clickhouse => b"clickhouse"
);

impl DatasetKind {
    pub fn port(&self) -> u16 {
        match self {
            DatasetKind::Crucible => CRUCIBLE_PORT,
            DatasetKind::Cockroach => COCKROACH_PORT,
            DatasetKind::Clickhouse => CLICKHOUSE_PORT,
        }
    }
}

impl From<internal_api::params::DatasetKind> for DatasetKind {
    fn from(k: internal_api::params::DatasetKind) -> Self {
        match k {
            internal_api::params::DatasetKind::Crucible => {
                DatasetKind::Crucible
            }
            internal_api::params::DatasetKind::Cockroach => {
                DatasetKind::Cockroach
            }
            internal_api::params::DatasetKind::Clickhouse => {
                DatasetKind::Clickhouse
            }
        }
    }
}
