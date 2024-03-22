// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use nexus_types::internal_api;
use serde::{Deserialize, Serialize};

impl_enum_type!(
    #[derive(SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "dataset_kind", schema = "public"))]
    pub struct DatasetKindEnum;

    #[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize, PartialEq)]
    #[diesel(sql_type = DatasetKindEnum)]
    pub enum DatasetKind;

    // Enum values
    Crucible => b"crucible"
    Cockroach => b"cockroach"
    Clickhouse => b"clickhouse"
    ClickhouseKeeper => b"clickhouse_keeper"
    ExternalDns => b"external_dns"
    InternalDns => b"internal_dns"
);

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
            internal_api::params::DatasetKind::ClickhouseKeeper => {
                DatasetKind::ClickhouseKeeper
            }
            internal_api::params::DatasetKind::ExternalDns => {
                DatasetKind::ExternalDns
            }
            internal_api::params::DatasetKind::InternalDns => {
                DatasetKind::InternalDns
            }
        }
    }
}
