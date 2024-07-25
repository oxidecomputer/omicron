// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::impl_enum_type;
use omicron_common::api::internal;
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

impl From<internal::shared::DatasetKind> for DatasetKind {
    fn from(k: internal::shared::DatasetKind) -> Self {
        match k {
            internal::shared::DatasetKind::Crucible => DatasetKind::Crucible,
            internal::shared::DatasetKind::Cockroach => DatasetKind::Cockroach,
            internal::shared::DatasetKind::Clickhouse => {
                DatasetKind::Clickhouse
            }
            internal::shared::DatasetKind::ClickhouseKeeper => {
                DatasetKind::ClickhouseKeeper
            }
            internal::shared::DatasetKind::ExternalDns => {
                DatasetKind::ExternalDns
            }
            internal::shared::DatasetKind::InternalDns => {
                DatasetKind::InternalDns
            }
        }
    }
}
