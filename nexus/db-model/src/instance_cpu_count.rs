// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use serde::Deserialize;
use serde::Serialize;
use std::convert::TryFrom;

#[derive(
    Copy, Clone, Debug, AsExpression, FromSqlRow, Serialize, Deserialize,
)]
#[diesel(sql_type = sql_types::BigInt)]
pub struct InstanceCpuCount(pub external::InstanceCpuCount);

NewtypeFrom! { () pub struct InstanceCpuCount(external::InstanceCpuCount); }
NewtypeDeref! { () pub struct InstanceCpuCount(external::InstanceCpuCount); }

impl ToSql<sql_types::BigInt, Pg> for InstanceCpuCount {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &i64::from(&self.0),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for InstanceCpuCount
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        external::InstanceCpuCount::try_from(i64::from_sql(bytes)?)
            .map(InstanceCpuCount)
            .map_err(|e| e.into())
    }
}

impl From<InstanceCpuCount> for sled_agent_client::types::InstanceCpuCount {
    fn from(i: InstanceCpuCount) -> Self {
        Self(i.0 .0)
    }
}
