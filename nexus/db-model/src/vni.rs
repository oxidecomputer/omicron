// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::Backend;
use diesel::deserialize;
use diesel::deserialize::FromSql;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::serialize;
use diesel::serialize::ToSql;
use diesel::sql_types;
use omicron_common::api::external;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

#[derive(
    Clone,
    Debug,
    Copy,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    Eq,
    PartialEq,
    JsonSchema,
)]
#[diesel(sql_type = sql_types::Int4)]
pub struct Vni(pub external::Vni);

impl<DB> ToSql<sql_types::Int4, DB> for Vni
where
    for<'c> DB: Backend<BindCollector<'c> = RawBytesBindCollector<DB>>,
    i32: ToSql<sql_types::Int4, DB>,
{
    fn to_sql<'b>(
        &'b self,
        out: &mut serialize::Output<'b, '_, DB>,
    ) -> serialize::Result {
        // Reborrowing is necessary to ensure that the lifetime of the temporary
        // i32 created here and `out` is the same, i.e., that `'b = '_`.
        i32::try_from(u32::from(self.0)).unwrap().to_sql(&mut out.reborrow())
    }
}

impl<DB> FromSql<sql_types::Int4, DB> for Vni
where
    DB: Backend,
    i32: FromSql<sql_types::Int4, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        Ok(Vni(external::Vni::try_from(i32::from_sql(bytes)?)?))
    }
}
