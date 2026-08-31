// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(
    Copy,
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[diesel(sql_type = sql_types::BigInt)]
#[repr(transparent)]
pub struct Generation(pub omicron_generation_kinds::Generation);

NewtypeFrom! { () pub struct Generation(omicron_generation_kinds::Generation); }
NewtypeDeref! { () pub struct Generation(omicron_generation_kinds::Generation); }

impl Generation {
    pub fn new() -> Self {
        Self(omicron_generation_kinds::Generation::new())
    }
}

impl ToSql<sql_types::BigInt, Pg> for Generation {
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

impl<DB> FromSql<sql_types::BigInt, DB> for Generation
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        omicron_generation_kinds::Generation::try_from(i64::from_sql(bytes)?)
            .map(Generation)
            .map_err(|e| e.into())
    }
}

impl TryFrom<i64> for Generation {
    type Error = omicron_generation_kinds::GenerationNegativeError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Self(omicron_generation_kinds::Generation::try_from(value)?))
    }
}
