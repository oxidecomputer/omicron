// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::{Backend, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// Representation of a [`u8`] in the database.
/// We need this because the database does not support unsigned types.
/// This handles converting from the database's INT2 to the actual u8.
#[derive(
    Copy,
    Clone,
    Debug,
    AsExpression,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    FromSqlRow,
    Serialize,
    Deserialize,
)]
#[diesel(sql_type = sql_types::Int2)]
#[repr(transparent)]
pub struct SqlU8(pub u8);

NewtypeFrom! { () pub struct SqlU8(u8); }
NewtypeDeref! { () pub struct SqlU8(u8); }

impl SqlU8 {
    pub fn new(port: u8) -> Self {
        Self(port)
    }
}

impl ToSql<sql_types::Int2, Pg> for SqlU8 {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i16 as ToSql<sql_types::Int2, Pg>>::to_sql(
            &i16::from(self.0),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Int2, DB> for SqlU8
where
    DB: Backend,
    i16: FromSql<sql_types::Int2, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        u8::try_from(i16::from_sql(bytes)?).map(SqlU8).map_err(|e| e.into())
    }
}
