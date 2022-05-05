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

/// Representation of a [`u16`] in the database.
/// We need this because the database does not support unsigned types.
/// This handles converting from the database's INT4 to the actual u16.
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
#[diesel(sql_type = sql_types::Int4)]
#[repr(transparent)]
pub struct SqlU16(pub u16);

NewtypeFrom! { () pub struct SqlU16(u16); }
NewtypeDeref! { () pub struct SqlU16(u16); }

impl SqlU16 {
    pub fn new(port: u16) -> Self {
        Self(port)
    }
}

impl ToSql<sql_types::Int4, Pg> for SqlU16 {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i32 as ToSql<sql_types::Int4, Pg>>::to_sql(
            &i32::from(self.0),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Int4, DB> for SqlU16
where
    DB: Backend,
    i32: FromSql<sql_types::Int4, DB>,
{
    fn from_sql(bytes: RawValue<DB>) -> deserialize::Result<Self> {
        u16::try_from(i32::from_sql(bytes)?).map(SqlU16).map_err(|e| e.into())
    }
}
