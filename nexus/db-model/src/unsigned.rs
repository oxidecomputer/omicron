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

/// Representation of a [`u8`] in the database. The database does not support
/// unsigned types; this adapter converts from the database's INT2 (a 2-byte
/// signed integer) to an actual u8.
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
    pub fn new(value: u8) -> Self {
        Self(value)
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
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        u8::try_from(i16::from_sql(bytes)?).map(SqlU8).map_err(|e| e.into())
    }
}

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
    JsonSchema,
)]
#[diesel(sql_type = sql_types::Int4)]
#[repr(transparent)]
pub struct SqlU16(pub u16);

NewtypeFrom! { () pub struct SqlU16(u16); }
NewtypeDeref! { () pub struct SqlU16(u16); }
NewtypeDisplay! { () pub struct SqlU16(u16); }

impl SqlU16 {
    pub fn new(value: u16) -> Self {
        Self(value)
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
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        u16::try_from(i32::from_sql(bytes)?).map(SqlU16).map_err(|e| e.into())
    }
}

/// Representation of a [`u32`] in the database.
/// We need this because the database does not support unsigned types.
/// This handles converting from the database's BIGINT to the actual u32.
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
    QueryId,
)]
#[diesel(sql_type = sql_types::BigInt)]
#[repr(transparent)]
pub struct SqlU32(pub u32);

NewtypeFrom! { () pub struct SqlU32(u32); }
NewtypeDeref! { () pub struct SqlU32(u32); }
NewtypeDisplay! { () pub struct SqlU32(u32); }

impl SqlU32 {
    pub fn new(value: u32) -> Self {
        Self(value)
    }
}

impl ToSql<sql_types::BigInt, Pg> for SqlU32 {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &i64::from(self.0),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::BigInt, DB> for SqlU32
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        u32::try_from(i64::from_sql(bytes)?).map(SqlU32).map_err(|e| e.into())
    }
}
