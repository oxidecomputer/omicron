// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Structures stored to the database.

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use parse_display::Display;
use ref_cast::RefCast;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Newtype wrapper around [external::Name].
#[derive(
    Clone,
    Debug,
    Display,
    AsExpression,
    FromSqlRow,
    Eq,
    Hash,
    PartialEq,
    Ord,
    PartialOrd,
    RefCast,
    JsonSchema,
    Serialize,
    Deserialize,
)]
#[diesel(sql_type = sql_types::Text)]
#[serde(transparent)]
#[repr(transparent)]
#[display("{0}")]
pub struct Name(pub external::Name);

impl From<Name> for external::NameOrId {
    fn from(name: Name) -> Self {
        Self::Name(name.0)
    }
}

NewtypeFrom! { () pub struct Name(external::Name); }
NewtypeDeref! { () pub struct Name(external::Name); }

impl<DB> ToSql<sql_types::Text, DB> for Name
where
    DB: Backend,
    str: ToSql<sql_types::Text, DB>,
{
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, DB>,
    ) -> serialize::Result {
        self.as_str().to_sql(out)
    }
}

// Deserialize the "Name" object from SQL TEXT.
impl<DB> FromSql<sql_types::Text, DB> for Name
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        String::from_sql(bytes)?.parse().map(Name).map_err(|e| e.into())
    }
}
