// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use parse_display::Display;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Newtype wrapper around [external::Digest]
#[derive(
    Clone,
    Debug,
    Display,
    AsExpression,
    FromSqlRow,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    JsonSchema,
    Serialize,
    Deserialize,
)]
#[diesel(sql_type = sql_types::Text)]
#[serde(transparent)]
#[repr(transparent)]
#[display("{0}")]
pub struct Digest(pub external::Digest);

NewtypeFrom! { () pub struct Digest(external::Digest); }
NewtypeDeref! { () pub struct Digest(external::Digest); }

impl ToSql<sql_types::Text, Pg> for Digest {
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <String as ToSql<sql_types::Text, Pg>>::to_sql(
            &self.0.to_string(),
            &mut out.reborrow(),
        )
    }
}

impl<DB> FromSql<sql_types::Text, DB> for Digest
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let digest: external::Digest = String::from_sql(bytes)?.parse()?;
        Ok(Digest(digest))
    }
}
