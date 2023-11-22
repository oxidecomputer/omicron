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

/// Newtype wrapper around [`external::L4PortRange`] so we can derive
/// diesel traits for it
#[derive(
    Clone, Copy, Debug, AsExpression, FromSqlRow, Serialize, Deserialize,
)]
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
pub struct L4PortRange(pub external::L4PortRange);
NewtypeFrom! { () pub struct L4PortRange(external::L4PortRange); }
NewtypeDeref! { () pub struct L4PortRange(external::L4PortRange); }

impl ToSql<sql_types::Text, Pg> for L4PortRange {
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

// Deserialize the "L4PortRange" object from SQL INT4.
impl<DB> FromSql<sql_types::Text, DB> for L4PortRange
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        String::from_sql(bytes)?.parse().map(L4PortRange).map_err(|e| e.into())
    }
}
