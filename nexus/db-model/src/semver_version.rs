// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::backend::{Backend, RawValue};
use diesel::deserialize::{self, FromSql};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_common::api::external;
use parse_display::Display;
use serde::{Deserialize, Serialize};

// We wrap semver::Version in external to impl JsonSchema, and we wrap it again
// here to impl ToSql/FromSql

#[derive(
    Clone,
    Debug,
    AsExpression,
    FromSqlRow,
    Serialize,
    Deserialize,
    PartialEq,
    Display,
)]
#[diesel(sql_type = sql_types::Text)]
#[display("{0}")]
pub struct SemverVersion(pub external::SemverVersion);

NewtypeFrom! { () pub struct SemverVersion(external::SemverVersion); }
NewtypeDeref! { () pub struct SemverVersion(external::SemverVersion); }

impl SemverVersion {
    pub fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self(external::SemverVersion(semver::Version::new(major, minor, patch)))
    }
}

impl<DB> ToSql<sql_types::Text, DB> for SemverVersion
where
    DB: Backend<BindCollector = RawBytesBindCollector<DB>>,
    String: ToSql<sql_types::Text, DB>,
{
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, DB>,
    ) -> serialize::Result {
        (self.0).0.to_string().to_sql(&mut out.reborrow())
    }
}

impl<DB> FromSql<sql_types::Text, DB> for SemverVersion
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(raw: RawValue<DB>) -> deserialize::Result<Self> {
        String::from_sql(raw)?
            .parse()
            .map(|s| SemverVersion(external::SemverVersion(s)))
            .map_err(|e| e.into())
    }
}
