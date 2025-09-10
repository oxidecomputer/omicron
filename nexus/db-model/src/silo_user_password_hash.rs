// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::DbTypedUuid;
use crate::to_db_typed_uuid;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use nexus_db_schema::schema::silo_user_password_hash;
use omicron_uuid_kinds::SiloUserKind;
use omicron_uuid_kinds::SiloUserUuid;
use parse_display::Display;
use ref_cast::RefCast;

/// Newtype wrapper around [`omicron_passwords::PasswordHashString`].
#[derive(
    Clone, Debug, Display, AsExpression, FromSqlRow, Eq, PartialEq, RefCast,
)]
#[diesel(sql_type = sql_types::Text)]
#[repr(transparent)]
#[display("{0}")]
pub struct PasswordHashString(pub omicron_passwords::PasswordHashString);

NewtypeFrom! {
    () pub struct PasswordHashString(pub omicron_passwords::PasswordHashString);
}
NewtypeDeref! {
    () pub struct PasswordHashString(pub omicron_passwords::PasswordHashString);
}

impl<DB> ToSql<sql_types::Text, DB> for PasswordHashString
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

// Deserialize the "PasswordHashString" object from SQL TEXT.
impl<DB> FromSql<sql_types::Text, DB> for PasswordHashString
where
    DB: Backend,
    String: FromSql<sql_types::Text, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        String::from_sql(bytes)?
            .parse()
            .map(PasswordHashString)
            .map_err(|e| e.into())
    }
}

/// A silo user's password hash
#[derive(Queryable, Insertable, Debug, Selectable)]
#[diesel(table_name = silo_user_password_hash)]
pub struct SiloUserPasswordHash {
    silo_user_id: DbTypedUuid<SiloUserKind>,
    pub hash: PasswordHashString,
    pub time_created: chrono::DateTime<chrono::Utc>,
}

impl SiloUserPasswordHash {
    pub fn new(silo_user_id: SiloUserUuid, hash: PasswordHashString) -> Self {
        Self {
            silo_user_id: to_db_typed_uuid(silo_user_id),
            hash,
            time_created: chrono::Utc::now(),
        }
    }

    pub fn silo_user_id(&self) -> SiloUserUuid {
        self.silo_user_id.into()
    }
}

/// An update to a silo user's password
#[derive(AsChangeset)]
#[diesel(table_name = silo_user_password_hash)]
pub struct SiloUserPasswordUpdate {
    pub hash: PasswordHashString,
    pub time_created: chrono::DateTime<chrono::Utc>,
}

impl SiloUserPasswordUpdate {
    pub fn new(hash: PasswordHashString) -> Self {
        Self { hash, time_created: chrono::Utc::now() }
    }
}
