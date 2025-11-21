// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Typed UUID instances.

use derive_where::derive_where;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use iddqd::{Comparable, Equivalent};
use omicron_uuid_kinds::{GenericUuid, TypedUuid, TypedUuidKind};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// Returns the corresponding `DbTypedUuid` for this `TypedUuid`.
///
/// Code external to the `db-model` crate sometimes needs a way to convert a
/// `TypedUuid` to a `DbTypedUuid`. We don't want `DbTypedUuid` to be used
/// anywhere, so we don't make it public. Instead, we expose this function.
#[inline]
pub fn to_db_typed_uuid<T: TypedUuidKind>(id: TypedUuid<T>) -> DbTypedUuid<T> {
    DbTypedUuid(id)
}

/// A UUID with information about the kind of type it is.
///
/// Despite the fact that this is marked `pub`, this is *private* to the
/// `db-model` crate (this type is not exported at the top level). External
/// users must use omicron-common's `TypedUuid`.
#[derive_where(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Hash)]
#[derive(AsExpression, FromSqlRow, Serialize, Deserialize, JsonSchema)]
#[diesel(sql_type = sql_types::Uuid)]
#[serde(transparent, bound = "")]
pub struct DbTypedUuid<T: TypedUuidKind>(pub(crate) TypedUuid<T>);

impl<T: TypedUuidKind, DB> ToSql<sql_types::Uuid, DB> for DbTypedUuid<T>
where
    DB: Backend,
    Uuid: ToSql<sql_types::Uuid, DB>,
{
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, DB>,
    ) -> serialize::Result {
        self.0.as_untyped_uuid().to_sql(out)
    }
}

impl<T: TypedUuidKind, DB> FromSql<sql_types::Uuid, DB> for DbTypedUuid<T>
where
    DB: Backend,
    Uuid: FromSql<sql_types::Uuid, DB>,
{
    #[inline]
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let id = Uuid::from_sql(bytes)?;
        Ok(TypedUuid::from_untyped_uuid(id).into())
    }
}

impl<T: TypedUuidKind> fmt::Debug for DbTypedUuid<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: TypedUuidKind> fmt::Display for DbTypedUuid<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: TypedUuidKind> FromStr for DbTypedUuid<T> {
    type Err = omicron_uuid_kinds::ParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TypedUuid::from_str(s)?.into())
    }
}

impl<T: TypedUuidKind> From<TypedUuid<T>> for DbTypedUuid<T> {
    #[inline]
    fn from(id: TypedUuid<T>) -> Self {
        Self(id)
    }
}

impl<T: TypedUuidKind> From<DbTypedUuid<T>> for TypedUuid<T> {
    #[inline]
    fn from(id: DbTypedUuid<T>) -> Self {
        id.0
    }
}

impl<T: TypedUuidKind> GenericUuid for DbTypedUuid<T> {
    #[inline]
    fn from_untyped_uuid(uuid: Uuid) -> Self {
        TypedUuid::from_untyped_uuid(uuid).into()
    }

    #[inline]
    fn into_untyped_uuid(self) -> Uuid {
        self.0.into_untyped_uuid()
    }

    #[inline]
    fn as_untyped_uuid(&self) -> &Uuid {
        self.0.as_untyped_uuid()
    }
}

impl<T: TypedUuidKind> Equivalent<TypedUuid<T>> for DbTypedUuid<T> {
    #[inline]
    fn equivalent(&self, other: &TypedUuid<T>) -> bool {
        self.0.as_untyped_uuid() == other.as_untyped_uuid()
    }
}

impl<T: TypedUuidKind> Comparable<TypedUuid<T>> for DbTypedUuid<T> {
    #[inline]
    fn compare(&self, key: &TypedUuid<T>) -> std::cmp::Ordering {
        self.0.as_untyped_uuid().cmp(key.as_untyped_uuid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omicron_uuid_kinds::SledUuid;
    use std::hash::{BuildHasher, RandomState};
    use test_strategy::proptest;

    /// Test that the `Hash` implementation is consistent, as required by
    /// `Equivalent`.
    #[proptest]
    fn test_hash_equality(id: SledUuid) {
        let db_id = DbTypedUuid::from(id);
        assert!(db_id.equivalent(&id));

        let hasher = RandomState::new();
        let id_hash = hasher.hash_one(&id);
        let db_id_hash = hasher.hash_one(&db_id);
        assert_eq!(id_hash, db_id_hash);
    }

    /// Test that the `compare` implementation is consistent, as required by
    /// `Comparable`.
    #[proptest]
    fn test_compare_consistency(id1: SledUuid, id2: SledUuid) {
        let db_id1 = DbTypedUuid::from(id1);
        assert_eq!(db_id1.compare(&id2), id1.cmp(&id2));
    }
}
