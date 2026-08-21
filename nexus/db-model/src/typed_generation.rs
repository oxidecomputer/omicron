// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Typed generation numbers.

use derive_where::derive_where;
use diesel::backend::Backend;
use diesel::deserialize::{self, FromSql};
use diesel::pg::Pg;
use diesel::serialize::{self, ToSql};
use diesel::sql_types;
use omicron_generation_kinds::{
    Generation, GenericGeneration, TypedGeneration, TypedGenerationKind,
};
use std::fmt;

/// Returns the corresponding `DbTypedGeneration` for this `TypedGeneration`.
#[inline]
pub fn to_db_typed_generation<T: TypedGenerationKind>(
    generation: TypedGeneration<T>,
) -> DbTypedGeneration<T> {
    DbTypedGeneration(generation)
}

/// A generation number with information about the kind of counter it is.
///
/// This is the Diesel representation of omicron-generation-kinds'
/// `TypedGeneration`.
///
/// This is `pub` because query builders in nexus-db-queries need it as a bind
/// type. Most code should use the upstream `TypedGeneration` as much as
/// possible, since that has much more infrastructure built around it.
#[derive_where(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Hash)]
#[derive(AsExpression, FromSqlRow)]
#[diesel(sql_type = sql_types::BigInt)]
pub struct DbTypedGeneration<T: TypedGenerationKind>(
    pub(crate) TypedGeneration<T>,
);

impl<T: TypedGenerationKind> ToSql<sql_types::BigInt, Pg>
    for DbTypedGeneration<T>
{
    fn to_sql<'a>(
        &'a self,
        out: &mut serialize::Output<'a, '_, Pg>,
    ) -> serialize::Result {
        <i64 as ToSql<sql_types::BigInt, Pg>>::to_sql(
            &self.0.as_i64(),
            &mut out.reborrow(),
        )
    }
}

impl<T: TypedGenerationKind, DB> FromSql<sql_types::BigInt, DB>
    for DbTypedGeneration<T>
where
    DB: Backend,
    i64: FromSql<sql_types::BigInt, DB>,
{
    #[inline]
    fn from_sql(bytes: DB::RawValue<'_>) -> deserialize::Result<Self> {
        let generation = TypedGeneration::<T>::try_from(i64::from_sql(bytes)?)?;
        Ok(Self(generation))
    }
}

impl<T: TypedGenerationKind> fmt::Debug for DbTypedGeneration<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: TypedGenerationKind> From<TypedGeneration<T>> for DbTypedGeneration<T> {
    #[inline]
    fn from(generation: TypedGeneration<T>) -> Self {
        Self(generation)
    }
}

impl<T: TypedGenerationKind> From<DbTypedGeneration<T>> for TypedGeneration<T> {
    #[inline]
    fn from(generation: DbTypedGeneration<T>) -> Self {
        generation.0
    }
}

impl<T: TypedGenerationKind> GenericGeneration for DbTypedGeneration<T> {
    #[inline]
    fn from_untyped_generation(generation: Generation) -> Self {
        TypedGeneration::from_untyped_generation(generation).into()
    }

    #[inline]
    fn into_untyped_generation(self) -> Generation {
        self.0.into_untyped_generation()
    }

    #[inline]
    fn as_untyped_generation(&self) -> &Generation {
        self.0.as_untyped_generation()
    }
}
