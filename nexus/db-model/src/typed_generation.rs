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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Returns the corresponding `DbTypedGeneration` for this `TypedGeneration`.
///
/// Code external to the `db-model` crate sometimes needs a way to convert a
/// `TypedGeneration` to a `DbTypedGeneration`. We don't want
/// `DbTypedGeneration` to be used anywhere, so we don't make it public.
/// Instead, we expose this function.
#[inline]
pub fn to_db_typed_generation<T: TypedGenerationKind>(
    generation: TypedGeneration<T>,
) -> DbTypedGeneration<T> {
    DbTypedGeneration(generation)
}

/// A generation number with information about the kind of counter it is.
///
/// Despite the fact that this is marked `pub`, this is *private* to the
/// `db-model` crate (this type is not exported at the top level). External
/// users must use omicron-generation-kinds' `TypedGeneration`.
#[derive_where(Clone, Copy, Eq, Ord, PartialEq, PartialOrd, Hash)]
#[derive(AsExpression, FromSqlRow, Serialize, Deserialize, JsonSchema)]
#[diesel(sql_type = sql_types::BigInt)]
#[serde(transparent, bound = "")]
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

impl<T: TypedGenerationKind> fmt::Display for DbTypedGeneration<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: TypedGenerationKind> FromStr for DbTypedGeneration<T> {
    type Err = omicron_generation_kinds::ParseError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(TypedGeneration::from_str(s)?.into())
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
