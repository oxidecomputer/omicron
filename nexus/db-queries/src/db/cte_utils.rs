// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for writing CTEs.

use diesel::associations::HasTable;
use diesel::expression::Expression;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::query_source::Table;

/// The default WHERE clause of a table, when treated as an UPDATE target.
pub(crate) type TableDefaultWhereClause<Table> =
    <Table as IntoUpdateTarget>::WhereClause;

// Short-hand type accessors.
pub(crate) type QueryFromClause<T> = <T as QuerySource>::FromClause;
pub(crate) type QuerySqlType<T> = <T as AsQuery>::SqlType;
pub(crate) type ExprSqlType<T> = <T as Expression>::SqlType;
type TableSqlType<T> = <T as AsQuery>::SqlType;

pub(crate) type BoxedQuery<T> = diesel::helper_types::IntoBoxed<'static, T, Pg>;
pub(crate) type BoxedDslOutput<T> =
    diesel::internal::table_macro::BoxedSelectStatement<
        'static,
        TableSqlType<T>,
        diesel::internal::table_macro::FromClause<T>,
        Pg,
    >;

/// Ensures that the type is a Diesel table, and that we can call ".table" and
/// ".into_boxed()" on it.
pub trait BoxableTable:
    HasTable<Table = Self>
    + 'static
    + Send
    + Table
    + IntoUpdateTarget
    + query_methods::BoxedDsl<'static, Pg, Output = BoxedDslOutput<Self>>
{
}
impl<T> BoxableTable for T where
    T: HasTable<Table = Self>
        + 'static
        + Send
        + Table
        + IntoUpdateTarget
        + query_methods::BoxedDsl<'static, Pg, Output = BoxedDslOutput<Self>>
{
}

/// Ensures that calling ".filter(predicate)" on this type is callable, and does
/// not change the underlying type.
pub trait FilterBy<Predicate>:
    query_methods::FilterDsl<Predicate, Output = Self>
{
}
impl<T, Predicate> FilterBy<Predicate> for T where
    T: query_methods::FilterDsl<Predicate, Output = Self>
{
}

/// Allows calling ".into_boxed" on an update statement.
pub trait BoxableUpdateStatement<Table, V>:
    query_methods::BoxedDsl<
    'static,
    Pg,
    Output = BoxedUpdateStatement<'static, Pg, Table, V>,
>
where
    Table: QuerySource,
{
}
impl<T, Table, V> BoxableUpdateStatement<Table, V> for T
where
    T: query_methods::BoxedDsl<
        'static,
        Pg,
        Output = BoxedUpdateStatement<'static, Pg, Table, V>,
    >,
    Table: QuerySource,
{
}
