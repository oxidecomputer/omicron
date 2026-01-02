// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for building string-based queries.
//!
//! These largely side-step Diesel's type system,
//! and are recommended for more complex CTE

use diesel::RunQueryDsl;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::sql_types;
use nexus_db_lookup::DbConnection;
use std::cell::Cell;
use std::marker::PhantomData;

// Keeps a counter to "how many bind parameters have been used" to
// aid in the construction of the query string.
struct BindParamCounter(Cell<i32>);
impl BindParamCounter {
    fn new() -> Self {
        Self(0.into())
    }
    fn next(&self) -> i32 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}

/// A "trusted" string, which can be used to construct SQL queries even
/// though it isn't static. We use "trust" to refer to "protection from
/// SQL injections".
///
/// This is basically a workaround for cases where we haven't yet been
/// able to construct a query at compile-time.
pub struct TrustedStr(TrustedStrVariants);

impl TrustedStr {
    /// Explicitly constructs a string, with a name that hopefully
    /// gives callers some pause when calling this API.
    ///
    /// If arbitrary user input is provided here, this string COULD
    /// cause SQL injection attacks, so each call-site should have a
    /// justification for "why it's safe".
    pub fn i_take_responsibility_for_validating_this_string(s: String) -> Self {
        Self(TrustedStrVariants::ValidatedExplicitly(s))
    }

    #[cfg(test)]
    pub fn as_str(&self) -> &str {
        match &self.0 {
            TrustedStrVariants::Static(s) => s,
            TrustedStrVariants::ValidatedExplicitly(s) => s.as_str(),
        }
    }
}

impl From<&'static str> for TrustedStr {
    fn from(s: &'static str) -> Self {
        Self(TrustedStrVariants::Static(s))
    }
}

// This enum should be kept non-pub to make it harder to accidentally
// construct a "ValidatedExplicitly" variant.
enum TrustedStrVariants {
    Static(&'static str),
    ValidatedExplicitly(String),
}

type BoxedQuery = diesel::query_builder::BoxedSqlQuery<
    'static,
    Pg,
    diesel::query_builder::SqlQuery,
>;

/// A small wrapper around [diesel::query_builder::BoxedSqlQuery] which
/// assists with counting bind parameters and recommends avoiding the usage of
/// any non-static strings in query construction.
// NOTE: I'd really like to eventually be able to construct SQL statements
// entirely at compile-time, but the combination of "const generics" and "const
// fns" in stable Rust just isn't there yet.
//
// It's definitely possible to create static string builders that operate
// entirely at compile-time, like:
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=26d0276648c3315f285372a19d0d492f
//
// But this relies on nightly features.
pub struct QueryBuilder {
    query: Option<BoxedQuery>,
    bind_counter: BindParamCounter,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            query: Some(diesel::sql_query("").into_boxed()),
            bind_counter: BindParamCounter::new(),
        }
    }

    /// Identifies that a bind parameter should exist in this location within
    /// the SQL string.
    ///
    /// This should be called the same number of times as [Self::bind]. It is,
    /// however, a distinct method, as "identifying bind params" should be
    /// decoupled from "using bind parameters" to have an efficient statement
    /// cache.
    pub fn param(&mut self) -> &mut Self {
        self.query = self
            .query
            .take()
            .map(|q| q.sql("$").sql(self.bind_counter.next().to_string()));
        self
    }

    /// Slightly more strict than the "sql" method of Diesel's SqlQuery.
    /// Only permits strings which have been validated intentionally to limit
    /// susceptibility to SQL injection.
    ///
    /// See the documentation of [TrustedStr] for more details.
    pub fn sql<S: Into<TrustedStr>>(&mut self, s: S) -> &mut Self {
        self.query = self.query.take().map(|q| match s.into().0 {
            TrustedStrVariants::Static(s) => q.sql(s),
            TrustedStrVariants::ValidatedExplicitly(s) => q.sql(s),
        });
        self
    }

    /// A call-through function to [diesel::query_builder::BoxedSqlQuery].
    pub fn bind<BindSt, Value>(&mut self, b: Value) -> &mut Self
    where
        Pg: sql_types::HasSqlType<BindSt>,
        Value: diesel::serialize::ToSql<BindSt, Pg> + Send + 'static,
        BindSt: Send + 'static,
    {
        self.query = self.query.take().map(|q| q.bind(b));
        self
    }

    /// Takes the final boxed query
    pub fn query<T>(self) -> TypedSqlQuery<T> {
        TypedSqlQuery { inner: self.query.unwrap(), _phantom: PhantomData }
    }
}

/// Diesel's [diesel::query_builder::BoxedSqlQuery] has a few drawbacks that
/// make this wrapper more palatable:
///
/// - It always implements "Query" with SqlType = Untyped, so a caller could try to
/// execute this query and get back any type.
/// - It forces the usage of "QueryableByName", which acts wrong if we're
/// returning multiple columns with the same name (this is normal! If you want
/// to UNION two objects that both have "id" columns, this happens).
#[derive(QueryId)]
pub struct TypedSqlQuery<T> {
    inner: diesel::query_builder::BoxedSqlQuery<
        'static,
        Pg,
        diesel::query_builder::SqlQuery,
    >,
    _phantom: PhantomData<T>,
}

impl<T> QueryFragment<Pg> for TypedSqlQuery<T> {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        self.inner.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl<T> RunQueryDsl<DbConnection> for TypedSqlQuery<T> {}

impl<T> Query for TypedSqlQuery<T> {
    type SqlType = T;
}

#[cfg(test)]
pub async fn expectorate_query_contents<T: QueryFragment<Pg>>(
    query: T,
    path: &str,
) {
    use omicron_test_utils::dev;

    let s =
        dev::db::format_sql(&diesel::debug_query::<Pg, _>(&query).to_string())
            .await
            .expect("Failed to format SQL");

    expectorate::assert_contents(path, &s);
}

/// Type alias for the SQL type of a selectable expression.
///
/// This is useful for the return type of a query built using [`QueryBuilder`].
pub type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;
