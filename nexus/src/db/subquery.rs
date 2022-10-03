// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subquery-related traits which may be derived for DB structures.

use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;

/// Represents a named subquery within a CTE.
///
/// For an expression like:
///
/// ```sql
/// WITH
///     foo as ...,
///     bar as ...,
/// SELECT * FROM bar;
/// ```
///
/// This trait represents one of the sub-query arms, such as "foo as ..." or
/// "bar as ...".
// This trait intentionally is agnostic to the SQL type of the subquery,
// meaning that it can be used by the [`CteBuilder`] within a [`Vec`].
pub trait Subquery: QueryFragment<Pg> + Send {
    /// Returns the underlying query fragment.
    ///
    /// For "<ALIAS> as <QUERY>", this refers to the "QUERY" portion
    /// of SQL.
    fn query(&self) -> &dyn QueryFragment<Pg>;
}

/// Trait which implies that the associated query may be used
/// as a query source.
///
/// For example, given the subquery:
///
/// ```sql
/// user_ids as (SELECT id FROM user)
/// ```
///
/// It should be possible to "SELECT" from `user_ids`. This trait
/// surfaces that underlying query source.
// TODO: Take a much closer look at "AliasSource". It doesn't solve
// the problem of grabbing the query fragment for you, but it might
// help for referencing the "origin" object (table in upstream, but
// plausibly a subquery too).
pub trait AsQuerySource {
    type QuerySource;
    fn query_source(&self) -> Self::QuerySource;
}

/// Describes the requirements to be subquery within a CTE:
/// - (Query) It must be a complete SQL query with a specific return type
/// - (QueryFragment) It must be capable of emitting a SQL string
// TODO: In the future, we may force this subquery to have named columns.
pub trait CteQuery: Query + QueryFragment<Pg> + Send {}

impl<T> CteQuery for T where T: Query + QueryFragment<Pg> + Send {}

/// A thin wrapper around a [`Subquery`].
///
/// Used to avoid orphan rules while creating blanket implementations.
pub struct CteSubquery(Box<dyn Subquery>);

impl QueryId for CteSubquery {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for CteSubquery {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        self.0.walk_ast(out.reborrow())?;
        out.push_sql(" AS (");
        self.0.query().walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}

pub struct CteBuilder {
    subqueries: Vec<CteSubquery>,
}

impl CteBuilder {
    pub fn new() -> Self {
        Self { subqueries: vec![] }
    }

    pub fn add_subquery<Q: Subquery + 'static>(mut self, subquery: Q) -> Self {
        self.subqueries.push(CteSubquery(Box::new(subquery)));
        self
    }

    // TODO: It would be nice if this could be typed?
    // It's not necessarily a Subquery, but it's probably a "Query" object
    // with a particular SQL type.
    pub fn build(self, statement: Box<dyn QueryFragment<Pg> + Send>) -> Cte {
        Cte { subqueries: self.subqueries, statement }
    }
}

pub struct Cte {
    subqueries: Vec<CteSubquery>,
    statement: Box<dyn QueryFragment<Pg> + Send>,
}

impl QueryFragment<Pg> for Cte {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql("WITH ");
        for (pos, query) in self.subqueries.iter().enumerate() {
            query.walk_ast(out.reborrow())?;
            if pos == self.subqueries.len() - 1 {
                out.push_sql(" ");
            } else {
                out.push_sql(", ");
            }
        }
        self.statement.walk_ast(out.reborrow())?;
        Ok(())
    }
}
