// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility allowing Diesel to EXPLAIN queries.

use super::pool::DbConnection;
use async_bb8_diesel::{AsyncRunQueryDsl, ConnectionManager, PoolError};
use async_trait::async_trait;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;

/// A wrapper around a runnable Diesel query, which EXPLAINs what it is doing.
///
/// Q: The Query we're explaining.
///
/// EXPLAIN: https://www.cockroachlabs.com/docs/stable/explain.html
pub trait Explainable<Q> {
    /// Syncronously issues an explain statement.
    fn explain(
        self,
        conn: &mut DbConnection,
    ) -> Result<String, diesel::result::Error>;
}

impl<Q> Explainable<Q> for Q
where
    Q: QueryFragment<Pg> + RunQueryDsl<DbConnection> + Sized,
{
    fn explain(
        self,
        conn: &mut DbConnection,
    ) -> Result<String, diesel::result::Error> {
        Ok(ExplainStatement { query: self }
            .get_results::<String>(conn)?
            .join("\n"))
    }
}

/// An async variant of [`Explainable`].
#[async_trait]
pub trait ExplainableAsync<Q> {
    /// Asynchronously issues an explain statement.
    async fn explain_async(
        self,
        pool: &bb8::Pool<ConnectionManager<DbConnection>>,
    ) -> Result<String, PoolError>;
}

#[async_trait]
impl<Q> ExplainableAsync<Q> for Q
where
    Q: QueryFragment<Pg> + RunQueryDsl<DbConnection> + Sized + Send + 'static,
{
    async fn explain_async(
        self,
        pool: &bb8::Pool<ConnectionManager<DbConnection>>,
    ) -> Result<String, PoolError> {
        Ok(ExplainStatement { query: self }
            .get_results_async::<String>(pool)
            .await?
            .join("\n"))
    }
}

// An EXPLAIN statement, wrapping an underlying query.
//
// This isn't `pub` because it's kinda weird to access "part" of the EXPLAIN
// output, which would be possible by calling "get_result" instead of
// "get_results". We'd like to be able to constrain callers such that they get
// all of the output or none of it.
//
// See the [`Explainable`] trait for why this exists.
struct ExplainStatement<Q> {
    query: Q,
}

impl<Q> QueryId for ExplainStatement<Q> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Q> Query for ExplainStatement<Q> {
    type SqlType = diesel::sql_types::Text;
}

impl<Q> RunQueryDsl<DbConnection> for ExplainStatement<Q> {}

impl<Q> QueryFragment<Pg> for ExplainStatement<Q>
where
    Q: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("EXPLAIN (");
        self.query.walk_ast(out.reborrow())?;
        out.push_sql(")");
        Ok(())
    }
}
