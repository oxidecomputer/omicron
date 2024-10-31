// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utility allowing Diesel to EXPLAIN queries.

// These utilities can be useful during development, so we don't want to
// `#[cfg(test)]` the module, but it's likely they won't be used outside of
// tests.
#![cfg_attr(not(test), allow(dead_code))]

use super::pool::DbConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_trait::async_trait;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::result::Error as DieselError;

/// A wrapper around a runnable Diesel query, which EXPLAINs what it is doing.
///
/// Q: The Query we're explaining.
///
/// EXPLAIN: <https://www.cockroachlabs.com/docs/stable/explain.html>
#[async_trait]
pub trait ExplainableAsync<Q> {
    /// Asynchronously issues an explain statement.
    async fn explain_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<String, DieselError>;
}

#[async_trait]
impl<Q> ExplainableAsync<Q> for Q
where
    Q: QueryFragment<Pg>
        + QueryId
        + RunQueryDsl<DbConnection>
        + Sized
        + Send
        + 'static,
{
    async fn explain_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<String, DieselError> {
        Ok(ExplainStatement { query: self }
            .get_results_async::<String>(conn)
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

impl<Q> QueryId for ExplainStatement<Q>
where
    Q: QueryId + 'static,
{
    type QueryId = ExplainStatement<Q>;
    const HAS_STATIC_QUERY_ID: bool = Q::HAS_STATIC_QUERY_ID;
}

impl<Q> Query for ExplainStatement<Q> {
    type SqlType = diesel::sql_types::Text;
}

impl<Q> RunQueryDsl<DbConnection> for ExplainStatement<Q> {}

impl<Q> QueryFragment<Pg> for ExplainStatement<Q>
where
    Q: QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql("EXPLAIN ");
        self.query.walk_ast(out.reborrow())?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db;
    use crate::db::pub_test_utils::TestDatabase;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::SelectableHelper;
    use expectorate::assert_contents;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    mod schema {
        use diesel::prelude::*;

        table! {
            test_users {
                id -> Uuid,
                age -> Int8,
                height -> Int8,
            }
        }
    }

    use schema::test_users;

    #[derive(Clone, Debug, Queryable, Insertable, PartialEq, Selectable)]
    #[diesel(table_name = test_users)]
    struct User {
        id: Uuid,
        age: i64,
        height: i64,
    }

    async fn create_schema(pool: &db::Pool) {
        pool.claim()
            .await
            .unwrap()
            .batch_execute_async(
                "CREATE TABLE test_users (
                id UUID PRIMARY KEY,
                age INT NOT NULL,
                height INT NOT NULL
            )",
            )
            .await
            .unwrap();
    }

    // Tests the ".explain_async()" method in an asynchronous context.
    #[tokio::test]
    async fn test_explain_async() {
        let logctx = dev::test_setup_log("test_explain_async");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        create_schema(&pool).await;

        use schema::test_users::dsl;
        let explanation = dsl::test_users
            .filter(dsl::id.eq(Uuid::nil()))
            .select(User::as_select())
            .explain_async(&conn)
            .await
            .unwrap();

        assert_contents("tests/output/test-explain-output", &explanation);
        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Tests that ".explain_async()" can tell us when we're doing full table
    // scans.
    #[tokio::test]
    async fn test_explain_full_table_scan() {
        let logctx = dev::test_setup_log("test_explain_full_table_scan");
        let db = TestDatabase::new_with_pool(&logctx.log).await;
        let pool = db.pool();
        let conn = pool.claim().await.unwrap();

        create_schema(&pool).await;

        use schema::test_users::dsl;
        let explanation = dsl::test_users
            .filter(dsl::age.eq(2))
            .select(User::as_select())
            .explain_async(&conn)
            .await
            .unwrap();

        assert!(
            explanation.contains("FULL SCAN"),
            "Expected [{}] to contain 'FULL SCAN'",
            explanation
        );
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
