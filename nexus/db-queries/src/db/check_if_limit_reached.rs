// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A query for checking if the number of records in a table exceeds
//! some limit.

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::QueryResult;
use diesel::Table;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::query_source::QuerySource;
use diesel::sql_types;
use nexus_db_errors::TransactionError;
use nexus_db_lookup::DbConnection;
use omicron_common::api::external::Error;

/// A query for (semi-)efficiently checking if the number of rows in a [`Table`]
/// exceeds a provided limit.
pub struct LimitQuery<T: Table> {
    limit: i64,
    from: <T as QuerySource>::FromClause,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IsLimitReached {
    Yes,
    No { count: u64 },
}

impl<T> LimitQuery<T>
where
    T: Table,
    <T as QuerySource>::FromClause: QueryFragment<Pg>,
{
    pub fn new(table: T, limit: u64) -> Result<Self, Error> {
        let limit = i64::try_from(limit).map_err(|e| {
            Error::invalid_value(
                "limit",
                format!("limit cannot be converted to i64: {e}"),
            )
        })?;
        Ok(Self { limit, from: table.from_clause() })
    }

    /// Check if the number of rows in the table has reached or exceeded the
    /// limit.
    ///
    /// # Usage Notes
    ///
    /// This will perform a "full-table scan", except for the fact that...it
    /// doesn't actually scan the entire table, due to the use of a `LIMIT`
    /// clause. However, CRDB is not smart enough to realize this, and will fail
    /// to execute this query unless it is performed in a transaction that runs
    /// the `ALLOW_FULL_TABLE_SCAN_SQL`. So, you have to do that for this to
    /// work.
    pub async fn check_if_limit_reached_async(
        self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<IsLimitReached, TransactionError<Error>>
    where
        Self: Send + 'static,
    {
        // Copy this out of `self` first`kq, because `load_async` takes the
        // query by value.
        let limit = self.limit;
        // self.first_async fails with `the trait bound
        // `TypedSqlQuery<BigInt>: diesel::Table` is not satisfied`.
        // So we use load_async, knowing that only one row will be
        // returned.
        let results = self.load_async::<i64>(conn).await?;

        // There must be exactly one row in the returned result.
        let count = *results.get(0).ok_or_else(|| {
            TransactionError::CustomError(Error::internal_error(
                "check_if_limit_reached query returned no values",
            ))
        })?;

        // Note count >= limit (and not count > limit): for a limit of 5000 we
        // want to fail if it's reached 5000.
        if count >= limit {
            Ok(IsLimitReached::Yes)
        } else {
            let count = u64::try_from(count).map_err(|_| {
                let error = Error::InternalError {
                    internal_message: format!(
                        "error converting record count {count} to u64 (how \
                            is it negative?)"
                    ),
                };
                TransactionError::CustomError(error)
            })?;
            Ok(IsLimitReached::No { count })
        }
    }
}

impl<T: Table> QueryFragment<Pg> for LimitQuery<T>
where
    T: Table,
    <T as QuerySource>::FromClause: QueryFragment<Pg>,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.push_sql(
            "SELECT COUNT(*) FROM (\
                SELECT 1 FROM ",
        );
        self.from.walk_ast(out.reborrow())?;
        out.push_sql(" LIMIT ");
        out.push_bind_param::<sql_types::BigInt, _>(&self.limit)?;
        out.push_sql(")");

        Ok(())
    }
}

impl<T> Query for LimitQuery<T>
where
    T: Table,
    <T as QuerySource>::FromClause: QueryFragment<Pg>,
{
    type SqlType = sql_types::BigInt;
}

impl<T> QueryId for LimitQuery<T>
where
    T: Table,
    <T as QuerySource>::FromClause: QueryFragment<Pg>,
{
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<T> diesel::RunQueryDsl<DbConnection> for LimitQuery<T>
where
    T: Table,
    <T as QuerySource>::FromClause: QueryFragment<Pg>,
{
}

impl<T> Clone for LimitQuery<T>
where
    T: Table,
    <T as QuerySource>::FromClause: Clone,
{
    fn clone(&self) -> Self {
        Self { from: self.from.clone(), limit: self.limit }
    }
}

#[cfg(test)]
mod tests {
    use super::IsLimitReached;
    use super::LimitQuery;
    use crate::db::DataStore;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
    use async_bb8_diesel::AsyncRunQueryDsl;
    use async_bb8_diesel::AsyncSimpleConnection;
    use diesel::prelude::*;
    use omicron_test_utils::dev;
    use uuid::Uuid;

    use std::sync::Arc;

    table! {
        test_limit_items (id) {
            id -> Uuid,
        }
    }

    #[derive(Clone, Insertable)]
    #[diesel(table_name = test_limit_items)]
    struct Item {
        id: Uuid,
    }

    async fn create_table(datastore: &DataStore) {
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        (*conn)
            .batch_execute_async(
                "CREATE TABLE test_limit_items (id UUID PRIMARY KEY);",
            )
            .await
            .unwrap();
    }

    struct Test {
        ids: Vec<Uuid>,
        datastore: Arc<DataStore>,
    }

    impl Test {
        /// Insert `n` new rows, recording their ids in `ids` so they can later be
        /// deleted by primary key (which avoids a full table scan).
        async fn insert_items(&mut self, n: usize) {
            let len = self.ids.len();
            let conn =
                self.datastore.pool_connection_for_tests().await.unwrap();
            let new: Vec<Item> =
                (0..n).map(|_| Item { id: Uuid::new_v4() }).collect();
            let inserted = diesel::insert_into(test_limit_items::table)
                .values(new.clone())
                .returning(test_limit_items::id)
                .get_results_async::<Uuid>(&*conn)
                .await
                .unwrap();
            eprintln!("[{len}].insert({n}) -> {inserted:?})");
            self.ids.extend(inserted);
            assert_eq!(self.ids.len(), len + n);
        }

        /// Delete `n` rows by primary key.
        async fn delete_items(&mut self, n: usize) {
            let conn =
                self.datastore.pool_connection_for_tests().await.unwrap();
            let len = self.ids.len();
            let to_delete = self.ids.split_off(len - n);
            let deleted = diesel::delete(
                test_limit_items::table
                    .filter(test_limit_items::id.eq_any(to_delete.clone())),
            )
            .execute_async(&*conn)
            .await
            .unwrap();
            assert_eq!(deleted, n, "expected to delete exactly {n} rows");
            eprintln!("[{len}].delete({n}) -> {to_delete:?})");
        }

        /// Run the limit query against `test_limit_items` in a transaction, the
        /// same way it would be used with a real table.
        async fn check_limit(&mut self, limit: u64) -> IsLimitReached {
            let len = self.ids.len();
            let conn =
                self.datastore.pool_connection_for_tests().await.unwrap();
            let result = self
                .datastore
                .transaction_non_retry_wrapper("test_check_if_limit_reached")
                .transaction(&conn, move |conn| async move {
                    // The query performs a (bounded) full table scan, which CRDB
                    // refuses to run unless it's been explicitly allowed for the
                    // transaction.
                    conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;
                    let query = LimitQuery::new(test_limit_items::table, limit)
                        .expect("limit should convert to i64");
                    query.check_if_limit_reached_async(&conn).await
                })
                .await
                .expect("check_if_limit_reached query should succeed");
            eprintln!("[{len}].check_if_limit_reached({limit}) -> {result:?}");
            result
        }
    }

    #[track_caller]
    fn assert_reached(result: IsLimitReached) {
        assert_eq!(
            result,
            IsLimitReached::Yes,
            "expected limit to be reached, but query returned {result:?}"
        );
    }

    #[track_caller]
    fn assert_not_reached(result: IsLimitReached, expected_count: u64) {
        assert_eq!(
            result,
            IsLimitReached::No { count: expected_count },
            "expected limit not to be reached (count {expected_count}), \
             but query returned {result:?}"
        );
    }

    // XXX(eliza): note that this is more or less a property test, and could
    // probably be rewritten as one pretty easily.
    #[tokio::test]
    async fn test_check_if_limit_reached() {
        let logctx = dev::test_setup_log("test_check_if_limit_reached");
        let db = TestDatabase::new_with_raw_datastore(&logctx.log).await;
        let datastore = db.datastore();

        create_table(&datastore).await;

        let mut test = Test { datastore: datastore.clone(), ids: Vec::new() };

        // Empty table. Note that a table is always "at" a limit of zero, even
        // when empty.
        assert_reached(test.check_limit(0).await);
        assert_not_reached(test.check_limit(1).await, 0);
        assert_not_reached(test.check_limit(5).await, 0);

        // 3 rows.
        test.insert_items(3).await;
        assert_not_reached(test.check_limit(5).await, 3);
        assert_not_reached(test.check_limit(4).await, 3);
        assert_reached(test.check_limit(3).await); // Exactly the limit
        assert_reached(test.check_limit(2).await);
        assert_reached(test.check_limit(1).await);

        // 5 rows
        test.insert_items(2).await;
        assert_eq!(test.ids.len(), 5);
        assert_reached(test.check_limit(5).await);
        assert_not_reached(test.check_limit(6).await, 5);
        assert_not_reached(test.check_limit(100).await, 5);

        // 15 rows
        test.insert_items(10).await;
        assert_eq!(test.ids.len(), 15);
        assert_reached(test.check_limit(5).await);
        assert_reached(test.check_limit(15).await);
        assert_not_reached(test.check_limit(16).await, 15);

        // 1 row (delete some records)
        test.delete_items(14).await;
        assert_eq!(test.ids.len(), 1);
        assert_not_reached(test.check_limit(5).await, 1);
        assert_reached(test.check_limit(1).await);
        assert_not_reached(test.check_limit(2).await, 1);

        // Empty the table again.
        test.delete_items(1).await;
        assert!(test.ids.is_empty());
        assert_not_reached(test.check_limit(1).await, 0);
        assert_not_reached(test.check_limit(5).await, 0);

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
