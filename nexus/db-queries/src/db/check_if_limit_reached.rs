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
use diesel::result::Error as DieselError;
use diesel::sql_types;
use nexus_db_lookup::DbConnection;
use omicron_common::api::external::Error;

/// A query for (semi-)efficiently checking if the number of rows in a [`Table`]
/// exceeds a provided limit.
pub struct LimitQuery<T: Table> {
    limit: i64,
    from: <T as QuerySource>::FromClause,
}

#[derive(Copy, Clone, Debug)]
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

    /// Check if the number of rows in the table exceeds the limit.
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
    ) -> Result<Result<IsLimitReached, Error>, DieselError>
    where
        Self: Send + 'static,
    {
        let limit = u64::try_from(self.limit).expect(
            "this i64 was initially converted from a u64, so it should \
             not be negative...",
        );

        // self.first_async fails with `the trait bound
        // `TypedSqlQuery<BigInt>: diesel::Table` is not satisfied`.
        // So we use load_async, knowing that only one row will be
        // returned.
        self.load_async::<i64>(conn).await.map(|results| {
            // There must be exactly one row in the returned result.
            let count = *results.get(0).ok_or_else(|| {
                Error::internal_error(
                    "check_if_limit_reached query returned no values",
                )
            })?;
            let count =
                u64::try_from(count).map_err(|_| Error::InternalError {
                    internal_message: format!(
                        "error converting record count {count} to u64 (how is \
                        it negative?)"
                    ),
                })?;

            // Note count >= limit (and not count > limit): for a limit of 5000 we
            // want to fail if it's reached 5000.
            if count >= limit {
                Ok(IsLimitReached::Yes)
            } else {
                Ok(IsLimitReached::No { count })
            }
        })
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
