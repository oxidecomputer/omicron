// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for paginating database queries.

use diesel::dsl::{Asc, Desc, Gt, Lt};
use diesel::expression::{AsExpression, Expression};
use diesel::expression_methods::BoolExpressionMethods;
use diesel::helper_types::*;
use diesel::pg::Pg;
use diesel::query_builder::AsQuery;
use diesel::query_dsl::methods as query_methods;
use diesel::sql_types::{Bool, SqlType};
use diesel::AppearsOnTable;
use diesel::Column;
use diesel::{ExpressionMethods, QueryDsl};
use omicron_common::api::external::DataPageParams;
use std::num::NonZeroU32;

// Shorthand alias for "the SQL type of the whole table".
type TableSqlType<T> = <T as AsQuery>::SqlType;

// Shorthand alias for the type made from "table.into_boxed()".
type BoxedQuery<T> = diesel::helper_types::IntoBoxed<'static, T, Pg>;
type BoxedDslOutput<T> = diesel::internal::table_macro::BoxedSelectStatement<
    'static,
    TableSqlType<T>,
    diesel::internal::table_macro::FromClause<T>,
    Pg,
>;

/// Uses `pagparams` to list a subset of rows in `table`, ordered by `column`.
pub fn paginated<T, C, M>(
    table: T,
    column: C,
    pagparams: &DataPageParams<'_, M>,
) -> BoxedQuery<T>
where
    // T is a table which can create a BoxedQuery.
    T: diesel::Table,
    T: query_methods::BoxedDsl<'static, Pg, Output = BoxedDslOutput<T>>,
    // C is a column which appears in T.
    C: 'static + Column + Copy + ExpressionMethods + AppearsOnTable<T>,
    // Required to compare the column with the marker type.
    C::SqlType: SqlType,
    M: Clone + AsExpression<C::SqlType>,
    // Defines the methods which can be called on "query", and tells
    // the compiler we're gonna output a BoxedQuery each time.
    BoxedQuery<T>: query_methods::OrderDsl<Desc<C>, Output = BoxedQuery<T>>,
    BoxedQuery<T>: query_methods::OrderDsl<Asc<C>, Output = BoxedQuery<T>>,
    BoxedQuery<T>: query_methods::FilterDsl<Gt<C, M>, Output = BoxedQuery<T>>,
    BoxedQuery<T>: query_methods::FilterDsl<Lt<C, M>, Output = BoxedQuery<T>>,
{
    let mut query = table.into_boxed().limit(pagparams.limit.get().into());
    let marker = pagparams.marker.map(|m| m.clone());
    match pagparams.direction {
        dropshot::PaginationOrder::Ascending => {
            if let Some(marker) = marker {
                query = query.filter(column.gt(marker));
            }
            query.order(column.asc())
        }
        dropshot::PaginationOrder::Descending => {
            if let Some(marker) = marker {
                query = query.filter(column.lt(marker));
            }
            query.order(column.desc())
        }
    }
}

/// Uses `pagparams` to list a subset of rows in `table`, ordered by `c1, and
/// then by `c2.
///
/// This is a two-column variation of the [`paginated`] function.
// NOTE: This function could probably be made generic over an arbitrary number
// of columns, but that'll either require modifying Diesel (to make "tuples of
// columns" implement a subset of ExpressionMethods) or making a macro to generate
// all the necessary bounds we need.
pub fn paginated_multicolumn<T, C1, C2, M1, M2>(
    table: T,
    (c1, c2): (C1, C2),
    pagparams: &DataPageParams<'_, (M1, M2)>,
) -> BoxedQuery<T>
where
    // T is a table which can create a BoxedQuery.
    T: diesel::Table,
    T: query_methods::BoxedDsl<
        'static,
        Pg,
        Output = diesel::internal::table_macro::BoxedSelectStatement<
            'static,
            TableSqlType<T>,
            diesel::internal::table_macro::FromClause<T>,
            Pg,
        >,
    >,
    // C1 & C2 are columns which appear in T.
    C1: 'static + Column + Copy + ExpressionMethods + AppearsOnTable<T>,
    C2: 'static + Column + Copy + ExpressionMethods + AppearsOnTable<T>,
    // Required to compare the columns with the marker types.
    C1::SqlType: SqlType,
    C2::SqlType: SqlType,
    M1: Clone + AsExpression<C1::SqlType>,
    M2: Clone + AsExpression<C2::SqlType>,
    // Necessary for "query.order(c1.desc())"
    BoxedQuery<T>: query_methods::OrderDsl<Desc<C1>, Output = BoxedQuery<T>>,
    // Necessary for "query.order(...).then_order_by(c2.desc())"
    BoxedQuery<T>:
        query_methods::ThenOrderDsl<Desc<C2>, Output = BoxedQuery<T>>,
    // Necessary for "query.order(c1.asc())"
    BoxedQuery<T>: query_methods::OrderDsl<Asc<C1>, Output = BoxedQuery<T>>,
    // Necessary for "query.order(...).then_order_by(c2.asc())"
    BoxedQuery<T>: query_methods::ThenOrderDsl<Asc<C2>, Output = BoxedQuery<T>>,

    // We'd like to be able to call:
    //
    //  c1.eq(v1).and(c2.gt(v2))
    //
    // This means "c1.eq(v1)" must implement BoolExpressionMethods, and
    // satisfy the requirements of the ".and" method.
    //
    // The LHS (c1.eq(v1)) must be a boolean expression:
    Eq<C1, M1>: Expression<SqlType = Bool>,
    // The RHS (c2.gt(v2)) must be a boolean expression:
    Gt<C2, M2>: Expression<SqlType = Bool>,
    // Putting it together, we should be able to filter by LHS.and(RHS):
    BoxedQuery<T>: query_methods::FilterDsl<
        And<Eq<C1, M1>, Gt<C2, M2>>,
        Output = BoxedQuery<T>,
    >,

    // We'd also like to be able to call:
    //
    //  c1.eq(v1).and(c2.lt(v2))
    //
    // We've already defined the bound on the LHS, so we add the equivalent
    // bounds on the RHS for the "Less than" variant.
    Lt<C2, M2>: Expression<SqlType = Bool>,
    BoxedQuery<T>: query_methods::FilterDsl<
        And<Eq<C1, M1>, Lt<C2, M2>>,
        Output = BoxedQuery<T>,
    >,

    // Necessary for "query.or_filter(c1.gt(v1))"
    BoxedQuery<T>:
        query_methods::OrFilterDsl<Gt<C1, M1>, Output = BoxedQuery<T>>,
    // Necessary for "query.or_filter(c1.lt(v1))"
    BoxedQuery<T>:
        query_methods::OrFilterDsl<Lt<C1, M1>, Output = BoxedQuery<T>>,
{
    let mut query = table.into_boxed().limit(pagparams.limit.get().into());
    let marker = pagparams.marker.map(|m| m.clone());
    match pagparams.direction {
        dropshot::PaginationOrder::Ascending => {
            if let Some((v1, v2)) = marker {
                query = query.filter(c1.eq(v1.clone()).and(c2.gt(v2)));
                query = query.or_filter(c1.gt(v1));
            }
            query.order(c1.asc()).then_order_by(c2.asc())
        }
        dropshot::PaginationOrder::Descending => {
            if let Some((v1, v2)) = marker {
                query = query.filter(c1.eq(v1.clone()).and(c2.lt(v2)));
                query = query.or_filter(c1.lt(v1));
            }
            query.order(c1.desc()).then_order_by(c2.desc())
        }
    }
}

/// Helper for querying a large number of records from the database in batches
///
/// Without this helper: a typical way to perform paginated queries would be to
/// invoke some existing "list" function in the datastore that itself is
/// paginated.  Such functions accept a `pagparams: &DataPageParams` argument
/// that uses a marker to identify where the next page of results starts.  For
/// the first call, the marker inside `pagparams` is `None`.  For subsequent
/// calls, it's typically some field from the last item returned in the previous
/// page.  You're finished when you get a result set smaller than the batch
/// size.
///
/// This helper takes care of most of the logic for you.  To use this, you first
/// create a `Paginator` with a specific batch_size.  Then you call `next()` in
/// a loop.  Each iteration will provide you with a `DataPageParams` to use to
/// call your list function.  When you've fetched the next page, you have to
/// let the helper look at it to determine if there's another page to fetch and
/// what marker to use.
///
/// ## Example
///
/// ```
/// use nexus_db_queries::db::pagination::Paginator;
/// use omicron_common::api::external::DataPageParams;
///
/// let batch_size = std::num::NonZeroU32::new(3).unwrap();
///
/// // Assume you've got an existing paginated "list items" function.
/// // This simple implementation returns a few full batches, then a partial
/// // batch.
/// type Marker = u32;
/// type Item = u32;
/// let do_query = |pagparams: &DataPageParams<'_, Marker> | {
///     match pagparams.marker {
///         None => (0..batch_size.get()).collect(),
///         Some(x) if *x < 2 * batch_size.get() => (x+1..x+1+batch_size.get()).collect(),
///         Some(x) => vec![*x + 1],
///     }
/// };
///
/// // This closure translates from one of the returned item to the field in
/// // that item that servers as the marker.  This example is contrived.
/// let item2marker: &dyn Fn(&Item) -> Marker = &|u: &u32| *u;
///
/// let mut all_records = Vec::new();
/// let mut paginator = Paginator::new(batch_size);
/// while let Some(p) = paginator.next() {
///     let records_batch = do_query(&p.current_pagparams());
///     paginator = p.found_batch(&records_batch, item2marker);
///     all_records.extend(records_batch.into_iter());
/// }
///
/// // Results are in `all_records`.
/// assert_eq!(all_records, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
/// ```
///
/// ## Design notes
///
/// The separation of `Paginator` and `PaginatorHelper` is aimed at making it
/// harder to misuse this interface.  We could skip the helper altogether and
/// just have `Paginator::next()` return the DatePageParams directly.  But you'd
/// still need a `Paginator::found_batch()`.  And it would be easy to forget to
/// call this, leading to an infinite loop at runtime.  To avoid this mistake,
/// `Paginator::next()` consumes `self`.  You can't get another `Paginator` back
/// until you use `PaginatorHelper::found_batch()`.  That also consumes `self`
/// so that you can't keep using the old `DataPageParams`.
pub struct Paginator<N> {
    batch_size: NonZeroU32,
    state: PaginatorState<N>,
}

impl<N> Paginator<N> {
    pub fn new(batch_size: NonZeroU32) -> Paginator<N> {
        Paginator { batch_size, state: PaginatorState::Initial }
    }

    pub fn next(self) -> Option<PaginatorHelper<N>> {
        match self.state {
            PaginatorState::Initial => Some(PaginatorHelper {
                batch_size: self.batch_size,
                marker: None,
            }),
            PaginatorState::Middle { marker } => Some(PaginatorHelper {
                batch_size: self.batch_size,
                marker: Some(marker),
            }),
            PaginatorState::Done => None,
        }
    }
}

enum PaginatorState<N> {
    Initial,
    Middle { marker: N },
    Done,
}

pub struct PaginatorHelper<N> {
    batch_size: NonZeroU32,
    marker: Option<N>,
}

impl<N> PaginatorHelper<N> {
    /// Returns the `DatePageParams` to use to fetch the next page of results
    pub fn current_pagparams(&self) -> DataPageParams<'_, N> {
        DataPageParams {
            marker: self.marker.as_ref(),
            direction: dropshot::PaginationOrder::Ascending,
            limit: self.batch_size,
        }
    }

    /// Report a page of results
    ///
    /// This function looks at the returned results to determine whether we've
    /// finished iteration or whether we need to fetch another page (and if so,
    /// this determines the marker for the next fetch operation).
    ///
    /// This function returns a `Paginator` used to make the next request.  See
    /// the example on `Paginator` for usage.
    pub fn found_batch<T>(
        self,
        batch: &[T],
        item2marker: &dyn Fn(&T) -> N,
    ) -> Paginator<N> {
        let state =
            if batch.len() < usize::try_from(self.batch_size.get()).unwrap() {
                PaginatorState::Done
            } else {
                // self.batch_size is non-zero, so if we got at least that many
                // items, then there's at least one.
                let last = batch.iter().last().unwrap();
                let marker = item2marker(last);
                PaginatorState::Middle { marker }
            };

        Paginator { batch_size: self.batch_size, state }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::db;
    use async_bb8_diesel::{AsyncRunQueryDsl, AsyncSimpleConnection};
    use diesel::SelectableHelper;
    use dropshot::PaginationOrder;
    use nexus_test_utils::db::test_setup_database;
    use omicron_common::api::external::DataPageParams;
    use omicron_test_utils::dev;
    use std::num::NonZeroU32;
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

    impl PartialEq<(i64, i64)> for User {
        fn eq(&self, other: &(i64, i64)) -> bool {
            self.age == other.0 && self.height == other.1
        }
    }

    async fn populate_users(pool: &db::Pool, values: &Vec<(i64, i64)>) {
        use schema::test_users::dsl;

        let conn = pool.claim().await.unwrap();

        // The indexes here work around the check that prevents full table
        // scans.
        conn.batch_execute_async(
            "CREATE TABLE test_users (
                    id UUID PRIMARY KEY,
                    age INT NOT NULL,
                    height INT NOT NULL
                );

                CREATE INDEX ON test_users (age, height);
                CREATE INDEX ON test_users (height, age);",
        )
        .await
        .unwrap();

        let users: Vec<User> = values
            .iter()
            .map(|(age, height)| User {
                id: Uuid::new_v4(),
                age: *age,
                height: *height,
            })
            .collect();

        diesel::insert_into(dsl::test_users)
            .values(users)
            .execute_async(&*conn)
            .await
            .unwrap();
    }

    // Shorthand for query execution to reduce total LoC.
    async fn execute_query(
        pool: &db::Pool,
        query: BoxedQuery<schema::test_users::dsl::test_users>,
    ) -> Vec<User> {
        let conn = pool.claim().await.unwrap();
        query.select(User::as_select()).load_async(&*conn).await.unwrap()
    }

    #[tokio::test]
    async fn test_paginated_single_column_ascending() {
        let logctx =
            dev::test_setup_log("test_paginated_single_column_ascending");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new_qorb_single_host(&cfg).await;

        use schema::test_users::dsl;

        populate_users(&pool, &vec![(1, 1), (2, 2), (3, 3)]).await;

        // Get the first paginated result.
        let mut pagparams = DataPageParams::<i64> {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(1).unwrap(),
        };
        let query = paginated(dsl::test_users, dsl::age, &pagparams);
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(1, 1)]);

        // Get the next paginated results, check that they arrived in the order
        // we expected.
        let marker = observed[0].age;
        pagparams.marker = Some(&marker);
        pagparams.limit = NonZeroU32::new(2).unwrap();
        let query = paginated(dsl::test_users, dsl::age, &pagparams);
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(2, 2), (3, 3)]);

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_paginated_single_column_descending() {
        let logctx =
            dev::test_setup_log("test_paginated_single_column_descending");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new_qorb_single_host(&cfg).await;

        use schema::test_users::dsl;

        populate_users(&pool, &vec![(1, 1), (2, 2), (3, 3)]).await;

        // Get the first paginated result.
        let mut pagparams = DataPageParams::<i64> {
            marker: None,
            direction: PaginationOrder::Descending,
            limit: NonZeroU32::new(1).unwrap(),
        };
        let query = paginated(dsl::test_users, dsl::age, &pagparams);
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(3, 3)]);

        // Get the next paginated results, check that they arrived in the order
        // we expected.
        let marker = observed[0].age;
        pagparams.marker = Some(&marker);
        pagparams.limit = NonZeroU32::new(2).unwrap();
        let query = paginated(dsl::test_users, dsl::age, &pagparams);
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(2, 2), (1, 1)]);

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_paginated_multicolumn_ascending() {
        let logctx =
            dev::test_setup_log("test_paginated_multicolumn_ascending");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new_qorb_single_host(&cfg).await;

        use schema::test_users::dsl;

        populate_users(&pool, &vec![(1, 1), (1, 2), (2, 1), (2, 3), (3, 1)])
            .await;

        // Get the first paginated result.
        let mut pagparams = DataPageParams::<(i64, i64)> {
            marker: None,
            direction: PaginationOrder::Ascending,
            limit: NonZeroU32::new(1).unwrap(),
        };
        let query = paginated_multicolumn(
            dsl::test_users,
            (dsl::age, dsl::height),
            &pagparams,
        );
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(1, 1)]);

        // Get the next paginated results, check that they arrived in the order
        // we expected.
        let marker = (observed[0].age, observed[0].height);
        pagparams.marker = Some(&marker);
        pagparams.limit = NonZeroU32::new(10).unwrap();
        let query = paginated_multicolumn(
            dsl::test_users,
            (dsl::age, dsl::height),
            &pagparams,
        );
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(1, 2), (2, 1), (2, 3), (3, 1)]);

        // Switch the order of columns to see height-first results.
        pagparams.marker = None;
        let query = paginated_multicolumn(
            dsl::test_users,
            (dsl::height, dsl::age),
            &pagparams,
        );
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(1, 1), (2, 1), (3, 1), (1, 2), (2, 3)]);

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_paginated_multicolumn_descending() {
        let logctx =
            dev::test_setup_log("test_paginated_multicolumn_descending");
        let mut db = test_setup_database(&logctx.log).await;
        let cfg = db::Config { url: db.pg_config().clone() };
        let pool = db::Pool::new_qorb_single_host(&cfg).await;

        use schema::test_users::dsl;

        populate_users(&pool, &vec![(1, 1), (1, 2), (2, 1), (2, 3), (3, 1)])
            .await;

        // Get the first paginated result.
        let mut pagparams = DataPageParams::<(i64, i64)> {
            marker: None,
            direction: PaginationOrder::Descending,
            limit: NonZeroU32::new(1).unwrap(),
        };
        let query = paginated_multicolumn(
            dsl::test_users,
            (dsl::age, dsl::height),
            &pagparams,
        );
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(3, 1)]);

        // Get the next paginated results, check that they arrived in the order
        // we expected.
        let marker = (observed[0].age, observed[0].height);
        pagparams.marker = Some(&marker);
        pagparams.limit = NonZeroU32::new(10).unwrap();
        let query = paginated_multicolumn(
            dsl::test_users,
            (dsl::age, dsl::height),
            &pagparams,
        );
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(2, 3), (2, 1), (1, 2), (1, 1)]);

        // Switch the order of columns to see height-first results.
        pagparams.marker = None;
        let query = paginated_multicolumn(
            dsl::test_users,
            (dsl::height, dsl::age),
            &pagparams,
        );
        let observed = execute_query(&pool, query).await;
        assert_eq!(observed, vec![(2, 3), (1, 2), (3, 1), (2, 1), (1, 1)]);

        let _ = db.cleanup().await;
        logctx.cleanup_successful();
    }

    #[test]
    fn test_paginator() {
        // The doctest exercises a basic case for Paginator.  Here we test some
        // edge cases.
        let batch_size = std::num::NonZeroU32::new(3).unwrap();

        type Marker = u32;
        #[derive(Debug, PartialEq, Eq)]
        struct Item {
            value: String,
            marker: Marker,
        }

        let do_list =
            |query: &dyn Fn(&DataPageParams<'_, Marker>) -> Vec<Item>| {
                let mut all_records = Vec::new();
                let mut paginator = Paginator::new(batch_size);
                while let Some(p) = paginator.next() {
                    let records_batch = query(&p.current_pagparams());
                    paginator =
                        p.found_batch(&records_batch, &|i: &Item| i.marker);
                    all_records.extend(records_batch.into_iter());
                }
                all_records
            };

        fn mkitem(v: u32) -> Item {
            Item { value: v.to_string(), marker: v }
        }

        // Trivial case: first page is empty
        assert_eq!(Vec::<Item>::new(), do_list(&|_| Vec::new()));

        // Exactly one batch-size worth of items
        // (exercises the cases where the last non-empty batch is full, and
        // where any batch is empty)
        let my_query =
            |pagparams: &DataPageParams<'_, Marker>| match &pagparams.marker {
                None => (0..batch_size.get()).map(mkitem).collect(),
                Some(_) => Vec::new(),
            };
        assert_eq!(vec![mkitem(0), mkitem(1), mkitem(2)], do_list(&my_query));
    }
}
