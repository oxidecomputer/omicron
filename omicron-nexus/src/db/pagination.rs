//! Interface for paginating database queries.

use diesel::expression::{AsExpression, NonAggregate};
use diesel::pg::Pg;
use diesel::query_builder::AsQuery;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::{ExpressionMethods, QueryDsl};
use omicron_common::api::external::DataPageParams;

type BoxedQuery<T> = BoxedSelectStatement<'static, <T as AsQuery>::SqlType, T, Pg>;

/// Uses `pagparams` to list a subset of rows in `table`, ordered by `column`.
pub fn paginated<'a, T, C, M>(
    table: T,
    column: C,
    pagparams: &DataPageParams<'a, M>,
) -> BoxedQuery<T>
where
    // T must be a Table.
    T: diesel::Table,
    // It should be possible to create a BoxedQuery from the table.
    T::Query:
        query_methods::BoxedDsl<'static, Pg, Output = BoxedQuery<T>>,
    // C is a column which appears in T.
    C: 'static
        + Copy
        + ExpressionMethods
        + diesel::AppearsOnTable<T>
        // NonAggregate required to be passed to gt/lt.
        + NonAggregate
        // QueryFragment required become ordered.
        + QueryFragment<Pg>
        + Send,
    // Required to appear in the RHS of an expression method (gt/lt).
    M: Clone + AsExpression<C::SqlType> + Send,
    // Required to filter the query by "column.{gt,lt}(marker)".
    <M as AsExpression<C::SqlType>>::Expression:
        'static + diesel::AppearsOnTable<T> + NonAggregate + QueryFragment<Pg> + Send,
    <T as diesel::query_builder::AsQuery>::SqlType: Send,
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
