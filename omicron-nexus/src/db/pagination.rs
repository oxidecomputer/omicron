//! Interface for paginating database queries.

use diesel::expression::{AsExpression, NonAggregate};
use diesel::pg::Pg;
use diesel::query_builder::AsQuery;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::*;
use diesel::query_dsl::methods as query_methods;
use diesel::{ExpressionMethods, QueryDsl};
use omicron_common::api::external::DataPageParams;

type BoxedQuery<'a, T> =
    BoxedSelectStatement<'a, <T as AsQuery>::SqlType, T, Pg>;

/// Uses `pagparams` to list a subset of rows in `table`, ordered by `column`.
pub fn paginated<'a, T, C, M>(
    table: T,
    column: C,
    pagparams: &DataPageParams<'a, M>,
) -> BoxedQuery<'a, T>
where
    // T must be a Table.
    T: diesel::Table,
    // It should be possible to create a BoxedQuery from the table.
    T::Query:
        query_methods::BoxedDsl<'static, Pg, Output = BoxedQuery<'static, T>>,
    // C is a column which appears in T.
    //
    C: 'a
        + Copy
        + ExpressionMethods
        + diesel::AppearsOnTable<T>
        // NonAggregate required to be passed to gt/lt.
        + NonAggregate
        // QueryFragment required become ordered.
        + QueryFragment<Pg>,
    // Required to appear in the RHS of an expression method (gt/lt).
    &'a M: AsExpression<C::SqlType>,
    // Required to filter the query by "column.{gt,lt}(marker)".
    <&'a M as AsExpression<C::SqlType>>::Expression:
        diesel::AppearsOnTable<T> + NonAggregate + QueryFragment<Pg>,
{
    let mut query = table.into_boxed().limit(pagparams.limit.get().into());
    match pagparams.direction {
        dropshot::PaginationOrder::Ascending => {
            if let Some(marker) = pagparams.marker {
                query = query.filter(column.gt(marker));
            }
            query.order(column.asc())
        }
        dropshot::PaginationOrder::Descending => {
            if let Some(marker) = pagparams.marker {
                query = query.filter(column.lt(marker));
            }
            query.order(column.desc())
        }
    }
}
