//! Interface for paginating database queries.

use diesel::dsl::{Asc, Desc, Gt, Lt};
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::query_builder::AsQuery;
use diesel::query_builder::BoxedSelectStatement;
use diesel::query_dsl::methods as query_methods;
use diesel::sql_types::SqlType;
use diesel::AppearsOnTable;
use diesel::Column;
use diesel::{ExpressionMethods, QueryDsl};
use omicron_common::api::external::DataPageParams;

// Shorthand alias for "the SQL type of the whole table".
type TableSqlType<T> = <T as AsQuery>::SqlType;

// Shorthand alias for the type made from "table.into_boxed()".
type BoxedQuery<T> = BoxedSelectStatement<'static, TableSqlType<T>, T, Pg>;

/// Uses `pagparams` to list a subset of rows in `table`, ordered by `column`.
pub fn paginated<T, C, M>(
    table: T,
    column: C,
    pagparams: &DataPageParams<M>,
) -> BoxedQuery<T>
where
    // T is a table which can create a BoxedQuery.
    T: diesel::Table,
    T: query_methods::BoxedDsl<'static, Pg, Output = BoxedQuery<T>>,
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
    let marker = pagparams.marker.as_ref().map(|m| m.clone());
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
