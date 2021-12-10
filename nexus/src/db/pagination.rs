// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for paginating database queries.

use diesel::dsl::{Asc, Desc, Gt, GtEq, Lt, LtEq};
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
// XXX rewrite in terms of paginated_multicolumn?
pub fn paginated<T, C, M>(
    table: T,
    column: C,
    pagparams: &DataPageParams<'_, M>,
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

/// Uses `pagparams` to list a subset of rows in `table`, ordered by the given
/// `columns`.
pub fn paginated_multicolumn<T, C1, C2, M1, M2>(
    table: T,
    (c1, c2): (C1, C2),
    pagparams: &DataPageParams<'_, (M1, M2)>,
) -> BoxedQuery<T>
where
    // T is a table which can create a BoxedQuery.
    T: diesel::Table,
    T: query_methods::BoxedDsl<'static, Pg, Output = BoxedQuery<T>>,
    // C1, C2 are columns that appear in T.
    C1: 'static + Column + Copy + ExpressionMethods + AppearsOnTable<T>,
    C2: 'static + Column + Copy + ExpressionMethods + AppearsOnTable<T>,
    // Required to compare the column with the marker type.
    C1::SqlType: SqlType,
    M1: Clone + AsExpression<C1::SqlType>,
    C2::SqlType: SqlType,
    M2: Clone + AsExpression<C2::SqlType>,
    // Defines the methods which can be called on "query", and tells
    // the compiler we're gonna output a BoxedQuery each time.
    BoxedQuery<T>:
        query_methods::OrderDsl<(Asc<C1>, Asc<C2>), Output = BoxedQuery<T>>,
    BoxedQuery<T>:
        query_methods::OrderDsl<(Desc<C1>, Desc<C2>), Output = BoxedQuery<T>>,
    BoxedQuery<T>:
        query_methods::FilterDsl<GtEq<C1, M1>, Output = BoxedQuery<T>>,
    BoxedQuery<T>:
        query_methods::FilterDsl<LtEq<C1, M1>, Output = BoxedQuery<T>>,
    BoxedQuery<T>: query_methods::FilterDsl<Gt<C2, M2>, Output = BoxedQuery<T>>,
    BoxedQuery<T>: query_methods::FilterDsl<Lt<C2, M2>, Output = BoxedQuery<T>>,
{
    let mut query = table.into_boxed().limit(pagparams.limit.get().into());
    let marker = pagparams.marker.map(|m| m.clone());
    match pagparams.direction {
        dropshot::PaginationOrder::Ascending => {
            if let Some((v1, v2)) = marker {
                query = query.filter(c1.ge(v1));
                query = query.filter(c2.gt(v2));
            }

            query.order((c1.asc(), c2.asc()))
        }
        dropshot::PaginationOrder::Descending => {
            if let Some((v1, v2)) = marker {
                query = query.filter(c1.le(v1));
                query = query.filter(c2.lt(v2));
            }
            query.order((c1.desc(), c2.desc()))
        }
    }
}
