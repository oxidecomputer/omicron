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
use diesel::query_builder::BoxedSelectStatement;
use diesel::query_dsl::methods as query_methods;
use diesel::sql_types::{Bool, SqlType};
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

/// Uses `pagparams` to list a subset of rows in `table`, ordered by `c1, and
/// then by `c2.
///
/// This is a two-column varaiation of the [`paginated`] function.
// NOTE: This function could probably be made generic over an arbitrary number
// of columns, but that'll either require modifying Diesel (to make "tuples of
// columns" implement a subsetof  ExpressionMethods) or making a macro to generate
// all the necessary bounds we need.
pub fn paginated_multicolumn<T, C1, C2, M1, M2>(
    table: T,
    (c1, c2): (C1, C2),
    pagparams: &DataPageParams<'_, (M1, M2)>,
) -> BoxedQuery<T>
where
    // T is a table which can create a BoxedQuery.
    T: diesel::Table,
    T: query_methods::BoxedDsl<'static, Pg, Output = BoxedQuery<T>>,
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
    // Necessary for "query.order(...).then_order_by(c1.asc())"
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
