// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! An expression wrapper for forcing errors from CTEs.

use async_bb8_diesel::RunError;
use diesel::expression::ValidGrouping;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::result::Error as DieselError;
use diesel::Expression;
use diesel::SelectableExpression;

/// Generates a wrapper around a boolean expression.
///
/// If the underlying expression is "true", this acts as a no-op.
/// If the underlying expression is "false", the error message is cast to a
/// boolean, which will probably fail, causing a casting error.
///
/// This can be useful for forcing an error condition out of a CTE.
#[derive(ValidGrouping, QueryId)]
pub struct TrueOrCastError<E> {
    expression: E,
    error: &'static str,
}

impl<E> TrueOrCastError<E>
where
    E: Expression<SqlType = diesel::sql_types::Bool>,
{
    pub const fn new(expression: E, error: &'static str) -> Self {
        Self { expression, error }
    }
}

impl<E> Expression for TrueOrCastError<E>
where
    E: Expression,
{
    type SqlType = E::SqlType;
}

impl<E, QS> diesel::AppearsOnTable<QS> for TrueOrCastError<E> where
    E: diesel::AppearsOnTable<QS>
{
}

impl<E, T> SelectableExpression<T> for TrueOrCastError<E> where
    E: SelectableExpression<T>
{
}

impl<E> QueryFragment<Pg> for TrueOrCastError<E>
where
    E: QueryFragment<Pg>,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql("CAST(IF(");
        // If this expression evaluates to "TRUE", then we treat the
        // entire fragment as CAST('TRUE' AS BOOL), which is a no-op boolean.
        self.expression.walk_ast(out.reborrow())?;
        out.push_sql(", \'TRUE\', \'");
        // However, if the expression evaluated to "FALSE", we try to cast
        // this string to a boolean.
        out.push_sql(self.error);
        out.push_sql("\') AS BOOL)");
        Ok(())
    }
}

/// Returns one of the sentinels if it matches the expected value from
/// a [`TrueOrCastError`].
pub fn matches_sentinel(
    e: &RunError,
    sentinels: &[&'static str],
) -> Option<&'static str> {
    use diesel::result::DatabaseErrorKind;
    use diesel::result::Error;

    fn bool_parse_error(sentinel: &'static str) -> String {
        format!(
            "could not parse \"{sentinel}\" as type bool: invalid bool value"
        )
    }

    match e {
        // Catch the specific errors designed to communicate the failures we
        // want to distinguish.
        RunError::Diesel(Error::DatabaseError(
            DatabaseErrorKind::Unknown,
            info,
        )) => {
            for sentinel in sentinels {
                if info.message() == bool_parse_error(sentinel) {
                    return Some(sentinel);
                }
            }
        }
        _ => {}
    }
    None
}
