// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Tools for creating aliases in diesel.

use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::Expression;
use diesel::SelectableExpression;

/// Allows an [`diesel::Expression`] to be referenced by a new name.
///
/// This generates an `<expression> AS <name>` SQL fragment.
///
///
/// For example:
///
/// ```ignore
/// diesel::sql_function!(fn gen_random_uuid() -> Uuid);
///
/// let query = sleds.select(
///     (
///         ExpressionAlias::<schema::services::dsl::id>(gen_random_uuid()),
///         ExpressionAlias::<schema::services::dsl::sled_id>(gen_random_uuid()),
///     ),
/// );
/// ```
///
/// Produces the following SQL:
///
/// ```sql
/// SELECT
///   gen_random_uuid() as id,
///   gen_random_uuid() as sled_id,
/// FROM sleds
/// ```
#[derive(diesel::expression::ValidGrouping, diesel::query_builder::QueryId)]
pub struct ExpressionAlias<E> {
    expr: E,
    name: &'static str,
}

impl<E> ExpressionAlias<E>
where
    E: Expression,
{
    pub fn new<C: diesel::Column>(expr: E) -> Self {
        Self { expr, name: C::NAME }
    }
}

impl<E> Expression for ExpressionAlias<E>
where
    E: Expression,
{
    type SqlType = E::SqlType;
}

impl<E, QS> diesel::AppearsOnTable<QS> for ExpressionAlias<E> where
    E: diesel::AppearsOnTable<QS>
{
}

impl<E, T> SelectableExpression<T> for ExpressionAlias<E> where
    E: SelectableExpression<T>
{
}

impl<E> QueryFragment<Pg> for ExpressionAlias<E>
where
    E: QueryFragment<Pg>,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        self.expr.walk_ast(out.reborrow())?;
        out.push_sql(" AS ");
        out.push_sql(&self.name);
        Ok(())
    }
}
