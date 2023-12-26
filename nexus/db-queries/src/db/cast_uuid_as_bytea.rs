// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Cast UUID to BYTES

use diesel::expression::ValidGrouping;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::Expression;
use diesel::SelectableExpression;

/// Cast an expression which evaluates to a Uuid and cast it to a Bytea. It's
/// that simple!
#[derive(ValidGrouping, QueryId)]
pub struct CastUuidToBytea<E> {
    expression: E,
}

impl<E> CastUuidToBytea<E>
where
    E: Expression<SqlType = diesel::sql_types::Uuid>,
{
    pub const fn new(expression: E) -> Self {
        Self { expression }
    }
}

impl<E> Expression for CastUuidToBytea<E>
where
    E: Expression,
{
    type SqlType = diesel::sql_types::Bytea;
}

impl<E, QS> diesel::AppearsOnTable<QS> for CastUuidToBytea<E> where
    E: diesel::AppearsOnTable<QS>
{
}

impl<E, T> SelectableExpression<T> for CastUuidToBytea<E> where
    E: SelectableExpression<T>
{
}

impl<E> QueryFragment<Pg> for CastUuidToBytea<E>
where
    E: QueryFragment<Pg>,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.push_sql("CAST(");
        self.expression.walk_ast(out.reborrow())?;
        out.push_sql(" as BYTEA)");

        Ok(())
    }
}
