// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::dsl::sql;
use diesel::expression::SqlLiteral;
use diesel::prelude::*;
use diesel::sql_types::Bool;
use diesel::upsert::IncompleteOnConflict;

/// An extension trait for incomplete [`ON CONFLICT`] expressions.
pub trait IncompleteOnConflictExt {
    type AsPartialIndexOutput;

    // TODO: explain what this does.
    fn as_partial_index(self) -> Self::AsPartialIndexOutput;
}

impl<Stmt, Target> IncompleteOnConflictExt
    for IncompleteOnConflict<Stmt, Target>
where
    Target: DecoratableTarget<SqlLiteral<Bool>>,
{
    type AsPartialIndexOutput =
        IncompleteOnConflict<Stmt, DecoratedBool<Target>>;

    fn as_partial_index(self) -> Self::AsPartialIndexOutput {
        #[allow(clippy::disallowed_methods)]
        {
            self.filter_target(sql::<Bool>("false"))
        }
    }
}

type DecoratedBool<Target> =
    <Target as DecoratableTarget<SqlLiteral<Bool>>>::FilterOutput;
