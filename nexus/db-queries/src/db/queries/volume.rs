// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Helper queries for working with volumes.

use crate::db;
use crate::db::pool::DbConnection;
use diesel::expression::is_aggregate;
use diesel::expression::ValidGrouping;
use diesel::pg::Pg;
use diesel::query_builder::AstPass;
use diesel::query_builder::Query;
use diesel::query_builder::QueryFragment;
use diesel::query_builder::QueryId;
use diesel::sql_types;
use diesel::Column;
use diesel::Expression;
use diesel::QueryResult;
use diesel::RunQueryDsl;

/// Produces a query fragment that will conditionally reduce the volume
/// references for region_snapshot rows whose snapshot_addr column is part of a
/// list.
///
/// The output should look like:
///
/// ```sql
///  update region_snapshot set
///   volume_references = volume_references - 1,
///   deleting = case when volume_references = 1
///    then true
///    else false
///   end
///  where
///   snapshot_addr in ('a1', 'a2', 'a3') and
///   volume_references >= 1 and
///   deleting = false
///  returning *
/// ```
#[must_use = "Queries must be executed"]
pub struct ConditionallyDecreaseReferences {
    snapshot_addrs: Vec<String>,
}

impl ConditionallyDecreaseReferences {
    pub fn new(snapshot_addrs: Vec<String>) -> Self {
        Self { snapshot_addrs }
    }
}

impl QueryId for ConditionallyDecreaseReferences {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl QueryFragment<Pg> for ConditionallyDecreaseReferences {
    fn walk_ast<'a>(&'a self, mut out: AstPass<'_, 'a, Pg>) -> QueryResult<()> {
        use db::schema::region_snapshot::dsl;

        out.push_sql("UPDATE ");
        dsl::region_snapshot.walk_ast(out.reborrow())?;
        out.push_sql(" SET ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" = ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" - 1, ");
        out.push_identifier(dsl::deleting::NAME)?;
        out.push_sql(" = CASE WHEN ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" = 1 THEN TRUE ELSE FALSE END WHERE ");
        out.push_identifier(dsl::snapshot_addr::NAME)?;
        out.push_sql(" IN (");

        // If self.snapshot_addrs is empty, this query fragment will
        // intentionally not update any region_snapshot rows.
        for (i, snapshot_addr) in self.snapshot_addrs.iter().enumerate() {
            out.push_bind_param::<sql_types::Text, String>(snapshot_addr)?;
            if i == self.snapshot_addrs.len() - 1 {
                out.push_sql(" ");
            } else {
                out.push_sql(", ");
            }
        }

        out.push_sql(") AND ");
        out.push_identifier(dsl::volume_references::NAME)?;
        out.push_sql(" >= 1 AND ");
        out.push_identifier(dsl::deleting::NAME)?;
        out.push_sql(" = false RETURNING *");

        Ok(())
    }
}

impl Expression for ConditionallyDecreaseReferences {
    type SqlType = sql_types::Array<db::model::RegionSnapshot>;
}

impl<GroupByClause> ValidGrouping<GroupByClause>
    for ConditionallyDecreaseReferences
{
    type IsAggregate = is_aggregate::Never;
}

impl RunQueryDsl<DbConnection> for ConditionallyDecreaseReferences {}

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

impl Query for ConditionallyDecreaseReferences {
    type SqlType = SelectableSql<db::model::RegionSnapshot>;
}
