// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for updating resource usage info.

use crate::db::alias::ExpressionAlias;
use crate::db::pool::DbConnection;
use crate::db::subquery::{AsQuerySource, Cte, CteBuilder, CteQuery};
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::{
    sql_types, BoolExpressionMethods, ExpressionMethods,
    NullableExpressionMethods, QueryDsl, RunQueryDsl,
};
use nexus_db_model::queries::resource_usage_update::{parent_org, parent_silo};

#[derive(Subquery, QueryId)]
#[subquery(name = parent_org)]
struct ParentOrg {
    query: Box<dyn CteQuery<SqlType = parent_org::SqlType>>,
}

impl ParentOrg {
    fn new(project_id: uuid::Uuid) -> Self {
        use crate::db::schema::project::dsl;
        Self {
            query: Box::new(
                dsl::project.filter(dsl::id.eq(project_id)).select((
                    ExpressionAlias::new::<parent_org::dsl::id>(
                        dsl::organization_id,
                    ),
                )),
            ),
        }
    }
}

#[derive(Subquery, QueryId)]
#[subquery(name = parent_silo)]
struct ParentSilo {
    query: Box<dyn CteQuery<SqlType = parent_silo::SqlType>>,
}

impl ParentSilo {
    fn new(parent_org: &ParentOrg) -> Self {
        use crate::db::schema::organization::dsl;
        Self {
            query: Box::new(
                dsl::organization
                    .filter(
                        dsl::id.eq(parent_org
                            .query_source()
                            .select(parent_org::id)
                            .single_value()
                            .assume_not_null()),
                    )
                    .select((ExpressionAlias::new::<parent_silo::dsl::id>(
                        dsl::silo_id,
                    ),)),
            ),
        }
    }
}

/// Constructs a CTE for updating resource usage information in all
/// collections for a particular object.
#[derive(QueryId)]
pub struct ResourceUsageUpdate {
    cte: Cte,
}

impl ResourceUsageUpdate {
    pub fn new_update_disk(
        project_id: uuid::Uuid,
        disk_bytes_diff: i64,
    ) -> Self {
        let parent_org = ParentOrg::new(project_id);
        let parent_silo = ParentSilo::new(&parent_org);

        use crate::db::schema::resource_usage::dsl;

        let final_update = Box::new(
            diesel::update(dsl::resource_usage)
                .set(
                    dsl::disk_bytes_used
                        .eq(dsl::disk_bytes_used + disk_bytes_diff),
                )
                .filter(
                    // Update the project
                    dsl::id
                        .eq(project_id)
                        // Update the organization containing the project
                        .or(dsl::id.eq(parent_org
                            .query_source()
                            .select(parent_org::id)
                            .single_value()
                            .assume_not_null()))
                        // Update the silo containing the organization
                        .or(dsl::id.eq(parent_silo
                            .query_source()
                            .select(parent_silo::id)
                            .single_value()
                            .assume_not_null())), // TODO: Presumably, we could also update the fleet containing
                                                  // the silo here. However, such an object does not exist in the
                                                  // database at the time of writing this comment.
                )
                .returning(dsl::id),
        );

        let cte = CteBuilder::new()
            .add_subquery(parent_org)
            .add_subquery(parent_silo)
            .build(final_update);

        Self { cte }
    }
}

impl QueryFragment<Pg> for ResourceUsageUpdate {
    fn walk_ast<'a>(
        &'a self,
        mut out: AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.unsafe_to_cache_prepared();

        self.cte.walk_ast(out.reborrow())?;
        Ok(())
    }
}

impl Query for ResourceUsageUpdate {
    type SqlType = sql_types::Uuid;
}

impl RunQueryDsl<DbConnection> for ResourceUsageUpdate {}
