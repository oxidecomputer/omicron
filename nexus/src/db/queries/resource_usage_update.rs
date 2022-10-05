// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for updating resource usage info.

use crate::db::alias::ExpressionAlias;
use crate::db::model::ResourceUsage;
use crate::db::pool::DbConnection;
use crate::db::subquery::{AsQuerySource, Cte, CteBuilder, CteQuery};
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::{
    sql_types, CombineDsl, ExpressionMethods, IntoSql, QueryDsl, RunQueryDsl,
    SelectableHelper,
};
use nexus_db_model::queries::resource_usage_update::{
    all_collections, parent_org, parent_silo,
};

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
                    .filter(dsl::id.eq_any(
                        parent_org.query_source().select(parent_org::id),
                    ))
                    .select((ExpressionAlias::new::<parent_silo::dsl::id>(
                        dsl::silo_id,
                    ),)),
            ),
        }
    }
}

#[derive(Subquery, QueryId)]
#[subquery(name = all_collections)]
struct AllCollections {
    query: Box<dyn CteQuery<SqlType = all_collections::SqlType>>,
}

impl AllCollections {
    fn new(
        project_id: uuid::Uuid,
        parent_org: &ParentOrg,
        parent_silo: &ParentSilo,
    ) -> Self {
        Self {
            query: Box::new(
                diesel::select((ExpressionAlias::new::<
                    all_collections::dsl::id,
                >(
                    project_id.into_sql::<sql_types::Uuid>()
                ),))
                .union(parent_org.query_source().select((
                    ExpressionAlias::new::<all_collections::dsl::id>(
                        parent_org::id,
                    ),
                )))
                .union(parent_silo.query_source().select((
                    ExpressionAlias::new::<all_collections::dsl::id>(
                        parent_silo::id,
                    ),
                ))), // TODO: Presumably, we could also update the fleet containing
                     // the silo here. However, such an object does not exist in the
                     // database at the time of writing this comment.
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
        let all_collections =
            AllCollections::new(project_id, &parent_org, &parent_silo);

        use crate::db::schema::resource_usage::dsl;

        let final_update = Box::new(
            diesel::update(dsl::resource_usage)
                .set(
                    dsl::disk_bytes_used
                        .eq(dsl::disk_bytes_used + disk_bytes_diff),
                )
                .filter(dsl::id.eq_any(
                    all_collections.query_source().select(all_collections::id),
                ))
                .returning(ResourceUsage::as_returning()),
        );

        let cte = CteBuilder::new()
            .add_subquery(parent_org)
            .add_subquery(parent_silo)
            .add_subquery(all_collections)
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

type SelectableSql<T> = <
    <T as diesel::Selectable<Pg>>::SelectExpression as diesel::Expression
>::SqlType;

impl Query for ResourceUsageUpdate {
    type SqlType = SelectableSql<ResourceUsage>;
}

impl RunQueryDsl<DbConnection> for ResourceUsageUpdate {}
