// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for updating resource usage info.

use crate::db::alias::ExpressionAlias;
use crate::db::model::VirtualResourceProvisioning;
use crate::db::pool::DbConnection;
use crate::db::schema::virtual_resource_provisioning;
use crate::db::subquery::{AsQuerySource, Cte, CteBuilder, CteQuery};
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::{
    sql_types, CombineDsl, ExpressionMethods, IntoSql, QueryDsl, RunQueryDsl,
    SelectableHelper,
};
use nexus_db_model::queries::virtual_resource_provisioning_update::{
    all_collections, parent_fleet, parent_org, parent_silo,
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
#[subquery(name = parent_fleet)]
struct ParentFleet {
    query: Box<dyn CteQuery<SqlType = parent_fleet::SqlType>>,
}

impl ParentFleet {
    fn new(parent_silo: &ParentSilo) -> Self {
        use crate::db::schema::silo::dsl;
        Self {
            query: Box::new(
                dsl::silo
                    .filter(dsl::id.eq_any(
                        parent_silo.query_source().select(parent_silo::id),
                    ))
                    .select((ExpressionAlias::new::<parent_fleet::dsl::id>(
                        dsl::fleet_id,
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
        parent_fleet: &ParentFleet,
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
                )))
                .union(parent_fleet.query_source().select((
                    ExpressionAlias::new::<all_collections::dsl::id>(
                        parent_fleet::id,
                    ),
                ))),
            ),
        }
    }
}

/// Constructs a CTE for updating resource usage information in all
/// collections for a particular object.
#[derive(QueryId)]
pub struct VirtualResourceProvisioningUpdate {
    cte: Cte,
}

impl VirtualResourceProvisioningUpdate {
    // Generic utility for updating all collections including this resource,
    // even transitively.
    //
    // Includes:
    // - Project
    // - Organization
    // - Silo
    // - Fleet
    fn apply_update<V>(project_id: uuid::Uuid, values: V) -> Self
    where
        V: diesel::AsChangeset<Target = virtual_resource_provisioning::table>,
        <V as diesel::AsChangeset>::Changeset:
            QueryFragment<Pg> + Send + 'static,
    {
        let parent_org = ParentOrg::new(project_id);
        let parent_silo = ParentSilo::new(&parent_org);
        let parent_fleet = ParentFleet::new(&parent_silo);
        let all_collections = AllCollections::new(
            project_id,
            &parent_org,
            &parent_silo,
            &parent_fleet,
        );

        use virtual_resource_provisioning::dsl;

        let final_update = Box::new(
            diesel::update(dsl::virtual_resource_provisioning)
                .set(values)
                .filter(dsl::id.eq_any(
                    all_collections.query_source().select(all_collections::id),
                ))
                .returning(VirtualResourceProvisioning::as_returning()),
        );

        let cte = CteBuilder::new()
            .add_subquery(parent_org)
            .add_subquery(parent_silo)
            .add_subquery(parent_fleet)
            .add_subquery(all_collections)
            .build(final_update);

        Self { cte }
    }

    pub fn new_update_disk(
        project_id: uuid::Uuid,
        disk_bytes_diff: i64,
    ) -> Self {
        use virtual_resource_provisioning::dsl;
        Self::apply_update(
            project_id,
            dsl::virtual_disk_bytes_provisioned
                .eq(dsl::virtual_disk_bytes_provisioned + disk_bytes_diff),
        )
    }

    pub fn new_update_cpus_and_ram(
        project_id: uuid::Uuid,
        cpus_diff: i64,
        ram_diff: i64,
    ) -> Self {
        use virtual_resource_provisioning::dsl;
        Self::apply_update(
            project_id,
            (
                dsl::cpus_provisioned.eq(dsl::cpus_provisioned + cpus_diff),
                dsl::ram_provisioned.eq(dsl::ram_provisioned + ram_diff),
            ),
        )
    }
}

impl QueryFragment<Pg> for VirtualResourceProvisioningUpdate {
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

impl Query for VirtualResourceProvisioningUpdate {
    type SqlType = SelectableSql<VirtualResourceProvisioning>;
}

impl RunQueryDsl<DbConnection> for VirtualResourceProvisioningUpdate {}
