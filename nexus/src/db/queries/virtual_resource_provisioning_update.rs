// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for updating resource provisioning info.

use crate::db::alias::ExpressionAlias;
use crate::db::model::ResourceTypeProvisioned;
use crate::db::model::VirtualResourceProvisioned;
use crate::db::model::VirtualResourceProvisioning;
use crate::db::pool::DbConnection;
use crate::db::schema::virtual_resource_provisioned;
use crate::db::schema::virtual_resource_provisioning;
use crate::db::subquery::{AsQuerySource, Cte, CteBuilder, CteQuery};
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::{
    sql_types, CombineDsl, ExpressionMethods, IntoSql,
    NullableExpressionMethods, QueryDsl, RunQueryDsl, SelectableHelper,
};
use nexus_db_model::queries::virtual_resource_provisioning_update::{
    all_collections, do_update, parent_fleet, parent_org, parent_silo,
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

#[derive(Subquery, QueryId)]
#[subquery(name = do_update)]
struct DoUpdate {
    query: Box<dyn CteQuery<SqlType = do_update::SqlType>>,
}

impl DoUpdate {
    fn new_for_insert(id: uuid::Uuid) -> Self {
        use virtual_resource_provisioned::dsl;

        let not_allocted = dsl::virtual_resource_provisioned
            .find(id)
            .count()
            .single_value()
            .assume_not_null()
            .eq(0);

        Self {
            query: Box::new(diesel::select((ExpressionAlias::new::<
                do_update::update,
            >(not_allocted),))),
        }
    }

    fn new_for_delete(id: uuid::Uuid) -> Self {
        use virtual_resource_provisioned::dsl;

        let already_allocated = dsl::virtual_resource_provisioned
            .find(id)
            .count()
            .single_value()
            .assume_not_null()
            .eq(1);

        Self {
            query: Box::new(diesel::select((ExpressionAlias::new::<
                do_update::update,
            >(already_allocated),))),
        }
    }
}

#[derive(Subquery, QueryId)]
#[subquery(name = virtual_resource_provisioning)]
struct UpdatedProvisions {
    query: Box<dyn CteQuery<SqlType = virtual_resource_provisioning::SqlType>>,
}

impl UpdatedProvisions {
    fn new<V>(
        all_collections: &AllCollections,
        do_update: &DoUpdate,
        values: V,
    ) -> Self
    where
        V: diesel::AsChangeset<Target = virtual_resource_provisioning::table>,
        <V as diesel::AsChangeset>::Changeset:
            QueryFragment<Pg> + Send + 'static,
    {
        use virtual_resource_provisioning::dsl;

        Self {
            query: Box::new(
                diesel::update(dsl::virtual_resource_provisioning)
                    .set(values)
                    .filter(
                        dsl::id.eq_any(
                            all_collections
                                .query_source()
                                .select(all_collections::id),
                        ),
                    )
                    .filter(
                        do_update
                            .query_source()
                            .select(do_update::update)
                            .single_value()
                            .assume_not_null(),
                    )
                    .returning(virtual_resource_provisioning::all_columns),
            ),
        }
    }
}

// This structure wraps a query, such that it can be used within a CTE.
//
// It generates a name that can be used by the "CteBuilder", but does not
// implement "AsQuerySource". This basically means:
// - It can be used to add data-modifying statements to the CTE
// - The result of the query cannot be referenced by subsequent queries
//
// NOTE: The name for each CTE arm should be unique, so this shouldn't be used
// multiple times within a single CTE. This restriction could be removed by
// generating unique identifiers.
struct UnreferenceableSubquery<Q>(Q);

impl<Q> QueryFragment<Pg> for UnreferenceableSubquery<Q>
where
    Q: QueryFragment<Pg> + Send + 'static,
{
    fn walk_ast<'a>(
        &'a self,
        mut out: diesel::query_builder::AstPass<'_, 'a, Pg>,
    ) -> diesel::QueryResult<()> {
        out.push_identifier("unused_cte_arm")?;
        Ok(())
    }
}

impl<Q> crate::db::subquery::Subquery for UnreferenceableSubquery<Q>
where
    Q: QueryFragment<Pg> + Send + 'static,
{
    fn query(&self) -> &dyn QueryFragment<Pg> {
        &self.0
    }
}

/// Constructs a CTE for updating resource provisioning information in all
/// collections for a particular object.
#[derive(QueryId)]
pub struct VirtualResourceProvisioningUpdate {
    cte: Cte,
}

impl VirtualResourceProvisioningUpdate {
    // Generic utility for updating all collections including this resource,
    // even transitively.
    //
    // Propagated updates include:
    // - Project
    // - Organization
    // - Silo
    // - Fleet
    //
    // Arguments:
    // - do_update: A boolean SQL query to answer the question: "Should this update
    // be applied"? This query is necessary for idempotency.
    // - update: A SQL query to actually modify the resource record. Generally
    // this is an "INSERT", "UPDATE", or "DELETE".
    // - project_id: The project to which the resource belongs.
    // - values: The updated values to propagate through collections (iff
    // "do_update" evaluates to "true").
    fn apply_update<U, V>(
        do_update: DoUpdate,
        update: U,
        project_id: uuid::Uuid,
        values: V,
    ) -> Self
    where
        U: QueryFragment<Pg> + crate::db::subquery::Subquery + Send + 'static,
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
        let updated_collections =
            UpdatedProvisions::new(&all_collections, &do_update, values);

        // TODO: Do we want to select from "all_collections" instead? Seems more
        // idempotent; it'll work even when we don't update anything...
        let final_select = Box::new(
            updated_collections
                .query_source()
                .select(VirtualResourceProvisioning::as_select()),
        );

        let cte = CteBuilder::new()
            .add_subquery(parent_org)
            .add_subquery(parent_silo)
            .add_subquery(parent_fleet)
            .add_subquery(all_collections)
            .add_subquery(do_update)
            .add_subquery(update)
            .add_subquery(updated_collections)
            .build(final_select);

        Self { cte }
    }

    pub fn new_insert_disk(
        id: uuid::Uuid,
        disk_byte_diff: i64,
        project_id: uuid::Uuid,
    ) -> Self {
        use virtual_resource_provisioned::dsl as resource_dsl;
        use virtual_resource_provisioning::dsl as collection_dsl;

        let mut provision =
            VirtualResourceProvisioned::new(id, ResourceTypeProvisioned::Disk);
        provision.virtual_disk_bytes_provisioned = disk_byte_diff;

        Self::apply_update(
            // We should insert the record if it does not already exist.
            DoUpdate::new_for_insert(id),
            // The query to actually insert the record.
            UnreferenceableSubquery(
                diesel::insert_into(resource_dsl::virtual_resource_provisioned)
                    .values(provision)
                    .on_conflict_do_nothing()
                    .returning(virtual_resource_provisioned::all_columns),
            ),
            // Within this project, org, silo, fleet...
            project_id,
            // ... We add the disk usage.
            collection_dsl::virtual_disk_bytes_provisioned
                .eq(collection_dsl::virtual_disk_bytes_provisioned
                    + disk_byte_diff),
        )
    }

    pub fn new_delete_disk(
        id: uuid::Uuid,
        disk_byte_diff: i64,
        project_id: uuid::Uuid,
    ) -> Self {
        use virtual_resource_provisioned::dsl as resource_dsl;
        use virtual_resource_provisioning::dsl as collection_dsl;

        Self::apply_update(
            // We should delete the record if it exists.
            DoUpdate::new_for_delete(id),
            // The query to actually delete the record.
            UnreferenceableSubquery(
                diesel::delete(resource_dsl::virtual_resource_provisioned)
                    .filter(resource_dsl::id.eq(id))
                    .returning(virtual_resource_provisioned::all_columns),
            ),
            // Within this project, org, silo, fleet...
            project_id,
            // ... We subtract the disk usage.
            collection_dsl::virtual_disk_bytes_provisioned
                .eq(collection_dsl::virtual_disk_bytes_provisioned
                    + disk_byte_diff),
        )
    }

    pub fn new_insert_instance(
        id: uuid::Uuid,
        cpus_diff: i64,
        ram_diff: i64,
        project_id: uuid::Uuid,
    ) -> Self {
        use virtual_resource_provisioned::dsl as resource_dsl;
        use virtual_resource_provisioning::dsl as collection_dsl;

        let mut provision = VirtualResourceProvisioned::new(
            id,
            ResourceTypeProvisioned::Instance,
        );
        provision.cpus_provisioned = cpus_diff;
        provision.ram_provisioned = ram_diff;

        Self::apply_update(
            // We should insert the record if it does not already exist.
            DoUpdate::new_for_insert(id),
            // The query to actually insert the record.
            UnreferenceableSubquery(
                diesel::insert_into(resource_dsl::virtual_resource_provisioned)
                    .values(provision)
                    .on_conflict_do_nothing()
                    .returning(virtual_resource_provisioned::all_columns),
            ),
            // Within this project, org, silo, fleet...
            project_id,
            // ... We update the resource usage.
            (
                collection_dsl::cpus_provisioned
                    .eq(collection_dsl::cpus_provisioned + cpus_diff),
                collection_dsl::ram_provisioned
                    .eq(collection_dsl::ram_provisioned + ram_diff),
            ),
        )
    }

    pub fn new_delete_instance(
        id: uuid::Uuid,
        cpus_diff: i64,
        ram_diff: i64,
        project_id: uuid::Uuid,
    ) -> Self {
        use virtual_resource_provisioned::dsl as resource_dsl;
        use virtual_resource_provisioning::dsl as collection_dsl;

        Self::apply_update(
            // We should delete the record if it exists.
            DoUpdate::new_for_delete(id),
            // The query to actually delete the record.
            UnreferenceableSubquery(
                diesel::delete(resource_dsl::virtual_resource_provisioned)
                    .filter(resource_dsl::id.eq(id))
                    .returning(virtual_resource_provisioned::all_columns),
            ),
            // Within this project, org, silo, fleet...
            project_id,
            // ... We update the resource usage.
            (
                collection_dsl::cpus_provisioned
                    .eq(collection_dsl::cpus_provisioned + cpus_diff),
                collection_dsl::ram_provisioned
                    .eq(collection_dsl::ram_provisioned + ram_diff),
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
