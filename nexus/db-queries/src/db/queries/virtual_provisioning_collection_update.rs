// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of queries for updating resource provisioning info.

use crate::db::alias::ExpressionAlias;
use crate::db::model::ByteCount;
use crate::db::model::ResourceTypeProvisioned;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::model::VirtualProvisioningResource;
use crate::db::pool::DbConnection;
use crate::db::schema::virtual_provisioning_collection;
use crate::db::schema::virtual_provisioning_resource;
use crate::db::subquery::{AsQuerySource, Cte, CteBuilder, CteQuery};
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::{
    sql_types, CombineDsl, ExpressionMethods, IntoSql,
    NullableExpressionMethods, QueryDsl, RunQueryDsl, SelectableHelper,
};
use nexus_db_model::queries::virtual_provisioning_collection_update::{
    all_collections, do_update, parent_silo,
};

#[derive(Subquery, QueryId)]
#[subquery(name = parent_silo)]
struct ParentSilo {
    query: Box<dyn CteQuery<SqlType = parent_silo::SqlType>>,
}

impl ParentSilo {
    fn new(project_id: uuid::Uuid) -> Self {
        use crate::db::schema::project::dsl;
        Self {
            query: Box::new(
                dsl::project.filter(dsl::id.eq(project_id)).select((
                    ExpressionAlias::new::<parent_silo::dsl::id>(dsl::silo_id),
                )),
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
        parent_silo: &ParentSilo,
        fleet_id: uuid::Uuid,
    ) -> Self {
        let project_id = project_id.into_sql::<sql_types::Uuid>();
        let fleet_id = fleet_id.into_sql::<sql_types::Uuid>();
        Self {
            query: Box::new(
                diesel::select((ExpressionAlias::new::<
                    all_collections::dsl::id,
                >(project_id),))
                .union(parent_silo.query_source().select((
                    ExpressionAlias::new::<all_collections::dsl::id>(
                        parent_silo::id,
                    ),
                )))
                .union(diesel::select((ExpressionAlias::new::<
                    all_collections::dsl::id,
                >(fleet_id),))),
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
        use virtual_provisioning_resource::dsl;

        let not_allocted = dsl::virtual_provisioning_resource
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
        use virtual_provisioning_resource::dsl;

        let already_allocated = dsl::virtual_provisioning_resource
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
#[subquery(name = virtual_provisioning_collection)]
struct UpdatedProvisions {
    query:
        Box<dyn CteQuery<SqlType = virtual_provisioning_collection::SqlType>>,
}

impl UpdatedProvisions {
    fn new<V>(
        all_collections: &AllCollections,
        do_update: &DoUpdate,
        values: V,
    ) -> Self
    where
        V: diesel::AsChangeset<Target = virtual_provisioning_collection::table>,
        <V as diesel::AsChangeset>::Changeset:
            QueryFragment<Pg> + Send + 'static,
    {
        use virtual_provisioning_collection::dsl;

        Self {
            query: Box::new(
                diesel::update(dsl::virtual_provisioning_collection)
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
                    .returning(virtual_provisioning_collection::all_columns),
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
pub struct VirtualProvisioningCollectionUpdate {
    cte: Cte,
}

impl VirtualProvisioningCollectionUpdate {
    // Generic utility for updating all collections including this resource,
    // even transitively.
    //
    // Propagated updates include:
    // - Project
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
        V: diesel::AsChangeset<Target = virtual_provisioning_collection::table>,
        <V as diesel::AsChangeset>::Changeset:
            QueryFragment<Pg> + Send + 'static,
    {
        let parent_silo = ParentSilo::new(project_id);
        let all_collections = AllCollections::new(
            project_id,
            &parent_silo,
            *crate::db::fixed_data::FLEET_ID,
        );
        let updated_collections =
            UpdatedProvisions::new(&all_collections, &do_update, values);

        // TODO: Do we want to select from "all_collections" instead? Seems more
        // idempotent; it'll work even when we don't update anything...
        let final_select = Box::new(
            updated_collections
                .query_source()
                .select(VirtualProvisioningCollection::as_select()),
        );

        let cte = CteBuilder::new()
            .add_subquery(parent_silo)
            .add_subquery(all_collections)
            .add_subquery(do_update)
            .add_subquery(update)
            .add_subquery(updated_collections)
            .build(final_select);

        Self { cte }
    }

    pub fn new_insert_storage(
        id: uuid::Uuid,
        disk_byte_diff: ByteCount,
        project_id: uuid::Uuid,
        storage_type: crate::db::datastore::StorageType,
    ) -> Self {
        use virtual_provisioning_collection::dsl as collection_dsl;
        use virtual_provisioning_resource::dsl as resource_dsl;

        let mut provision =
            VirtualProvisioningResource::new(id, storage_type.into());
        provision.virtual_disk_bytes_provisioned = disk_byte_diff;

        Self::apply_update(
            // We should insert the record if it does not already exist.
            DoUpdate::new_for_insert(id),
            // The query to actually insert the record.
            UnreferenceableSubquery(
                diesel::insert_into(
                    resource_dsl::virtual_provisioning_resource,
                )
                .values(provision)
                .on_conflict_do_nothing()
                .returning(virtual_provisioning_resource::all_columns),
            ),
            // Within this project, silo, fleet...
            project_id,
            // ... We add the disk usage.
            (
                collection_dsl::time_modified.eq(diesel::dsl::now),
                collection_dsl::virtual_disk_bytes_provisioned
                    .eq(collection_dsl::virtual_disk_bytes_provisioned
                        + disk_byte_diff),
            ),
        )
    }

    pub fn new_delete_storage(
        id: uuid::Uuid,
        disk_byte_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> Self {
        use virtual_provisioning_collection::dsl as collection_dsl;
        use virtual_provisioning_resource::dsl as resource_dsl;

        Self::apply_update(
            // We should delete the record if it exists.
            DoUpdate::new_for_delete(id),
            // The query to actually delete the record.
            UnreferenceableSubquery(
                diesel::delete(resource_dsl::virtual_provisioning_resource)
                    .filter(resource_dsl::id.eq(id))
                    .returning(virtual_provisioning_resource::all_columns),
            ),
            // Within this project, silo, fleet...
            project_id,
            // ... We subtract the disk usage.
            (
                collection_dsl::time_modified.eq(diesel::dsl::now),
                collection_dsl::virtual_disk_bytes_provisioned
                    .eq(collection_dsl::virtual_disk_bytes_provisioned
                        - disk_byte_diff),
            ),
        )
    }

    pub fn new_insert_instance(
        id: uuid::Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> Self {
        use virtual_provisioning_collection::dsl as collection_dsl;
        use virtual_provisioning_resource::dsl as resource_dsl;

        let mut provision = VirtualProvisioningResource::new(
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
                diesel::insert_into(
                    resource_dsl::virtual_provisioning_resource,
                )
                .values(provision)
                .on_conflict_do_nothing()
                .returning(virtual_provisioning_resource::all_columns),
            ),
            // Within this project, silo, fleet...
            project_id,
            // ... We update the resource usage.
            (
                collection_dsl::time_modified.eq(diesel::dsl::now),
                collection_dsl::cpus_provisioned
                    .eq(collection_dsl::cpus_provisioned + cpus_diff),
                collection_dsl::ram_provisioned
                    .eq(collection_dsl::ram_provisioned + ram_diff),
            ),
        )
    }

    pub fn new_delete_instance(
        id: uuid::Uuid,
        max_instance_gen: i64,
        cpus_diff: i64,
        ram_diff: ByteCount,
        project_id: uuid::Uuid,
    ) -> Self {
        use crate::db::schema::instance::dsl as instance_dsl;
        use virtual_provisioning_collection::dsl as collection_dsl;
        use virtual_provisioning_resource::dsl as resource_dsl;

        Self::apply_update(
            // We should delete the record if it exists.
            DoUpdate::new_for_delete(id),
            // The query to actually delete the record.
            //
            // The filter condition here ensures that the provisioning record is
            // only deleted if the corresponding instance has a generation
            // number less than the supplied `max_instance_gen`. This allows a
            // caller that is about to apply an instance update that will stop
            // the instance and that bears generation G to avoid deleting
            // resources if the instance generation was already advanced to or
            // past G.
            //
            // If the relevant instance ID is not in the database, then some
            // other operation must have ensured the instance was previously
            // stopped (because that's the only way it could have been deleted),
            // and that operation should have cleaned up the resources already,
            // in which case there's nothing to do here.
            //
            // There is an additional "direct" filter on the target resource ID
            // to avoid a full scan of the resource table.
            UnreferenceableSubquery(
                diesel::delete(resource_dsl::virtual_provisioning_resource)
                    .filter(resource_dsl::id.eq(id))
                    .filter(
                        resource_dsl::id.nullable().eq(instance_dsl::instance
                            .filter(instance_dsl::id.eq(id))
                            .filter(
                                instance_dsl::state_generation
                                    .lt(max_instance_gen),
                            )
                            .select(instance_dsl::id)
                            .single_value()),
                    )
                    .returning(virtual_provisioning_resource::all_columns),
            ),
            // Within this project, silo, fleet...
            project_id,
            // ... We update the resource usage.
            (
                collection_dsl::time_modified.eq(diesel::dsl::now),
                collection_dsl::cpus_provisioned
                    .eq(collection_dsl::cpus_provisioned - cpus_diff),
                collection_dsl::ram_provisioned
                    .eq(collection_dsl::ram_provisioned - ram_diff),
            ),
        )
    }
}

impl QueryFragment<Pg> for VirtualProvisioningCollectionUpdate {
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

impl Query for VirtualProvisioningCollectionUpdate {
    type SqlType = SelectableSql<VirtualProvisioningCollection>;
}

impl RunQueryDsl<DbConnection> for VirtualProvisioningCollectionUpdate {}
