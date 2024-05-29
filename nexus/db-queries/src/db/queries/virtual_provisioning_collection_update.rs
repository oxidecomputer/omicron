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
use crate::db::true_or_cast_error::matches_sentinel;
use crate::db::true_or_cast_error::TrueOrCastError;
use db_macros::Subquery;
use diesel::pg::Pg;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::result::Error as DieselError;
use diesel::{
    sql_types, BoolExpressionMethods, CombineDsl, ExpressionMethods, IntoSql,
    JoinOnDsl, NullableExpressionMethods, QueryDsl, RunQueryDsl,
    SelectableHelper,
};
use nexus_db_model::queries::virtual_provisioning_collection_update::{
    all_collections, do_update, parent_silo, quotas, silo_provisioned,
};
use omicron_common::api::external;
use omicron_common::api::external::MessagePair;

const NOT_ENOUGH_CPUS_SENTINEL: &'static str = "Not enough cpus";
const NOT_ENOUGH_MEMORY_SENTINEL: &'static str = "Not enough memory";
const NOT_ENOUGH_STORAGE_SENTINEL: &'static str = "Not enough storage";

/// Translates a generic pool error to an external error based
/// on messages which may be emitted when provisioning virtual resources
/// such as instances and disks.
pub fn from_diesel(e: DieselError) -> external::Error {
    use crate::db::error;

    let sentinels = [
        NOT_ENOUGH_CPUS_SENTINEL,
        NOT_ENOUGH_MEMORY_SENTINEL,
        NOT_ENOUGH_STORAGE_SENTINEL,
    ];
    if let Some(sentinel) = matches_sentinel(&e, &sentinels) {
        match sentinel {
            NOT_ENOUGH_CPUS_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                         "vCPU Limit Exceeded: Not enough vCPUs to complete request. Either stop unused instances to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                         "User tried to allocate an instance but the virtual provisioning resource table indicated that there were not enough CPUs available to satisfy the request.".to_string(),
                    )
                }
            }
            NOT_ENOUGH_MEMORY_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                         "Memory Limit Exceeded: Not enough memory to complete request. Either stop unused instances to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                         "User tried to allocate an instance but the virtual provisioning resource table indicated that there were not enough RAM available to satisfy the request.".to_string(),
                    )
                }
            }
            NOT_ENOUGH_STORAGE_SENTINEL => {
                return external::Error::InsufficientCapacity {
                    message: MessagePair::new_full(
                         "Storage Limit Exceeded: Not enough storage to complete request. Either remove unneeded disks and snapshots to free up resources or contact the rack operator to request a capacity increase.".to_string(),
                         "User tried to allocate a disk or snapshot but the virtual provisioning resource table indicated that there were not enough storage available to satisfy the request.".to_string(),
                    )
                }
            }
            _ => {}
        }
    }
    error::public_error_from_diesel(e, error::ErrorHandler::Server)
}

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
    fn new_for_insert(
        silo_provisioned: &SiloProvisioned,
        quotas: &Quotas,
        resource: VirtualProvisioningResource,
    ) -> Self {
        use virtual_provisioning_resource::dsl;

        let cpus_provisioned_delta =
            resource.cpus_provisioned.into_sql::<sql_types::BigInt>();
        let memory_provisioned_delta =
            i64::from(resource.ram_provisioned).into_sql::<sql_types::BigInt>();
        let storage_provisioned_delta =
            i64::from(resource.virtual_disk_bytes_provisioned)
                .into_sql::<sql_types::BigInt>();

        let not_allocted = dsl::virtual_provisioning_resource
            .find(resource.id)
            .count()
            .single_value()
            .assume_not_null()
            .eq(0);

        let has_sufficient_cpus = quotas
            .query_source()
            .select(quotas::cpus)
            .single_value()
            .assume_not_null()
            .ge(silo_provisioned
                .query_source()
                .select(silo_provisioned::cpus_provisioned)
                .single_value()
                .assume_not_null()
                + cpus_provisioned_delta);

        let has_sufficient_memory = quotas
            .query_source()
            .select(quotas::memory)
            .single_value()
            .assume_not_null()
            .ge(silo_provisioned
                .query_source()
                .select(silo_provisioned::ram_provisioned)
                .single_value()
                .assume_not_null()
                + memory_provisioned_delta);

        let has_sufficient_storage = quotas
            .query_source()
            .select(quotas::storage)
            .single_value()
            .assume_not_null()
            .ge(silo_provisioned
                .query_source()
                .select(silo_provisioned::virtual_disk_bytes_provisioned)
                .single_value()
                .assume_not_null()
                + storage_provisioned_delta);

        Self {
            query: Box::new(diesel::select((ExpressionAlias::new::<
                do_update::update,
            >(
                not_allocted
                    .and(TrueOrCastError::new(
                        cpus_provisioned_delta.eq(0).or(has_sufficient_cpus),
                        NOT_ENOUGH_CPUS_SENTINEL,
                    ))
                    .and(TrueOrCastError::new(
                        memory_provisioned_delta
                            .eq(0)
                            .or(has_sufficient_memory),
                        NOT_ENOUGH_MEMORY_SENTINEL,
                    ))
                    .and(TrueOrCastError::new(
                        storage_provisioned_delta
                            .eq(0)
                            .or(has_sufficient_storage),
                        NOT_ENOUGH_STORAGE_SENTINEL,
                    )),
            ),))),
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

#[derive(Subquery, QueryId)]
#[subquery(name = quotas)]
struct Quotas {
    query: Box<dyn CteQuery<SqlType = quotas::SqlType>>,
}

impl Quotas {
    // TODO: We could potentially skip this in cases where we know we're removing a resource instead of inserting
    fn new(parent_silo: &ParentSilo) -> Self {
        use crate::db::schema::silo_quotas::dsl;
        Self {
            query: Box::new(
                dsl::silo_quotas
                    .inner_join(
                        parent_silo
                            .query_source()
                            .on(dsl::silo_id.eq(parent_silo::id)),
                    )
                    .select((
                        dsl::silo_id,
                        dsl::cpus,
                        ExpressionAlias::new::<quotas::dsl::memory>(
                            dsl::memory_bytes,
                        ),
                        ExpressionAlias::new::<quotas::dsl::storage>(
                            dsl::storage_bytes,
                        ),
                    )),
            ),
        }
    }
}

#[derive(Subquery, QueryId)]
#[subquery(name = silo_provisioned)]
struct SiloProvisioned {
    query: Box<dyn CteQuery<SqlType = silo_provisioned::SqlType>>,
}

impl SiloProvisioned {
    fn new(parent_silo: &ParentSilo) -> Self {
        use virtual_provisioning_collection::dsl;
        Self {
            query: Box::new(
                dsl::virtual_provisioning_collection
                    .inner_join(
                        parent_silo
                            .query_source()
                            .on(dsl::id.eq(parent_silo::id)),
                    )
                    .select((
                        dsl::id,
                        dsl::cpus_provisioned,
                        dsl::ram_provisioned,
                        dsl::virtual_disk_bytes_provisioned,
                    )),
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

/// The virtual resource collection is only updated when a resource is inserted
/// or deleted from the resource provisioning table. By probing for the presence
/// or absence of a resource, we can update collections at the same time as we
/// create or destroy the resource, which helps make the operation idempotent.
enum UpdateKind {
    Insert(VirtualProvisioningResource),
    Delete(uuid::Uuid),
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
        update_kind: UpdateKind,
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

        let quotas = Quotas::new(&parent_silo);
        let silo_provisioned = SiloProvisioned::new(&parent_silo);

        let do_update = match update_kind {
            UpdateKind::Insert(resource) => {
                DoUpdate::new_for_insert(&silo_provisioned, &quotas, resource)
            }
            UpdateKind::Delete(id) => DoUpdate::new_for_delete(id),
        };

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
            .add_subquery(quotas)
            .add_subquery(silo_provisioned)
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
            UpdateKind::Insert(provision.clone()),
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
            UpdateKind::Delete(id),
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
            UpdateKind::Insert(provision.clone()),
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
            UpdateKind::Delete(id),
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::raw_query_builder::expectorate_query_contents;
    use uuid::Uuid;

    // These tests are a bit of a "change detector", but they're here to help
    // with debugging too. If you change this query, it can be useful to see
    // exactly how the output SQL has been altered.

    #[tokio::test]
    async fn expectorate_query_insert_storage() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let disk_byte_diff = 2048.try_into().unwrap();
        let storage_type = crate::db::datastore::StorageType::Disk;

        let query = VirtualProvisioningCollectionUpdate::new_insert_storage(
            id,
            disk_byte_diff,
            project_id,
            storage_type,
        );
        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_insert_storage.sql",
        ).await;
    }

    #[tokio::test]
    async fn expectorate_query_delete_storage() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let disk_byte_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_delete_storage(
            id,
            disk_byte_diff,
            project_id,
        );

        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_delete_storage.sql",
        ).await;
    }

    #[tokio::test]
    async fn expectorate_query_insert_instance() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 4;
        let ram_diff = 2048.try_into().unwrap();

        let query = VirtualProvisioningCollectionUpdate::new_insert_instance(
            id, cpus_diff, ram_diff, project_id,
        );

        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_insert_instance.sql",
        ).await;
    }

    #[tokio::test]
    async fn expectorate_query_delete_instance() {
        let id = Uuid::nil();
        let project_id = Uuid::nil();
        let cpus_diff = 4;
        let ram_diff = 2048.try_into().unwrap();
        let max_instance_gen = 0;

        let query = VirtualProvisioningCollectionUpdate::new_delete_instance(
            id,
            max_instance_gen,
            cpus_diff,
            ram_diff,
            project_id,
        );

        expectorate_query_contents(
            &query,
            "tests/output/virtual_provisioning_collection_update_delete_instance.sql",
        ).await;
    }
}
