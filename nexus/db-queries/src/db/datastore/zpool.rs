// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Zpool`]s.

use super::DataStore;
use super::SQL_BATCH_SIZE;
use crate::authz;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::OpContext;
use crate::db::identity::Asset;
use crate::db::model::DbTypedUuid;
use crate::db::model::PhysicalDisk;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::PhysicalDiskState;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::upsert::excluded;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::PhysicalDiskKind;
use nexus_db_model::to_db_typed_uuid;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::SledUuid;
use omicron_uuid_kinds::ZpoolKind;
use omicron_uuid_kinds::ZpoolUuid;
use uuid::Uuid;

impl DataStore {
    pub async fn zpool_insert(
        &self,
        opctx: &OpContext,
        zpool: Zpool,
    ) -> CreateResult<Zpool> {
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        let zpool =
            Self::zpool_insert_on_connection(&conn, opctx, zpool).await?;
        Ok(zpool)
    }

    /// Stores a new zpool in the database.
    pub async fn zpool_insert_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        zpool: Zpool,
    ) -> Result<Zpool, TransactionError<Error>> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use nexus_db_schema::schema::zpool::dsl;

        let sled_id = zpool.sled_id();
        let pool = Sled::insert_resource(
            sled_id.into(),
            diesel::insert_into(dsl::zpool)
                .values(zpool.clone())
                .on_conflict(dsl::id)
                .do_update()
                .set((
                    dsl::time_modified.eq(Utc::now()),
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                )),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::by_id(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::Zpool,
                    &zpool.id().to_string(),
                ),
            ),
        })?;

        Ok(pool)
    }

    /// Fetches a page of the list of all zpools on U.2 disks in all sleds
    async fn zpool_list_all_external(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, DbTypedUuid<ZpoolKind>>,
    ) -> ListResultVec<(Zpool, PhysicalDisk)> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use nexus_db_schema::schema::physical_disk::dsl as dsl_physical_disk;
        use nexus_db_schema::schema::zpool::dsl as dsl_zpool;
        paginated(dsl_zpool::zpool, dsl_zpool::id, pagparams)
            .filter(dsl_zpool::time_deleted.is_null())
            .inner_join(
                nexus_db_schema::schema::physical_disk::table.on(
                    dsl_zpool::physical_disk_id.eq(dsl_physical_disk::id).and(
                        dsl_physical_disk::variant.eq(PhysicalDiskKind::U2),
                    ),
                ),
            )
            .select((Zpool::as_select(), PhysicalDisk::as_select()))
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List all zpools on U.2 disks in all sleds, making as many queries as
    /// needed to get them all
    ///
    /// This should generally not be used in API handlers or other
    /// latency-sensitive contexts, but it can make sense in saga actions or
    /// background tasks.
    pub async fn zpool_list_all_external_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<(Zpool, PhysicalDisk)> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        opctx.check_complex_operations_allowed()?;
        let mut zpools = Vec::new();
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        while let Some(p) = paginator.next() {
            let batch = self
                .zpool_list_all_external(opctx, &p.current_pagparams())
                .await?;
            paginator = p.found_batch(&batch, &|(z, _)| z.id().into());
            zpools.extend(batch);
        }

        Ok(zpools)
    }

    /// Returns all (non-deleted) zpools on decommissioned (or deleted) disks
    pub async fn zpool_on_decommissioned_disk_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, DbTypedUuid<ZpoolKind>>,
    ) -> ListResultVec<Zpool> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::zpool::dsl as zpool_dsl;

        paginated(zpool_dsl::zpool, zpool_dsl::id, pagparams)
            .filter(zpool_dsl::time_deleted.is_null())
            // Note the LEFT JOIN here -- we want to see zpools where the
            // physical disk has been deleted too.
            .left_join(
                physical_disk_dsl::physical_disk
                    .on(physical_disk_dsl::id.eq(zpool_dsl::physical_disk_id)),
            )
            .filter(
                // The physical disk has been either explicitly decommissioned,
                // or has been deleted altogether.
                physical_disk_dsl::disk_state
                    .eq(PhysicalDiskState::Decommissioned)
                    .or(physical_disk_dsl::id.is_null())
                    .or(
                        // NOTE: We should probably get rid of this altogether
                        // (it's kinda implied by "Decommissioned", being a
                        // terminal state) but this is an extra cautious
                        // statement.
                        physical_disk_dsl::time_deleted.is_not_null(),
                    ),
            )
            .select(Zpool::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Soft-deletes the Zpools and cleans up all associated DB resources.
    ///
    /// This should only be called for zpools on physical disks which
    /// have been decommissioned.
    ///
    /// In order:
    /// - Finds all datasets within the zpool
    /// - Ensures that no regions nor region snapshots are using these datasets
    /// - Soft-deletes the datasets within the zpool
    /// - Soft-deletes the zpool itself
    pub async fn zpool_delete_self_and_all_datasets(
        &self,
        opctx: &OpContext,
        zpool_id: ZpoolUuid,
    ) -> DeleteResult {
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        Self::zpool_delete_self_and_all_datasets_on_connection(
            &conn, opctx, zpool_id,
        )
        .await
    }

    /// See: [Self::zpool_delete_self_and_all_datasets]
    pub(crate) async fn zpool_delete_self_and_all_datasets_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        opctx: &OpContext,
        zpool_id: ZpoolUuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let now = Utc::now();
        use nexus_db_schema::schema::crucible_dataset::dsl as dataset_dsl;
        use nexus_db_schema::schema::zpool::dsl as zpool_dsl;

        let zpool_id = *zpool_id.as_untyped_uuid();

        // Get the IDs of all datasets to-be-deleted
        let dataset_ids: Vec<Uuid> = dataset_dsl::crucible_dataset
            .filter(dataset_dsl::time_deleted.is_null())
            .filter(dataset_dsl::pool_id.eq(zpool_id))
            .select(dataset_dsl::id)
            .load_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Verify that there are no regions nor region snapshots using this
        // dataset
        use nexus_db_schema::schema::region::dsl as region_dsl;
        let region_count = region_dsl::region
            .filter(region_dsl::dataset_id.eq_any(dataset_ids.clone()))
            .count()
            .first_async::<i64>(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if region_count > 0 {
            return Err(Error::unavail(&format!(
                "Cannot delete this zpool; it has {region_count} regions"
            )));
        }

        use nexus_db_schema::schema::region_snapshot::dsl as region_snapshot_dsl;
        let region_snapshot_count = region_snapshot_dsl::region_snapshot
            .filter(region_snapshot_dsl::dataset_id.eq_any(dataset_ids))
            .count()
            .first_async::<i64>(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        if region_snapshot_count > 0 {
            return Err(Error::unavail(&format!(
                "Cannot delete this zpool; it has {region_snapshot_count} \
                region snapshots"
            )));
        }

        // Ensure the crucible datasets are deleted
        diesel::update(dataset_dsl::crucible_dataset)
            .filter(dataset_dsl::time_deleted.is_null())
            .filter(dataset_dsl::pool_id.eq(zpool_id))
            .set(dataset_dsl::time_deleted.eq(now))
            .execute_async(conn)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // Ensure the zpool is deleted
        diesel::update(zpool_dsl::zpool)
            .filter(zpool_dsl::id.eq(zpool_id))
            .filter(zpool_dsl::time_deleted.is_null())
            .set(zpool_dsl::time_deleted.eq(now))
            .execute_async(conn)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    pub async fn zpool_get_sled_if_in_service(
        &self,
        opctx: &OpContext,
        id: ZpoolUuid,
    ) -> LookupResult<SledUuid> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::zpool::dsl as zpool_dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let id = zpool_dsl::zpool
            .filter(zpool_dsl::id.eq(id.into_untyped_uuid()))
            .filter(zpool_dsl::time_deleted.is_null())
            .inner_join(
                physical_disk_dsl::physical_disk
                    .on(zpool_dsl::physical_disk_id.eq(physical_disk_dsl::id)),
            )
            .filter(
                physical_disk_dsl::disk_policy
                    .eq(PhysicalDiskPolicy::InService),
            )
            .select(zpool_dsl::sled_id)
            .first_async::<Uuid>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::Zpool,
                        LookupType::by_id(id.into_untyped_uuid()),
                    ),
                )
            })?;

        Ok(SledUuid::from_untyped_uuid(id))
    }

    pub async fn zpool_set_control_plane_storage_buffer(
        &self,
        opctx: &OpContext,
        id: ZpoolUuid,
        control_plane_storage_buffer: i64,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::zpool::dsl;

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        info!(
            opctx.log,
            "changing {id} control plane storage buffer to \
            {control_plane_storage_buffer}",
        );

        diesel::update(dsl::zpool)
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .set(
                dsl::control_plane_storage_buffer
                    .eq(control_plane_storage_buffer),
            )
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}
