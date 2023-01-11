// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`VirtualProvisioningCollection`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::ByteCount;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::pool::DbConnection;
use crate::db::queries::virtual_provisioning_collection_update::VirtualProvisioningCollectionUpdate;
use async_bb8_diesel::{AsyncRunQueryDsl, PoolError};
use diesel::prelude::*;
use omicron_common::api::external::{DeleteResult, Error};
use uuid::Uuid;

/// The types of resources which can consume storage space.
pub enum StorageType {
    Disk,
    Snapshot,
}

impl From<StorageType> for crate::db::model::ResourceTypeProvisioned {
    fn from(
        storage_type: StorageType,
    ) -> crate::db::model::ResourceTypeProvisioned {
        match storage_type {
            StorageType::Disk => {
                crate::db::model::ResourceTypeProvisioned::Disk
            }
            StorageType::Snapshot => {
                crate::db::model::ResourceTypeProvisioned::Snapshot
            }
        }
    }
}

impl DataStore {
    /// Create a [`VirtualProvisioningCollection`] object.
    pub async fn virtual_provisioning_collection_create(
        &self,
        opctx: &OpContext,
        virtual_provisioning_collection: VirtualProvisioningCollection,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let pool = self.pool_authorized(opctx).await?;
        self.virtual_provisioning_collection_create_on_connection(
            pool,
            virtual_provisioning_collection,
        )
        .await
    }

    pub(crate) async fn virtual_provisioning_collection_create_on_connection<
        ConnErr,
    >(
        &self,
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection, ConnErr>
              + Sync),
        virtual_provisioning_collection: VirtualProvisioningCollection,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error>
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::schema::virtual_provisioning_collection::dsl;

        let provisions: Vec<VirtualProvisioningCollection> =
            diesel::insert_into(dsl::virtual_provisioning_collection)
                .values(virtual_provisioning_collection)
                .on_conflict_do_nothing()
                .get_results_async(conn)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(
                        PoolError::from(e),
                        ErrorHandler::Server,
                    )
                })?;
        self.virtual_provisioning_collection_producer
            .append_all_metrics(&provisions);
        Ok(provisions)
    }

    pub async fn virtual_provisioning_collection_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<VirtualProvisioningCollection, Error> {
        use db::schema::virtual_provisioning_collection::dsl;

        let virtual_provisioning_collection =
            dsl::virtual_provisioning_collection
                .find(id)
                .select(VirtualProvisioningCollection::as_select())
                .get_result_async(self.pool_authorized(opctx).await?)
                .await
                .map_err(|e| {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                })?;
        Ok(virtual_provisioning_collection)
    }

    /// Delete a [`VirtualProvisioningCollection`] object.
    pub async fn virtual_provisioning_collection_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> DeleteResult {
        let pool = self.pool_authorized(opctx).await?;
        self.virtual_provisioning_collection_delete_on_connection(pool, id)
            .await
    }

    /// Delete a [`VirtualProvisioningCollection`] object.
    pub(crate) async fn virtual_provisioning_collection_delete_on_connection<
        ConnErr,
    >(
        &self,
        conn: &(impl async_bb8_diesel::AsyncConnection<DbConnection, ConnErr>
              + Sync),
        id: Uuid,
    ) -> DeleteResult
    where
        ConnErr: From<diesel::result::Error> + Send + 'static,
        PoolError: From<ConnErr>,
    {
        use db::schema::virtual_provisioning_collection::dsl;

        // NOTE: We don't really need to extract the value we're deleting from
        // the DB, but by doing so, we can validate that we haven't
        // miscalculated our usage accounting.
        let collection = diesel::delete(dsl::virtual_provisioning_collection)
            .filter(dsl::id.eq(id))
            .returning(VirtualProvisioningCollection::as_select())
            .get_result_async(conn)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    PoolError::from(e),
                    ErrorHandler::Server,
                )
            })?;
        assert!(
            collection.is_empty(),
            "Collection deleted while non-empty: {collection:?}"
        );
        Ok(())
    }

    // TODO: These could 100% act on model types:
    // - Would help with identifying UUID
    // - Would help with project ID lookup
    // - Would help with calculating resource usage
    //
    // I think we just need to validate that the model exists when we make these
    // calls? Maybe it could be an optional helper?

    pub async fn virtual_provisioning_collection_insert_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_insert_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
            StorageType::Disk,
        )
        .await
    }

    pub async fn virtual_provisioning_collection_insert_snapshot(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_insert_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
            StorageType::Snapshot,
        )
        .await
    }

    /// Transitively updates all provisioned disk provisions from project -> fleet.
    async fn virtual_provisioning_collection_insert_storage(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
        storage_type: StorageType,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_insert_storage(
                id,
                disk_byte_diff,
                project_id,
                storage_type,
            )
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        self.virtual_provisioning_collection_producer
            .append_disk_metrics(&provisions);
        Ok(provisions)
    }

    pub async fn virtual_provisioning_collection_delete_disk(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_delete_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
        )
        .await
    }

    pub async fn virtual_provisioning_collection_delete_snapshot(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        self.virtual_provisioning_collection_delete_storage(
            opctx,
            id,
            project_id,
            disk_byte_diff,
        )
        .await
    }

    // Transitively updates all provisioned disk provisions from project -> fleet.
    async fn virtual_provisioning_collection_delete_storage(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        disk_byte_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_delete_storage(
                id,
                disk_byte_diff,
                project_id,
            )
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        self.virtual_provisioning_collection_producer
            .append_disk_metrics(&provisions);
        Ok(provisions)
    }

    /// Transitively updates all CPU/RAM provisions from project -> fleet.
    pub async fn virtual_provisioning_collection_insert_instance(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_insert_instance(
                id, cpus_diff, ram_diff, project_id,
            )
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        self.virtual_provisioning_collection_producer
            .append_cpu_metrics(&provisions);
        Ok(provisions)
    }

    /// Transitively updates all CPU/RAM provisions from project -> fleet.
    pub async fn virtual_provisioning_collection_delete_instance(
        &self,
        opctx: &OpContext,
        id: Uuid,
        project_id: Uuid,
        cpus_diff: i64,
        ram_diff: ByteCount,
    ) -> Result<Vec<VirtualProvisioningCollection>, Error> {
        let provisions =
            VirtualProvisioningCollectionUpdate::new_delete_instance(
                id, cpus_diff, ram_diff, project_id,
            )
            .get_results_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        self.virtual_provisioning_collection_producer
            .append_cpu_metrics(&provisions);
        Ok(provisions)
    }
}
