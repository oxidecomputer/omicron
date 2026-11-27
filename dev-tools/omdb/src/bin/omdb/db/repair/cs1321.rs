// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::check_allow_destructive::DestructiveOperationToken;
use crate::db::repair::CorruptedStateInfo;
use crate::db::repair::DatabaseCorruption;
use crate::db::repair::common;
use anyhow::Result;
use async_bb8_diesel::AsyncConnection;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::AsyncSimpleConnection;
use diesel::prelude::*;
use nexus_db_lookup::DbConnection;
use nexus_db_model::LocalStorageUnencryptedDatasetAllocation;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use nexus_db_queries::db::datastore;
use nexus_db_queries::db::datastore::LocalStorageDisk;
use nexus_db_queries::db::queries::ALLOW_FULL_TABLE_SCAN_SQL;
use slog::Logger;
use slog::o;
use std::sync::Arc;
use uuid::Uuid;

/// customer-support#1321 resulted from an error in the local storage allocation
/// query, where it was not properly checking if the higher level disk was
/// deleted before allocating local storage for it. In a race between instance
/// reservation and disk deletion, this would assign a non-deleted local storage
/// allocation to a deleted disk.
pub struct ZombieLocalStorageAllocations {
    datastore: Arc<DataStore>,
    log: Logger,
}

impl ZombieLocalStorageAllocations {
    /// Find all deleted disks backed by local storage where the unencrypted
    /// dataset allocation remains undeleted.
    pub async fn find_disks_with_zombie_local_storage_allocations(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
    ) -> Result<Vec<Uuid>> {
        // Find all deleted disks that point to a local storage unencrypted
        // dataset allocation that is not deleted.

        let zombie_disk_ids: Vec<Uuid> = conn.transaction_async(async move |conn| {
            conn.batch_execute_async(ALLOW_FULL_TABLE_SCAN_SQL).await?;

            use nexus_db_schema::schema::disk::dsl;
            use nexus_db_schema::schema::disk_type_local_storage::dsl as dtls_dsl;
            use nexus_db_schema::schema::local_storage_unencrypted_dataset_allocation::dsl as lsuda_dsl;

            dsl::disk
                .inner_join(dtls_dsl::disk_type_local_storage.on(
                    dsl::id.eq(dtls_dsl::disk_id)
                ))
                .inner_join(lsuda_dsl::local_storage_unencrypted_dataset_allocation.on(
                    dtls_dsl::local_storage_unencrypted_dataset_allocation_id
                        .eq(lsuda_dsl::id.nullable())
                ))
                .filter(lsuda_dsl::time_deleted.is_null())
                .filter(dsl::time_deleted.is_not_null())
                .select(dsl::id)
                .load_async(&conn)
                .await
        })
        .await?;

        Ok(zombie_disk_ids)
    }

    pub async fn get_allocation_for_deleted_disk(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        disk_id: Uuid,
    ) -> Result<LocalStorageUnencryptedDatasetAllocation> {
        let opctx =
            OpContext::for_tests(self.log.clone(), self.datastore.clone());

        // Note: we cannot use LookupPath for deleted disks as it will return a
        // 404. Go directly to the table instead.
        let disk: nexus_db_model::Disk = {
            use nexus_db_schema::schema::disk::dsl;

            dsl::disk
                .filter(dsl::id.eq(disk_id))
                .select(nexus_db_model::Disk::as_select())
                .get_result_async(conn)
                .await?
        };

        let datastore::Disk::LocalStorage(LocalStorageDisk {
            disk: _,
            disk_type_local_storage: _,
            local_storage_dataset_allocation,
        }) = self.datastore.disk_get_with_model(&opctx, disk).await?
        else {
            panic!("should be local storage disk");
        };

        let local_storage_dataset_allocation =
            local_storage_dataset_allocation.unwrap();

        let datastore::LocalStorageAllocation::Unencrypted(allocation) =
            local_storage_dataset_allocation
        else {
            panic!("only unencrypted allocation created");
        };

        Ok(allocation)
    }

    pub async fn garbage_collect_auto(
        &self,
        _: DestructiveOperationToken,
    ) -> Result<()> {
        let conn = self.datastore.pool_connection_for_tests().await?;

        let disk_with_zombie_allocations = self
            .find_disks_with_zombie_local_storage_allocations(&conn)
            .await?;

        for disk_id in disk_with_zombie_allocations {
            let allocation =
                self.get_allocation_for_deleted_disk(&conn, disk_id).await?;

            eprintln!(
                "disk {disk_id} has a zombie allocation {}",
                allocation.id(),
            );

            common::remove_local_storage_allocation(
                &self.log,
                &conn,
                &allocation,
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl DatabaseCorruption for ZombieLocalStorageAllocations {
    async fn new(datastore: Arc<DataStore>, log: &Logger) -> Self {
        ZombieLocalStorageAllocations {
            datastore,
            log: log.new(o!("check" => "zombie_local_storage_allocations")),
        }
    }

    async fn detect(&self) -> Result<Vec<CorruptedStateInfo>> {
        let conn = self.datastore.pool_connection_for_tests().await?;

        let disk_with_zombie_allocations = self
            .find_disks_with_zombie_local_storage_allocations(&conn)
            .await?;

        let mut results = vec![];

        for disk_id in disk_with_zombie_allocations {
            let allocation =
                self.get_allocation_for_deleted_disk(&conn, disk_id).await?;

            results.push(CorruptedStateInfo {
                message: format!(
                    "disk {disk_id} has a zombie allocation {}",
                    allocation.id(),
                ),
            });
        }

        Ok(results)
    }
}
