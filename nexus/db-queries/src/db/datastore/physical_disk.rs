// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`PhysicalDisk`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::model::ApplySledFilterExt;
use crate::db::model::InvPhysicalDisk;
use crate::db::model::PhysicalDisk;
use crate::db::model::PhysicalDiskKind;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::PhysicalDiskState;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::pagination::paginated;
use crate::db::TransactionError;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_types::deployment::SledFilter;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use uuid::Uuid;

impl DataStore {
    /// Inserts a physical disk and zpool together in a transaction
    pub async fn physical_disk_and_zpool_insert(
        &self,
        opctx: &OpContext,
        disk: PhysicalDisk,
        zpool: Zpool,
    ) -> Result<(), Error> {
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        let err = OptionalError::new();

        self.transaction_retry_wrapper("physical_disk_adoption")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let disk = disk.clone();
                let zpool = zpool.clone();
                async move {
                    // Verify that the sled into which we are inserting the disk
                    // and zpool pair is still in-service.
                    //
                    // Although the "physical_disk_insert" and "zpool_insert"
                    // functions below check that the Sled hasn't been deleted,
                    // they do not currently check that the Sled has not been
                    // expunged.
                    Self::check_sled_in_service_on_connection(
                        &conn,
                        disk.sled_id,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    Self::physical_disk_insert_on_connection(
                        &conn, opctx, disk,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    Self::zpool_insert_on_connection(&conn, opctx, zpool)
                        .await
                        .map_err(|txn_error| txn_error.into_diesel(&err))?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                match err.take() {
                    // A called function performed its own error propagation.
                    Some(txn_error) => txn_error.into(),
                    // The transaction setup/teardown itself encountered a diesel error.
                    None => public_error_from_diesel(e, ErrorHandler::Server),
                }
            })?;
        Ok(())
    }

    /// Stores a new physical disk in the database.
    ///
    /// - If the Vendor, Serial, and Model fields are the same as an existing
    /// row in the table, an error is thrown.
    /// - If the primary key (ID) is the same as an existing row in the table,
    /// an error is thrown.
    pub async fn physical_disk_insert(
        &self,
        opctx: &OpContext,
        disk: PhysicalDisk,
    ) -> CreateResult<PhysicalDisk> {
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        let disk = Self::physical_disk_insert_on_connection(&conn, opctx, disk)
            .await?;
        Ok(disk)
    }

    pub async fn physical_disk_insert_on_connection(
        conn: &async_bb8_diesel::Connection<db::DbConnection>,
        opctx: &OpContext,
        disk: PhysicalDisk,
    ) -> Result<PhysicalDisk, TransactionError<Error>> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::physical_disk::dsl;

        let sled_id = disk.sled_id;
        let disk_in_db = Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::physical_disk).values(disk.clone()),
        )
        .insert_and_get_result_async(conn)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::PhysicalDisk,
                    &disk.id().to_string(),
                ),
            ),
        })?;

        Ok(disk_in_db)
    }

    pub async fn physical_disk_update_policy(
        &self,
        opctx: &OpContext,
        id: Uuid,
        policy: PhysicalDiskPolicy,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::physical_disk::dsl;

        diesel::update(dsl::physical_disk.filter(dsl::id.eq(id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::disk_policy.eq(policy))
            .execute_async(&*self.pool_connection_authorized(&opctx).await?)
            .await
            .map_err(|err| {
                public_error_from_diesel(err, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn physical_disk_update_state(
        &self,
        opctx: &OpContext,
        id: Uuid,
        state: PhysicalDiskState,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::physical_disk::dsl;

        diesel::update(dsl::physical_disk.filter(dsl::id.eq(id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::disk_state.eq(state))
            .execute_async(&*self.pool_connection_authorized(&opctx).await?)
            .await
            .map_err(|err| {
                public_error_from_diesel(err, ErrorHandler::Server)
            })?;
        Ok(())
    }

    /// Returns all physical disks which:
    ///
    /// - Appear on in-service sleds
    /// - Appear in inventory
    /// - Do not have any records of expungement
    ///
    /// If "inventory_collection_id" is not associated with a collection, this
    /// function returns an empty list, rather than failing.
    pub async fn physical_disk_uninitialized_list(
        &self,
        opctx: &OpContext,
        inventory_collection_id: CollectionUuid,
    ) -> ListResultVec<InvPhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use db::schema::inv_physical_disk::dsl as inv_physical_disk_dsl;
        use db::schema::physical_disk::dsl as physical_disk_dsl;
        use db::schema::sled::dsl as sled_dsl;

        sled_dsl::sled
            // If the sled is not in-service, drop the list immediately.
            .filter(sled_dsl::time_deleted.is_null())
            .sled_filter(SledFilter::InService)
            // Look up all inventory physical disks that could match this sled
            .inner_join(
                inv_physical_disk_dsl::inv_physical_disk.on(
                    inv_physical_disk_dsl::inv_collection_id
                        .eq(inventory_collection_id.into_untyped_uuid())
                        .and(inv_physical_disk_dsl::sled_id.eq(sled_dsl::id))
                        .and(
                            inv_physical_disk_dsl::variant
                                .eq(PhysicalDiskKind::U2),
                        ),
                ),
            )
            // Filter out any disks in the inventory for which we have ever had
            // a control plane disk.
            .filter(diesel::dsl::not(diesel::dsl::exists(
                physical_disk_dsl::physical_disk
                    .select(0.into_sql::<diesel::sql_types::Integer>())
                    .filter(physical_disk_dsl::sled_id.eq(sled_dsl::id))
                    .filter(physical_disk_dsl::variant.eq(PhysicalDiskKind::U2))
                    .filter(
                        physical_disk_dsl::vendor
                            .eq(inv_physical_disk_dsl::vendor),
                    )
                    .filter(
                        physical_disk_dsl::model
                            .eq(inv_physical_disk_dsl::model),
                    )
                    .filter(
                        physical_disk_dsl::serial
                            .eq(inv_physical_disk_dsl::serial),
                    ),
            )))
            .select(InvPhysicalDisk::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn physical_disk_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<PhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::physical_disk::dsl;
        paginated(dsl::physical_disk, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(PhysicalDisk::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn sled_list_physical_disks(
        &self,
        opctx: &OpContext,
        sled_id: Uuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<PhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::physical_disk::dsl;
        paginated(dsl::physical_disk, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::sled_id.eq(sled_id))
            .select(PhysicalDisk::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Deletes a disk from the database.
    pub async fn physical_disk_delete(
        &self,
        opctx: &OpContext,
        vendor: String,
        serial: String,
        model: String,
        sled_id: Uuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let now = Utc::now();
        use db::schema::physical_disk::dsl;
        diesel::update(dsl::physical_disk)
            .filter(dsl::vendor.eq(vendor))
            .filter(dsl::serial.eq(serial))
            .filter(dsl::model.eq(model))
            .filter(dsl::sled_id.eq(sled_id))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::datastore::test::{
        sled_baseboard_for_test, sled_system_hardware_for_test,
    };
    use crate::db::datastore::test_utils::datastore_test;
    use crate::db::lookup::LookupPath;
    use crate::db::model::{PhysicalDiskKind, Sled, SledUpdate};
    use dropshot::PaginationOrder;
    use nexus_db_model::Generation;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::ByteCount;
    use omicron_common::disk::DiskIdentity;
    use omicron_test_utils::dev;
    use sled_agent_client::types::DiskVariant;
    use sled_agent_client::types::InventoryDisk;
    use std::net::{Ipv6Addr, SocketAddrV6};
    use std::num::NonZeroU32;

    async fn create_test_sled(db: &DataStore) -> Sled {
        let sled_id = Uuid::new_v4();
        let addr = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);
        let rack_id = Uuid::new_v4();
        let sled_update = SledUpdate::new(
            sled_id,
            addr,
            sled_baseboard_for_test(),
            sled_system_hardware_for_test(),
            rack_id,
            Generation::new(),
        );
        let (sled, _) = db
            .sled_upsert(sled_update)
            .await
            .expect("Could not upsert sled during test prep");
        sled
    }

    fn list_disk_params() -> DataPageParams<'static, Uuid> {
        DataPageParams {
            limit: NonZeroU32::new(32).unwrap(),
            direction: PaginationOrder::Ascending,
            marker: None,
        }
    }

    #[tokio::test]
    async fn physical_disk_insert_same_uuid_collides() {
        let logctx =
            dev::test_setup_log("physical_disk_insert_same_uuid_collides");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;
        let sled_id = sled.id();

        // Insert a disk
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_id,
        );
        let first_observed_disk = datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");
        assert_eq!(disk.id(), first_observed_disk.id());

        // Insert a disk with an identical UUID
        let err = datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect_err("Should have failed upserting disk");

        assert!(
            err.to_string()
                .contains("Object (of type PhysicalDisk) already exists"),
            "{err}"
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_insert_different_disks() {
        let logctx =
            dev::test_setup_log("physical_disk_insert_different_disks");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;
        let sled_id = sled.id();

        // Insert a disk
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_id,
        );
        datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");

        // Insert a second disk
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Noxide"),
            String::from("456"),
            String::from("UnrealDisk"),
            PhysicalDiskKind::M2,
            sled_id,
        );
        datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");

        let pagparams = list_disk_params();
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_id, &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 2);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_deletion_idempotency() {
        let logctx = dev::test_setup_log("physical_disk_deletion_idempotency");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;

        // Insert a disk
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled.id(),
        );
        datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");
        let pagparams = list_disk_params();
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 1);

        // Delete the inserted disk
        datastore
            .physical_disk_delete(
                &opctx,
                disk.vendor.clone(),
                disk.serial.clone(),
                disk.model.clone(),
                disk.sled_id,
            )
            .await
            .expect("Failed to delete disk");
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());

        // Deleting again should not throw an error
        datastore
            .physical_disk_delete(
                &opctx,
                disk.vendor,
                disk.serial,
                disk.model,
                disk.sled_id,
            )
            .await
            .expect("Failed to delete disk");

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // This test simulates the following behavior:
    //
    // - Sled A reports a disk
    // - Disk is detached from Sled A (and the detach is reported to Nexus)
    // - Disk is attached into Sled B
    #[tokio::test]
    async fn physical_disk_insert_delete_reupsert_new_sled() {
        let logctx = dev::test_setup_log(
            "physical_disk_insert_delete_reupsert_new_sled",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // Insert a disk
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_a.id(),
        );
        datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");
        let pagparams = list_disk_params();
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_a.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 1);
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_b.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());

        // Delete the inserted disk
        datastore
            .physical_disk_delete(
                &opctx,
                disk.vendor,
                disk.serial,
                disk.model,
                disk.sled_id,
            )
            .await
            .expect("Failed to delete disk");
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_a.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_b.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());

        // Attach the disk to the second sled
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_b.id(),
        );
        datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed second attempt at upserting disk");

        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_a.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_b.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 1);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // This test simulates the following behavior:
    //
    // - Sled A reports a disk
    // - Sled A is taken offline, and the disk is removed.
    // - While offline, the disk is detached from Sled A (but there's no
    // notification to Nexus).
    // - Disk is attached into Sled B
    #[tokio::test]
    async fn physical_disk_insert_reupsert_new_sled() {
        let logctx =
            dev::test_setup_log("physical_disk_insert_reupsert_new_sled");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // Insert a disk
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_a.id(),
        );
        datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");
        let pagparams = list_disk_params();
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_a.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 1);
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_b.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());

        // Remove the disk from the first sled
        datastore
            .physical_disk_delete(
                &opctx,
                disk.vendor.clone(),
                disk.serial.clone(),
                disk.model.clone(),
                disk.sled_id,
            )
            .await
            .expect("Failed to delete disk");

        // "Report the disk" from the second sled
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_b.id(),
        );
        datastore
            .physical_disk_insert(&opctx, disk.clone())
            .await
            .expect("Failed second attempt at upserting disk");

        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_a.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_b.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 1);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    // Most of this data doesn't matter, but adds a sled
    // to an inventory with a supplied set of disks.
    fn add_sled_to_inventory(
        builder: &mut nexus_inventory::CollectionBuilder,
        sled: &Sled,
        disks: Vec<sled_agent_client::types::InventoryDisk>,
    ) {
        builder
            .found_sled_inventory(
                "fake sled agent",
                sled_agent_client::types::Inventory {
                    baseboard: sled_agent_client::types::Baseboard::Gimlet {
                        identifier: sled.serial_number().to_string(),
                        model: sled.part_number().to_string(),
                        revision: 0,
                    },
                    reservoir_size: ByteCount::from(1024),
                    sled_role: sled_agent_client::types::SledRole::Gimlet,
                    sled_agent_address: "[::1]:56792".parse().unwrap(),
                    sled_id: sled.id(),
                    usable_hardware_threads: 10,
                    usable_physical_ram: ByteCount::from(1024 * 1024),
                    disks,
                    zpools: vec![],
                },
            )
            .unwrap();
    }

    fn create_inv_disk(serial: String, slot: i64) -> InventoryDisk {
        InventoryDisk {
            identity: DiskIdentity {
                serial,
                vendor: "vendor".to_string(),
                model: "model".to_string(),
            },
            variant: DiskVariant::U2,
            slot,
        }
    }

    fn create_disk_zpool_combo(
        sled_id: Uuid,
        inv_disk: &InventoryDisk,
    ) -> (PhysicalDisk, Zpool) {
        let disk = PhysicalDisk::new(
            Uuid::new_v4(),
            inv_disk.identity.vendor.clone(),
            inv_disk.identity.serial.clone(),
            inv_disk.identity.model.clone(),
            PhysicalDiskKind::U2,
            sled_id,
        );

        let zpool = Zpool::new(Uuid::new_v4(), sled_id, disk.id());
        (disk, zpool)
    }

    #[tokio::test]
    async fn physical_disk_cannot_insert_to_expunged_sled() {
        let logctx =
            dev::test_setup_log("physical_disk_cannot_insert_to_expunged_sled");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;

        // We can insert a disk into a sled that is not yet expunged
        let inv_disk = create_inv_disk("serial-001".to_string(), 1);
        let (disk, zpool) = create_disk_zpool_combo(sled.id(), &inv_disk);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk, zpool)
            .await
            .unwrap();

        // Mark the sled as expunged
        let sled_lookup =
            LookupPath::new(&opctx, &datastore).sled_id(sled.id());
        let (authz_sled,) =
            sled_lookup.lookup_for(authz::Action::Modify).await.unwrap();
        datastore
            .sled_set_policy_to_expunged(&opctx, &authz_sled)
            .await
            .unwrap();

        // Now that the sled is expunged, inserting the disk should fail
        let inv_disk = create_inv_disk("serial-002".to_string(), 2);
        let (disk, zpool) = create_disk_zpool_combo(sled.id(), &inv_disk);
        let err = datastore
            .physical_disk_and_zpool_insert(&opctx, disk, zpool)
            .await
            .unwrap_err();

        let expected = format!("Sled {} is not in service", sled.id());
        let actual = err.to_string();
        assert!(
            actual.contains(&expected),
            "Expected string: {expected} within actual error: {actual}",
        );

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_uninitialized_list() {
        let logctx = dev::test_setup_log("physical_disk_uninitialized_list");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // No inventory -> No uninitialized disks
        let uninitialized_disks = datastore
            .physical_disk_uninitialized_list(
                &opctx,
                CollectionUuid::new_v4(), // Collection that does not exist
            )
            .await
            .expect("Failed to look up uninitialized disks");
        assert!(uninitialized_disks.is_empty());

        // Create inventory disks for both sleds
        let mut builder = nexus_inventory::CollectionBuilder::new("test");
        let disks_a = vec![
            create_inv_disk("serial-001".to_string(), 1),
            create_inv_disk("serial-002".to_string(), 2),
            create_inv_disk("serial-003".to_string(), 3),
        ];
        let disks_b = vec![
            create_inv_disk("serial-101".to_string(), 1),
            create_inv_disk("serial-102".to_string(), 2),
            create_inv_disk("serial-103".to_string(), 3),
        ];
        add_sled_to_inventory(&mut builder, &sled_a, disks_a.clone());
        add_sled_to_inventory(&mut builder, &sled_b, disks_b.clone());
        let collection = builder.build();
        let collection_id = collection.id;
        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert collection");

        // Now when we list the uninitialized disks, we should see everything in
        // the inventory.
        let uninitialized_disks = datastore
            .physical_disk_uninitialized_list(&opctx, collection_id)
            .await
            .expect("Failed to list uninitialized disks");
        assert_eq!(uninitialized_disks.len(), 6);

        // Normalize the data a bit -- convert to nexus types, and sort vecs for
        // stability in the comparison.
        let mut uninitialized_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            uninitialized_disks.into_iter().map(|d| d.into()).collect();
        uninitialized_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        let mut expected_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            disks_a
                .iter()
                .map(|d| d.clone().into())
                .chain(disks_b.iter().map(|d| d.clone().into()))
                .collect();
        expected_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        assert_eq!(uninitialized_disks, expected_disks);

        // Let's create control plane objects for some of these disks.
        //
        // They should no longer show up when we list uninitialized devices.
        //
        // This creates disks for: 001, 002, and 101.
        // It leaves the following uninitialized: 003, 102, 103
        let (disk_001, zpool) =
            create_disk_zpool_combo(sled_a.id(), &disks_a[0]);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_001, zpool)
            .await
            .unwrap();
        let (disk_002, zpool) =
            create_disk_zpool_combo(sled_a.id(), &disks_a[1]);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_002, zpool)
            .await
            .unwrap();
        let (disk_101, zpool) =
            create_disk_zpool_combo(sled_b.id(), &disks_b[0]);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_101, zpool)
            .await
            .unwrap();

        let uninitialized_disks = datastore
            .physical_disk_uninitialized_list(&opctx, collection_id)
            .await
            .expect("Failed to list uninitialized disks");
        assert_eq!(uninitialized_disks.len(), 3);

        // Pay careful attention to our indexing below.
        //
        // We're grabbing the last disk of "disks_a" (which still is
        // uninitailized) and the last two disks of "disks_b" (of which both are
        // still uninitialized).
        let mut uninitialized_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            uninitialized_disks.into_iter().map(|d| d.into()).collect();
        uninitialized_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        let mut expected_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            disks_a[2..3]
                .iter()
                .map(|d| d.clone().into())
                .chain(disks_b[1..3].iter().map(|d| d.clone().into()))
                .collect();
        expected_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        assert_eq!(uninitialized_disks, expected_disks);

        // Create physical disks for all remaining devices.
        //
        // Observe no remaining uninitialized disks.
        let (disk_003, zpool) =
            create_disk_zpool_combo(sled_a.id(), &disks_a[2]);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_003.clone(), zpool)
            .await
            .unwrap();
        let (disk_102, zpool) =
            create_disk_zpool_combo(sled_b.id(), &disks_b[1]);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_102.clone(), zpool)
            .await
            .unwrap();
        let (disk_103, zpool) =
            create_disk_zpool_combo(sled_b.id(), &disks_b[2]);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_103.clone(), zpool)
            .await
            .unwrap();

        let uninitialized_disks = datastore
            .physical_disk_uninitialized_list(&opctx, collection_id)
            .await
            .expect("Failed to list uninitialized disks");
        assert_eq!(uninitialized_disks.len(), 0);

        // Expunge some disks, observe that they do not re-appear as
        // initialized.
        use db::schema::physical_disk::dsl;

        // Set a disk to "deleted".
        let now = Utc::now();
        diesel::update(dsl::physical_disk)
            .filter(dsl::id.eq(disk_003.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(
                &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
            )
            .await
            .unwrap();

        // Set another disk to "expunged"
        diesel::update(dsl::physical_disk)
            .filter(dsl::id.eq(disk_102.id()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::disk_policy.eq(PhysicalDiskPolicy::Expunged))
            .execute_async(
                &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
            )
            .await
            .unwrap();

        // The set of uninitialized disks should remain at zero
        let uninitialized_disks = datastore
            .physical_disk_uninitialized_list(&opctx, collection_id)
            .await
            .expect("Failed to list uninitialized disks");
        assert_eq!(uninitialized_disks.len(), 0);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }
}
