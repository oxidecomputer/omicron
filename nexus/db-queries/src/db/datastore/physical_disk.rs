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
use crate::db::model::PhysicalDisk;
use crate::db::model::Sled;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::upsert::{excluded, on_constraint};
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use uuid::Uuid;

impl DataStore {
    /// Stores a new physical disk in the database.
    ///
    /// - If the Vendor, Serial, and Model fields are the same as an existing
    /// row in the table, the following fields may be updated:
    ///   - Sled ID
    ///   - Time Deleted
    ///   - Time Modified
    /// - If the primary key (ID) is the same as an existing row in the table,
    /// an error is thrown.
    pub async fn physical_disk_upsert(
        &self,
        opctx: &OpContext,
        disk: PhysicalDisk,
    ) -> CreateResult<PhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use db::schema::physical_disk::dsl;

        let now = Utc::now();
        let sled_id = disk.sled_id;
        let disk_in_db = Sled::insert_resource(
            sled_id,
            diesel::insert_into(dsl::physical_disk)
                .values(disk.clone())
                .on_conflict(on_constraint("vendor_serial_model_unique"))
                .do_update()
                .set((
                    dsl::sled_id.eq(excluded(dsl::sled_id)),
                    dsl::time_deleted.eq(Option::<DateTime<Utc>>::None),
                    dsl::time_modified.eq(now),
                )),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(&opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Sled,
                lookup_type: LookupType::ById(sled_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
        })?;

        Ok(disk_in_db)
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
    use crate::db::model::{PhysicalDiskKind, Sled, SledUpdate};
    use dropshot::PaginationOrder;
    use nexus_db_model::Generation;
    use nexus_test_utils::db::test_setup_database;
    use nexus_types::identity::Asset;
    use omicron_test_utils::dev;
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
        db.sled_upsert(sled_update)
            .await
            .expect("Could not upsert sled during test prep")
    }

    fn list_disk_params() -> DataPageParams<'static, Uuid> {
        DataPageParams {
            limit: NonZeroU32::new(32).unwrap(),
            direction: PaginationOrder::Ascending,
            marker: None,
        }
    }

    // Only checking some fields:
    // - The UUID of the disk may actually not be the same as the upserted one;
    // the "vendor/serial/model" value is the more critical unique identifier.
    // NOTE: Could we derive a UUID from the VSM values?
    // - The 'time' field precision can be modified slightly when inserted into
    // the DB.
    fn assert_disks_equal_ignore_uuid(lhs: &PhysicalDisk, rhs: &PhysicalDisk) {
        assert_eq!(lhs.time_deleted().is_some(), rhs.time_deleted().is_some());
        assert_eq!(lhs.vendor, rhs.vendor);
        assert_eq!(lhs.serial, rhs.serial);
        assert_eq!(lhs.model, rhs.model);
        assert_eq!(lhs.variant, rhs.variant);
        assert_eq!(lhs.sled_id, rhs.sled_id);
    }

    #[tokio::test]
    async fn physical_disk_upsert_different_uuid_idempotent() {
        let logctx = dev::test_setup_log(
            "physical_disk_upsert_different_uuid_idempotent",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;
        let sled_id = sled.id();

        // Insert a disk
        let disk = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_id,
        );
        let first_observed_disk = datastore
            .physical_disk_upsert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");
        assert_eq!(disk.uuid(), first_observed_disk.uuid());
        assert_disks_equal_ignore_uuid(&disk, &first_observed_disk);

        // Observe the inserted disk
        let pagparams = list_disk_params();
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_id, &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 1);
        assert_eq!(disk.uuid(), disks[0].uuid());
        assert_disks_equal_ignore_uuid(&disk, &disks[0]);

        // Insert the same disk, with a different UUID primary key
        let disk_again = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_id,
        );
        let second_observed_disk = datastore
            .physical_disk_upsert(&opctx, disk_again.clone())
            .await
            .expect("Failed second upsert of physical disk");
        // This check is pretty important - note that we return the original
        // UUID, not the new one.
        assert_ne!(disk_again.uuid(), second_observed_disk.uuid());
        assert_eq!(disk_again.id(), second_observed_disk.id());
        assert_disks_equal_ignore_uuid(&disk_again, &second_observed_disk);
        assert!(
            first_observed_disk.time_modified()
                <= second_observed_disk.time_modified()
        );

        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_id, &pagparams)
            .await
            .expect("Failed to re-list physical disks");

        // We'll use the old primary key
        assert_eq!(disks.len(), 1);
        assert_eq!(disk.uuid(), disks[0].uuid());
        assert_ne!(disk_again.uuid(), disks[0].uuid());
        assert_disks_equal_ignore_uuid(&disk, &disks[0]);
        assert_disks_equal_ignore_uuid(&disk_again, &disks[0]);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_upsert_same_uuid_idempotent() {
        let logctx =
            dev::test_setup_log("physical_disk_upsert_same_uuid_idempotent");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;
        let sled_id = sled.id();

        // Insert a disk
        let disk = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_id,
        );
        let first_observed_disk = datastore
            .physical_disk_upsert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");
        assert_eq!(disk.uuid(), first_observed_disk.uuid());

        // Insert a disk with an identical UUID
        let second_observed_disk = datastore
            .physical_disk_upsert(&opctx, disk.clone())
            .await
            .expect("Should have succeeded upserting disk");
        assert_eq!(disk.uuid(), second_observed_disk.uuid());
        assert!(
            first_observed_disk.time_modified()
                <= second_observed_disk.time_modified()
        );
        assert_disks_equal_ignore_uuid(
            &first_observed_disk,
            &second_observed_disk,
        );

        let pagparams = list_disk_params();
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled_id, &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert_eq!(disks.len(), 1);

        db.cleanup().await.unwrap();
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_upsert_different_disks() {
        let logctx =
            dev::test_setup_log("physical_disk_upsert_different_disks");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled = create_test_sled(&datastore).await;
        let sled_id = sled.id();

        // Insert a disk
        let disk = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_id,
        );
        datastore
            .physical_disk_upsert(&opctx, disk.clone())
            .await
            .expect("Failed first attempt at upserting disk");

        // Insert a second disk
        let disk = PhysicalDisk::new(
            String::from("Noxide"),
            String::from("456"),
            String::from("UnrealDisk"),
            PhysicalDiskKind::M2,
            sled_id,
        );
        datastore
            .physical_disk_upsert(&opctx, disk.clone())
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
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled.id(),
        );
        datastore
            .physical_disk_upsert(&opctx, disk.clone())
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
    async fn physical_disk_upsert_delete_reupsert_new_sled() {
        let logctx = dev::test_setup_log(
            "physical_disk_upsert_delete_reupsert_new_sled",
        );
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // Insert a disk
        let disk = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_a.id(),
        );
        datastore
            .physical_disk_upsert(&opctx, disk.clone())
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

        // "Report the disk" from the second sled
        let disk = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_b.id(),
        );
        datastore
            .physical_disk_upsert(&opctx, disk.clone())
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
    async fn physical_disk_upsert_reupsert_new_sled() {
        let logctx =
            dev::test_setup_log("physical_disk_upsert_reupsert_new_sled");
        let mut db = test_setup_database(&logctx.log).await;
        let (opctx, datastore) = datastore_test(&logctx, &db).await;

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // Insert a disk
        let disk = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_a.id(),
        );
        datastore
            .physical_disk_upsert(&opctx, disk.clone())
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

        // "Report the disk" from the second sled
        let disk = PhysicalDisk::new(
            String::from("Oxide"),
            String::from("123"),
            String::from("FakeDisk"),
            PhysicalDiskKind::U2,
            sled_b.id(),
        );
        datastore
            .physical_disk_upsert(&opctx, disk.clone())
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
}
