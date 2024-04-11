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
use crate::db::model::to_db_sled_policy;
use crate::db::model::InvPhysicalDisk;
use crate::db::model::PhysicalDisk;
use crate::db::model::PhysicalDiskKind;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::PhysicalDiskState;
use crate::db::model::Sled;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_types::external_api::views::SledPolicy;
use nexus_types::external_api::views::SledProvisionPolicy;
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
    /// row in the table, an error is thrown.
    /// - If the primary key (ID) is the same as an existing row in the table,
    /// an error is thrown.
    pub async fn physical_disk_insert(
        &self,
        opctx: &OpContext,
        disk: PhysicalDisk,
    ) -> CreateResult<PhysicalDisk> {
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        Self::physical_disk_insert_on_connection(&conn, opctx, disk).await
    }

    pub async fn physical_disk_insert_on_connection(
        conn: &async_bb8_diesel::Connection<db::DbConnection>,
        opctx: &OpContext,
        disk: PhysicalDisk,
    ) -> CreateResult<PhysicalDisk> {
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
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel(e, ErrorHandler::Server)
            }
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
    pub async fn physical_disk_uninitialized_list(
        &self,
        opctx: &OpContext,
        inventory_collection_id: Uuid,
        sled_id: Uuid,
    ) -> ListResultVec<InvPhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use db::schema::inv_physical_disk::dsl as inv_physical_disk_dsl;
        use db::schema::physical_disk::dsl as physical_disk_dsl;
        use db::schema::sled::dsl as sled_dsl;

        // This is kind of frustrating - we'd like to be able to apply this
        // condition to all "in service" sleds, regardless of provisionability,
        // but the sled policy crate explicitly avoids exporting that type.
        //
        // As a workaround, we must create the corresponding external types
        // and manually convert them back into the database types.
        let in_service = to_db_sled_policy(SledPolicy::InService {
            provision_policy: SledProvisionPolicy::Provisionable,
        });
        let in_service_non_provisionable =
            to_db_sled_policy(SledPolicy::InService {
                provision_policy: SledProvisionPolicy::NonProvisionable,
            });

        sled_dsl::sled
            // If the sled is not in-service, drop the list immediately.
            .filter(
                sled_dsl::sled_policy
                    .eq(in_service)
                    .or(sled_dsl::sled_policy.eq(in_service_non_provisionable)),
            )
            // Look up all inventory physical disks that could match this sled
            .inner_join(
                inv_physical_disk_dsl::inv_physical_disk.on(
                    inv_physical_disk_dsl::inv_collection_id
                        .eq(inventory_collection_id)
                        .and(inv_physical_disk_dsl::sled_id.eq(sled_id))
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
                    .filter(physical_disk_dsl::sled_id.eq(sled_id))
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
                .contains("duplicate key value violates unique constraint"),
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
}
