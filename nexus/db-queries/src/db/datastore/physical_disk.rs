// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`PhysicalDisk`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::model::ApplySledFilterExt;
use crate::db::model::InvPhysicalDisk;
use crate::db::model::PhysicalDisk;
use crate::db::model::PhysicalDiskAdoptionRequest;
use crate::db::model::PhysicalDiskKind;
use crate::db::model::PhysicalDiskPolicy;
use crate::db::model::PhysicalDiskState;
use crate::db::model::Sled;
use crate::db::model::Zpool;
use crate::db::model::to_db_typed_uuid;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::{
    DatabaseErrorKind as DieselErrorKind, Error as DieselError,
};
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::TransactionError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_db_model::ApplyPhysicalDiskFilterExt;
use nexus_types::deployment::{DiskFilter, SledFilter};
use nexus_types::external_api::physical_disk::PhysicalDiskManufacturerIdentity;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_uuid_kinds::CollectionUuid;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::PhysicalDiskAdoptionRequestUuid;
use omicron_uuid_kinds::PhysicalDiskUuid;
use omicron_uuid_kinds::SledUuid;
use uuid::Uuid;

impl DataStore {
    /// Inserts a physical disk and zpool together in a transaction if there is
    /// a valid adoption request.
    ///
    /// Soft-deletes the adoption request.
    pub async fn physical_disk_and_zpool_insert(
        &self,
        opctx: &OpContext,
        disk: PhysicalDisk,
        zpool: Zpool,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
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
                        disk.sled_id(),
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    // Delete the adoption request if it exists. If it doesn't exist
                    // this will return an error.
                    Self::physical_disk_adoption_request_delete_on_connection(
                        &conn,
                        disk.clone().into(),
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                    Self::physical_disk_insert_on_connection(&conn, disk)
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
                    Some(txn_error) => txn_error.into_public_ignore_retries(),
                    // The transaction setup/teardown itself encountered a diesel error.
                    None => public_error_from_diesel(e, ErrorHandler::Server),
                }
            })?;
        Ok(())
    }

    /// Create a row in `physical_disk_adoption_request` for a given `disk_id`
    /// as long as a non-expunged disk isn't present in the `physical_disk`
    /// table for the same disk_id.
    ///
    /// This request is idempotent. If an existing adoption request exists,
    /// success is returned.
    pub async fn physical_disk_enable_adoption(
        &self,
        opctx: &OpContext,
        disk_id: PhysicalDiskManufacturerIdentity,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::physical_disk_adoption_request::dsl as adoption_dsl;

        let conn = &*self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::<TransactionError<Error>>::new();
        let txn_res = self
            .transaction_retry_wrapper("physical_disk_enable_adoption")
            .transaction(&conn, |conn| {
                let vendor = disk_id.vendor.clone();
                let serial = disk_id.serial.clone();
                let model = disk_id.model.clone();
                let err = err.clone();
                async move {
                    // Check for an active (non-expunged) physical disk with
                    // the same vendor/serial/model.
                    let active_disk_exists = physical_disk_dsl::physical_disk
                        .filter(physical_disk_dsl::vendor.eq(vendor.clone()))
                        .filter(physical_disk_dsl::serial.eq(serial.clone()))
                        .filter(physical_disk_dsl::model.eq(model.clone()))
                        .filter(physical_disk_dsl::time_deleted.is_null())
                        .filter(
                            physical_disk_dsl::disk_policy
                                .ne(PhysicalDiskPolicy::Expunged),
                        )
                        .select(physical_disk_dsl::id)
                        .first_async::<Uuid>(&conn)
                        .await
                        .optional()?;

                    if active_disk_exists.is_some() {
                        return Err(err.bail(
                            Error::conflict(format!(
                                "physical disk already adopted: {}:{}:{}",
                                vendor, model, serial
                            ))
                            .into(),
                        ));
                    }

                    // Insert the adoption request.
                    diesel::insert_into(
                        adoption_dsl::physical_disk_adoption_request,
                    )
                    .values((
                        adoption_dsl::id.eq(Uuid::new_v4()),
                        adoption_dsl::vendor.eq(vendor),
                        adoption_dsl::serial.eq(serial),
                        adoption_dsl::model.eq(model),
                        adoption_dsl::time_created.eq(Utc::now()),
                    ))
                    .execute_async(&conn)
                    .await?;

                    Ok(())
                }
            })
            .await;

        // Check for a unique index violation for an active
        // adoption request with the same vendor/serial/model.
        //
        // Return Ok(()) in this case as the request is idempotent.
        if let Err(e) = txn_res {
            match err.take() {
                // A called function performed its own error propagation.
                Some(txn_error) => {
                    return Err(txn_error.into_public_ignore_retries());
                }
                // The transaction setup/teardown itself encountered a diesel error.
                None => match e {
                    DieselError::DatabaseError(
                        DieselErrorKind::UniqueViolation,
                        _,
                    ) => {
                        return Ok(());
                    }
                    _ => {
                        return Err(public_error_from_diesel(
                            e,
                            ErrorHandler::Server,
                        ));
                    }
                },
            }
        }

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
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        let disk = Self::physical_disk_insert_on_connection(&conn, disk)
            .await
            .map_err(|err| err.into_public_ignore_retries())?;
        Ok(disk)
    }

    pub async fn physical_disk_insert_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        disk: PhysicalDisk,
    ) -> Result<PhysicalDisk, TransactionError<Error>> {
        use nexus_db_schema::schema::physical_disk::dsl;

        let sled_id = disk.sled_id();
        let disk_in_db = Sled::insert_resource(
            sled_id.into(),
            diesel::insert_into(dsl::physical_disk).values(disk.clone()),
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
        id: PhysicalDiskUuid,
        policy: PhysicalDiskPolicy,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk::dsl;
        let now = Utc::now();

        diesel::update(
            dsl::physical_disk.filter(dsl::id.eq(to_db_typed_uuid(id))),
        )
        .filter(dsl::time_deleted.is_null())
        .set((dsl::disk_policy.eq(policy), dsl::time_modified.eq(now)))
        .execute_async(&*self.pool_connection_authorized(&opctx).await?)
        .await
        .map_err(|err| public_error_from_diesel(err, ErrorHandler::Server))?;
        Ok(())
    }

    pub async fn physical_disk_update_state(
        &self,
        opctx: &OpContext,
        id: PhysicalDiskUuid,
        state: PhysicalDiskState,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk::dsl;
        let now = Utc::now();

        diesel::update(
            dsl::physical_disk.filter(dsl::id.eq(to_db_typed_uuid(id))),
        )
        .filter(dsl::time_deleted.is_null())
        .set((dsl::disk_state.eq(state), dsl::time_modified.eq(now)))
        .execute_async(&*self.pool_connection_authorized(&opctx).await?)
        .await
        .map_err(|err| public_error_from_diesel(err, ErrorHandler::Server))?;
        Ok(())
    }

    /// Enable adoption for any physical disks whose vendor/model/serial has not
    /// been seen before.
    ///
    /// We only want to mandate that expunged disks get manually re-adopted by
    /// operators. For newly discovered disks we want to auto-adopt them. We
    /// are doing this to maintain our current operational simplicity, such that
    /// when a user adds a new sled to a rack with new disks they don't have to
    /// manually add individual disks. This allows us to safely re-add expunged
    /// disks without changing unrelated operator behavior at the same time.
    ///
    /// This still leaves a security hole such that arbitrary disks can be
    /// inserted and automatically adopted. Eventually we want to make it so
    /// that all new physical disks are manually adopted, but this is a more
    /// complicated change.
    pub async fn physical_disk_enable_adoption_for_all_new_disks_in_inventory(
        &self,
        opctx: &OpContext,
        inventory_collection_id: CollectionUuid,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let conn = &*self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::<TransactionError<Error>>::new();

        use nexus_db_schema::schema::physical_disk_adoption_request::dsl as adoption_dsl;

        self.transaction_retry_wrapper(
            "physical_disk_enable_adoption_all_new_disks",
        )
        .transaction(&conn, |conn| {
            let err = err.clone();

            async move {
                // Find all new disks
                let new_inv_disks = self
                    .physical_disk_list_new_inventory_on_connection(
                        &conn,
                        inventory_collection_id,
                    )
                    .await
                    .map_err(|txn_error| txn_error.into_diesel(&err))?;

                // Enable adoption for each disk.
                for disk in new_inv_disks {
                    // Insert the adoption request.
                    diesel::insert_into(
                        adoption_dsl::physical_disk_adoption_request,
                    )
                    .values((
                        adoption_dsl::id.eq(Uuid::new_v4()),
                        adoption_dsl::vendor.eq(disk.vendor),
                        adoption_dsl::serial.eq(disk.serial),
                        adoption_dsl::model.eq(disk.model),
                        adoption_dsl::time_created.eq(Utc::now()),
                    ))
                    .execute_async(&conn)
                    .await?;
                }
                Ok(())
            }
        })
        .await
        .map_err(|e| {
            match err.take() {
                // A called function performed its own error propagation.
                Some(txn_error) => txn_error.into_public_ignore_retries(),
                // The transaction setup/teardown itself encountered a diesel error.
                None => public_error_from_diesel(e, ErrorHandler::Server),
            }
        })?;
        Ok(())
    }

    /// Returns all physical disks in inventory that are inserted in
    /// active sleds and do not yet have a record in `physical_disk` or
    /// `physical_disk_adoption_request` tables.
    pub async fn physical_disk_list_new_inventory_on_connection(
        &self,
        conn: &async_bb8_diesel::Connection<DbConnection>,
        inventory_collection_id: CollectionUuid,
    ) -> Result<Vec<InvPhysicalDisk>, TransactionError<Error>> {
        use nexus_db_schema::schema::inv_physical_disk::dsl as inv_physical_disk_dsl;
        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::physical_disk_adoption_request::dsl as adoption_request_dsl;
        use nexus_db_schema::schema::sled::dsl as sled_dsl;

        let disks = sled_dsl::sled
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
                    .filter(physical_disk_dsl::variant.eq(PhysicalDiskKind::U2))
                    .filter(physical_disk_dsl::sled_id.eq(sled_dsl::id))
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
            // Filter out physical disks that exist in
            // `physical_disk_adoption_request`. This means they are in the
            // process of being adopted already.
            .filter(diesel::dsl::not(diesel::dsl::exists(
                adoption_request_dsl::physical_disk_adoption_request
                    .select(0.into_sql::<diesel::sql_types::Integer>())
                    .filter(adoption_request_dsl::time_deleted.is_null())
                    .filter(
                        adoption_request_dsl::vendor
                            .eq(inv_physical_disk_dsl::vendor),
                    )
                    .filter(
                        adoption_request_dsl::model
                            .eq(inv_physical_disk_dsl::model),
                    )
                    .filter(
                        adoption_request_dsl::serial
                            .eq(inv_physical_disk_dsl::serial),
                    ),
            )))
            .select(InvPhysicalDisk::as_select())
            .get_results_async(conn)
            .await?;

        Ok(disks)
    }

    /// Returns all physical disks which are eligible for adoption.
    ///
    /// These disks:
    ///
    /// - Appear on in-service sleds
    /// - Appear in inventory
    /// - Have adoption requests
    ///
    /// If "inventory_collection_id" is not associated with a collection, this
    /// function returns an empty list, rather than failing.
    pub async fn physical_disk_adoptable_list(
        &self,
        opctx: &OpContext,
        inventory_collection_id: CollectionUuid,
    ) -> ListResultVec<InvPhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::inv_physical_disk::dsl as inv_physical_disk_dsl;
        use nexus_db_schema::schema::physical_disk_adoption_request::dsl as adoption_request_dsl;
        use nexus_db_schema::schema::sled::dsl as sled_dsl;

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
            // Ensure that each inventory disk has a valid adoption request
            .inner_join(
                adoption_request_dsl::physical_disk_adoption_request.on(
                    adoption_request_dsl::vendor
                        .eq(inv_physical_disk_dsl::vendor)
                        .and(
                            adoption_request_dsl::model
                                .eq(inv_physical_disk_dsl::model),
                        )
                        .and(
                            adoption_request_dsl::serial
                                .eq(inv_physical_disk_dsl::serial),
                        )
                        .and(adoption_request_dsl::time_deleted.is_null()),
                ),
            )
            .select(InvPhysicalDisk::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Returns all physical disks which:
    ///
    /// - Appear on in-service sleds
    /// - Appear in inventory
    /// - Do not have adoption requests
    /// - Do not have active physical disk records
    ///
    /// If "inventory_collection_id" is not associated with a collection, this
    /// function returns an empty list, rather than failing.
    pub async fn physical_disk_unadopted_list(
        &self,
        opctx: &OpContext,
        inventory_collection_id: CollectionUuid,
    ) -> ListResultVec<InvPhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::inv_physical_disk::dsl as inv_physical_disk_dsl;
        use nexus_db_schema::schema::physical_disk::dsl as physical_disk_dsl;
        use nexus_db_schema::schema::physical_disk_adoption_request::dsl as adoption_request_dsl;
        use nexus_db_schema::schema::sled::dsl as sled_dsl;

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
            // Filter out any disks in the inventory where we have a control
            // plane disk that is active.
            .filter(diesel::dsl::not(diesel::dsl::exists(
                physical_disk_dsl::physical_disk
                    .select(0.into_sql::<diesel::sql_types::Integer>())
                    .filter(physical_disk_dsl::sled_id.eq(sled_dsl::id))
                    .filter(physical_disk_dsl::variant.eq(PhysicalDiskKind::U2))
                    .filter(physical_disk_dsl::time_deleted.is_null())
                    .filter(
                        physical_disk_dsl::disk_policy
                            .ne(PhysicalDiskPolicy::Expunged),
                    )
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
            // Filter out physical disks that exist in `physical_disk_adoption_request`. This means
            // they are in the process of being adopted already.
            .filter(diesel::dsl::not(diesel::dsl::exists(
                adoption_request_dsl::physical_disk_adoption_request
                    .select(0.into_sql::<diesel::sql_types::Integer>())
                    .filter(adoption_request_dsl::time_deleted.is_null())
                    .filter(
                        adoption_request_dsl::vendor
                            .eq(inv_physical_disk_dsl::vendor),
                    )
                    .filter(
                        adoption_request_dsl::model
                            .eq(inv_physical_disk_dsl::model),
                    )
                    .filter(
                        adoption_request_dsl::serial
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
        disk_filter: DiskFilter,
    ) -> ListResultVec<PhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk::dsl;
        paginated(dsl::physical_disk, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .physical_disk_filter(disk_filter)
            .select(PhysicalDisk::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return a page of active physical disk adoption requests.
    pub async fn physical_disk_adoption_request_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<PhysicalDiskAdoptionRequest> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk_adoption_request::dsl;
        paginated(dsl::physical_disk_adoption_request, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(PhysicalDiskAdoptionRequest::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return a page of physical disks for a given sled id
    pub async fn sled_list_physical_disks(
        &self,
        opctx: &OpContext,
        sled_id: SledUuid,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<PhysicalDisk> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk::dsl;
        paginated(dsl::physical_disk, dsl::id, pagparams)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::sled_id.eq(to_db_typed_uuid(sled_id)))
            .select(PhysicalDisk::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Decommissions a single expunged disk.
    ///
    /// This is a no-op if the disk is already decommissioned.
    pub async fn physical_disk_decommission(
        &self,
        opctx: &OpContext,
        id: PhysicalDiskUuid,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        use nexus_db_schema::schema::physical_disk::dsl;
        let now = Utc::now();
        let conn = &*self.pool_connection_authorized(&opctx).await?;
        diesel::update(dsl::physical_disk)
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::disk_policy.eq(PhysicalDiskPolicy::Expunged))
            .filter(dsl::disk_state.ne(PhysicalDiskState::Decommissioned))
            .set((
                dsl::disk_state.eq(PhysicalDiskState::Decommissioned),
                dsl::time_modified.eq(now),
            ))
            .execute_async(conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(())
    }

    /// Deletes a disk from the database.
    pub async fn physical_disk_delete(
        &self,
        opctx: &OpContext,
        id: PhysicalDiskUuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let now = Utc::now();
        use nexus_db_schema::schema::physical_disk::dsl;
        diesel::update(dsl::physical_disk)
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map(|_rows_modified| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    // Delete an adoption request from the database
    pub async fn physical_disk_adoption_request_delete(
        &self,
        opctx: &OpContext,
        id: PhysicalDiskAdoptionRequestUuid,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, &authz::FLEET).await?;
        let now = Utc::now();
        use nexus_db_schema::schema::physical_disk_adoption_request::dsl;
        let rows_modified = diesel::update(dsl::physical_disk_adoption_request)
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        if rows_modified != 1 {
            return Err(Error::non_resourcetype_not_found(format!(
                "No active adoption request found for id: {id}"
            ))
            .into());
        }

        Ok(())
    }

    // Delete an adoption request from the database when given a connection
    async fn physical_disk_adoption_request_delete_on_connection(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        id: PhysicalDiskManufacturerIdentity,
    ) -> Result<(), TransactionError<Error>> {
        let now = Utc::now();
        use nexus_db_schema::schema::physical_disk_adoption_request::dsl;
        let rows_modified = diesel::update(dsl::physical_disk_adoption_request)
            .filter(dsl::vendor.eq(id.vendor.clone()))
            .filter(dsl::serial.eq(id.serial.clone()))
            .filter(dsl::model.eq(id.model.clone()))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(conn)
            .await?;

        if rows_modified != 1 {
            return Err(Error::non_resourcetype_not_found(format!(
                "No adoption request found for physical disk: {}:{}:{}",
                id.vendor, id.serial, id.model
            ))
            .into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::model::{PhysicalDiskKind, Sled};
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::SledUpdateBuilder;
    use dropshot::PaginationOrder;
    use iddqd::IdOrdMap;
    use nexus_db_lookup::LookupPath;
    use nexus_types::identity::Asset;
    use omicron_common::api::external::ByteCount;
    use omicron_common::disk::{DiskIdentity, DiskVariant};
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::ZpoolUuid;
    use sled_agent_types::inventory::{
        Baseboard, ConfigReconcilerInventoryStatus, Inventory, InventoryDisk,
        OmicronFileSourceResolverInventory, SledCpuFamily, SledRole,
        SvcsEnabledNotOnlineResult,
    };
    use std::num::NonZeroU32;

    async fn create_test_sled(db: &DataStore) -> Sled {
        let sled_update = SledUpdateBuilder::new().build();
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore).await;
        let sled_id = sled.id();

        // Insert a disk
        let disk = PhysicalDisk::from_parts(
            PhysicalDiskUuid::new_v4(),
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_insert_different_disks() {
        let logctx =
            dev::test_setup_log("physical_disk_insert_different_disks");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore).await;
        let sled_id = sled.id();

        // Insert a disk
        let disk = PhysicalDisk::from_parts(
            PhysicalDiskUuid::new_v4(),
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
        let disk = PhysicalDisk::from_parts(
            PhysicalDiskUuid::new_v4(),
            String::from("Noxide"),
            String::from("456"),
            String::from("UnrealDisk"),
            PhysicalDiskKind::U2,
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_deletion_idempotency() {
        let logctx = dev::test_setup_log("physical_disk_deletion_idempotency");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore).await;

        // Insert a disk
        let disk_id = PhysicalDiskUuid::new_v4();
        let disk = PhysicalDisk::from_parts(
            disk_id,
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
            .physical_disk_delete(&opctx, disk_id)
            .await
            .expect("Failed to delete disk");
        let disks = datastore
            .sled_list_physical_disks(&opctx, sled.id(), &pagparams)
            .await
            .expect("Failed to list physical disks");
        assert!(disks.is_empty());

        // Deleting again should not throw an error
        datastore
            .physical_disk_delete(&opctx, disk_id)
            .await
            .expect("Failed to delete disk");

        db.terminate().await;
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // Insert a disk
        let disk_id = PhysicalDiskUuid::new_v4();
        let disk = PhysicalDisk::from_parts(
            disk_id,
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
            .physical_disk_delete(&opctx, disk_id)
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
        let disk_id = PhysicalDiskUuid::new_v4();
        let disk = PhysicalDisk::from_parts(
            disk_id,
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

        db.terminate().await;
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
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // Insert a disk
        let disk_id = PhysicalDiskUuid::new_v4();
        let disk = PhysicalDisk::from_parts(
            disk_id,
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
            .physical_disk_delete(&opctx, disk_id)
            .await
            .expect("Failed to delete disk");

        // "Report the disk" from the second sled
        let disk = PhysicalDisk::from_parts(
            PhysicalDiskUuid::new_v4(),
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    // Most of this data doesn't matter, but adds a sled
    // to an inventory with a supplied set of disks.
    fn add_sled_to_inventory(
        builder: &mut nexus_inventory::CollectionBuilder,
        sled: &Sled,
        disks: Vec<InventoryDisk>,
    ) {
        builder
            .found_sled_inventory(
                "fake sled agent",
                Inventory {
                    baseboard: Baseboard::Gimlet {
                        identifier: sled.serial_number().to_string(),
                        model: sled.part_number().to_string(),
                        revision: 0,
                    },
                    reservoir_size: ByteCount::from(1024),
                    sled_role: SledRole::Gimlet,
                    sled_agent_address: "[::1]:56792".parse().unwrap(),
                    sled_id: sled.id(),
                    usable_hardware_threads: 10,
                    usable_physical_ram: ByteCount::from(1024 * 1024),
                    cpu_family: SledCpuFamily::AmdMilan,
                    disks,
                    zpools: vec![],
                    datasets: vec![],
                    ledgered_sled_config: None,
                    reconciler_status:
                        ConfigReconcilerInventoryStatus::NotYetRun,
                    last_reconciliation: None,
                    file_source_resolver:
                        OmicronFileSourceResolverInventory::new_fake(),
                    smf_services_enabled_not_online:
                        SvcsEnabledNotOnlineResult::DataUnavailable,
                    reference_measurements: IdOrdMap::new(),
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
            active_firmware_slot: 1,
            next_active_firmware_slot: None,
            number_of_firmware_slots: 1,
            slot1_is_read_only: true,
            slot_firmware_versions: vec![Some("TEST1".to_string())],
        }
    }

    // helper function to eaisly convert from [`InventoryDisk`] to
    // [`nexus_types::inventory::PhysicalDisk`]
    fn inv_phys_disk_to_nexus_phys_disk(
        inv_phys_disk: &InvPhysicalDisk,
    ) -> nexus_types::inventory::PhysicalDisk {
        use nexus_types::inventory::NvmeFirmware;
        use nexus_types::inventory::PhysicalDiskFirmware;

        // We reuse `create_inv_disk` so that we get the same NVMe firmware
        // values throughout the tests.
        let inv_disk = create_inv_disk("unused".to_string(), 0);

        nexus_types::inventory::PhysicalDisk {
            identity: DiskIdentity {
                vendor: inv_phys_disk.vendor.clone(),
                model: inv_phys_disk.model.clone(),
                serial: inv_phys_disk.serial.clone(),
            },
            variant: inv_phys_disk.variant.into(),
            slot: inv_phys_disk.slot,
            firmware: PhysicalDiskFirmware::Nvme(NvmeFirmware {
                active_slot: inv_disk.active_firmware_slot,
                next_active_slot: inv_disk.next_active_firmware_slot,
                number_of_slots: inv_disk.number_of_firmware_slots,
                slot1_is_read_only: inv_disk.slot1_is_read_only,
                slot_firmware_versions: inv_disk.slot_firmware_versions,
            }),
        }
    }

    fn create_disk_zpool_combo(
        sled_id: SledUuid,
        inv_disk: &InventoryDisk,
    ) -> (PhysicalDisk, Zpool) {
        let disk = PhysicalDisk::from_parts(
            PhysicalDiskUuid::new_v4(),
            inv_disk.identity.vendor.clone(),
            inv_disk.identity.serial.clone(),
            inv_disk.identity.model.clone(),
            PhysicalDiskKind::U2,
            sled_id,
        );

        let zpool = Zpool::new(
            ZpoolUuid::new_v4(),
            sled_id,
            disk.id(),
            ByteCount::from(0).into(),
        );
        (disk, zpool)
    }

    #[tokio::test]
    async fn physical_disk_cannot_insert_to_expunged_sled() {
        let logctx =
            dev::test_setup_log("physical_disk_cannot_insert_to_expunged_sled");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled = create_test_sled(&datastore).await;

        // We can insert a disk into a sled that is not yet expunged
        let inv_disk = create_inv_disk("serial-001".to_string(), 1);
        let (disk, zpool) = create_disk_zpool_combo(sled.id(), &inv_disk);

        // We need an adoption request before we can adopt the disk
        // Adoption is idempotent
        datastore
            .physical_disk_enable_adoption(&opctx, disk.clone().into())
            .await
            .expect("adoption succeeds");
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk, zpool)
            .await
            .unwrap();

        // Mark the sled as expunged
        let sled_lookup = LookupPath::new(&opctx, datastore).sled_id(sled.id());
        let (authz_sled,) =
            sled_lookup.lookup_for(authz::Action::Modify).await.unwrap();
        datastore
            .sled_set_policy_to_expunged(&opctx, &authz_sled)
            .await
            .unwrap();

        // Now that the sled is expunged, inserting the disk should fail
        let inv_disk = create_inv_disk("serial-002".to_string(), 2);
        let (disk, zpool) = create_disk_zpool_combo(sled.id(), &inv_disk);
        // We don't want it to fail because it doesn't have an adoption request
        // though
        datastore
            .physical_disk_enable_adoption(&opctx, disk.clone().into())
            .await
            .expect("adoption succeeds");
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

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_unadopted_list() {
        let logctx = dev::test_setup_log("physical_disk_unadopted_list");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // No inventory -> No unadopted disks
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(
                &opctx,
                CollectionUuid::new_v4(), // Collection that does not exist
            )
            .await
            .expect("Failed to look up unadopted disks");
        assert!(unadopted_disks.is_empty());

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

        // Now when we list the unadopted disks, we should see everything in
        // the inventory.
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 6);

        // Normalize the data a bit -- convert to nexus types, and sort vecs for
        // stability in the comparison.
        let mut unadopted_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            unadopted_disks
                .into_iter()
                .map(|d| inv_phys_disk_to_nexus_phys_disk(&d))
                .collect();
        unadopted_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        let mut expected_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            disks_a
                .iter()
                .map(|d| d.clone().into())
                .chain(disks_b.iter().map(|d| d.clone().into()))
                .collect();
        expected_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        assert_eq!(unadopted_disks, expected_disks);

        // Let's create control plane objects for some of these disks.
        //
        // They should no longer show up when we list unadopted devices.
        //
        // This creates disks for: 001, 002, and 101.
        // It leaves the following unadopted: 003, 102, 103
        //
        // We make sure we submit an adoption request before we attempt to
        // create the disk and zpools as we don't want automatic adoption in the
        // background task that uses this datastore method.
        let (disk_001, zpool) =
            create_disk_zpool_combo(sled_a.id(), &disks_a[0]);
        datastore
            .physical_disk_enable_adoption(&opctx, disk_001.clone().into())
            .await
            .expect("adoption request succeeds");
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_001, zpool)
            .await
            .unwrap();
        let (disk_002, zpool) =
            create_disk_zpool_combo(sled_a.id(), &disks_a[1]);
        datastore
            .physical_disk_enable_adoption(&opctx, disk_002.clone().into())
            .await
            .expect("adoption request succeeds");
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_002, zpool)
            .await
            .unwrap();
        let (disk_101, zpool) =
            create_disk_zpool_combo(sled_b.id(), &disks_b[0]);
        datastore
            .physical_disk_enable_adoption(&opctx, disk_101.clone().into())
            .await
            .expect("adoption request succeeds");
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_101, zpool)
            .await
            .unwrap();

        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 3);

        // Pay careful attention to our indexing below.
        //
        // We're grabbing the last disk of "disks_a" (which still is
        // uninitialized) and the last two disks of "disks_b" (of which both are
        // still unadopted).
        let mut unadopted_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            unadopted_disks
                .into_iter()
                .map(|d| inv_phys_disk_to_nexus_phys_disk(&d))
                .collect();
        unadopted_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        let mut expected_disks: Vec<nexus_types::inventory::PhysicalDisk> =
            disks_a[2..3]
                .iter()
                .map(|d| d.clone().into())
                .chain(disks_b[1..3].iter().map(|d| d.clone().into()))
                .collect();
        expected_disks
            .sort_by(|a, b| a.identity.partial_cmp(&b.identity).unwrap());
        assert_eq!(unadopted_disks, expected_disks);

        // Create physical disks for all remaining devices.
        //
        // Observe no remaining unadopted disks.
        let (disk_003, zpool) =
            create_disk_zpool_combo(sled_a.id(), &disks_a[2]);
        datastore
            .physical_disk_enable_adoption(&opctx, disk_003.clone().into())
            .await
            .expect("adoption request succeeds");
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_003.clone(), zpool)
            .await
            .unwrap();
        let (disk_102, zpool) =
            create_disk_zpool_combo(sled_b.id(), &disks_b[1]);
        datastore
            .physical_disk_enable_adoption(&opctx, disk_102.clone().into())
            .await
            .expect("adoption request succeeds");
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_102.clone(), zpool)
            .await
            .unwrap();
        let (disk_103, zpool) =
            create_disk_zpool_combo(sled_b.id(), &disks_b[2]);
        datastore
            .physical_disk_enable_adoption(&opctx, disk_103.clone().into())
            .await
            .expect("adoption request succeeds");
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk_103.clone(), zpool)
            .await
            .unwrap();

        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 0);

        // Expunge some disks, observe that they are now unadopted again. This allows
        // re-adding them to the control plane.
        use nexus_db_schema::schema::physical_disk::dsl;

        // Set a disk to "deleted".
        let now = Utc::now();
        diesel::update(dsl::physical_disk)
            .filter(dsl::id.eq(to_db_typed_uuid(disk_003.id())))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(now))
            .execute_async(
                &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
            )
            .await
            .unwrap();

        // Set another disk to "expunged"
        diesel::update(dsl::physical_disk)
            .filter(dsl::id.eq(to_db_typed_uuid(disk_102.id())))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::disk_policy.eq(PhysicalDiskPolicy::Expunged))
            .execute_async(
                &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
            )
            .await
            .unwrap();

        // The set of unadopted disks should include the expunged/deleted
        // disks
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 2);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_adoptable_list() {
        let logctx = dev::test_setup_log("physical_disk_adoptable_list");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();

        let sled_a = create_test_sled(&datastore).await;
        let sled_b = create_test_sled(&datastore).await;

        // No inventory -> No adoptable disks
        let adoptable_disks = datastore
            .physical_disk_adoptable_list(
                &opctx,
                CollectionUuid::new_v4(), // Collection that does not exist
            )
            .await
            .expect("Failed to look up adoptable disks");
        assert!(adoptable_disks.is_empty());

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

        // We have inventory, but no adoption requests so we should still list
        // 0 disks.
        let adoptable_disks = datastore
            .physical_disk_adoptable_list(&opctx, collection_id)
            .await
            .expect("Failed to look up adoptable disks");
        assert!(adoptable_disks.is_empty());

        // All 6 disks should be unadopted
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 6);

        // Make two disks adoptable
        datastore
            .physical_disk_enable_adoption(
                &opctx,
                disks_a[0].identity.clone().into(),
            )
            .await
            .expect("adoption request succeeds");
        datastore
            .physical_disk_enable_adoption(
                &opctx,
                disks_b[0].identity.clone().into(),
            )
            .await
            .expect("adoption request succeeds");

        // Adoption is idempotent
        datastore
            .physical_disk_enable_adoption(
                &opctx,
                disks_b[0].identity.clone().into(),
            )
            .await
            .expect("adoption request succeeds");

        // We should now have 2 adoptable disks
        let adoptable_disks = datastore
            .physical_disk_adoptable_list(&opctx, collection_id)
            .await
            .expect("Failed to look up adoptable disks");
        assert_eq!(adoptable_disks.len(), 2);

        // The remaining 4 disks should be unadopted
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 4);

        // soft-deleting both adoptable disks makes them unadoptable again
        DataStore::physical_disk_adoption_request_delete_on_connection(
            &conn,
            disks_a[0].identity.clone().into(),
        )
        .await
        .expect("successful soft delete");
        DataStore::physical_disk_adoption_request_delete_on_connection(
            &conn,
            disks_b[0].identity.clone().into(),
        )
        .await
        .expect("successful soft delete");
        let adoptable_disks = datastore
            .physical_disk_adoptable_list(&opctx, collection_id)
            .await
            .expect("Failed to look up adoptable disks");
        assert!(adoptable_disks.is_empty());

        // All 6 disks should be unadopted again
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 6);

        // Adding one of the same disks back again should make it adoptable
        datastore
            .physical_disk_enable_adoption(
                &opctx,
                disks_a[0].identity.clone().into(),
            )
            .await
            .expect("adoption request succeeds");
        let adoptable_disks = datastore
            .physical_disk_adoptable_list(&opctx, collection_id)
            .await
            .expect("Failed to look up adoptable disks");
        assert_eq!(adoptable_disks.len(), 1);

        // We should now only have 5 uninitialized disks
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 5);

        // Making a disk adoption reqeust for a disk that is not in inventory
        // will not make the disk show up as adoptable. We allow the request to
        // succeed but not the adoption. This means an operator can insert the
        // disk after the fact.
        let mut disk_not_in_inventory: PhysicalDiskManufacturerIdentity =
            disks_a[0].identity.clone().into();
        disk_not_in_inventory.vendor = "some-other-vendor".to_string();
        datastore
            .physical_disk_enable_adoption(&opctx, disk_not_in_inventory)
            .await
            .expect("adoption request succeeds");
        let adoptable_disks = datastore
            .physical_disk_adoptable_list(&opctx, collection_id)
            .await
            .expect("Failed to look up adoptable disks");
        assert_eq!(adoptable_disks.len(), 1);

        // We should still have only 5 uninitialized disks
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 5);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_insert_disk_and_zpool_fails_without_adoption() {
        let logctx = dev::test_setup_log(
            "physical_disk_insert_disk_and_zpool_fails_without_adoption",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let sled_a = create_test_sled(&datastore).await;

        // No inventory -> No unadopted disks
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(
                &opctx,
                CollectionUuid::new_v4(), // Collection that does not exist
            )
            .await
            .expect("Failed to look up unadopted disks");
        assert!(unadopted_disks.is_empty());

        // Create an inventory disk
        let mut builder = nexus_inventory::CollectionBuilder::new("test");
        let inv_disk = create_inv_disk("serial-001".to_string(), 1);
        add_sled_to_inventory(&mut builder, &sled_a, vec![inv_disk.clone()]);
        let collection = builder.build();
        let collection_id = collection.id;
        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert collection");

        // Now when we list the unadopted disks, we should see our disk.
        let unadopted_disks = datastore
            .physical_disk_unadopted_list(&opctx, collection_id)
            .await
            .expect("Failed to list unadopted disks");
        assert_eq!(unadopted_disks.len(), 1);

        // Fail to create a control plane object for our disk because we have
        // not called `datastore.physical_disk_enable_adoption`.
        let (disk, zpool) = create_disk_zpool_combo(sled_a.id(), &inv_disk);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk, zpool)
            .await
            .expect_err("insertion fails without an adoption request");

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn physical_disk_list_new_inventory_on_connection() {
        let logctx = dev::test_setup_log(
            "physical_disk_list_new_inventory_on_connection",
        );
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());
        let conn = datastore.pool_connection_for_tests().await.unwrap();
        let sled = create_test_sled(&datastore).await;

        // No inventory -> No new disks
        let new = datastore
            .physical_disk_list_new_inventory_on_connection(
                &conn,
                CollectionUuid::new_v4(),
            )
            .await
            .expect("failed to list new disks");

        assert!(new.is_empty());

        // Create an inventory disk
        let mut builder = nexus_inventory::CollectionBuilder::new("test");
        let inv_disk = create_inv_disk("serial-001".to_string(), 1);
        add_sled_to_inventory(&mut builder, &sled, vec![inv_disk.clone()]);
        let collection = builder.build();
        let collection_id = collection.id;

        datastore
            .inventory_insert_collection(&opctx, &collection)
            .await
            .expect("failed to insert collection");

        // We should get this disk returned as brand new
        let new = datastore
            .physical_disk_list_new_inventory_on_connection(
                &conn,
                collection_id,
            )
            .await
            .expect("failed to list new disks");

        assert_eq!(new.len(), 1);

        // Enabling adoption for all new disks should ensure this disk is
        // adopted
        datastore
            .physical_disk_enable_adoption_for_all_new_disks_in_inventory(
                &opctx,
                collection_id,
            )
            .await
            .expect("enabling adoption should succeed");

        // We should no longer treat this disk as new since it has an
        // adoption request.
        let new = datastore
            .physical_disk_list_new_inventory_on_connection(
                &conn,
                collection_id,
            )
            .await
            .expect("failed to list new disks");
        assert!(new.is_empty());

        // Adopting the disk should also continue to not return it as an new disk
        let (disk, zpool) = create_disk_zpool_combo(sled.id(), &inv_disk);
        datastore
            .physical_disk_and_zpool_insert(&opctx, disk.clone(), zpool)
            .await
            .unwrap();
        let new = datastore
            .physical_disk_list_new_inventory_on_connection(
                &conn,
                collection_id,
            )
            .await
            .expect("failed to list new disks");
        assert!(new.is_empty());

        // Expunging the disk should also continue to not return it as an new disk
        use nexus_db_schema::schema::physical_disk::dsl;
        diesel::update(dsl::physical_disk)
            .filter(dsl::id.eq(to_db_typed_uuid(disk.id())))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::disk_policy.eq(PhysicalDiskPolicy::Expunged))
            .execute_async(
                &*datastore.pool_connection_authorized(&opctx).await.unwrap(),
            )
            .await
            .unwrap();
        let new = datastore
            .physical_disk_list_new_inventory_on_connection(
                &conn,
                collection_id,
            )
            .await
            .expect("failed to list new disks");
        assert!(new.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
