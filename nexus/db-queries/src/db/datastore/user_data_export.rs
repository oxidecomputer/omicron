// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`UserDataExportRecord`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db::model::Image;
use crate::db::model::Snapshot;
use crate::db::model::SqlU16;
use crate::db::model::UserDataExportRecord;
use crate::db::model::UserDataExportResource;
use crate::db::model::UserDataExportResourceType;
use crate::db::model::UserDataExportState;
use crate::db::model::ipv6;
use crate::db::model::to_db_typed_uuid;
use crate::db::update_and_check::UpdateAndCheck;
use crate::db::update_and_check::UpdateStatus;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::OptionalExtension;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_lookup::DbConnection;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupResult;
use omicron_common::api::external::UpdateResult;
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::UserDataExportUuid;
use omicron_uuid_kinds::VolumeUuid;
use std::net::SocketAddrV6;
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct UserDataExportChangeset {
    /// Resources that need records created for them
    pub request_required: Vec<UserDataExportResource>,

    /// Records that need the create saga run
    pub create_required: Vec<UserDataExportRecord>,

    /// Records that need the delete saga run
    pub delete_required: Vec<UserDataExportRecord>,
}

impl DataStore {
    async fn user_data_export_create_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<Error>,
        id: UserDataExportUuid,
        resource: UserDataExportResource,
    ) -> Result<UserDataExportRecord, diesel::result::Error> {
        let user_data_export = UserDataExportRecord::new(id, resource);

        use nexus_db_schema::schema::user_data_export::dsl;

        // Has an export with this id been created already? If so,
        // return that.
        let existing_export: Option<UserDataExportRecord> =
            dsl::user_data_export
                .filter(dsl::id.eq(to_db_typed_uuid(id)))
                .select(UserDataExportRecord::as_select())
                .first_async(conn)
                .await
                .optional()?;

        if let Some(existing_export) = existing_export {
            return Ok(existing_export);
        }

        // Does the resource being referenced still exist?
        let resource_id: Uuid = match resource {
            UserDataExportResource::Snapshot { id } => {
                use nexus_db_schema::schema::snapshot::dsl as snapshot_dsl;

                let snapshot: Option<Snapshot> = snapshot_dsl::snapshot
                    .filter(snapshot_dsl::id.eq(id))
                    .select(Snapshot::as_select())
                    .first_async(conn)
                    .await
                    .optional()?;

                let still_here = match snapshot {
                    Some(snapshot) => snapshot.time_deleted().is_none(),
                    None => false,
                };

                if !still_here {
                    return Err(err.bail(Error::non_resourcetype_not_found(
                        format!("snapshot with id {id} not found or deleted"),
                    )));
                }

                id
            }

            UserDataExportResource::Image { id } => {
                use nexus_db_schema::schema::image::dsl as image_dsl;

                let image: Option<Image> = image_dsl::image
                    .filter(image_dsl::id.eq(id))
                    .select(Image::as_select())
                    .first_async(conn)
                    .await
                    .optional()?;

                let still_here = match image {
                    Some(image) => image.time_deleted().is_none(),
                    None => false,
                };

                if !still_here {
                    return Err(err.bail(Error::non_resourcetype_not_found(
                        format!("image with id {id} not found or deleted"),
                    )));
                }

                id
            }
        };

        // Does an export object for this resource exist? The unique index would
        // catch this but this is our opportunity to return a nicer error type.
        let existing_export: Option<UserDataExportRecord> =
            dsl::user_data_export
                .filter(dsl::resource_id.eq(resource_id))
                .filter(dsl::state.ne(UserDataExportState::Deleted))
                .select(UserDataExportRecord::as_select())
                .first_async(conn)
                .await
                .optional()?;

        if existing_export.is_some() {
            return Err(err.bail(Error::conflict(format!(
                "export already exists for resource {resource_id}"
            ))));
        }

        // Otherwise, insert the new export object
        let rows_inserted = diesel::insert_into(dsl::user_data_export)
            .values(user_data_export.clone())
            .execute_async(conn)
            .await?;

        if rows_inserted != 1 {
            return Err(err.bail(Error::internal_error(&format!(
                "{rows_inserted} rows inserted!"
            ))));
        }

        Ok(user_data_export)
    }

    pub async fn user_data_export_create_for_snapshot(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        snapshot_id: Uuid,
    ) -> CreateResult<UserDataExportRecord> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("user_data_export_create_for_snapshot")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::user_data_export_create_in_txn(
                        &conn,
                        err,
                        id,
                        UserDataExportResource::Snapshot { id: snapshot_id },
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn user_data_export_create_for_image(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        image_id: Uuid,
    ) -> CreateResult<UserDataExportRecord> {
        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("user_data_export_create_for_image")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    Self::user_data_export_create_in_txn(
                        &conn,
                        err,
                        id,
                        UserDataExportResource::Image { id: image_id },
                    )
                    .await
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    pub async fn user_data_export_lookup_by_id(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .select(UserDataExportRecord::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return any non-Deleted user data export row for a volume
    pub async fn user_data_export_lookup_by_volume_id(
        &self,
        opctx: &OpContext,
        id: VolumeUuid,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::volume_id.eq(to_db_typed_uuid(id)))
            .filter(dsl::state.ne(UserDataExportState::Deleted))
            .select(UserDataExportRecord::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return any non-Deleted user data export row for a snapshot
    pub async fn user_data_export_lookup_for_snapshot(
        &self,
        opctx: &OpContext,
        snapshot_id: Uuid,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::resource_type.eq(UserDataExportResourceType::Snapshot))
            .filter(dsl::resource_id.eq(snapshot_id))
            .filter(dsl::state.ne(UserDataExportState::Deleted))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Return any non-Deleted user data export row for an image
    pub async fn user_data_export_lookup_for_image(
        &self,
        opctx: &OpContext,
        image_id: Uuid,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::resource_type.eq(UserDataExportResourceType::Image))
            .filter(dsl::resource_id.eq(image_id))
            .filter(dsl::state.ne(UserDataExportState::Deleted))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Compute the work required related to user data export objects. Return:
    ///
    /// - which resources do not have any user data export object and need a
    ///   record created
    ///
    /// - which user data export records are in state Requested that need the
    ///   associated create saga run
    ///
    /// - which user data export records have been marked Deleted that need the
    ///   associated delete saga run
    ///
    /// This function also marks user data export records as deleted if the
    /// associated resource was itself delete.
    pub async fn compute_user_data_export_changeset(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<UserDataExportChangeset> {
        opctx.check_complex_operations_allowed()?;

        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::image::dsl as image_dsl;
        use nexus_db_schema::schema::snapshot::dsl as snapshot_dsl;
        use nexus_db_schema::schema::user_data_export::dsl;

        let mut changeset = UserDataExportChangeset::default();

        // Check for undeleted snapshots or images that do not yet have user
        // data export objects.

        let snapshots: Vec<Snapshot> = snapshot_dsl::snapshot
            .left_join(dsl::user_data_export.on(
                dsl::resource_id.eq(snapshot_dsl::id).and(
                    dsl::state.nullable().ne(UserDataExportState::Deleted),
                ),
            ))
            .filter(snapshot_dsl::time_deleted.is_null())
            // `is_null` will match on cases where there isn't an export row
            .filter(dsl::id.is_null())
            .select(Snapshot::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for snapshot in snapshots {
            changeset
                .request_required
                .push(UserDataExportResource::Snapshot { id: snapshot.id() });
        }

        let project_images: Vec<Image> = image_dsl::image
            .left_join(dsl::user_data_export.on(
                dsl::resource_id.eq(image_dsl::id).and(
                    dsl::state.nullable().ne(UserDataExportState::Deleted),
                ),
            ))
            .filter(image_dsl::time_deleted.is_null())
            .filter(image_dsl::project_id.is_not_null())
            // `is_null` will match on cases where there isn't an export row
            .filter(dsl::id.is_null())
            .select(Image::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for image in project_images {
            changeset
                .request_required
                .push(UserDataExportResource::Image { id: image.id() });
        }

        let silo_images: Vec<Image> = image_dsl::image
            .left_join(dsl::user_data_export.on(
                dsl::resource_id.eq(image_dsl::id).and(
                    dsl::state.nullable().ne(UserDataExportState::Deleted),
                ),
            ))
            .filter(image_dsl::time_deleted.is_null())
            .filter(image_dsl::project_id.is_null())
            // `is_null` will match on cases where there isn't an export row
            .filter(dsl::id.is_null())
            .select(Image::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for image in silo_images {
            changeset
                .request_required
                .push(UserDataExportResource::Image { id: image.id() });
        }

        // Delete any user data export record where the higher level object
        // (snapshot, image) was soft or hard deleted.

        diesel::update(dsl::user_data_export)
            .filter(dsl::resource_type.eq(UserDataExportResourceType::Snapshot))
            .filter(diesel::dsl::not(
                dsl::resource_id.eq_any(
                    snapshot_dsl::snapshot
                        .filter(snapshot_dsl::time_deleted.is_null())
                        .select(snapshot_dsl::id),
                ),
            ))
            .set(dsl::resource_deleted.eq(true))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        diesel::update(dsl::user_data_export)
            .filter(dsl::resource_type.eq(UserDataExportResourceType::Image))
            .filter(diesel::dsl::not(
                dsl::resource_id.eq_any(
                    image_dsl::image
                        .filter(image_dsl::time_deleted.is_null())
                        .select(image_dsl::id),
                ),
            ))
            .set(dsl::resource_deleted.eq(true))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        let records: Vec<UserDataExportRecord> = dsl::user_data_export
            .filter(dsl::resource_deleted.eq(true))
            .select(UserDataExportRecord::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for record in records {
            changeset.delete_required.push(record);
        }

        // Running the create saga is also required for any record in state
        // Requested - do this after marking the records as deleted above so
        // this query can filter on that.

        let mut records = dsl::user_data_export
            .filter(dsl::state.eq(UserDataExportState::Requested))
            .filter(dsl::resource_deleted.eq(false))
            .select(UserDataExportRecord::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        changeset.create_required.append(&mut records);

        Ok(changeset)
    }

    /// Mark any records where the Pantry address is not in the list of
    /// in-service addresses as deleted.
    ///
    /// Returns how many records were marked for deletion.
    pub async fn user_data_export_mark_expunged_deleted(
        &self,
        opctx: &OpContext,
        in_service_pantries: Vec<ipv6::Ipv6Addr>,
    ) -> UpdateResult<usize> {
        opctx.check_complex_operations_allowed()?;

        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        diesel::update(dsl::user_data_export)
            .filter(diesel::dsl::not(
                dsl::pantry_ip.eq_any(in_service_pantries),
            ))
            .set(dsl::resource_deleted.eq(true))
            .execute_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn user_data_export_mark_deleted(
        &self,
        id: UserDataExportUuid,
    ) -> DeleteResult {
        let conn = self.pool_connection_unauthorized().await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .set(dsl::resource_deleted.eq(true))
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn set_user_data_export_requested_to_assigning(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        operating_saga_id: Uuid,
        generation: i64,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_data_export::dsl;

        // Otherwise, need to impl QueryFragment for TypedUuid<_>!
        let untyped_id: Uuid = id.into_untyped_uuid();

        let updated = diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(untyped_id))
            .filter(dsl::state.eq(UserDataExportState::Requested))
            .filter(dsl::operating_saga_id.is_null())
            .filter(dsl::generation.lt(generation))
            .filter(dsl::resource_deleted.eq(false))
            .set((
                dsl::state.eq(UserDataExportState::Assigning),
                dsl::operating_saga_id.eq(operating_saga_id),
                dsl::generation.eq(generation),
            ))
            .check_if_exists::<UserDataExportRecord>(untyped_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    // Return Ok if this call set the fields
                    let ok_conditions = [
                        record.state() == UserDataExportState::Assigning,
                        record.operating_saga_id() == Some(operating_saga_id),
                        record.generation() == generation,
                    ];

                    if ok_conditions.into_iter().all(|v| v) {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "failed to transition {:?} from requested to \
                            assigning with operating saga id {} generation {}",
                            record, operating_saga_id, generation,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    pub async fn unset_user_data_export_requested_to_assigning(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        operating_saga_id: Uuid,
        generation: i64,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_data_export::dsl;

        // Otherwise, need to impl QueryFragment for TypedUuid<_>!
        let untyped_id: Uuid = id.into_untyped_uuid();

        let updated = diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(untyped_id))
            .filter(dsl::state.eq(UserDataExportState::Assigning))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(dsl::generation.eq(generation))
            .set((
                dsl::state.eq(UserDataExportState::Requested),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<UserDataExportRecord>(untyped_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    // If a previous call to this function "unlocked" the
                    // record, and something else has bumped the generation
                    // number, then return Ok
                    if record.generation() > generation {
                        Ok(())
                    } else {
                        // Return Ok if this call set the fields
                        let ok_conditions = [
                            record.state() == UserDataExportState::Requested,
                            record.operating_saga_id() == None,
                            record.generation() == generation,
                        ];

                        if ok_conditions.into_iter().all(|v| v) {
                            Ok(())
                        } else {
                            Err(Error::conflict(format!(
                                "failed to transition {:?} from assigning to \
                                requested with operating saga id {:?} \
                                generation {}",
                                record, operating_saga_id, generation,
                            )))
                        }
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    pub async fn set_user_data_export_assigning_to_live(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        operating_saga_id: Uuid,
        generation: i64,
        pantry_address: SocketAddrV6,
        volume_id: VolumeUuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_data_export::dsl;

        // Otherwise, need to impl QueryFragment for TypedUuid<_>!
        let untyped_id: Uuid = id.into_untyped_uuid();

        let updated = diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(untyped_id))
            .filter(dsl::state.eq(UserDataExportState::Assigning))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(dsl::generation.eq(generation))
            .set((
                dsl::state.eq(UserDataExportState::Live),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
                dsl::pantry_ip.eq(ipv6::Ipv6Addr::from(pantry_address.ip())),
                dsl::pantry_port.eq(SqlU16::from(pantry_address.port())),
                dsl::volume_id.eq(to_db_typed_uuid(volume_id)),
            ))
            .check_if_exists::<UserDataExportRecord>(untyped_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    // If a previous call to this function "unlocked" the
                    // record, and something else has bumped the generation
                    // number, then return Ok
                    if record.generation() > generation {
                        Ok(())
                    } else {
                        // Return Ok if this call set the fields
                        let ok_conditions = [
                            record.state() == UserDataExportState::Live,
                            record.operating_saga_id() == None,
                            record.pantry_address() == Some(pantry_address),
                            record.volume_id() == Some(volume_id),
                        ];

                        if ok_conditions.into_iter().all(|v| v) {
                            Ok(())
                        } else {
                            Err(Error::conflict(format!(
                                "failed to transition {:?} from assigning to \
                                live with operating saga id {} generation {} \
                                pantry_address {} volume {}",
                                record,
                                operating_saga_id,
                                generation,
                                pantry_address,
                                volume_id,
                            )))
                        }
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    pub async fn set_user_data_export_live_to_deleting(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        operating_saga_id: Uuid,
        generation: i64,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_data_export::dsl;

        // Otherwise, need to impl QueryFragment for TypedUuid<_>!
        let untyped_id: Uuid = id.into_untyped_uuid();

        let updated = diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(untyped_id))
            .filter(dsl::state.eq(UserDataExportState::Live))
            .filter(dsl::operating_saga_id.is_null())
            .filter(dsl::generation.lt(generation))
            .filter(dsl::resource_deleted.eq(true))
            .set((
                dsl::state.eq(UserDataExportState::Deleting),
                dsl::operating_saga_id.eq(operating_saga_id),
                dsl::generation.eq(generation),
            ))
            .check_if_exists::<UserDataExportRecord>(untyped_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    // Return Ok if this call set the fields
                    let ok_conditions = [
                        record.state() == UserDataExportState::Deleting,
                        record.operating_saga_id() == Some(operating_saga_id),
                        record.generation() == generation,
                    ];

                    if ok_conditions.into_iter().all(|v| v) {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "failed to transition {:?} from live to \
                            deleting with operating saga id {} generation {}",
                            record, operating_saga_id, generation,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    pub async fn unset_user_data_export_live_to_deleting(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        operating_saga_id: Uuid,
        generation: i64,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_data_export::dsl;

        // Otherwise, need to impl QueryFragment for TypedUuid<_>!
        let untyped_id: Uuid = id.into_untyped_uuid();

        let updated = diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(untyped_id))
            .filter(dsl::state.eq(UserDataExportState::Deleting))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(dsl::generation.eq(generation))
            .set((
                dsl::state.eq(UserDataExportState::Live),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<UserDataExportRecord>(untyped_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    // If a previous call to this function "unlocked" the
                    // record, and something else has bumped the generation
                    // number, then return Ok
                    if record.generation() > generation {
                        Ok(())
                    } else {
                        // Return Ok if this call set the fields
                        let ok_conditions = [
                            record.state() == UserDataExportState::Live,
                            record.operating_saga_id() == None,
                            record.generation() == generation,
                        ];

                        if ok_conditions.into_iter().all(|v| v) {
                            Ok(())
                        } else {
                            Err(Error::conflict(format!(
                                "failed to transition {:?} from deleting to \
                                live with operating saga id {:?} generation {}",
                                record, operating_saga_id, generation,
                            )))
                        }
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    pub async fn set_user_data_export_deleting_to_deleted(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
        operating_saga_id: Uuid,
        generation: i64,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_data_export::dsl;

        // Otherwise, need to impl QueryFragment for TypedUuid<_>!
        let untyped_id: Uuid = id.into_untyped_uuid();

        let updated = diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(untyped_id))
            .filter(dsl::state.eq(UserDataExportState::Deleting))
            .filter(dsl::operating_saga_id.eq(operating_saga_id))
            .filter(dsl::generation.eq(generation))
            .set((
                dsl::state.eq(UserDataExportState::Deleted),
                dsl::operating_saga_id.eq(Option::<Uuid>::None),
            ))
            .check_if_exists::<UserDataExportRecord>(untyped_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    // Return Ok if this call set the fields - do not need to
                    // check if the generation number bumped here, as no
                    // saga will start from the Deleted state
                    let ok_conditions = [
                        record.state() == UserDataExportState::Deleted,
                        record.operating_saga_id() == None,
                        record.generation() == generation,
                    ];

                    if ok_conditions.into_iter().all(|v| v) {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "failed to transition {:?} from deleting to \
                            deleted with operating saga id {:?} generation {}",
                            record, operating_saga_id, generation,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }

    pub async fn set_user_data_export_requested_to_deleted(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
    ) -> Result<(), Error> {
        use nexus_db_schema::schema::user_data_export::dsl;

        // Otherwise, need to impl QueryFragment for TypedUuid<_>!
        let untyped_id: Uuid = id.into_untyped_uuid();

        let updated = diesel::update(dsl::user_data_export)
            .filter(dsl::id.eq(untyped_id))
            .filter(dsl::state.eq(UserDataExportState::Requested))
            .filter(dsl::operating_saga_id.is_null())
            .filter(dsl::resource_deleted.eq(true))
            .set((dsl::state.eq(UserDataExportState::Deleted),))
            .check_if_exists::<UserDataExportRecord>(untyped_id)
            .execute_and_check(&*self.pool_connection_authorized(opctx).await?)
            .await;

        match updated {
            Ok(result) => match result.status {
                UpdateStatus::Updated => Ok(()),

                UpdateStatus::NotUpdatedButExists => {
                    let record = result.found;

                    // Return Ok if this call updated the record
                    if record.state() == UserDataExportState::Deleted {
                        Ok(())
                    } else {
                        Err(Error::conflict(format!(
                            "failed to transition {:?} from requested to \
                            deleted",
                            record,
                        )))
                    }
                }
            },

            Err(e) => Err(public_error_from_diesel(e, ErrorHandler::Server)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::authz;
    use crate::db::model::SnapshotState;

    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::create_project;
    use crate::db::pub_test_utils::helpers::create_project_image;
    use crate::db::pub_test_utils::helpers::create_project_snapshot;

    use nexus_db_lookup::LookupPath;

    use omicron_test_utils::dev;
    use omicron_uuid_kinds::VolumeUuid;

    use std::collections::BTreeSet;
    use std::net::Ipv6Addr;

    const PROJECT_NAME: &str = "bobs-barrel-of-bits";
    const LARGE_NUMBER_OF_ROWS: usize = 3000;

    #[tokio::test]
    async fn test_resource_id_collision() {
        let logctx = dev::test_setup_log("test_resource_id_collision");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let snapshot = create_project_snapshot(
            &opctx,
            &datastore,
            &authz_project,
            Uuid::new_v4(),
            "snap",
        )
        .await;

        datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                snapshot.id(),
            )
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                snapshot.id(),
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that an empty changeset is returned when there are no records.
    #[tokio::test]
    async fn test_changeset_nothing_noop() {
        let logctx = dev::test_setup_log("test_changeset_nothing_noop");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that an empty changeset is returned when snapshot and image
    /// records have an associated user data export object in state Live
    /// already.
    #[tokio::test]
    async fn test_changeset_noop() {
        let logctx = dev::test_setup_log("test_changeset_noop");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let snapshot = create_project_snapshot(
            &opctx,
            &datastore,
            &authz_project,
            Uuid::new_v4(),
            "snap",
        )
        .await;

        let record = datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                snapshot.id(),
            )
            .await
            .unwrap();

        let operating_saga_id = Uuid::new_v4();

        datastore
            .set_user_data_export_requested_to_assigning(
                &opctx,
                record.id(),
                operating_saga_id,
                1,
            )
            .await
            .unwrap();

        datastore
            .set_user_data_export_assigning_to_live(
                &opctx,
                record.id(),
                operating_saga_id,
                1,
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0),
                VolumeUuid::new_v4(),
            )
            .await
            .unwrap();

        let image =
            create_project_image(&opctx, &datastore, &authz_project, "image")
                .await;

        let record = datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.id(),
            )
            .await
            .unwrap();

        datastore
            .set_user_data_export_requested_to_assigning(
                &opctx,
                record.id(),
                operating_saga_id,
                1,
            )
            .await
            .unwrap();

        datastore
            .set_user_data_export_assigning_to_live(
                &opctx,
                record.id(),
                operating_saga_id,
                1,
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0),
                VolumeUuid::new_v4(),
            )
            .await
            .unwrap();

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that adding a record is required if a snapshot does not have an
    /// associated user data export record.
    #[tokio::test]
    async fn test_changeset_create_snapshot() {
        let logctx = dev::test_setup_log("test_changeset_create_snapshot");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let snapshot = create_project_snapshot(
            &opctx,
            &datastore,
            &authz_project,
            Uuid::new_v4(),
            "snap",
        )
        .await;

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert_eq!(changeset.request_required.len(), 1);
        assert_eq!(
            changeset.request_required[0],
            UserDataExportResource::Snapshot { id: snapshot.id() }
        );
        assert_eq!(changeset.create_required.len(), 0);
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that adding a record is required if an image does not have an
    /// associated user data export record.
    #[tokio::test]
    async fn test_changeset_create_image() {
        let logctx = dev::test_setup_log("test_changeset_create_image");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let image =
            create_project_image(&opctx, &datastore, &authz_project, "image")
                .await;

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert_eq!(changeset.request_required.len(), 1);
        assert_eq!(
            changeset.request_required[0],
            UserDataExportResource::Image { id: image.id() }
        );
        assert_eq!(changeset.create_required.len(), 0);
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that deletion of the associated user data export object is
    /// required when the snapshot is deleted.
    #[tokio::test]
    async fn test_changeset_delete_snapshot() {
        let logctx = dev::test_setup_log("test_changeset_delete_snapshot");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let snapshot = create_project_snapshot(
            &opctx,
            &datastore,
            &authz_project,
            Uuid::new_v4(),
            "snap",
        )
        .await;

        let (.., authz_snapshot) = LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot.id())
            .lookup_for(authz::Action::Read)
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                snapshot.id(),
            )
            .await
            .unwrap();

        datastore
            .project_delete_snapshot(
                &opctx,
                &authz_snapshot,
                &snapshot,
                vec![SnapshotState::Creating],
            )
            .await
            .unwrap();

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert_eq!(changeset.delete_required.len(), 1);
        assert_eq!(
            changeset.delete_required[0].resource(),
            UserDataExportResource::Snapshot { id: snapshot.id() }
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that deletion of the associated user data export object is
    /// required when the image is deleted.
    #[tokio::test]
    async fn test_changeset_delete_image() {
        let logctx = dev::test_setup_log("test_changeset_delete_image");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let image =
            create_project_image(&opctx, &datastore, &authz_project, "image")
                .await;

        datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.id(),
            )
            .await
            .unwrap();

        let (.., authz_image, db_image) = LookupPath::new(&opctx, datastore)
            .project_image_id(image.id())
            .fetch_for(authz::Action::Read)
            .await
            .unwrap();

        datastore
            .project_image_delete(&opctx, &authz_image, db_image)
            .await
            .unwrap();

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert_eq!(changeset.delete_required.len(), 1);
        assert_eq!(
            changeset.delete_required[0].resource(),
            UserDataExportResource::Image { id: image.id() }
        );

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert the DB queries can deal with a large number of rows when finding
    /// records to create.
    #[tokio::test]
    async fn test_changeset_create_large() {
        let logctx = dev::test_setup_log("test_changeset_create_large");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let mut created_snapshot_ids: BTreeSet<Uuid> = BTreeSet::default();

        for i in 0..LARGE_NUMBER_OF_ROWS {
            let snapshot_string = format!("snap{i}");

            let snapshot = create_project_snapshot(
                &opctx,
                &datastore,
                &authz_project,
                Uuid::new_v4(),
                &snapshot_string,
            )
            .await;

            created_snapshot_ids.insert(snapshot.id());
        }

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert_eq!(changeset.request_required.len(), LARGE_NUMBER_OF_ROWS);
        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        let mut changeset_snapshot_ids: BTreeSet<Uuid> = BTreeSet::default();

        for snapshot in &changeset.request_required {
            let UserDataExportResource::Snapshot { id } = snapshot else {
                panic!("wrong changeset resource");
            };

            changeset_snapshot_ids.insert(*id);
        }

        assert_eq!(created_snapshot_ids, changeset_snapshot_ids);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert the DB queries can deal with a large number of rows when there's
    /// nothing to do
    #[tokio::test]
    async fn test_changeset_noop_large() {
        let logctx = dev::test_setup_log("test_changeset_noop_large");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        for i in 0..LARGE_NUMBER_OF_ROWS {
            let snapshot_string = format!("snap{i}");

            let snapshot = create_project_snapshot(
                &opctx,
                &datastore,
                &authz_project,
                Uuid::new_v4(),
                &snapshot_string,
            )
            .await;

            let record = datastore
                .user_data_export_create_for_snapshot(
                    &opctx,
                    UserDataExportUuid::new_v4(),
                    snapshot.id(),
                )
                .await
                .unwrap();

            let operating_saga_id = Uuid::new_v4();

            datastore
                .set_user_data_export_requested_to_assigning(
                    &opctx,
                    record.id(),
                    operating_saga_id,
                    1,
                )
                .await
                .unwrap();

            datastore
                .set_user_data_export_assigning_to_live(
                    &opctx,
                    record.id(),
                    operating_saga_id,
                    1,
                    SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0),
                    VolumeUuid::new_v4(),
                )
                .await
                .unwrap();
        }

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert the DB queries can deal with a large number of rows when finding
    /// records to delete
    #[tokio::test]
    async fn test_changeset_delete_large() {
        let logctx = dev::test_setup_log("test_changeset_delete_large");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let mut created_snapshot_ids: BTreeSet<Uuid> = BTreeSet::default();

        for i in 0..LARGE_NUMBER_OF_ROWS {
            let snapshot_string = format!("snap{i}");

            let snapshot = create_project_snapshot(
                &opctx,
                &datastore,
                &authz_project,
                Uuid::new_v4(),
                &snapshot_string,
            )
            .await;

            let (.., authz_snapshot) = LookupPath::new(&opctx, datastore)
                .snapshot_id(snapshot.id())
                .lookup_for(authz::Action::Read)
                .await
                .unwrap();

            datastore
                .user_data_export_create_for_snapshot(
                    &opctx,
                    UserDataExportUuid::new_v4(),
                    snapshot.id(),
                )
                .await
                .unwrap();

            datastore
                .project_delete_snapshot(
                    &opctx,
                    &authz_snapshot,
                    &snapshot,
                    vec![SnapshotState::Creating],
                )
                .await
                .unwrap();

            created_snapshot_ids.insert(snapshot.id());
        }

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert_eq!(changeset.delete_required.len(), LARGE_NUMBER_OF_ROWS);

        let mut changeset_snapshot_ids: BTreeSet<Uuid> = BTreeSet::default();

        for record in &changeset.delete_required {
            let UserDataExportResource::Snapshot { id } = record.resource()
            else {
                panic!("wrong changeset resource");
            };

            changeset_snapshot_ids.insert(id);
        }

        assert_eq!(created_snapshot_ids, changeset_snapshot_ids);

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Distribute user data export objects to a number of Pantries, then
    /// simulate one of those being expunged, and validate that the affected
    /// records will be marked for deletion.
    #[tokio::test]
    async fn test_delete_records_for_expunged_pantries() {
        let logctx =
            dev::test_setup_log("test_delete_records_for_expunged_pantries");
        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let pantry_ips = [
            SocketAddrV6::new(
                Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0, 0x0, 0x0, 0x1, 0x0),
                0,
                0,
                0,
            ),
            SocketAddrV6::new(
                Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0, 0x0, 0x0, 0x3, 0x0),
                0,
                0,
                0,
            ),
            SocketAddrV6::new(
                Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0, 0x0, 0x0, 0x5, 0x0),
                0,
                0,
                0,
            ),
        ];

        let operating_saga_id = Uuid::new_v4();
        let generation: i64 = 1;

        for i in 0..LARGE_NUMBER_OF_ROWS {
            let snapshot_string = format!("snap{i}");

            let snapshot = create_project_snapshot(
                &opctx,
                &datastore,
                &authz_project,
                Uuid::new_v4(),
                &snapshot_string,
            )
            .await;

            let record = datastore
                .user_data_export_create_for_snapshot(
                    &opctx,
                    UserDataExportUuid::new_v4(),
                    snapshot.id(),
                )
                .await
                .unwrap();

            datastore
                .set_user_data_export_requested_to_assigning(
                    &opctx,
                    record.id(),
                    operating_saga_id,
                    generation,
                )
                .await
                .unwrap();

            datastore
                .set_user_data_export_assigning_to_live(
                    &opctx,
                    record.id(),
                    operating_saga_id,
                    generation,
                    pantry_ips[i % 3],
                    VolumeUuid::new_v4(),
                )
                .await
                .unwrap();
        }

        let in_service_pantries = vec![
            Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0, 0x0, 0x0, 0x1, 0x0)
                .into(),
            Ipv6Addr::new(0xfd00, 0x1122, 0x3344, 0x0, 0x0, 0x0, 0x5, 0x0)
                .into(),
        ];

        datastore
            .user_data_export_mark_expunged_deleted(&opctx, in_service_pantries)
            .await
            .unwrap();

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert_eq!(changeset.delete_required.len(), LARGE_NUMBER_OF_ROWS / 3);

        for record in &changeset.delete_required {
            assert_eq!(
                record.pantry_address(),
                Some(SocketAddrV6::new(
                    Ipv6Addr::new(
                        0xfd00, 0x1122, 0x3344, 0x0, 0x0, 0x0, 0x3, 0x0
                    ),
                    0,
                    0,
                    0,
                )),
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_cannot_transition_to_assigning_if_deleted() {
        let logctx = dev::test_setup_log(
            "test_cannot_transition_to_assigning_if_deleted",
        );

        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let image =
            create_project_image(&opctx, &datastore, &authz_project, "image")
                .await;

        let id = UserDataExportUuid::new_v4();

        let record = datastore
            .user_data_export_create_for_image(&opctx, id, image.identity.id)
            .await
            .unwrap();

        let (.., authz_image, db_image) = LookupPath::new(&opctx, datastore)
            .project_image_id(image.id())
            .fetch_for(authz::Action::Read)
            .await
            .unwrap();

        datastore
            .project_image_delete(&opctx, &authz_image, db_image)
            .await
            .unwrap();

        // Running the changeset computation will set the user data export
        // record's resource_deleted field to true

        let _changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();

        // This shouldn't work anymore

        datastore
            .set_user_data_export_requested_to_assigning(
                &opctx,
                record.id(),
                Uuid::new_v4(),
                1,
            )
            .await
            .unwrap_err();

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that multiple user data export objects can be created, but not be
    /// Live at the same time.
    #[tokio::test]
    async fn test_user_data_export_duplication() {
        let logctx = dev::test_setup_log("test_user_data_export_duplication");

        let log = logctx.log.new(o!());
        let db = TestDatabase::new_with_datastore(&log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let (authz_project, _) =
            create_project(&opctx, &datastore, PROJECT_NAME).await;

        let image =
            create_project_image(&opctx, &datastore, &authz_project, "image")
                .await;

        // The computed changeset should include this image, it doesn't have a
        // record yet.

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();
        assert_eq!(
            &changeset.request_required,
            &[UserDataExportResource::Image { id: image.id() }],
        );
        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        // First, create a user data export object for that image.

        let id = UserDataExportUuid::new_v4();

        let record = datastore
            .user_data_export_create_for_image(&opctx, id, image.identity.id)
            .await
            .unwrap();

        // The computed changeset should report that the create saga is
        // required.

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();
        assert!(changeset.request_required.is_empty());
        assert_eq!(changeset.create_required.len(), 1);
        assert_eq!(changeset.create_required[0].id(), record.id());
        assert!(changeset.delete_required.is_empty());

        // Assert that another record cannot be created for the same image yet.

        datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.identity.id,
            )
            .await
            .unwrap_err();

        // Transition the record to Assigning and assert another record cannot
        // be created yet.

        let operating_saga_id = Uuid::new_v4();

        datastore
            .set_user_data_export_requested_to_assigning(
                &opctx,
                record.id(),
                operating_saga_id,
                1,
            )
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.identity.id,
            )
            .await
            .unwrap_err();

        // The changeset should now be empty.

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();
        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        // Transition to Live, same test.

        datastore
            .set_user_data_export_assigning_to_live(
                &opctx,
                record.id(),
                operating_saga_id,
                1,
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 8080, 0, 0),
                VolumeUuid::new_v4(),
            )
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.identity.id,
            )
            .await
            .unwrap_err();

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();
        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        // Mark the record as deleted.

        datastore.user_data_export_mark_deleted(record.id()).await.unwrap();

        // The changeset should now show that the record needs to have the
        // associated delete saga run.

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();
        assert!(changeset.request_required.is_empty());
        assert!(changeset.create_required.is_empty());
        assert_eq!(changeset.delete_required.len(), 1);
        assert_eq!(changeset.delete_required[0].id(), record.id());

        // Transition to Deleting, same test.

        datastore
            .set_user_data_export_live_to_deleting(
                &opctx,
                record.id(),
                operating_saga_id,
                2,
            )
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.identity.id,
            )
            .await
            .unwrap_err();

        // Transition to Deleted

        datastore
            .set_user_data_export_deleting_to_deleted(
                &opctx,
                record.id(),
                operating_saga_id,
                2,
            )
            .await
            .unwrap();

        // The changeset should now show that a record for this iamge needs to
        // be created, along with the old record still needing the associated
        // delete saga.

        let changeset =
            datastore.compute_user_data_export_changeset(&opctx).await.unwrap();
        assert_eq!(
            &changeset.request_required,
            &[UserDataExportResource::Image { id: image.id() }],
        );
        assert!(changeset.create_required.is_empty());
        assert_eq!(changeset.delete_required.len(), 1);
        assert_eq!(changeset.delete_required[0].id(), record.id());

        // Now it should work.

        datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.identity.id,
            )
            .await
            .unwrap();

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
