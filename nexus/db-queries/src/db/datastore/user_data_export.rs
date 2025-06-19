// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`UserDataExportRecord`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::Image;
use crate::db::model::Snapshot;
use crate::db::model::UserDataExportRecord;
use crate::db::model::UserDataExportResource;
use crate::db::model::UserDataExportResourceType;
use crate::db::model::ipv6;
use crate::db::model::to_db_typed_uuid;
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
use omicron_uuid_kinds::UserDataExportUuid;
use omicron_uuid_kinds::VolumeUuid;
use std::net::SocketAddrV6;
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct UserDataExportChangeset {
    pub create_required: Vec<UserDataExportResource>,
    pub delete_required: Vec<UserDataExportRecord>,
}

impl DataStore {
    async fn user_data_export_create_in_txn(
        conn: &async_bb8_diesel::Connection<DbConnection>,
        err: OptionalError<Error>,
        id: UserDataExportUuid,
        resource: UserDataExportResource,
        pantry_address: SocketAddrV6,
        volume_id: VolumeUuid,
    ) -> Result<UserDataExportRecord, diesel::result::Error> {
        let user_data_export =
            UserDataExportRecord::new(id, resource, pantry_address, volume_id);

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

        // Does an export object for this resource exist?
        let existing_export: Option<UserDataExportRecord> =
            dsl::user_data_export
                .filter(dsl::resource_id.eq(resource_id))
                .select(UserDataExportRecord::as_select())
                .first_async(conn)
                .await
                .optional()?;

        if existing_export.is_some() {
            return Err(err
                .bail(Error::conflict("export already started for resource")));
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
        authz_snapshot: &authz::Snapshot,
        pantry_address: SocketAddrV6,
        volume_id: VolumeUuid,
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
                        UserDataExportResource::Snapshot {
                            id: authz_snapshot.id(),
                        },
                        pantry_address,
                        volume_id,
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
        pantry_address: SocketAddrV6,
        volume_id: VolumeUuid,
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
                        pantry_address,
                        volume_id,
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

    pub async fn user_data_export_delete(
        &self,
        opctx: &OpContext,
        id: UserDataExportUuid,
    ) -> DeleteResult {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        diesel::delete(dsl::user_data_export)
            .filter(dsl::id.eq(to_db_typed_uuid(id)))
            .execute_async(&*conn)
            .await
            .map(|_| ())
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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

    pub async fn user_data_export_lookup_by_volume_id(
        &self,
        opctx: &OpContext,
        id: VolumeUuid,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::volume_id.eq(to_db_typed_uuid(id)))
            .select(UserDataExportRecord::as_select())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn user_data_export_lookup_for_snapshot(
        &self,
        opctx: &OpContext,
        authz_snapshot: &authz::Snapshot,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::resource_type.eq(UserDataExportResourceType::Snapshot))
            .filter(dsl::resource_id.eq(authz_snapshot.id()))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn user_data_export_lookup_for_project_image(
        &self,
        opctx: &OpContext,
        authz_image: &authz::ProjectImage,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::resource_type.eq(UserDataExportResourceType::Image))
            .filter(dsl::resource_id.eq(authz_image.id()))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn user_data_export_lookup_for_silo_image(
        &self,
        opctx: &OpContext,
        authz_image: &authz::SiloImage,
    ) -> LookupResult<Option<UserDataExportRecord>> {
        let conn = self.pool_connection_authorized(opctx).await?;

        use nexus_db_schema::schema::user_data_export::dsl;

        dsl::user_data_export
            .filter(dsl::resource_type.eq(UserDataExportResourceType::Image))
            .filter(dsl::resource_id.eq(authz_image.id()))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Based on which read-only resources exist and are not deleted, return a
    /// changeset indicating which user data export objects need to be created
    /// and which need to be deleted.
    pub async fn user_data_export_changeset(
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
            .left_join(
                dsl::user_data_export.on(dsl::resource_id.eq(snapshot_dsl::id)),
            )
            .filter(snapshot_dsl::time_deleted.is_null())
            // `is_null` will match on cases where there isn't an export row
            .filter(dsl::id.is_null())
            .select(Snapshot::as_select())
            .load_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        for snapshot in snapshots {
            changeset
                .create_required
                .push(UserDataExportResource::Snapshot { id: snapshot.id() });
        }

        let project_images: Vec<Image> = image_dsl::image
            .left_join(
                dsl::user_data_export.on(dsl::resource_id.eq(image_dsl::id)),
            )
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
                .create_required
                .push(UserDataExportResource::Image { id: image.id() });
        }

        let silo_images: Vec<Image> = image_dsl::image
            .left_join(
                dsl::user_data_export.on(dsl::resource_id.eq(image_dsl::id)),
            )
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
                .create_required
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

        Ok(changeset)
    }

    /// Remove any records where the Pantry address is not in the list of
    /// in-service addresses.
    ///
    /// Returns how many records were marked for deletion.
    pub async fn user_data_export_delete_expunged(
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let (.., authz_snapshot) = LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot.id())
            .lookup_for(authz::Action::Read)
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                &authz_snapshot,
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                VolumeUuid::new_v4(),
            )
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                &authz_snapshot,
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                VolumeUuid::new_v4(),
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
            datastore.user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that an empty changeset is returned when snapshot and image
    /// records have an associated user data export object already.
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

        let (.., authz_snapshot) = LookupPath::new(&opctx, datastore)
            .snapshot_id(snapshot.id())
            .lookup_for(authz::Action::Read)
            .await
            .unwrap();

        datastore
            .user_data_export_create_for_snapshot(
                &opctx,
                UserDataExportUuid::new_v4(),
                &authz_snapshot,
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                VolumeUuid::new_v4(),
            )
            .await
            .unwrap();

        let image =
            create_project_image(&opctx, &datastore, &authz_project, "image")
                .await;

        datastore
            .user_data_export_create_for_image(
                &opctx,
                UserDataExportUuid::new_v4(),
                image.id(),
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                VolumeUuid::new_v4(),
            )
            .await
            .unwrap();

        let changeset =
            datastore.user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.create_required.is_empty());
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that creation is required if a snapshot does not have an
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
            datastore.user_data_export_changeset(&opctx).await.unwrap();

        assert_eq!(changeset.create_required.len(), 1);
        assert_eq!(
            changeset.create_required[0],
            UserDataExportResource::Snapshot { id: snapshot.id() }
        );
        assert!(changeset.delete_required.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Assert that creation is required if an image does not have an associated
    /// user data export record.
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
            datastore.user_data_export_changeset(&opctx).await.unwrap();

        assert_eq!(changeset.create_required.len(), 1);
        assert_eq!(
            changeset.create_required[0],
            UserDataExportResource::Image { id: image.id() }
        );
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
                &authz_snapshot,
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                VolumeUuid::new_v4(),
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
            datastore.user_data_export_changeset(&opctx).await.unwrap();

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
                SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                VolumeUuid::new_v4(),
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
            datastore.user_data_export_changeset(&opctx).await.unwrap();

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
            datastore.user_data_export_changeset(&opctx).await.unwrap();

        assert_eq!(changeset.create_required.len(), LARGE_NUMBER_OF_ROWS);
        assert!(changeset.delete_required.is_empty());

        let mut changeset_snapshot_ids: BTreeSet<Uuid> = BTreeSet::default();

        for snapshot in &changeset.create_required {
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

            let (.., authz_snapshot) = LookupPath::new(&opctx, datastore)
                .snapshot_id(snapshot.id())
                .lookup_for(authz::Action::Read)
                .await
                .unwrap();

            datastore
                .user_data_export_create_for_snapshot(
                    &opctx,
                    UserDataExportUuid::new_v4(),
                    &authz_snapshot,
                    SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                    VolumeUuid::new_v4(),
                )
                .await
                .unwrap();
        }

        let changeset =
            datastore.user_data_export_changeset(&opctx).await.unwrap();

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
                    &authz_snapshot,
                    SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0),
                    VolumeUuid::new_v4(),
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
            datastore.user_data_export_changeset(&opctx).await.unwrap();

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
    /// simulate one of those being expunged, and validaate that the affected
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
                    &authz_snapshot,
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
            .user_data_export_delete_expunged(&opctx, in_service_pantries)
            .await
            .unwrap();

        let changeset =
            datastore.user_data_export_changeset(&opctx).await.unwrap();

        assert!(changeset.create_required.is_empty());
        assert_eq!(changeset.delete_required.len(), LARGE_NUMBER_OF_ROWS / 3);

        for record in &changeset.delete_required {
            assert_eq!(
                record.pantry_address(),
                SocketAddrV6::new(
                    Ipv6Addr::new(
                        0xfd00, 0x1122, 0x3344, 0x0, 0x0, 0x0, 0x3, 0x0
                    ),
                    0,
                    0,
                    0,
                ),
            );
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
