use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::model::Image;
use crate::db::model::Project;
use crate::db::model::ProjectImage;
use crate::db::model::Silo;
use crate::db::model::SiloImage;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::public_error_from_diesel;
use nexus_db_model::Name;
use nexus_types::identity::Resource;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use omicron_common::api::external::http_pagination::PaginatedBy;
use ref_cast::RefCast;
use uuid::Uuid;

use super::DataStore;
use nexus_db_errors::OptionalError;

impl DataStore {
    pub async fn project_image_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Image> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use nexus_db_schema::schema::project_image::dsl as project_dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => paginated(
                project_dsl::project_image,
                project_dsl::id,
                &pagparams,
            ),
            PaginatedBy::Name(pagparams) => paginated(
                project_dsl::project_image,
                project_dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(project_dsl::time_deleted.is_null())
        .filter(project_dsl::project_id.eq(authz_project.id()))
        .select(ProjectImage::as_select())
        .load_async::<ProjectImage>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        .map(|v| v.into_iter().map(|v| v.into()).collect())
    }

    pub async fn silo_image_list(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Image> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use nexus_db_schema::schema::silo_image::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::silo_image, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::silo_image,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::time_deleted.is_null())
        .filter(dsl::silo_id.eq(authz_silo.id()))
        .select(SiloImage::as_select())
        .load_async::<SiloImage>(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        .map(|v| v.into_iter().map(|v| v.into()).collect())
    }

    pub async fn silo_image_create(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_image: SiloImage,
    ) -> CreateResult<Image> {
        let image: Image = silo_image.into();
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        let name = image.name().clone();
        let silo_id = image.silo_id;

        use nexus_db_schema::schema::image::dsl;
        let image: Image = Silo::insert_resource(
            silo_id,
            diesel::insert_into(dsl::image)
                .values(image)
                .on_conflict(dsl::id)
                .do_update()
                .set(dsl::time_modified.eq(dsl::time_modified)),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_silo.not_found(),
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::ProjectImage,
                    name.as_str(),
                ),
            ),
        })?;
        Ok(image)
    }

    pub async fn project_image_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        project_image: ProjectImage,
    ) -> CreateResult<Image> {
        let project_id = project_image.project_id;
        let image: Image = project_image.into();
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let name = image.name().clone();

        use nexus_db_schema::schema::image::dsl;
        let image: Image = Project::insert_resource(
            project_id,
            diesel::insert_into(dsl::image)
                .values(image)
                .on_conflict(dsl::id)
                .do_update()
                .set(dsl::time_modified.eq(dsl::time_modified)),
        )
        .insert_and_get_result_async(
            &*self.pool_connection_authorized(opctx).await?,
        )
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_project.not_found(),
            AsyncInsertError::DatabaseError(e) => public_error_from_diesel(
                e,
                ErrorHandler::Conflict(
                    ResourceType::ProjectImage,
                    name.as_str(),
                ),
            ),
        })?;
        Ok(image)
    }

    /// Promote a project image to a silo image.
    ///
    /// Atomically updates the image record (clearing project_id) and adjusts
    /// only the project-level physical provisioning collection in a single
    /// transaction. The PPR row stays alive and silo/fleet totals are
    /// unchanged, eliminating the race window where the PPR would be absent.
    pub async fn project_image_promote(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        authz_project_image: &authz::ProjectImage,
        project_image: &ProjectImage,
    ) -> UpdateResult<Image> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;
        opctx.authorize(authz::Action::Modify, authz_project_image).await?;

        let read_only_phys = nexus_db_model::distributed_disk_physical_bytes(
            nexus_db_model::VirtualDiskBytes(project_image.size.into()),
        );
        let read_only_phys_bytes = crate::db::model::ByteCount::from(
            read_only_phys.into_byte_count(),
        );

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        let image_id = authz_project_image.id();
        let project_id = project_image.project_id;
        let image_name = project_image.name().as_str().to_string();

        let image = self
            .transaction_retry_wrapper("project_image_promote")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let image_name = image_name.clone();
                async move {
                    use nexus_db_schema::schema::image::dsl;

                    // Step 1: Update the image record to clear project_id
                    // (making it silo-scoped).
                    let image = diesel::update(dsl::image)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(image_id))
                        .set((
                            dsl::project_id.eq(None::<Uuid>),
                            dsl::time_modified.eq(Utc::now()),
                        ))
                        .returning(Image::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail(public_error_from_diesel(
                                e,
                                ErrorHandler::Conflict(
                                    ResourceType::SiloImage,
                                    &image_name,
                                ),
                            ))
                        })?;

                    // Step 2: Conditionally subtract read_only from the
                    // project-level physical provisioning collection. If a
                    // surviving disk in this project references the image,
                    // the disk keeps the read_only charge alive at project
                    // level, so we skip the subtraction.
                    diesel::sql_query(
                        "UPDATE omicron.public.physical_provisioning_collection \
                         SET time_modified = now(), \
                             physical_read_only_disk_bytes = \
                               physical_read_only_disk_bytes - \
                               CASE WHEN EXISTS ( \
                                 SELECT 1 \
                                 FROM omicron.public.disk_type_crucible dtc \
                                 INNER JOIN omicron.public.disk d \
                                   ON d.id = dtc.disk_id \
                                 WHERE dtc.origin_image = $1 \
                                   AND d.project_id = $2 \
                                   AND d.time_deleted IS NULL \
                               ) THEN 0 ELSE $3 END \
                         WHERE id = $2",
                    )
                    .bind::<diesel::sql_types::Uuid, _>(image_id)
                    .bind::<diesel::sql_types::Uuid, _>(project_id)
                    .bind::<diesel::sql_types::Int8, _>(
                        read_only_phys_bytes.0.to_bytes() as i64,
                    )
                    .execute_async(&conn)
                    .await?;

                    Ok(image)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(image)
    }

    /// Demote a silo image to a project image.
    ///
    /// Atomically updates the image record (setting project_id) and adjusts
    /// only the project-level physical provisioning collection in a single
    /// transaction. The PPR row stays alive and silo/fleet totals are
    /// unchanged, eliminating the race window where the PPR would be absent.
    pub async fn silo_image_demote(
        &self,
        opctx: &OpContext,
        authz_silo_image: &authz::SiloImage,
        authz_project: &authz::Project,
        silo_image: &SiloImage,
    ) -> UpdateResult<Image> {
        opctx.authorize(authz::Action::Modify, authz_silo_image).await?;
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let read_only_phys = nexus_db_model::distributed_disk_physical_bytes(
            nexus_db_model::VirtualDiskBytes(silo_image.size.into()),
        );
        let read_only_phys_bytes = crate::db::model::ByteCount::from(
            read_only_phys.into_byte_count(),
        );

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;
        let image_id = authz_silo_image.id();
        let target_project_id = authz_project.id();
        let image_name = silo_image.name().as_str().to_string();

        let image = self
            .transaction_retry_wrapper("silo_image_demote")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let image_name = image_name.clone();
                async move {
                    use nexus_db_schema::schema::image::dsl;

                    // Step 1: Update the image record to set project_id
                    // (making it project-scoped).
                    let image: Image = diesel::update(dsl::image)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(image_id))
                        .set((
                            dsl::project_id.eq(Some(target_project_id)),
                            dsl::time_modified.eq(Utc::now()),
                        ))
                        .returning(Image::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail(public_error_from_diesel(
                                e,
                                ErrorHandler::Conflict(
                                    ResourceType::ProjectImage,
                                    &image_name,
                                ),
                            ))
                        })?;

                    // Step 2: Conditionally add read_only to the
                    // project-level physical provisioning collection. If a
                    // disk in the target project already references this
                    // image (and already charged read_only at project
                    // level), we skip the addition to avoid double-counting.
                    diesel::sql_query(
                        "UPDATE omicron.public.physical_provisioning_collection \
                         SET time_modified = now(), \
                             physical_read_only_disk_bytes = \
                               physical_read_only_disk_bytes + \
                               CASE WHEN EXISTS ( \
                                 SELECT 1 \
                                 FROM omicron.public.disk_type_crucible dtc \
                                 INNER JOIN omicron.public.disk d \
                                   ON d.id = dtc.disk_id \
                                 WHERE dtc.origin_image = $1 \
                                   AND d.project_id = $2 \
                                   AND d.time_deleted IS NULL \
                               ) THEN 0 ELSE $3 END \
                         WHERE id = $2",
                    )
                    .bind::<diesel::sql_types::Uuid, _>(image_id)
                    .bind::<diesel::sql_types::Uuid, _>(target_project_id)
                    .bind::<diesel::sql_types::Int8, _>(
                        read_only_phys_bytes.0.to_bytes() as i64,
                    )
                    .execute_async(&conn)
                    .await?;

                    Ok(image)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(image)
    }

    pub async fn silo_image_delete(
        &self,
        opctx: &OpContext,
        authz_image: &authz::SiloImage,
        image: SiloImage,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_image).await?;
        self.image_delete(opctx, image.into()).await
    }

    pub async fn project_image_delete(
        &self,
        opctx: &OpContext,
        authz_image: &authz::ProjectImage,
        image: ProjectImage,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_image).await?;
        self.image_delete(opctx, image.into()).await
    }

    async fn image_delete(
        &self,
        opctx: &OpContext,
        image: Image,
    ) -> DeleteResult {
        use nexus_db_schema::schema::image::dsl;
        diesel::update(dsl::image)
            .filter(dsl::id.eq(image.id()))
            .set(dsl::time_deleted.eq(Utc::now()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Undo a soft-delete by clearing `time_deleted` on an image record.
    /// Used by saga undo paths that need to restore a deleted image.
    pub async fn image_undelete(
        &self,
        opctx: &OpContext,
        image_id: Uuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::image::dsl;
        diesel::update(dsl::image)
            .filter(dsl::id.eq(image_id))
            .set(dsl::time_deleted.eq(Option::<chrono::DateTime<Utc>>::None))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        Ok(())
    }

    /// Atomically create a project image record AND insert its physical
    /// provisioning in a single transaction. This eliminates the race
    /// window where the image record exists but has no PPR entry.
    pub async fn project_image_create_and_account(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        project_image: ProjectImage,
    ) -> CreateResult<Image> {
        let project_id = project_image.project_id;
        let image: Image = project_image.into();
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let name = image.name().clone();
        let read_only_phys = nexus_db_model::distributed_disk_physical_bytes(
            nexus_db_model::VirtualDiskBytes(image.size.into()),
        );
        let read_only_phys_bytes = crate::db::model::ByteCount::from(
            read_only_phys.into_byte_count(),
        );

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        let created_image = self
            .transaction_retry_wrapper("project_image_create_and_account")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let image = image.clone();
                let name = name.clone();
                async move {
                    use crate::db::queries::physical_provisioning_collection_update::PhysicalProvisioningCollectionUpdate;
                    use nexus_db_schema::schema::image::dsl;

                    // Step 1: Create the image record.
                    let created_image: Image = Project::insert_resource(
                        project_id,
                        diesel::insert_into(dsl::image)
                            .values(image)
                            .on_conflict(dsl::id)
                            .do_update()
                            .set(dsl::time_modified.eq(dsl::time_modified)),
                    )
                    .insert_and_get_result_async(&conn)
                    .await
                    .map_err(|e| match e {
                        AsyncInsertError::CollectionNotFound => {
                            err.bail(authz_project.not_found())
                        }
                        AsyncInsertError::DatabaseError(e) => {
                            err.bail(public_error_from_diesel(
                                e,
                                ErrorHandler::Conflict(
                                    ResourceType::ProjectImage,
                                    name.as_str(),
                                ),
                            ))
                        }
                    })?;

                    // Step 2: Insert physical provisioning for the image.
                    let zero: crate::db::model::ByteCount =
                        0.try_into().unwrap();
                    PhysicalProvisioningCollectionUpdate::new_insert_storage(
                        created_image.id(),
                        zero,
                        zero,
                        read_only_phys_bytes,
                        zero,
                        zero,
                        read_only_phys_bytes,
                        project_id,
                        super::StorageType::Image,
                    )
                    .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(
                        &conn,
                    )
                    .await
                    .map_err(|e| {
                        err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                    })?;

                    Ok(created_image)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(created_image)
    }

    /// Atomically create a silo image record AND insert its physical
    /// provisioning in a single transaction.
    pub async fn silo_image_create_and_account(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        silo_image: SiloImage,
    ) -> CreateResult<Image> {
        let image: Image = silo_image.into();
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        let name = image.name().clone();
        let silo_id = image.silo_id;
        let read_only_phys = nexus_db_model::distributed_disk_physical_bytes(
            nexus_db_model::VirtualDiskBytes(image.size.into()),
        );
        let read_only_phys_bytes = crate::db::model::ByteCount::from(
            read_only_phys.into_byte_count(),
        );

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        let created_image = self
            .transaction_retry_wrapper("silo_image_create_and_account")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let image = image.clone();
                let name = name.clone();
                async move {
                    use crate::db::queries::physical_provisioning_collection_update::PhysicalProvisioningCollectionUpdate;
                    use nexus_db_schema::schema::image::dsl;

                    // Step 1: Create the image record.
                    let created_image: Image = Silo::insert_resource(
                        silo_id,
                        diesel::insert_into(dsl::image)
                            .values(image)
                            .on_conflict(dsl::id)
                            .do_update()
                            .set(dsl::time_modified.eq(dsl::time_modified)),
                    )
                    .insert_and_get_result_async(&conn)
                    .await
                    .map_err(|e| match e {
                        AsyncInsertError::CollectionNotFound => {
                            err.bail(authz_silo.not_found())
                        }
                        AsyncInsertError::DatabaseError(e) => {
                            err.bail(public_error_from_diesel(
                                e,
                                ErrorHandler::Conflict(
                                    ResourceType::ProjectImage,
                                    name.as_str(),
                                ),
                            ))
                        }
                    })?;

                    // Step 2: Insert physical provisioning at silo level.
                    let zero: crate::db::model::ByteCount =
                        0.try_into().unwrap();
                    PhysicalProvisioningCollectionUpdate::new_insert_storage_silo_level(
                        created_image.id(),
                        zero,
                        zero,
                        read_only_phys_bytes,
                        silo_id,
                        super::StorageType::Image,
                    )
                    .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(
                        &conn,
                    )
                    .await
                    .map_err(|e| {
                        err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                    })?;

                    Ok(created_image)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })?;

        Ok(created_image)
    }

    /// Atomically delete a project image's physical provisioning AND
    /// soft-delete the image record in a single transaction.
    pub async fn project_image_delete_and_account(
        &self,
        opctx: &OpContext,
        authz_image: &authz::ProjectImage,
        image: ProjectImage,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_image).await?;

        let read_only_phys = nexus_db_model::distributed_disk_physical_bytes(
            nexus_db_model::VirtualDiskBytes(image.size.into()),
        );
        let read_only_phys_bytes = crate::db::model::ByteCount::from(
            read_only_phys.into_byte_count(),
        );
        let image_id = image.id();
        let project_id = image.project_id;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("project_image_delete_and_account")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    use crate::db::queries::physical_provisioning_collection_update::{
                        DedupInfo, PhysicalProvisioningCollectionUpdate,
                    };
                    use nexus_db_schema::schema::image::dsl;

                    // Step 1: Delete physical provisioning (with dedup
                    // against surviving disks).
                    let zero: crate::db::model::ByteCount =
                        0.try_into().unwrap();
                    let dedup = DedupInfo::ImageDelete { image_id };
                    PhysicalProvisioningCollectionUpdate::new_delete_storage_deduped(
                        image_id,
                        zero,
                        zero,
                        read_only_phys_bytes,
                        zero,
                        zero,
                        read_only_phys_bytes,
                        project_id,
                        dedup,
                    )
                    .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(
                        &conn,
                    )
                    .await
                    .map_err(|e| {
                        err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                    })?;

                    // Step 2: Soft-delete the image record.
                    diesel::update(dsl::image)
                        .filter(dsl::id.eq(image_id))
                        .set(dsl::time_deleted.eq(Utc::now()))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
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

    /// Atomically delete a silo image's physical provisioning AND
    /// soft-delete the image record in a single transaction.
    pub async fn silo_image_delete_and_account(
        &self,
        opctx: &OpContext,
        authz_image: &authz::SiloImage,
        image: SiloImage,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_image).await?;

        let read_only_phys = nexus_db_model::distributed_disk_physical_bytes(
            nexus_db_model::VirtualDiskBytes(image.size.into()),
        );
        let read_only_phys_bytes = crate::db::model::ByteCount::from(
            read_only_phys.into_byte_count(),
        );
        let image_id = image.id();
        let silo_id = image.silo_id;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("silo_image_delete_and_account")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
                    use crate::db::queries::physical_provisioning_collection_update::{
                        DedupInfo, PhysicalProvisioningCollectionUpdate,
                    };
                    use nexus_db_schema::schema::image::dsl;

                    // Step 1: Delete physical provisioning (with dedup
                    // against surviving disks).
                    let zero: crate::db::model::ByteCount =
                        0.try_into().unwrap();
                    let dedup = DedupInfo::ImageDelete { image_id };
                    PhysicalProvisioningCollectionUpdate::new_delete_storage_silo_level_deduped(
                        image_id,
                        zero,
                        zero,
                        read_only_phys_bytes,
                        silo_id,
                        dedup,
                    )
                    .get_results_async::<crate::db::model::PhysicalProvisioningCollection>(
                        &conn,
                    )
                    .await
                    .map_err(|e| {
                        err.bail(crate::db::queries::physical_provisioning_collection_update::from_diesel(e))
                    })?;

                    // Step 2: Soft-delete the image record.
                    diesel::update(dsl::image)
                        .filter(dsl::id.eq(image_id))
                        .set(dsl::time_deleted.eq(Utc::now()))
                        .execute_async(&conn)
                        .await?;

                    Ok(())
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
}
