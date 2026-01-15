use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::datastore::StorageType;
use crate::db::model::Image;
use crate::db::model::Project;
use crate::db::model::ProjectImage;
use crate::db::model::Silo;
use crate::db::model::SiloImage;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::pagination::paginated;
use crate::db::queries::virtual_provisioning_collection_update::VirtualProvisioningCollectionUpdate;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_errors::ErrorHandler;
use nexus_db_errors::OptionalError;
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

    /// Promotes a project image to a silo image.
    ///
    /// This operation:
    /// 1. Updates the image record to remove the project association
    /// 2. Updates virtual provisioning to move accounting from project to silo
    ///
    /// All operations are performed in a transaction to ensure atomicity.
    pub async fn project_image_promote(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        authz_project_image: &authz::ProjectImage,
        project_image: &ProjectImage,
    ) -> UpdateResult<Image> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;
        opctx.authorize(authz::Action::Modify, authz_project_image).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        let image_id = authz_project_image.id();
        let project_id = project_image.project_id;
        let silo_id = authz_silo.id();
        let size = project_image.size;
        let image_name = project_image.name().as_str().to_string();

        self.transaction_retry_wrapper("project_image_promote")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let image_name = image_name.clone();
                async move {
                    // Delete project-level accounting.
                    //
                    // This removes the virtual_provisioning_resource entry and
                    // decrements project, silo, and fleet collections.
                    VirtualProvisioningCollectionUpdate::new_delete_storage(
                        image_id,
                        size,
                        project_id,
                    )
                    .get_results_async::<VirtualProvisioningCollection>(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            crate::db::queries::virtual_provisioning_collection_update::from_diesel(e)
                        })
                    })?;

                    // Insert silo-level accounting.
                    //
                    // This creates a new virtual_provisioning_resource entry and
                    // increments silo and fleet collections.
                    // Net effect: project decremented, silo and fleet unchanged.
                    VirtualProvisioningCollectionUpdate::new_insert_silo_storage(
                        image_id,
                        size,
                        silo_id,
                        StorageType::Image,
                    )
                    .get_results_async::<VirtualProvisioningCollection>(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            crate::db::queries::virtual_provisioning_collection_update::from_diesel(e)
                        })
                    })?;

                    // Update the image record to remove project association.
                    use nexus_db_schema::schema::image::dsl;
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
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Conflict(
                                        ResourceType::SiloImage,
                                        image_name.as_str(),
                                    ),
                                )
                            })
                        })?;

                    Ok(image)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Demotes a silo image to a project image.
    ///
    /// This operation:
    /// 1. Updates the image record to add the project association
    /// 2. Updates virtual provisioning to move accounting from silo to project
    ///
    /// All operations are performed in a transaction to ensure atomicity.
    /// If the project's quota would be exceeded, the operation fails and
    /// rolls back.
    pub async fn silo_image_demote(
        &self,
        opctx: &OpContext,
        authz_silo_image: &authz::SiloImage,
        authz_project: &authz::Project,
        silo_image: &SiloImage,
    ) -> UpdateResult<Image> {
        opctx.authorize(authz::Action::Modify, authz_silo_image).await?;
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let conn = self.pool_connection_authorized(opctx).await?;
        let err = OptionalError::new();

        let image_id = authz_silo_image.id();
        let project_id = authz_project.id();
        let silo_id = silo_image.silo_id;
        let size = silo_image.size;
        let image_name = silo_image.name().as_str().to_string();

        self.transaction_retry_wrapper("silo_image_demote")
            .transaction(&conn, |conn| {
                let err = err.clone();
                let image_name = image_name.clone();
                async move {
                    // Delete silo-level accounting.
                    //
                    // This removes the virtual_provisioning_resource entry and
                    // decrements silo and fleet collections.
                    VirtualProvisioningCollectionUpdate::new_delete_silo_storage(
                        image_id,
                        size,
                        silo_id,
                    )
                    .get_results_async::<VirtualProvisioningCollection>(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            crate::db::queries::virtual_provisioning_collection_update::from_diesel(e)
                        })
                    })?;

                    // Insert project-level accounting.
                    //
                    // This creates a new virtual_provisioning_resource entry and
                    // increments project, silo, and fleet collections.
                    // Net effect: project incremented, silo and fleet unchanged.
                    // This will fail if the silo quota would be exceeded.
                    VirtualProvisioningCollectionUpdate::new_insert_storage(
                        image_id,
                        size,
                        project_id,
                        StorageType::Image,
                    )
                    .get_results_async::<VirtualProvisioningCollection>(&conn)
                    .await
                    .map_err(|e| {
                        err.bail_retryable_or_else(e, |e| {
                            crate::db::queries::virtual_provisioning_collection_update::from_diesel(e)
                        })
                    })?;

                    // Update the image record to add project association.
                    use nexus_db_schema::schema::image::dsl;
                    let image: Image = diesel::update(dsl::image)
                        .filter(dsl::time_deleted.is_null())
                        .filter(dsl::id.eq(image_id))
                        .set((
                            dsl::project_id.eq(Some(project_id)),
                            dsl::time_modified.eq(Utc::now()),
                        ))
                        .returning(Image::as_returning())
                        .get_result_async(&conn)
                        .await
                        .map_err(|e| {
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Conflict(
                                        ResourceType::ProjectImage,
                                        image_name.as_str(),
                                    ),
                                )
                            })
                        })?;

                    Ok(image)
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })
    }

    /// Deletes a silo image record.
    ///
    /// Note: Does not update the corresponding accounting for space used by the
    /// image. That's the responsibility of the caller (the image deletion
    /// saga).
    pub async fn silo_image_delete(
        &self,
        opctx: &OpContext,
        authz_image: &authz::SiloImage,
        image: SiloImage,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_image).await?;
        self.image_delete(opctx, image.into()).await
    }

    /// Deletes a project image record.
    ///
    /// Note: Does not update the corresponding accounting for space used by the
    /// image. That's the responsibility of the caller (the image deletion
    /// saga).
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
}
