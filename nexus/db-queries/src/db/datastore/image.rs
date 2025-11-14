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

    pub async fn project_image_promote(
        &self,
        opctx: &OpContext,
        authz_silo_image_list: &authz::SiloImageList,
        authz_project_image: &authz::ProjectImage,
        project_image: &ProjectImage,
    ) -> UpdateResult<Image> {
        // Check if the user can create silo images (promote from project images).
        // We use SiloImageList to allow limited-collaborators to promote images
        // without granting them the broader create_child permission on Silo.
        opctx
            .authorize(authz::Action::CreateChild, authz_silo_image_list)
            .await?;
        opctx.authorize(authz::Action::Modify, authz_project_image).await?;

        use nexus_db_schema::schema::image::dsl;
        let image = diesel::update(dsl::image)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project_image.id()))
            .set((
                dsl::project_id.eq(None::<Uuid>),
                dsl::time_modified.eq(Utc::now()),
            ))
            .returning(Image::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SiloImage,
                        project_image.name().as_str(),
                    ),
                )
            })?;

        Ok(image)
    }

    pub async fn silo_image_demote(
        &self,
        opctx: &OpContext,
        authz_silo_image: &authz::SiloImage,
        authz_project: &authz::Project,
        silo_image: &SiloImage,
    ) -> UpdateResult<Image> {
        opctx.authorize(authz::Action::Modify, authz_silo_image).await?;
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        use nexus_db_schema::schema::image::dsl;
        let image: Image = diesel::update(dsl::image)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_silo_image.id()))
            .set((
                dsl::project_id.eq(Some(authz_project.id())),
                dsl::time_modified.eq(Utc::now()),
            ))
            .returning(Image::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::ProjectImage,
                        silo_image.name().as_str(),
                    ),
                )
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
}
