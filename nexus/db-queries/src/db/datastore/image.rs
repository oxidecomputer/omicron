use chrono::Utc;
use diesel::prelude::*;
use nexus_db_model::Name;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;

use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::Image;
use crate::db::model::Project;
use crate::db::model::ProjectImage;
use crate::db::model::Silo;
use crate::db::model::SiloImage;
use crate::db::pagination::paginated;

use async_bb8_diesel::AsyncRunQueryDsl;
use uuid::Uuid;

use super::DataStore;

impl DataStore {
    pub async fn project_image_list(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        authz_project: &authz::Project,
        include_silo_images: bool,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Image> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::image::dsl;
        use db::schema::project_image::dsl as project_dsl;
        match include_silo_images {
            true => match pagparams {
                PaginatedBy::Id(pagparams) => {
                    paginated(dsl::image, dsl::id, &pagparams)
                }
                PaginatedBy::Name(pagparams) => paginated(
                    dsl::image,
                    dsl::name,
                    &pagparams.map_name(|n| Name::ref_cast(n)),
                ),
            }
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::silo_id.eq(authz_silo.id()))
            .filter(
                dsl::project_id
                    .is_null()
                    .or(dsl::project_id.eq(authz_project.id())),
            )
            .select(Image::as_select())
            .load_async::<Image>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            }),
            false => match pagparams {
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
            .load_async::<ProjectImage>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
            .map(|v| v.into_iter().map(|v| v.into()).collect()),
        }
    }

    pub async fn silo_image_list(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Image> {
        opctx.authorize(authz::Action::ListChildren, authz_silo).await?;

        use db::schema::silo_image::dsl;
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
        .load_async::<SiloImage>(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
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

        use db::schema::image::dsl;
        let image: Image = Silo::insert_resource(
            silo_id,
            diesel::insert_into(dsl::image)
                .values(image)
                .on_conflict(dsl::id)
                .do_update()
                .set(dsl::time_modified.eq(dsl::time_modified)),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_silo.not_found(),
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::ProjectImage,
                        name.as_str(),
                    ),
                )
            }
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

        use db::schema::image::dsl;
        let image: Image = Project::insert_resource(
            project_id,
            diesel::insert_into(dsl::image)
                .values(image)
                .on_conflict(dsl::id)
                .do_update()
                .set(dsl::time_modified.eq(dsl::time_modified)),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => authz_project.not_found(),
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::ProjectImage,
                        name.as_str(),
                    ),
                )
            }
        })?;
        Ok(image)
    }

    pub async fn project_image_promote(
        &self,
        opctx: &OpContext,
        authz_silo: &authz::Silo,
        authz_project_image: &authz::ProjectImage,
    ) -> UpdateResult<Image> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;
        opctx.authorize(authz::Action::Modify, authz_project_image).await?;

        use db::schema::image::dsl;
        let image: Image = diesel::update(dsl::image)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project_image.id()))
            .set((
                dsl::project_id.eq(None::<Uuid>),
                dsl::time_modified.eq(Utc::now()),
            ))
            .returning(Image::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project_image),
                )
            })?;
        Ok(image)
    }
}
