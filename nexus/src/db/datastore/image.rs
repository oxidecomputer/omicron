use diesel::prelude::*;
use nexus_db_model::Name;
use nexus_types::identity::Resource;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::InternalContext;
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
use crate::db::pagination::paginated;

use async_bb8_diesel::AsyncRunQueryDsl;

use super::DataStore;

impl DataStore {
    pub async fn image_list(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Image> {
        opctx.authorize(authz::Action::ListChildren, authz_project).await?;

        use db::schema::image::dsl;
        match pagparams {
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
        .filter(
            dsl::project_id.eq(authz_project.id()).or(dsl::is_global.eq(true)),
        )
        .select(Image::as_select())
        .load_async::<Image>(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn image_create(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        image: Image,
    ) -> CreateResult<Image> {
        opctx.authorize(authz::Action::CreateChild, authz_project).await?;

        let name = image.name().clone();
        let project_id = image.project_id;

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
                    ErrorHandler::Conflict(ResourceType::Image, name.as_str()),
                )
            }
        })?;
        Ok(image)
    }

    pub async fn image_promote(
        &self,
        opctx: &OpContext,
        authz_image: &authz::Image,
    ) -> UpdateResult<Image> {
        opctx.authorize(authz::Action::Modify, authz_image).await?;
        let authz_silo = opctx.authn.silo_required().internal_context(
            "fetching silo permissions for image promotion",
        )?;
        // Promoting an image impacts all projects in the silo so we ensure the user
        // has the equivalent permissions of creating a resource in the silo.
        opctx.authorize(authz::Action::CreateChild, &authz_silo).await?;

        use db::schema::image::dsl;
        diesel::update(dsl::image)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_image.id()))
            .set(dsl::is_global.eq(true))
            .returning(Image::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_image),
                )
            })
    }
}
