// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to [`GlobalImage`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::model::GlobalImage;
use crate::db::model::Name;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::ResourceType;

impl DataStore {
    pub async fn global_image_list_images(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<GlobalImage> {
        opctx
            .authorize(authz::Action::ListChildren, &authz::GLOBAL_IMAGE_LIST)
            .await?;

        use db::schema::global_image::dsl;
        paginated(dsl::global_image, dsl::name, pagparams)
            .filter(dsl::time_deleted.is_null())
            .select(GlobalImage::as_select())
            .load_async::<GlobalImage>(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn global_image_create_image(
        &self,
        opctx: &OpContext,
        image: GlobalImage,
    ) -> CreateResult<GlobalImage> {
        opctx
            .authorize(authz::Action::CreateChild, &authz::GLOBAL_IMAGE_LIST)
            .await?;

        use db::schema::global_image::dsl;
        let name = image.name().clone();
        diesel::insert_into(dsl::global_image)
            .values(image)
            .on_conflict(dsl::id)
            .do_nothing()
            .returning(GlobalImage::as_returning())
            .get_result_async(self.pool())
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Image, name.as_str()),
                )
            })
    }
}
