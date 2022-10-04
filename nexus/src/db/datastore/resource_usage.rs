// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`ResourceUsage`]s.

use super::DataStore;
use crate::context::OpContext;
use crate::db;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::model::ResourceUsage;
use crate::db::queries::resource_usage_update::ResourceUsageUpdate;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use uuid::Uuid;

impl DataStore {
    /// Create a resource_usage
    pub async fn resource_usage_create(
        &self,
        opctx: &OpContext,
        resource_usage: ResourceUsage,
    ) -> Result<(), Error> {
        use db::schema::resource_usage::dsl;

        diesel::insert_into(dsl::resource_usage)
            .values(resource_usage)
            .on_conflict_do_nothing()
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn resource_usage_get(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> Result<ResourceUsage, Error> {
        use db::schema::resource_usage::dsl;

        let resource_usage = dsl::resource_usage
            .find(id)
            .select(ResourceUsage::as_select())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(resource_usage)
    }

    /// Delete a resource_usage
    pub async fn resource_usage_delete(
        &self,
        opctx: &OpContext,
        id: Uuid,
    ) -> DeleteResult {
        use db::schema::resource_usage::dsl;

        diesel::delete(dsl::resource_usage)
            .filter(dsl::id.eq(id))
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn resource_usage_update_disk(
        &self,
        opctx: &OpContext,
        project_id: Uuid,
        disk_byte_diff: i64,
    ) -> Result<(), Error> {
        ResourceUsageUpdate::new_update_disk(project_id, disk_byte_diff)
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(e, ErrorHandler::Server)
            })?;
        Ok(())
    }
}
