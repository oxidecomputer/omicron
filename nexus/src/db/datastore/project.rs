// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Project`]s.

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel_pool;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::model::Name;
use crate::db::model::Organization;
use crate::db::model::Project;
use crate::db::model::ProjectUpdate;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use uuid::Uuid;

impl DataStore {
    /// Create a project
    pub async fn project_create(
        &self,
        opctx: &OpContext,
        org: &authz::Organization,
        project: Project,
    ) -> CreateResult<Project> {
        use db::schema::project::dsl;

        opctx.authorize(authz::Action::CreateChild, org).await?;

        let name = project.name().as_str().to_string();
        let organization_id = project.organization_id;
        Organization::insert_resource(
            organization_id,
            diesel::insert_into(dsl::project).values(project),
        )
        .insert_and_get_result_async(self.pool_authorized(opctx).await?)
        .await
        .map_err(|e| match e {
            AsyncInsertError::CollectionNotFound => Error::ObjectNotFound {
                type_name: ResourceType::Organization,
                lookup_type: LookupType::ById(organization_id),
            },
            AsyncInsertError::DatabaseError(e) => {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::Conflict(ResourceType::Project, &name),
                )
            }
        })
    }

    /// Delete a project
    // TODO-correctness This needs to check whether there are any resources that
    // depend on the Project (Disks, Instances).  We can do this with a
    // generation counter that gets bumped when these resources are created.
    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_project).await?;

        use db::schema::project::dsl;

        let now = Utc::now();
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project.id()))
            .set(dsl::time_deleted.eq(now))
            .returning(Project::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            })?;
        Ok(())
    }

    pub async fn projects_list_by_id(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<Project> {
        use db::schema::project::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_org).await?;

        paginated(dsl::project, dsl::id, pagparams)
            .filter(dsl::organization_id.eq(authz_org.id()))
            .filter(dsl::time_deleted.is_null())
            .select(Project::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    pub async fn projects_list_by_name(
        &self,
        opctx: &OpContext,
        authz_org: &authz::Organization,
        pagparams: &DataPageParams<'_, Name>,
    ) -> ListResultVec<Project> {
        use db::schema::project::dsl;

        opctx.authorize(authz::Action::ListChildren, authz_org).await?;

        paginated(dsl::project, dsl::name, &pagparams)
            .filter(dsl::organization_id.eq(authz_org.id()))
            .filter(dsl::time_deleted.is_null())
            .select(Project::as_select())
            .load_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))
    }

    /// Updates a project (clobbering update -- no etag)
    pub async fn project_update(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        updates: ProjectUpdate,
    ) -> UpdateResult<Project> {
        opctx.authorize(authz::Action::Modify, authz_project).await?;

        use db::schema::project::dsl;
        diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project.id()))
            .set(updates)
            .returning(Project::as_returning())
            .get_result_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            })
    }
}
