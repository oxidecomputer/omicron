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
use crate::db::error::diesel_pool_result_optional;
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

    async fn ensure_no_instances_in_project(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        use db::schema::instance;

        // Make sure there are no instances present within this project.
        let instance_found = diesel_pool_result_optional(
            instance::dsl::instance
                .filter(instance::dsl::project_id.eq(authz_project.id()))
                .filter(instance::dsl::time_deleted.is_null())
                .select(instance::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if instance_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "project to be deleted contains an instance"
                    .to_string(),
            });
        }

        Ok(())
    }

    async fn ensure_no_disks_in_project(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        use db::schema::disk;

        // Make sure there are no disks present within this project.
        let disk_found = diesel_pool_result_optional(
            disk::dsl::disk
                .filter(disk::dsl::project_id.eq(authz_project.id()))
                .filter(disk::dsl::time_deleted.is_null())
                .select(disk::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if disk_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "project to be deleted contains an disk".to_string(),
            });
        }

        Ok(())
    }

    async fn ensure_no_images_in_project(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        use db::schema::image;

        // Make sure there are no images present within this project.
        let image_found = diesel_pool_result_optional(
            image::dsl::image
                .filter(image::dsl::project_id.eq(authz_project.id()))
                .filter(image::dsl::time_deleted.is_null())
                .select(image::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if image_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "project to be deleted contains an image".to_string(),
            });
        }

        Ok(())
    }

    async fn ensure_no_snapshots_in_project(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        use db::schema::snapshot;

        // Make sure there are no snapshots present within this project.
        let snapshot_found = diesel_pool_result_optional(
            snapshot::dsl::snapshot
                .filter(snapshot::dsl::project_id.eq(authz_project.id()))
                .filter(snapshot::dsl::time_deleted.is_null())
                .select(snapshot::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if snapshot_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "project to be deleted contains a snapshot"
                    .to_string(),
            });
        }

        Ok(())
    }

    async fn ensure_no_vpcs_in_project(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        use db::schema::vpc;

        // Make sure there are no vpcs present within this project.
        let vpc_found = diesel_pool_result_optional(
            vpc::dsl::vpc
                .filter(vpc::dsl::project_id.eq(authz_project.id()))
                .filter(vpc::dsl::time_deleted.is_null())
                .select(vpc::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if vpc_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "project to be deleted contains a vpc".to_string(),
            });
        }

        Ok(())
    }

    async fn ensure_no_ip_pools_in_project(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        use db::schema::ip_pool;

        // Make sure there are no ip_pools present within this project.
        let ip_pool_found = diesel_pool_result_optional(
            ip_pool::dsl::ip_pool
                .filter(ip_pool::dsl::project_id.eq(authz_project.id()))
                .filter(ip_pool::dsl::time_deleted.is_null())
                .select(ip_pool::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if ip_pool_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "project to be deleted contains a ip pool".to_string(),
            });
        }

        Ok(())
    }

    async fn ensure_no_external_ips_in_project(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
    ) -> DeleteResult {
        use db::schema::external_ip;

        // Make sure there are no external_ips present within this project.
        let external_ip_found = diesel_pool_result_optional(
            external_ip::dsl::external_ip
                .filter(external_ip::dsl::project_id.eq(authz_project.id()))
                .filter(external_ip::dsl::time_deleted.is_null())
                .select(external_ip::dsl::id)
                .limit(1)
                .first_async::<Uuid>(self.pool_authorized(opctx).await?)
                .await,
        )
        .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;
        if external_ip_found.is_some() {
            return Err(Error::InvalidRequest {
                message: "project to be deleted contains an external IP"
                    .to_string(),
            });
        }

        Ok(())
    }

    /// Delete a project
    pub async fn project_delete(
        &self,
        opctx: &OpContext,
        authz_project: &authz::Project,
        db_project: &db::model::Project,
    ) -> DeleteResult {
        opctx.authorize(authz::Action::Delete, authz_project).await?;

        // Verify that child resources do not exist.
        self.ensure_no_instances_in_project(opctx, authz_project).await?;
        self.ensure_no_disks_in_project(opctx, authz_project).await?;
        self.ensure_no_images_in_project(opctx, authz_project).await?;
        self.ensure_no_snapshots_in_project(opctx, authz_project).await?;
        self.ensure_no_vpcs_in_project(opctx, authz_project).await?;
        self.ensure_no_ip_pools_in_project(opctx, authz_project).await?;
        self.ensure_no_external_ips_in_project(opctx, authz_project).await?;

        use db::schema::project::dsl;

        let now = Utc::now();
        let updated_rows = diesel::update(dsl::project)
            .filter(dsl::time_deleted.is_null())
            .filter(dsl::id.eq(authz_project.id()))
            .filter(dsl::rcgen.eq(db_project.rcgen))
            .set(dsl::time_deleted.eq(now))
            .returning(Project::as_returning())
            .execute_async(self.pool_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel_pool(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            })?;

        if updated_rows == 0 {
            return Err(Error::InvalidRequest {
                message: "deletion failed due to concurrent modification"
                    .to_string(),
            });
        }
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
