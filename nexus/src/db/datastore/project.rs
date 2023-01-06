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
use crate::db::error::TransactionError;
use crate::db::identity::Resource;
use crate::db::model::CollectionTypeProvisioned;
use crate::db::model::Name;
use crate::db::model::Organization;
use crate::db::model::Project;
use crate::db::model::ProjectUpdate;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::pagination::paginated;
use async_bb8_diesel::{AsyncConnection, AsyncRunQueryDsl, PoolError};
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

// Generates internal functions used for validation during project deletion.
// Used simply to reduce boilerplate.
//
// It assumes:
//
// - $i is an identifier for a type of resource.
// - $i has a corresponding "db::schema::$i", which has a project_id,
// time_deleted, and $label field.
// - If $label is supplied, it must be a mandatory column of the table
// which is (1) looked up, and (2) used in an error message, if the resource.
// exists in the project. Otherwise, it is assumbed to be a "Uuid" named "id".
macro_rules! generate_fn_to_ensure_none_in_project {
    ($i:ident, $label:ident, $label_ty:ty) => {
        ::paste::paste! {
            async fn [<ensure_no_ $i s_in_project>](
                &self,
                opctx: &OpContext,
                authz_project: &authz::Project,
            ) -> DeleteResult {
                use db::schema::$i;

                let maybe_label = diesel_pool_result_optional(
                    $i::dsl::$i
                        .filter($i::dsl::project_id.eq(authz_project.id()))
                        .filter($i::dsl::time_deleted.is_null())
                        .select($i::dsl::$label)
                        .limit(1)
                        .first_async::<$label_ty>(self.pool_authorized(opctx).await?)
                        .await,
                )
                .map_err(|e| public_error_from_diesel_pool(e, ErrorHandler::Server))?;

                if let Some(label) = maybe_label {
                    let object = stringify!($i).replace('_', " ");
                    const VOWELS: [char; 5] = ['a', 'e', 'i', 'o', 'u'];
                    let article = if VOWELS.iter().any(|&v| object.starts_with(v)) {
                        "an"
                    } else {
                        "a"
                    };

                    return Err(Error::InvalidRequest {
                        message: format!("project to be deleted contains {article} {object}: {label}"),
                    });
                }

                Ok(())
            }
        }
    };
    ($i:ident) => {
        generate_fn_to_ensure_none_in_project!($i, id, Uuid);
    };
}

impl DataStore {
    /// Create a project
    pub async fn project_create(
        &self,
        opctx: &OpContext,
        org: &authz::Organization,
        project: Project,
    ) -> CreateResult<(authz::Project, Project)> {
        use db::schema::project::dsl;

        opctx.authorize(authz::Action::CreateChild, org).await?;

        let name = project.name().as_str().to_string();
        let organization_id = project.organization_id;
        let db_project = self
            .pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let project = Organization::insert_resource(
                    organization_id,
                    diesel::insert_into(dsl::project).values(project),
                )
                .insert_and_get_result_async(&conn)
                .await
                .map_err(|e| match e {
                    AsyncInsertError::CollectionNotFound => {
                        Error::ObjectNotFound {
                            type_name: ResourceType::Organization,
                            lookup_type: LookupType::ById(organization_id),
                        }
                    }
                    AsyncInsertError::DatabaseError(e) => {
                        public_error_from_diesel_pool(
                            e,
                            ErrorHandler::Conflict(
                                ResourceType::Project,
                                &name,
                            ),
                        )
                    }
                })?;

                // Create resource provisioning for the project.
                self.virtual_provisioning_collection_create_on_connection(
                    &conn,
                    VirtualProvisioningCollection::new(
                        project.id(),
                        CollectionTypeProvisioned::Project,
                    ),
                )
                .await?;
                Ok(project)
            })
            .await
            .map_err(|e| match e {
                TransactionError::CustomError(e) => e,
                TransactionError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
            })?;

        Ok((
            authz::Project::new(
                org.clone(),
                db_project.id(),
                LookupType::ByName(db_project.name().to_string()),
            ),
            db_project,
        ))
    }

    generate_fn_to_ensure_none_in_project!(instance, name, String);
    generate_fn_to_ensure_none_in_project!(disk, name, String);
    generate_fn_to_ensure_none_in_project!(image, name, String);
    generate_fn_to_ensure_none_in_project!(snapshot, name, String);
    generate_fn_to_ensure_none_in_project!(vpc, name, String);

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

        use db::schema::project::dsl;

        type TxnError = TransactionError<Error>;
        self.pool_authorized(opctx)
            .await?
            .transaction_async(|conn| async move {
                let now = Utc::now();
                let updated_rows = diesel::update(dsl::project)
                    .filter(dsl::time_deleted.is_null())
                    .filter(dsl::id.eq(authz_project.id()))
                    .filter(dsl::rcgen.eq(db_project.rcgen))
                    .set(dsl::time_deleted.eq(now))
                    .returning(Project::as_returning())
                    .execute_async(&conn)
                    .await
                    .map_err(|e| {
                        public_error_from_diesel_pool(
                            PoolError::from(e),
                            ErrorHandler::NotFoundByResource(authz_project),
                        )
                    })?;

                if updated_rows == 0 {
                    return Err(TxnError::CustomError(Error::InvalidRequest {
                        message:
                            "deletion failed due to concurrent modification"
                                .to_string(),
                    }));
                }

                self.virtual_provisioning_collection_delete_on_connection(
                    &conn,
                    db_project.id(),
                )
                .await?;
                Ok(())
            })
            .await
            .map_err(|e| match e {
                TxnError::CustomError(e) => e,
                TxnError::Pool(e) => {
                    public_error_from_diesel_pool(e, ErrorHandler::Server)
                }
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
