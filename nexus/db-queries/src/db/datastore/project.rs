// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods on [`Project`]s.

use super::DataStore;
use crate::authz;
use crate::authz::ApiResource;
use crate::context::OpContext;
use crate::db;
use crate::db::collection_insert::AsyncInsertError;
use crate::db::collection_insert::DatastoreCollection;
use crate::db::error::public_error_from_diesel;
use crate::db::error::ErrorHandler;
use crate::db::identity::Resource;
use crate::db::model::CollectionTypeProvisioned;
use crate::db::model::Name;
use crate::db::model::Project;
use crate::db::model::ProjectUpdate;
use crate::db::model::Silo;
use crate::db::model::VirtualProvisioningCollection;
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use async_bb8_diesel::OptionalExtension;
use chrono::Utc;
use diesel::prelude::*;
use nexus_db_fixed_data::project::SERVICES_PROJECT;
use nexus_types::silo::INTERNAL_SILO_ID;
use omicron_common::api::external::http_pagination::PaginatedBy;
use omicron_common::api::external::CreateResult;
use omicron_common::api::external::DeleteResult;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::api::external::ListResultVec;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::api::external::UpdateResult;
use ref_cast::RefCast;

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

                let maybe_label = $i::dsl::$i
                    .filter($i::dsl::project_id.eq(authz_project.id()))
                    .filter($i::dsl::time_deleted.is_null())
                    .select($i::dsl::$label)
                    .limit(1)
                    .first_async::<$label_ty>(&*self.pool_connection_authorized(opctx).await?)
                    .await
                    .optional()
                    .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

                if let Some(label) = maybe_label {
                    let object = stringify!($i).replace('_', " ");
                    const VOWELS: [char; 5] = ['a', 'e', 'i', 'o', 'u'];
                    let article = if VOWELS.iter().any(|&v| object.starts_with(v)) {
                        "an"
                    } else {
                        "a"
                    };

                    return Err(Error::invalid_request(
                        format!("project to be deleted contains {article} {object}: {label}")
                    ));
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
    /// Load built-in projects into the database
    pub async fn load_builtin_projects(
        &self,
        opctx: &OpContext,
    ) -> Result<(), Error> {
        opctx.authorize(authz::Action::Modify, &authz::DATABASE).await?;

        debug!(opctx.log, "attempting to create built-in projects");

        let (authz_silo,) = db::lookup::LookupPath::new(&opctx, self)
            .silo_id(INTERNAL_SILO_ID)
            .lookup_for(authz::Action::CreateChild)
            .await?;

        self.project_create_in_silo(
            opctx,
            SERVICES_PROJECT.clone(),
            &authz_silo,
        )
        .await
        .map(|_| ())
        .or_else(|e| match e {
            Error::ObjectAlreadyExists { .. } => Ok(()),
            _ => Err(e),
        })?;

        info!(opctx.log, "created built-in services project");

        Ok(())
    }

    /// Create a project
    pub async fn project_create(
        &self,
        opctx: &OpContext,
        project: Project,
    ) -> CreateResult<(authz::Project, Project)> {
        let authz_silo = opctx
            .authn
            .silo_required()
            .internal_context("creating a Project")?;
        self.project_create_in_silo(opctx, project, &authz_silo).await
    }

    /// Create a project in the given silo.
    async fn project_create_in_silo(
        &self,
        opctx: &OpContext,
        project: Project,
        authz_silo: &authz::Silo,
    ) -> CreateResult<(authz::Project, Project)> {
        opctx.authorize(authz::Action::CreateChild, authz_silo).await?;

        let silo_id = authz_silo.id();
        let authz_silo_inner = authz_silo.clone();

        use db::schema::project::dsl;

        let err = OptionalError::new();
        let name = project.name().as_str().to_string();
        let conn = self.pool_connection_authorized(opctx).await?;

        let db_project = self
            .transaction_retry_wrapper("project_create_in_silo")
            .transaction(&conn, |conn| {
                let err = err.clone();

                let authz_silo_inner = authz_silo_inner.clone();
                let name = name.clone();
                let project = project.clone();
                async move {
                    let project: Project = Silo::insert_resource(
                        silo_id,
                        diesel::insert_into(dsl::project).values(project),
                    )
                    .insert_and_get_result_async(&conn)
                    .await
                    .map_err(|e| match e {
                        AsyncInsertError::CollectionNotFound => {
                            err.bail(authz_silo_inner.not_found())
                        }
                        AsyncInsertError::DatabaseError(diesel_error) => err
                            .bail_retryable_or_else(
                                diesel_error,
                                |diesel_error| {
                                    public_error_from_diesel(
                                        diesel_error,
                                        ErrorHandler::Conflict(
                                            ResourceType::Project,
                                            &name,
                                        ),
                                    )
                                },
                            ),
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
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;

        Ok((
            authz::Project::new(
                authz_silo.clone(),
                db_project.id(),
                LookupType::ByName(db_project.name().to_string()),
            ),
            db_project,
        ))
    }

    generate_fn_to_ensure_none_in_project!(instance, name, String);
    generate_fn_to_ensure_none_in_project!(disk, name, String);
    generate_fn_to_ensure_none_in_project!(floating_ip, name, String);
    generate_fn_to_ensure_none_in_project!(project_image, name, String);
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
        self.ensure_no_floating_ips_in_project(opctx, authz_project).await?;
        self.ensure_no_project_images_in_project(opctx, authz_project).await?;
        self.ensure_no_snapshots_in_project(opctx, authz_project).await?;
        self.ensure_no_vpcs_in_project(opctx, authz_project).await?;

        use db::schema::project::dsl;

        let err = OptionalError::new();
        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("project_delete")
            .transaction(&conn, |conn| {
                let err = err.clone();
                async move {
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
                            err.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::NotFoundByResource(
                                        authz_project,
                                    ),
                                )
                            })
                        })?;

                    if updated_rows == 0 {
                        return Err(err.bail(Error::invalid_request(
                            "deletion failed due to concurrent modification",
                        )));
                    }

                    self.virtual_provisioning_collection_delete_on_connection(
                        &opctx.log,
                        &conn,
                        db_project.id(),
                    )
                    .await?;
                    Ok(())
                }
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    return err;
                }
                public_error_from_diesel(e, ErrorHandler::Server)
            })?;
        Ok(())
    }

    pub async fn projects_list(
        &self,
        opctx: &OpContext,
        pagparams: &PaginatedBy<'_>,
    ) -> ListResultVec<Project> {
        let authz_silo =
            opctx.authn.silo_required().internal_context("listing Projects")?;
        opctx.authorize(authz::Action::ListChildren, &authz_silo).await?;

        use db::schema::project::dsl;
        match pagparams {
            PaginatedBy::Id(pagparams) => {
                paginated(dsl::project, dsl::id, &pagparams)
            }
            PaginatedBy::Name(pagparams) => paginated(
                dsl::project,
                dsl::name,
                &pagparams.map_name(|n| Name::ref_cast(n)),
            ),
        }
        .filter(dsl::silo_id.eq(authz_silo.id()))
        .filter(dsl::time_deleted.is_null())
        .select(Project::as_select())
        .load_async(&*self.pool_connection_authorized(opctx).await?)
        .await
        .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByResource(authz_project),
                )
            })
    }
}
