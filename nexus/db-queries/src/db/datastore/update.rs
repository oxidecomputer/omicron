// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to updates and artifacts.

use std::collections::HashMap;

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::{public_error_from_diesel, ErrorHandler};
use crate::db::model::{
    ComponentUpdate, SemverVersion, SystemUpdate, UpdateDeployment,
    UpdateStatus, UpdateableComponent,
};
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use chrono::Utc;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_model::{
    ArtifactHash, SystemUpdateComponentUpdate, TufArtifact, TufRepo,
    TufRepoDescription,
};
use nexus_types::identity::Asset;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, ListResultVec, LookupResult,
    LookupType, ResourceType, TufRepoInsertStatus, UpdateResult,
};
use swrite::{swrite, SWrite};
use uuid::Uuid;

/// The return value of [`DataStore::update_tuf_repo_description_insert`].
///
/// This is similar to [`external::TufRepoInsertResponse`], but uses
/// nexus-db-model's types instead of external types.
pub struct TufRepoInsertResponse {
    pub recorded: TufRepoDescription,
    pub status: TufRepoInsertStatus,
}

impl TufRepoInsertResponse {
    pub fn into_external(self) -> external::TufRepoInsertResponse {
        external::TufRepoInsertResponse {
            recorded: self.recorded.into_external(),
            status: self.status,
        }
    }
}

async fn artifacts_for_repo(
    repo_id: Uuid,
    conn: &async_bb8_diesel::Connection<crate::db::DbConnection>,
) -> Result<Vec<TufArtifact>, DieselError> {
    use db::schema::tuf_artifact::dsl as tuf_artifact_dsl;
    use db::schema::tuf_repo_artifact::dsl as tuf_repo_artifact_dsl;

    let join_on_dsl = tuf_artifact_dsl::name
        .eq(tuf_repo_artifact_dsl::tuf_artifact_name)
        .and(
            tuf_artifact_dsl::version
                .eq(tuf_repo_artifact_dsl::tuf_artifact_version),
        )
        .and(
            tuf_artifact_dsl::kind.eq(tuf_repo_artifact_dsl::tuf_artifact_kind),
        );
    // Don't bother paginating because each repo should only have a few (under
    // 20) artifacts.
    tuf_repo_artifact_dsl::tuf_repo_artifact
        .filter(tuf_repo_artifact_dsl::tuf_repo_id.eq(repo_id))
        .inner_join(tuf_artifact_dsl::tuf_artifact.on(join_on_dsl))
        .select(TufArtifact::as_select())
        .load_async(conn)
        .await
}

impl DataStore {
    /// Inserts a new TUF repository into the database.
    ///
    /// Returns the repository just inserted, or an existing
    /// `TufRepoDescription` if one was already found. (This is not an upsert.)
    pub async fn update_tuf_repo_insert(
        &self,
        opctx: &OpContext,
        description: TufRepoDescription,
    ) -> CreateResult<TufRepoInsertResponse> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let log = opctx.log.new(
            slog::o!(
                "method" => "update_tuf_repo_insert",
                "uploaded_system_version" => description.repo.system_version.to_string(),
            ),
        );

        let err = OptionalError::new();
        let err2 = err.clone();

        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("update_tuf_repo_insert")
            .transaction(&conn, move |conn| {
                insert_impl(
                    log.clone(),
                    conn,
                    description.clone(),
                    err2.clone(),
                )
            })
            .await
            .map_err(|e| {
                if let Some(err) = err.take() {
                    err.into()
                } else {
                    public_error_from_diesel(e, ErrorHandler::Server)
                }
            })
    }

    /// Returns the TUF repo description corresponding to this hash.
    pub async fn update_tuf_repo_lookup_by_hash(
        &self,
        opctx: &OpContext,
        hash: ArtifactHash,
    ) -> LookupResult<TufRepoDescription> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use db::schema::tuf_repo::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let repo = dsl::tuf_repo
            .filter(dsl::sha256.eq(hash))
            .select(TufRepo::as_select())
            .first_async::<TufRepo>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::TufRepo,
                        LookupType::ByCompositeId(hash.to_string()),
                    ),
                )
            })?;

        let artifacts = artifacts_for_repo(repo.id, &conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(TufRepoDescription { repo, artifacts })
    }

    pub async fn upsert_system_update(
        &self,
        opctx: &OpContext,
        update: SystemUpdate,
    ) -> CreateResult<SystemUpdate> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::system_update::dsl::*;

        diesel::insert_into(system_update)
            .values(update.clone())
            .on_conflict(version)
            .do_update()
            // for now the only modifiable field is time_modified, but we intend
            // to add more metadata to this model
            .set(time_modified.eq(Utc::now()))
            .returning(SystemUpdate::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::SystemUpdate,
                        &update.version.to_string(),
                    ),
                )
            })
    }

    // version is unique but not the primary key, so we can't use LookupPath to handle this for us
    pub async fn system_update_fetch_by_version(
        &self,
        opctx: &OpContext,
        target: SemverVersion,
    ) -> LookupResult<SystemUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::system_update::dsl::*;

        let version_string = target.to_string();

        system_update
            .filter(version.eq(target))
            .select(SystemUpdate::as_select())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::SystemUpdate,
                        LookupType::ByCompositeId(version_string),
                    ),
                )
            })
    }

    pub async fn create_component_update(
        &self,
        opctx: &OpContext,
        system_update_id: Uuid,
        update: ComponentUpdate,
    ) -> CreateResult<ComponentUpdate> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        // TODO: make sure system update with that ID exists first
        // let (.., db_system_update) = LookupPath::new(opctx, &self)

        use db::schema::component_update;
        use db::schema::system_update_component_update as join_table;

        let version_string = update.version.to_string();

        let conn = self.pool_connection_authorized(opctx).await?;

        self.transaction_retry_wrapper("create_component_update")
            .transaction(&conn, |conn| {
                let update = update.clone();
                async move {
                    let db_update =
                        diesel::insert_into(component_update::table)
                            .values(update.clone())
                            .returning(ComponentUpdate::as_returning())
                            .get_result_async(&conn)
                            .await?;

                    diesel::insert_into(join_table::table)
                        .values(SystemUpdateComponentUpdate {
                            system_update_id,
                            component_update_id: update.id(),
                        })
                        .returning(SystemUpdateComponentUpdate::as_returning())
                        .get_result_async(&conn)
                        .await?;

                    Ok(db_update)
                }
            })
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::ComponentUpdate,
                        &version_string,
                    ),
                )
            })
    }

    pub async fn system_updates_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<SystemUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::system_update::dsl::*;

        paginated(system_update, id, pagparams)
            .select(SystemUpdate::as_select())
            .order(version.desc())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn system_update_components_list(
        &self,
        opctx: &OpContext,
        system_update_id: Uuid,
    ) -> ListResultVec<ComponentUpdate> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::component_update;
        use db::schema::system_update_component_update as join_table;

        component_update::table
            .inner_join(join_table::table)
            .filter(join_table::columns::system_update_id.eq(system_update_id))
            .select(ComponentUpdate::as_select())
            .get_results_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn create_updateable_component(
        &self,
        opctx: &OpContext,
        component: UpdateableComponent,
    ) -> CreateResult<UpdateableComponent> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        // make sure system version exists
        let sys_version = component.system_version.clone();
        self.system_update_fetch_by_version(opctx, sys_version).await?;

        use db::schema::updateable_component::dsl::*;

        diesel::insert_into(updateable_component)
            .values(component.clone())
            .returning(UpdateableComponent::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::UpdateableComponent,
                        &component.id().to_string(), // TODO: more informative identifier
                    ),
                )
            })
    }

    pub async fn updateable_components_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<UpdateableComponent> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        paginated(updateable_component, id, pagparams)
            .select(UpdateableComponent::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn lowest_component_system_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SemverVersion> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        updateable_component
            .select(system_version)
            .order(system_version.asc())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn highest_component_system_version(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<SemverVersion> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::updateable_component::dsl::*;

        updateable_component
            .select(system_version)
            .order(system_version.desc())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn create_update_deployment(
        &self,
        opctx: &OpContext,
        deployment: UpdateDeployment,
    ) -> CreateResult<UpdateDeployment> {
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        diesel::insert_into(update_deployment)
            .values(deployment.clone())
            .returning(UpdateDeployment::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::Conflict(
                        ResourceType::UpdateDeployment,
                        &deployment.id().to_string(),
                    ),
                )
            })
    }

    pub async fn steady_update_deployment(
        &self,
        opctx: &OpContext,
        deployment_id: Uuid,
    ) -> UpdateResult<UpdateDeployment> {
        // TODO: use authz::UpdateDeployment as the input so we can check Modify
        // on that instead
        opctx.authorize(authz::Action::CreateChild, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        diesel::update(update_deployment)
            .filter(id.eq(deployment_id))
            .set((
                status.eq(UpdateStatus::Steady),
                time_modified.eq(diesel::dsl::now),
            ))
            .returning(UpdateDeployment::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::UpdateDeployment,
                        LookupType::ById(deployment_id),
                    ),
                )
            })
    }

    pub async fn update_deployments_list_by_id(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<UpdateDeployment> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        paginated(update_deployment, id, pagparams)
            .select(UpdateDeployment::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    pub async fn latest_update_deployment(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<UpdateDeployment> {
        opctx.authorize(authz::Action::ListChildren, &authz::FLEET).await?;

        use db::schema::update_deployment::dsl::*;

        update_deployment
            .select(UpdateDeployment::as_returning())
            .order(time_created.desc())
            .first_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

// This is a separate method mostly to make rustfmt not bail out on long lines
// of text.
async fn insert_impl(
    log: slog::Logger,
    conn: async_bb8_diesel::Connection<crate::db::DbConnection>,
    desc: TufRepoDescription,
    err: OptionalError<InsertError>,
) -> Result<TufRepoInsertResponse, DieselError> {
    let repo = {
        use db::schema::tuf_repo::dsl;

        // Load the existing repo by the system version, if
        // any.
        let existing_repo = dsl::tuf_repo
            .filter(dsl::system_version.eq(desc.repo.system_version.clone()))
            .select(TufRepo::as_select())
            .first_async::<TufRepo>(&conn)
            .await
            .optional()?;

        if let Some(existing_repo) = existing_repo {
            // It doesn't matter whether the UUID of the repo matches or not,
            // since it's uniquely generated. But do check the hash.
            if existing_repo.sha256 != desc.repo.sha256 {
                return Err(err.bail(InsertError::RepoHashMismatch {
                    system_version: desc.repo.system_version,
                    uploaded: desc.repo.sha256,
                    existing: existing_repo.sha256,
                }));
            }

            // Just return the existing repo along with all of its artifacts.
            let artifacts = artifacts_for_repo(existing_repo.id, &conn).await?;

            let recorded =
                TufRepoDescription { repo: existing_repo, artifacts };
            return Ok(TufRepoInsertResponse {
                recorded,
                status: TufRepoInsertStatus::AlreadyExists,
            });
        }

        // This will fail if this ID or system version already exists with a
        // different hash, but that's a weird situation that should error out
        // anyway (IDs are not user controlled, hashes are).
        diesel::insert_into(dsl::tuf_repo)
            .values(desc.repo.clone())
            .execute_async(&conn)
            .await?;
        desc.repo.clone()
    };

    // Since we've inserted a new repo, we also need to insert the
    // corresponding artifacts.
    let all_artifacts = {
        use db::schema::tuf_artifact::dsl;

        // Multiple repos can have the same artifacts, so we shouldn't error
        // out if we find an existing artifact. However, we should check that
        // the SHA256 hash and length matches if an existing artifact matches.

        let mut filter_dsl = dsl::tuf_artifact.into_boxed();
        // XXX: Why can't this work with borrowed values?
        for artifact in desc.artifacts.clone() {
            filter_dsl = filter_dsl.or_filter(
                dsl::name
                    .eq(artifact.id.name)
                    .and(dsl::version.eq(artifact.id.version))
                    .and(dsl::kind.eq(artifact.id.kind)),
            );
        }

        let results = filter_dsl
            .select(TufArtifact::as_select())
            .load_async(&conn)
            .await?;
        debug!(
            log,
            "found {} existing artifacts", results.len();
            "results" => ?results,
        );

        let results_by_id = results
            .iter()
            .map(|artifact| (&artifact.id, artifact))
            .collect::<HashMap<_, _>>();

        // uploaded_and_existing contains non-matching artifacts in pairs of
        // (uploaded, currently in db).
        let mut uploaded_and_existing = Vec::new();
        let mut new_artifacts = Vec::new();
        let mut all_artifacts = Vec::new();

        for uploaded_artifact in desc.artifacts.clone() {
            let Some(&existing_artifact) =
                results_by_id.get(&uploaded_artifact.id)
            else {
                // This is a new artifact.
                new_artifacts.push(uploaded_artifact.clone());
                all_artifacts.push(uploaded_artifact);
                continue;
            };

            if existing_artifact.sha256 != uploaded_artifact.sha256
                || existing_artifact.artifact_size()
                    != uploaded_artifact.artifact_size()
            {
                uploaded_and_existing.push((
                    uploaded_artifact.clone(),
                    existing_artifact.clone(),
                ));
            } else {
                all_artifacts.push(uploaded_artifact);
            }
        }

        if !uploaded_and_existing.is_empty() {
            debug!(log, "uploaded artifacts don't match existing artifacts";
                "uploaded_and_existing" => ?uploaded_and_existing,
            );
            return Err(err.bail(InsertError::ArtifactMismatch {
                uploaded_and_existing,
            }));
        }

        debug!(
            log,
            "inserting {} new artifacts", new_artifacts.len();
            "new_artifacts" => ?new_artifacts,
        );

        // Insert new artifacts into the database.
        diesel::insert_into(dsl::tuf_artifact)
            .values(new_artifacts)
            .execute_async(&conn)
            .await?;
        all_artifacts
    };

    // Finally, insert all the associations into the tuf_repo_artifact table.
    {
        use db::schema::tuf_repo_artifact::dsl;

        let mut values = Vec::new();
        // XXX: Why can't this work with borrowed values?
        for artifact in desc.artifacts.clone() {
            slog::debug!(
                log,
                "inserting artifact into tuf_repo_artifact table";
                "artifact" => %artifact.id,
            );
            values.push((
                dsl::tuf_repo_id.eq(desc.repo.id),
                dsl::tuf_artifact_name.eq(artifact.id.name),
                dsl::tuf_artifact_version.eq(artifact.id.version),
                dsl::tuf_artifact_kind.eq(artifact.id.kind),
            ));
        }

        diesel::insert_into(dsl::tuf_repo_artifact)
            .values(values)
            .execute_async(&conn)
            .await?;
    }

    let recorded = TufRepoDescription { repo, artifacts: all_artifacts };
    Ok(TufRepoInsertResponse {
        recorded,
        status: TufRepoInsertStatus::Inserted,
    })
}

#[derive(Clone, Debug)]
enum InsertError {
    /// The SHA256 of the uploaded repository doesn't match the SHA256 of the
    /// existing repository with the same system version.
    RepoHashMismatch {
        system_version: SemverVersion,
        uploaded: ArtifactHash,
        existing: ArtifactHash,
    },
    /// The SHA256 or length of one or more artifacts doesn't match the
    /// corresponding entries in the database.
    ArtifactMismatch {
        // Pairs of (uploaded, existing) artifacts.
        uploaded_and_existing: Vec<(TufArtifact, TufArtifact)>,
    },
}

impl From<InsertError> for external::Error {
    fn from(e: InsertError) -> Self {
        match e {
            InsertError::RepoHashMismatch {
                system_version,
                uploaded,
                existing,
            } => external::Error::conflict(format!(
                "Uploaded repository with system version {} has SHA256 hash \
                     {}, but existing repository has SHA256 hash {}.",
                system_version, uploaded, existing,
            )),
            InsertError::ArtifactMismatch { uploaded_and_existing } => {
                // Build a message out of uploaded and existing artifacts.
                let mut message = "Uploaded artifacts don't match existing \
                                   artifacts with same IDs:\n"
                    .to_string();
                for (uploaded, existing) in uploaded_and_existing {
                    swrite!(
                        message,
                        "- Uploaded artifact {} has SHA256 hash {} and length \
                         {}, but existing artifact {} has SHA256 hash {} and \
                         length {}.\n",
                        uploaded.id,
                        uploaded.sha256,
                        uploaded.artifact_size(),
                        existing.id,
                        existing.sha256,
                        existing.artifact_size(),
                    );
                }

                external::Error::conflict(message)
            }
        }
    }
}
