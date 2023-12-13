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
    LookupType, ResourceType, UpdateResult,
};
use swrite::{swrite, SWrite};
use uuid::Uuid;

#[derive(Clone, Debug)]
enum TufRepoDescriptionError {
    /// The SHA256 or length of one or more artifacts doesn't match the
    /// corresponding entries in the database.
    ArtifactMismatch {
        // Pairs of (uploaded, existing) artifacts.
        uploaded_and_existing: Vec<(TufArtifact, TufArtifact)>,
    },
}

impl From<TufRepoDescriptionError> for external::Error {
    fn from(e: TufRepoDescriptionError) -> Self {
        match e {
            TufRepoDescriptionError::ArtifactMismatch {
                uploaded_and_existing,
            } => {
                // Build a message out of uploaded and existing artifacts.
                let mut message = String::new();
                for (uploaded, existing) in uploaded_and_existing {
                    swrite!(
                        message,
                        "Uploaded artifact {} has SHA256 hash {} and length \
                         {}, but existing artifact {} has SHA256 hash {} and \
                         length {}.\n",
                        uploaded.display_id(),
                        uploaded.sha256,
                        uploaded.artifact_length(),
                        existing.display_id(),
                        existing.sha256,
                        existing.artifact_length(),
                    );
                }

                external::Error::conflict(message)
            }
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
    /// `TufRepoDescription` if one was already found.
    pub async fn tuf_repo_description_insert(
        &self,
        opctx: &OpContext,
        description: TufRepoDescription,
    ) -> CreateResult<TufRepoDescription> {
        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;

        let err = OptionalError::new();
        let err2 = err.clone();

        // Create a transaction, and add all the records as part of it.
        let conn = self.pool_connection_authorized(opctx).await?;
        self.transaction_retry_wrapper("tuf_repo_description_upsert")
            .transaction(&conn, move |conn| {
                let description = description.clone();
                let err = err2.clone();

                async move {
                    {
                        use db::schema::tuf_repo::dsl;

                        // Load the existing repo by the SHA256 hash, if any.
                        let existing_repo = dsl::tuf_repo
                            .filter(dsl::sha256.eq(description.repo.sha256))
                            .select(TufRepo::as_select())
                            .first_async::<TufRepo>(&conn)
                            .await
                            .optional()?;

                        if let Some(existing_repo) = existing_repo {
                            // It doesn't really matter whether the ID of the
                            // repo matches or not, since it's uniquely
                            // generated. Just return the existing repo along
                            // with all of its artifacts.
                            let artifacts =
                                artifacts_for_repo(existing_repo.id, &conn)
                                    .await?;

                            return Ok(TufRepoDescription {
                                repo: existing_repo,
                                artifacts,
                            });
                        }

                        // This will fail if this ID already exists with a
                        // different hash, but that's a weird situation that
                        // should error out anyway (IDs are not user
                        // controlled, hashes are).
                        diesel::insert_into(dsl::tuf_repo)
                            .values(description.repo.clone())
                            .execute_async(&conn)
                            .await?;
                    };

                    // Since we've inserted a new repo, we also need to insert
                    // the corresponding artifacts.
                    {
                        use db::schema::tuf_artifact::dsl;

                        // Build an index of the artifacts by their (name,
                        // version, kind) triples.
                        let artifacts_by_triple = description
                            .artifacts
                            .iter()
                            .map(|artifact| {
                                (
                                    (
                                        &artifact.name,
                                        &artifact.version,
                                        &artifact.kind,
                                    ),
                                    artifact,
                                )
                            })
                            .collect::<HashMap<_, _>>();

                        // Multiple repos can have the same artifacts, so we
                        // shouldn't error out if we find an existing artifact.
                        // However, we should check that the SHA256 hash and
                        // length matches if an existing artifact matches.

                        let mut filter_dsl = dsl::tuf_artifact.into_boxed();
                        // XXX: Why can't this work with borrowed values?
                        for artifact in description.artifacts.clone() {
                            filter_dsl = filter_dsl.or_filter(
                                dsl::name
                                    .eq(artifact.name)
                                    .and(dsl::version.eq(artifact.version))
                                    .and(dsl::kind.eq(artifact.kind)),
                            );
                        }

                        let results = filter_dsl
                            .select(TufArtifact::as_select())
                            .load_async(&conn)
                            .await?;

                        let mut uploaded_and_existing = Vec::new();
                        let mut new_artifacts = Vec::new();

                        for artifact in results {
                            let Some(&uploaded_artifact) = artifacts_by_triple
                                .get(&(
                                    &artifact.name,
                                    &artifact.version,
                                    &artifact.kind,
                                ))
                            else {
                                // This is a new artifact.
                                new_artifacts.push(artifact);
                                continue;
                            };

                            if uploaded_artifact.sha256 != artifact.sha256
                                || uploaded_artifact.artifact_length()
                                    != artifact.artifact_length()
                            {
                                uploaded_and_existing.push((
                                    uploaded_artifact.clone(),
                                    artifact,
                                ));
                            }
                        }

                        if !uploaded_and_existing.is_empty() {
                            return Err(err.bail(
                                TufRepoDescriptionError::ArtifactMismatch {
                                    uploaded_and_existing,
                                },
                            ));
                        }

                        // Insert new artifacts into the database.
                        diesel::insert_into(dsl::tuf_artifact)
                            .values(new_artifacts)
                            .execute_async(&conn)
                            .await?;
                    }

                    // Finally, insert all the associations into the tuf_repo_artifact table.
                    {
                        use db::schema::tuf_repo_artifact::dsl;

                        let mut values = Vec::new();
                        // XXX: Why can't this work with borrowed values?
                        for artifact in description.artifacts.clone() {
                            values.push((
                                dsl::tuf_repo_id.eq(description.repo.id),
                                dsl::tuf_artifact_name.eq(artifact.name),
                                dsl::tuf_artifact_version.eq(artifact.version),
                                dsl::tuf_artifact_kind.eq(artifact.kind),
                            ));
                        }

                        diesel::insert_into(dsl::tuf_repo_artifact)
                            .values(values)
                            .execute_async(&conn)
                            .await?;
                    }

                    // Just return the original description. This is okay to do
                    // because:
                    //
                    // 1. This is a new repo that we inserted (the existing
                    //    repo case is handled by an early return).
                    // 2. We either inserted new artifact records from this
                    //    description, or reused existing ones. If we reused
                    //    existing artifact records, note that the only fields
                    //    are name, version, kind, SHA256 hash, and length. All
                    //    five fields are checked for equivalence above, which
                    //    means that existing records are identical to new
                    //    ones. (If this changes in the future, we'll want to
                    //    return existing rows more explicitly.)
                    Ok(description)
                }
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
    pub async fn tuf_repo_description_lookup_by_hash(
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
