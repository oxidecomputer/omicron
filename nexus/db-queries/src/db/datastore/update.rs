// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to updates and artifacts.

use std::collections::HashMap;

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db;
use crate::db::error::{ErrorHandler, public_error_from_diesel};
use crate::db::model::SemverVersion;
use crate::db::pagination::paginated;
use crate::transaction_retry::OptionalError;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_model::{ArtifactHash, TufArtifact, TufRepo, TufRepoDescription};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, Generation, ListResultVec,
    LookupResult, LookupType, ResourceType, TufRepoInsertStatus,
};
use omicron_uuid_kinds::TufRepoKind;
use omicron_uuid_kinds::TypedUuid;
use swrite::{SWrite, swrite};
use uuid::Uuid;

/// The return value of [`DataStore::update_tuf_repo_insert`].
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
    repo_id: TypedUuid<TufRepoKind>,
    conn: &async_bb8_diesel::Connection<crate::db::DbConnection>,
) -> Result<Vec<TufArtifact>, DieselError> {
    use db::schema::tuf_artifact::dsl as tuf_artifact_dsl;
    use db::schema::tuf_repo_artifact::dsl as tuf_repo_artifact_dsl;

    let join_on_dsl =
        tuf_artifact_dsl::id.eq(tuf_repo_artifact_dsl::tuf_artifact_id);
    // Don't bother paginating because each repo should only have a few (under
    // 20) artifacts.
    tuf_repo_artifact_dsl::tuf_repo_artifact
        .filter(
            tuf_repo_artifact_dsl::tuf_repo_id
                .eq(nexus_db_model::to_db_typed_uuid(repo_id)),
        )
        .inner_join(tuf_artifact_dsl::tuf_artifact.on(join_on_dsl))
        .select(TufArtifact::as_select())
        .load_async(conn)
        .await
}

impl DataStore {
    /// Inserts a new TUF repository into the database.
    ///
    /// Returns the repository just inserted, or an existing
    /// `TufRepoDescription` if one was already found. (This is not an upsert,
    /// because if we know about an existing repo but with different contents,
    /// we reject that.)
    pub async fn update_tuf_repo_insert(
        &self,
        opctx: &OpContext,
        description: &external::TufRepoDescription,
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
                insert_impl(log.clone(), conn, description, err2.clone())
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

    /// Returns the TUF repo description corresponding to this system version.
    pub async fn update_tuf_repo_get(
        &self,
        opctx: &OpContext,
        system_version: SemverVersion,
    ) -> LookupResult<TufRepoDescription> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use db::schema::tuf_repo::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        let repo = dsl::tuf_repo
            .filter(dsl::system_version.eq(system_version.clone()))
            .select(TufRepo::as_select())
            .first_async::<TufRepo>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::TufRepo,
                        LookupType::ByCompositeId(system_version.to_string()),
                    ),
                )
            })?;

        let artifacts = artifacts_for_repo(repo.id.into(), &conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(TufRepoDescription { repo, artifacts })
    }

    /// Returns the list of all TUF repo artifacts known to the system.
    pub async fn update_tuf_artifact_list(
        &self,
        opctx: &OpContext,
        generation: Generation,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<TufArtifact> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use db::schema::tuf_artifact::dsl;

        let generation = nexus_db_model::Generation(generation);
        paginated(dsl::tuf_artifact, dsl::id, pagparams)
            .filter(dsl::generation_added.le(generation))
            .select(TufArtifact::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Returns the current TUF repo generation number.
    pub async fn update_tuf_generation_get(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<Generation> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        get_generation(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }
}

// This is a separate method mostly to make rustfmt not bail out on long lines
// of text.
async fn insert_impl(
    log: slog::Logger,
    conn: async_bb8_diesel::Connection<crate::db::DbConnection>,
    desc: &external::TufRepoDescription,
    err: OptionalError<InsertError>,
) -> Result<TufRepoInsertResponse, DieselError> {
    // Load the current generation from the database and increment it, then
    // use that when creating the `TufRepoDescription`. If we determine there
    // are any artifacts to be inserted, we update the generation to this value
    // later.
    let old_generation = get_generation(&conn).await?;
    let new_generation = old_generation.next();
    let desc = TufRepoDescription::from_external(desc.clone(), new_generation);

    let repo = {
        use db::schema::tuf_repo::dsl;

        // Load the existing repo by the system version, if any.
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
            let artifacts =
                artifacts_for_repo(existing_repo.id.into(), &conn).await?;

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
        for artifact in desc.artifacts.clone() {
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
        debug!(
            log,
            "found {} existing artifacts", results.len();
            "results" => ?results,
        );

        let results_by_id = results
            .iter()
            .map(|artifact| (artifact.nvk(), artifact))
            .collect::<HashMap<_, _>>();

        // uploaded_and_existing contains non-matching artifacts in pairs of
        // (uploaded, currently in db).
        let mut uploaded_and_existing = Vec::new();
        let mut new_artifacts = Vec::new();
        let mut all_artifacts = Vec::new();

        for uploaded_artifact in desc.artifacts.clone() {
            let Some(&existing_artifact) =
                results_by_id.get(&uploaded_artifact.nvk())
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

        if !new_artifacts.is_empty() {
            // Since we are inserting new artifacts, we need to bump the
            // generation number.
            debug!(log, "setting new TUF repo generation";
                "generation" => new_generation,
            );
            put_generation(&conn, old_generation.into(), new_generation.into())
                .await?;

            // Insert new artifacts into the database.
            diesel::insert_into(dsl::tuf_artifact)
                .values(new_artifacts)
                .execute_async(&conn)
                .await?;
        }

        all_artifacts
    };

    // Finally, insert all the associations into the tuf_repo_artifact table.
    {
        use db::schema::tuf_repo_artifact::dsl;

        let mut values = Vec::new();
        for artifact in desc.artifacts.clone() {
            slog::debug!(
                log,
                "inserting artifact into tuf_repo_artifact table";
                "artifact" => %artifact.id,
            );
            values.push((
                dsl::tuf_repo_id.eq(desc.repo.id),
                dsl::tuf_artifact_id.eq(artifact.id),
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

async fn get_generation(
    conn: &async_bb8_diesel::Connection<crate::db::DbConnection>,
) -> Result<Generation, DieselError> {
    use db::schema::tuf_generation::dsl;

    let generation: nexus_db_model::Generation = dsl::tuf_generation
        .filter(dsl::singleton.eq(true))
        .select(dsl::generation)
        .get_result_async(conn)
        .await?;
    Ok(generation.0)
}

async fn put_generation(
    conn: &async_bb8_diesel::Connection<crate::db::DbConnection>,
    old_generation: nexus_db_model::Generation,
    new_generation: nexus_db_model::Generation,
) -> Result<nexus_db_model::Generation, DieselError> {
    use db::schema::tuf_generation::dsl;

    // We use `get_result_async` instead of `execute_async` to check that we
    // updated exactly one row.
    diesel::update(dsl::tuf_generation.filter(
        dsl::singleton.eq(true).and(dsl::generation.eq(old_generation)),
    ))
    .set(dsl::generation.eq(new_generation))
    .returning(dsl::generation)
    .get_result_async(conn)
    .await
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
