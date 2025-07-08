// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to updates and artifacts.

use std::collections::HashMap;

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::SemverVersion;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_errors::OptionalError;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_lookup::DbConnection;
use nexus_db_model::{
    ArtifactHash, TufArtifact, TufRepo, TufRepoDescription, TufTrustRoot,
    to_db_typed_uuid,
};
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, Generation,
    ListResultVec, LookupResult, LookupType, ResourceType, TufRepoInsertStatus,
};
use omicron_uuid_kinds::GenericUuid;
use omicron_uuid_kinds::TufRepoKind;
use omicron_uuid_kinds::TufTrustRootUuid;
use omicron_uuid_kinds::TypedUuid;
use swrite::{SWrite, swrite};
use tufaceous_artifact::ArtifactVersion;
use uuid::Uuid;

/// The return value of [`DataStore::tuf_repo_insert`].
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
    conn: &async_bb8_diesel::Connection<DbConnection>,
) -> Result<Vec<TufArtifact>, DieselError> {
    use nexus_db_schema::schema::tuf_artifact::dsl as tuf_artifact_dsl;
    use nexus_db_schema::schema::tuf_repo_artifact::dsl as tuf_repo_artifact_dsl;

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
    pub async fn tuf_repo_insert(
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

    /// Returns a TUF repo description.
    pub async fn tuf_repo_get_by_id(
        &self,
        opctx: &OpContext,
        repo_id: TypedUuid<TufRepoKind>,
    ) -> LookupResult<TufRepoDescription> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::tuf_repo::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;
        let repo = dsl::tuf_repo
            .filter(dsl::id.eq(to_db_typed_uuid(repo_id)))
            .select(TufRepo::as_select())
            .first_async::<TufRepo>(&*conn)
            .await
            .map_err(|e| {
                public_error_from_diesel(
                    e,
                    ErrorHandler::NotFoundByLookup(
                        ResourceType::TufRepo,
                        LookupType::ById(repo_id.into_untyped_uuid()),
                    ),
                )
            })?;

        let artifacts = artifacts_for_repo(repo.id.into(), &conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;
        Ok(TufRepoDescription { repo, artifacts })
    }

    /// Returns the TUF repo description corresponding to this system version.
    pub async fn tuf_repo_get_by_version(
        &self,
        opctx: &OpContext,
        system_version: SemverVersion,
    ) -> LookupResult<TufRepoDescription> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::tuf_repo::dsl;

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
    pub async fn tuf_list_repos(
        &self,
        opctx: &OpContext,
        generation: Generation,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<TufArtifact> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::tuf_artifact::dsl;

        let generation = nexus_db_model::Generation(generation);
        paginated(dsl::tuf_artifact, dsl::id, pagparams)
            .filter(dsl::generation_added.le(generation))
            .select(TufArtifact::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Returns the current TUF repo generation number.
    pub async fn tuf_get_generation(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<Generation> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        get_generation(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// List the trusted TUF root roles in the trust store.
    pub async fn tuf_trust_root_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<TufTrustRoot> {
        use nexus_db_schema::schema::tuf_trust_root::dsl;

        opctx
            .authorize(
                authz::Action::ListChildren,
                &authz::UPDATE_TRUST_ROOT_LIST,
            )
            .await?;
        paginated(dsl::tuf_trust_root, dsl::id, pagparams)
            .select(TufTrustRoot::as_select())
            .filter(dsl::time_deleted.is_null())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert a trusted TUF root role into the trust store.
    pub async fn tuf_trust_root_insert(
        &self,
        opctx: &OpContext,
        trust_root: TufTrustRoot,
    ) -> CreateResult<TufTrustRoot> {
        use nexus_db_schema::schema::tuf_trust_root::dsl;

        opctx
            .authorize(
                authz::Action::CreateChild,
                &authz::UPDATE_TRUST_ROOT_LIST,
            )
            .await?;
        diesel::insert_into(dsl::tuf_trust_root)
            .values(trust_root)
            .returning(TufTrustRoot::as_returning())
            .get_result_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Remove a TUF root role from the trust store (by setting the time_deleted
    /// field).
    pub async fn tuf_trust_root_delete(
        &self,
        opctx: &OpContext,
        authz_trust_root: &authz::TufTrustRoot,
        trust_root_id: TufTrustRootUuid,
    ) -> DeleteResult {
        use nexus_db_schema::schema::tuf_trust_root::dsl;

        opctx.authorize(authz::Action::Delete, authz_trust_root).await?;
        diesel::update(dsl::tuf_trust_root)
            .filter(dsl::id.eq(to_db_typed_uuid(trust_root_id)))
            .filter(dsl::time_deleted.is_null())
            .set(dsl::time_deleted.eq(chrono::Utc::now()))
            .execute_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            .map(|_| ())
    }
}

// This is a separate method mostly to make rustfmt not bail out on long lines
// of text.
async fn insert_impl(
    log: slog::Logger,
    conn: async_bb8_diesel::Connection<DbConnection>,
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
        use nexus_db_schema::schema::tuf_repo::dsl;

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
        use nexus_db_schema::schema::tuf_artifact::dsl;

        // Multiple repos can have the same artifacts, so we shouldn't error
        // out if we find an existing artifact. However, we should check that
        // the SHA256 hash and length matches if an existing artifact matches.

        let mut filter_dsl = dsl::tuf_artifact.into_boxed();
        for artifact in desc.artifacts.clone() {
            filter_dsl = filter_dsl
                .or_filter(
                    // Look up artifacts by name/version/kind.
                    dsl::name
                        .eq(artifact.name)
                        .and(dsl::version.eq(artifact.version))
                        .and(dsl::kind.eq(artifact.kind.clone())),
                )
                .or_filter(
                    // Also look up artifacts by kind/hash.
                    dsl::kind
                        .eq(artifact.kind)
                        .and(dsl::sha256.eq(artifact.sha256)),
                );
        }

        let results = filter_dsl
            .select(TufArtifact::as_select())
            .load_async(&conn)
            .await?;
        debug!(
            log,
            "found {} existing artifacts with nvk lookup", results.len();
            "results" => ?results,
        );

        let results_by_id = results
            .iter()
            .map(|artifact| (artifact.nvk(), artifact))
            .collect::<HashMap<_, _>>();
        let results_by_hash_id = results
            .iter()
            .map(|artifact| ((&artifact.kind, artifact.sha256), artifact))
            .collect::<HashMap<_, _>>();

        // uploaded_and_existing contains non-matching artifacts in pairs of
        // (uploaded, currently in db).
        let mut nvk_mismatch = Vec::new();
        let mut hash_id_mismatch = Vec::new();
        let mut new_artifacts = Vec::new();
        let mut all_artifacts = Vec::new();

        enum ArtifactStatus {
            New,
            Existing,
            Mismatch,
        }

        impl ArtifactStatus {
            fn mark_existing(&mut self) {
                match self {
                    ArtifactStatus::New => *self = ArtifactStatus::Existing,
                    ArtifactStatus::Existing | ArtifactStatus::Mismatch => (),
                }
            }

            fn mark_mismatch(&mut self) {
                match self {
                    ArtifactStatus::New | ArtifactStatus::Existing => {
                        *self = ArtifactStatus::Mismatch
                    }
                    ArtifactStatus::Mismatch => (),
                }
            }
        }

        for uploaded_artifact in desc.artifacts.clone() {
            let mut status = ArtifactStatus::New;
            if let Some(&existing_nvk) =
                results_by_id.get(&uploaded_artifact.nvk())
            {
                status.mark_existing();
                if existing_nvk.sha256 != uploaded_artifact.sha256
                    || existing_nvk.artifact_size()
                        != uploaded_artifact.artifact_size()
                {
                    nvk_mismatch.push((
                        uploaded_artifact.clone(),
                        existing_nvk.clone(),
                    ));
                    status.mark_mismatch();
                }
            };

            if let Some(&existing_hash) = results_by_hash_id
                .get(&(&uploaded_artifact.kind, uploaded_artifact.sha256))
            {
                status.mark_existing();
                if existing_hash.name != uploaded_artifact.name
                    || existing_hash.version != uploaded_artifact.version
                {
                    hash_id_mismatch.push((
                        uploaded_artifact.clone(),
                        existing_hash.clone(),
                    ));
                    status.mark_mismatch();
                }
            };

            // This is a new artifact.
            match status {
                ArtifactStatus::New => {
                    new_artifacts.push(uploaded_artifact.clone());
                    all_artifacts.push(uploaded_artifact);
                }
                ArtifactStatus::Existing => {
                    all_artifacts.push(uploaded_artifact);
                }
                ArtifactStatus::Mismatch => {
                    // This is an error case -- we'll return an error before
                    // `all_artifacts` is considered.
                }
            }
        }

        if !nvk_mismatch.is_empty() || !hash_id_mismatch.is_empty() {
            debug!(log, "uploaded artifacts don't match existing artifacts";
                "nvk_mismatch" => ?nvk_mismatch,
                "hash_id_mismatch" => ?hash_id_mismatch,
            );
            return Err(err.bail(InsertError::ArtifactMismatch {
                nvk_mismatch,
                hash_id_mismatch,
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
        use nexus_db_schema::schema::tuf_repo_artifact::dsl;

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
    conn: &async_bb8_diesel::Connection<DbConnection>,
) -> Result<Generation, DieselError> {
    use nexus_db_schema::schema::tuf_generation::dsl;

    let generation: nexus_db_model::Generation = dsl::tuf_generation
        .filter(dsl::singleton.eq(true))
        .select(dsl::generation)
        .get_result_async(conn)
        .await?;
    Ok(generation.0)
}

async fn put_generation(
    conn: &async_bb8_diesel::Connection<DbConnection>,
    old_generation: nexus_db_model::Generation,
    new_generation: nexus_db_model::Generation,
) -> Result<nexus_db_model::Generation, DieselError> {
    use nexus_db_schema::schema::tuf_generation::dsl;

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
    /// Some uploaded artifacts doesn't match the corresponding entries in the
    /// database.
    ArtifactMismatch {
        // Artifacts for which the name/version/kind were the same, but the hash
        // or length were different.
        nvk_mismatch: Vec<(TufArtifact, TufArtifact)>,

        // Artifacts for which the kind/hash were the same, but the name or
        // version were different.
        hash_id_mismatch: Vec<(TufArtifact, TufArtifact)>,
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
            InsertError::ArtifactMismatch {
                nvk_mismatch,
                hash_id_mismatch,
            } => {
                // Build a message out of uploaded and existing artifacts.
                let mut message = "Uploaded artifacts don't match existing \
                                   artifacts with same IDs:\n"
                    .to_string();
                for (uploaded, existing) in nvk_mismatch {
                    // Uploaded and existing artifacts are matched by their
                    // name/version/kinds.
                    swrite!(
                        message,
                        "- For artifact {}, uploaded SHA256 hash {} and length \
                         {}, but existing artifact has SHA256 hash {} and \
                         length {}.\n",
                        display_nvk(uploaded.nvk()),
                        uploaded.sha256,
                        uploaded.artifact_size(),
                        existing.sha256,
                        existing.artifact_size(),
                    );
                }

                for (uploaded, existing) in hash_id_mismatch {
                    swrite!(
                        message,
                        "- For artifact {}, uploaded name {} and version {}, \
                           but existing artifact has name {} and version {}.\n",
                        display_kind_hash(&uploaded.kind, uploaded.sha256),
                        uploaded.name,
                        uploaded.version,
                        existing.name,
                        existing.version,
                    );
                }

                external::Error::conflict(message)
            }
        }
    }
}

fn display_nvk(
    (name, version, kind): (&str, &ArtifactVersion, &str),
) -> String {
    format!("(name: {name}, version: {version}, kind: {kind})")
}

fn display_kind_hash(kind: &str, hash: ArtifactHash) -> String {
    format!("(kind: {kind}, hash: {hash})")
}
