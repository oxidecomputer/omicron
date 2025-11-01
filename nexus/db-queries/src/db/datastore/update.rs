// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods related to updates and artifacts.

use std::collections::HashMap;

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::datastore::SQL_BATCH_SIZE;
use crate::db::datastore::target_release::RecentTargetReleases;
use crate::db::model::SemverVersion;
use crate::db::pagination::Paginator;
use crate::db::pagination::paginated;
use async_bb8_diesel::AsyncRunQueryDsl;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use nexus_db_errors::OptionalError;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_lookup::DbConnection;
use nexus_db_model::{
    ArtifactHash, TargetRelease, TufArtifact, TufRepo, TufRepoDescription,
    TufRepoUpload, TufTrustRoot, to_db_typed_uuid,
};
use nexus_types::external_api::views::TufRepoUploadStatus;
use omicron_common::api::external::{
    self, CreateResult, DataPageParams, DeleteResult, Generation,
    ListResultVec, LookupResult, LookupType, ResourceType, UpdateResult,
};
use omicron_common::api::external::{Error, InternalContext};
use omicron_uuid_kinds::{GenericUuid, TufRepoUuid};
use semver::Version;
use swrite::{SWrite, swrite};
use tufaceous_artifact::ArtifactVersion;
use uuid::Uuid;

async fn artifacts_for_repo(
    repo_id: TufRepoUuid,
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
    ) -> CreateResult<TufRepoUpload> {
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
        repo_id: TufRepoUuid,
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

    /// Returns the TUF repo corresponding to this system version.
    pub async fn tuf_repo_get_by_version(
        &self,
        opctx: &OpContext,
        system_version: SemverVersion,
    ) -> LookupResult<TufRepo> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::tuf_repo::dsl;

        let conn = self.pool_connection_authorized(opctx).await?;

        dsl::tuf_repo
            .filter(dsl::system_version.eq(system_version.clone()))
            .filter(dsl::time_pruned.is_null())
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
            })
    }

    /// Given a TUF repo ID, get its version. We could use `tuf_repo_get_by_id`,
    /// but that makes an additional query for the artifacts that we don't need
    /// in the code that uses this method.
    pub async fn tuf_repo_get_version(
        &self,
        opctx: &OpContext,
        tuf_repo_id: &TufRepoUuid,
    ) -> LookupResult<semver::Version> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        use nexus_db_schema::schema::tuf_repo;
        tuf_repo::table
            .select(tuf_repo::system_version)
            .filter(tuf_repo::id.eq(tuf_repo_id.into_untyped_uuid()))
            .first_async::<SemverVersion>(&*conn)
            .await
            .map(|v| v.0)
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
            // looking up a non-existent ID will 500, but it doesn't
            // automatically include the bad ID
            .with_internal_context(|| {
                format!("tuf_repo_get_version {tuf_repo_id}")
            })
    }

    /// Pages through the list of all not-yet-pruned TUF repos in the system
    pub async fn tuf_list_repos_unpruned(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Uuid>,
    ) -> ListResultVec<TufRepo> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::tuf_repo::dsl;

        paginated(dsl::tuf_repo, dsl::id, pagparams)
            .filter(dsl::time_pruned.is_null())
            .select(TufRepo::as_select())
            .load_async(&*self.pool_connection_authorized(opctx).await?)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Lists all unpruned TUF repos, making as many queries as needed to get
    /// them all
    ///
    /// This should not be used from contexts that shouldn't make lots of
    /// database queries (e.g., API endpoints).
    ///
    /// Since this involves pagination, this is not a consistent snapshot.
    /// Consider using `tuf_get_generation()` before calling this function and
    /// then making any subsequent queries conditional on the generation not
    /// having changed.
    pub async fn tuf_list_repos_unpruned_batched(
        &self,
        opctx: &OpContext,
    ) -> ListResultVec<TufRepo> {
        opctx.check_complex_operations_allowed()?;
        let mut paginator = Paginator::new(
            SQL_BATCH_SIZE,
            dropshot::PaginationOrder::Ascending,
        );
        let mut rv = Vec::new();
        while let Some(p) = paginator.next() {
            let batch = self
                .tuf_list_repos_unpruned(opctx, &p.current_pagparams())
                .await
                .internal_context("fetching page of TUF repos")?;
            paginator = p.found_batch(&batch, &|a| a.id.into_untyped_uuid());
            rv.extend(batch);
        }
        Ok(rv)
    }

    /// Marks the given TUF repo as eligible for pruning
    ///
    /// Callers are expected to verify that it's safe to prune this TUF repo.
    ///
    /// `recent_releases` comes from `target_release_fetch_recent_distinct()`.
    /// As part of verifying that it's safe to prune this TUF repo, callers are
    /// expected to check that it's not the current or immediately previous
    /// target release.
    ///
    /// This transaction will be conditional on:
    ///
    /// - the current TUF generation matching `initial_tuf_generation`
    ///   (i.e., we will not prune a release if some other query has added or
    ///   pruned a release since the caller fetched this generation)
    /// - the current target release generation matching what it was when
    ///   `recent_releases` was fetched (because this would invalidate the check
    ///   mentioned above that we're not pruning the target release).
    pub async fn tuf_repo_mark_pruned(
        &self,
        opctx: &OpContext,
        initial_tuf_generation: Generation,
        recent_releases: &RecentTargetReleases,
        tuf_repo_id: TufRepoUuid,
    ) -> UpdateResult<()> {
        // Double-check that the caller's done their diligence.
        //
        // These are not the primary way that we check these conditions.
        // They're a belt-and-suspenders check, since we have this information
        // available.
        if recent_releases.count < 2 {
            return Err(Error::internal_error(
                "must have fetched at least two recent releases to properly \
                 validate that an important release is not being pruned",
            ));
        }
        if recent_releases.releases.contains(&tuf_repo_id) {
            return Err(Error::internal_error(
                "attempting to prune a current or recent target release",
            ));
        }

        opctx.authorize(authz::Action::Modify, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let error = OptionalError::new();
        self.transaction_retry_wrapper("tuf_repo_mark_pruned")
            .transaction(&conn, |txn| {
                let error = error.clone();
                async move {
                    // If the target release generation has changed, bail out.
                    // This means someone has changed the target release, which
                    // means they may have set it to the repo we're trying to
                    // prune.  (This check could be more fine-grained, but this
                    // is adequate for now)
                    let target_release_generation_now = {
                        use nexus_db_schema::schema::target_release::dsl;
                        dsl::target_release
                            .select(TargetRelease::as_select())
                            .order_by(dsl::generation.desc())
                            .limit(1)
                            .first_async(&txn)
                            .await
                            .map_err(|e| {
                                error.bail_retryable_or_else(e, |e| {
                                    public_error_from_diesel(
                                        e,
                                        ErrorHandler::Server,
                                    )
                                    .internal_context(
                                        "fetching latest target_release \
                                         generation",
                                    )
                                })
                            })?
                            .generation
                            .0
                    };
                    if target_release_generation_now
                        != recent_releases.target_release_generation
                    {
                        return Err(error.bail(Error::conflict(format!(
                            "bailing out to avoid risk of marking current \
                                 target release pruned: target release has \
                                 changed since check (currently {}, was {})",
                            target_release_generation_now,
                            recent_releases.target_release_generation
                        ))));
                    }

                    // If the TUF repo generation has changed, bail out.
                    // Someone else is adding or pruning repos.  Force the
                    // caller to re-evaluate.  This is probably more
                    // conservative than necessary, but ensures that two Nexus
                    // instances don't concurrently decide to prune a lot more
                    // than either of them would on their own because they chose
                    // different repos to keep.
                    let tuf_generation_now =
                        get_generation(&txn).await.map_err(|e| {
                            error.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                                .internal_context(
                                    "fetching latest TUF generation",
                                )
                            })
                        })?;
                    if tuf_generation_now != initial_tuf_generation {
                        return Err(error.bail(Error::conflict(format!(
                            "bailing out to avoid risk of pruning too much: \
                             tuf repo generation has changed since check \
                             (currently {}, was {})",
                            tuf_generation_now, initial_tuf_generation,
                        ))));
                    }

                    // Try to mark the repo pruned.
                    use nexus_db_schema::schema::tuf_repo::dsl;
                    let count = diesel::update(dsl::tuf_repo)
                        .filter(dsl::id.eq(to_db_typed_uuid(tuf_repo_id)))
                        .filter(dsl::time_pruned.is_null())
                        .set(dsl::time_pruned.eq(chrono::Utc::now()))
                        .execute_async(&txn)
                        .await
                        .map_err(|e| {
                            error.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                                .internal_context("marking TUF repo pruned")
                            })
                        })?;

                    // If we made any changes, bump the TUF repo generation.
                    // This is necessary because that generation covers the set
                    // of TUF repos whose artifacts should be replicated.
                    if count > 0 {
                        put_generation(
                            &txn,
                            tuf_generation_now.into(),
                            tuf_generation_now.next().into(),
                        )
                        .await
                        .map_err(|e| {
                            error.bail_retryable_or_else(e, |e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                                .internal_context("bumping TUF generation")
                            })
                        })?;
                    }

                    Ok(())
                }
            })
            .await
            .map_err(|e| match error.take() {
                Some(err) => err,
                None => public_error_from_diesel(e, ErrorHandler::Server),
            })
    }

    /// List the artifacts present in a TUF repo.
    pub async fn tuf_list_repo_artifacts(
        &self,
        opctx: &OpContext,
        repo_id: TufRepoUuid,
    ) -> ListResultVec<TufArtifact> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        artifacts_for_repo(repo_id, &conn)
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

    /// List all TUF repositories (without artifacts) ordered by system version
    /// (newest first by default).
    pub async fn tuf_repo_list(
        &self,
        opctx: &OpContext,
        pagparams: &DataPageParams<'_, Version>,
    ) -> ListResultVec<TufRepo> {
        opctx.authorize(authz::Action::Read, &authz::FLEET).await?;

        use nexus_db_schema::schema::tuf_repo;

        let conn = self.pool_connection_authorized(opctx).await?;

        let marker_owner = pagparams
            .marker
            .map(|version| SemverVersion::from(version.clone()));
        let db_pagparams = DataPageParams {
            marker: marker_owner.as_ref(),
            direction: pagparams.direction,
            limit: pagparams.limit,
        };

        paginated(tuf_repo::table, tuf_repo::system_version, &db_pagparams)
            .filter(tuf_repo::time_pruned.is_null())
            .select(TufRepo::as_select())
            .load_async(&*conn)
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
    ) -> DeleteResult {
        use nexus_db_schema::schema::tuf_trust_root::dsl;

        opctx.authorize(authz::Action::Delete, authz_trust_root).await?;
        diesel::update(dsl::tuf_trust_root)
            .filter(dsl::id.eq(to_db_typed_uuid(authz_trust_root.id())))
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
) -> Result<TufRepoUpload, DieselError> {
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

        if let Some(mut existing_repo) = existing_repo {
            // It doesn't matter whether the UUID of the repo matches or not,
            // since it's uniquely generated. But do check the hash.
            if existing_repo.sha256 != desc.repo.sha256 {
                return Err(err.bail(InsertError::RepoHashMismatch {
                    system_version: desc.repo.system_version,
                    uploaded: desc.repo.sha256,
                    existing: existing_repo.sha256,
                }));
            }

            // This repo matches a previous record, so reset `time_created` to
            // now and ensure `time_pruned` is set to NULL.
            existing_repo.time_created = chrono::Utc::now();
            existing_repo.time_pruned = None;
            diesel::update(dsl::tuf_repo)
                .filter(dsl::id.eq(existing_repo.id))
                .set((
                    dsl::time_created.eq(existing_repo.time_created),
                    dsl::time_pruned.eq(existing_repo.time_pruned),
                ))
                .execute_async(&conn)
                .await?;

            // Just return the existing repo along with all of its artifacts.
            let artifacts =
                artifacts_for_repo(existing_repo.id.into(), &conn).await?;

            let recorded =
                TufRepoDescription { repo: existing_repo, artifacts };
            return Ok(TufRepoUpload {
                recorded,
                status: TufRepoUploadStatus::AlreadyExists,
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

        enum ArtifactStatus<'a> {
            New,
            Existing(&'a TufArtifact),
            Mismatch,
        }

        impl<'a> ArtifactStatus<'a> {
            fn mark_existing(&mut self, artifact: &'a TufArtifact) {
                match self {
                    ArtifactStatus::New => {
                        *self = ArtifactStatus::Existing(artifact)
                    }
                    ArtifactStatus::Existing(_) | ArtifactStatus::Mismatch => {
                        ()
                    }
                }
            }

            fn mark_mismatch(&mut self) {
                match self {
                    ArtifactStatus::New | ArtifactStatus::Existing(_) => {
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
                status.mark_existing(existing_nvk);
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
                status.mark_existing(existing_hash);
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
                ArtifactStatus::Existing(existing_artifact) => {
                    all_artifacts.push(existing_artifact.clone());
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
        for artifact in &all_artifacts {
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
    Ok(TufRepoUpload { recorded, status: TufRepoUploadStatus::Inserted })
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

#[cfg(test)]
mod test {
    use crate::db::datastore::SQL_BATCH_SIZE;
    use crate::db::pub_test_utils::TestDatabase;
    use crate::db::pub_test_utils::helpers::insert_test_tuf_repo;
    use nexus_db_model::TargetRelease;
    use omicron_test_utils::dev;
    use slog_error_chain::InlineErrorChain;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn test_repo_mark_pruned() {
        let logctx = dev::test_setup_log("test_repo_mark_pruned");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Initially, there should be no TUF repos.
        let repos = datastore
            .tuf_list_repos_unpruned_batched(opctx)
            .await
            .expect("listing all repos");
        assert!(repos.is_empty());

        // Add one TUF repo to the database.
        let repo1id = insert_test_tuf_repo(opctx, datastore, 1).await;

        // Make sure it's there.
        let repos = datastore
            .tuf_list_repos_unpruned_batched(opctx)
            .await
            .expect("listing all repos");
        assert!(!repos.is_empty());
        assert!(repos.iter().any(|r| r.id() == repo1id));

        // Now prune that one.
        let tuf_generation1 = datastore
            .tuf_get_generation(opctx)
            .await
            .expect("fetching TUF generation");
        let recent1 = datastore
            .target_release_fetch_recent_distinct(opctx, 2)
            .await
            .expect("fetching recent target releases");
        datastore
            .tuf_repo_mark_pruned(opctx, tuf_generation1, &recent1, repo1id)
            .await
            .expect("pruning release");

        // Make sure it was pruned.
        let repos = datastore
            .tuf_list_repos_unpruned_batched(opctx)
            .await
            .expect("listing all repos");
        assert!(repos.is_empty());

        // Now set up a more realistic case.
        let old_target_repo1_id =
            insert_test_tuf_repo(opctx, datastore, 2).await;
        let old_target_repo2_id =
            insert_test_tuf_repo(opctx, datastore, 3).await;
        let old_target_repo3_id =
            insert_test_tuf_repo(opctx, datastore, 4).await;
        let old_target_repo4_id =
            insert_test_tuf_repo(opctx, datastore, 5).await;
        let new_upload1 = insert_test_tuf_repo(opctx, datastore, 10).await;
        let new_upload2 = insert_test_tuf_repo(opctx, datastore, 11).await;
        let new_upload3 = insert_test_tuf_repo(opctx, datastore, 12).await;

        // Make sure they're all there.
        let repos = datastore
            .tuf_list_repos_unpruned_batched(opctx)
            .await
            .expect("listing all repos");
        assert_eq!(repos.len(), 7);

        // Set the target release a few times.
        let initial = datastore
            .target_release_get_current(opctx)
            .await
            .expect("initial target release");
        let mut next = initial;
        for repo_id in [
            old_target_repo1_id,
            old_target_repo2_id,
            old_target_repo3_id,
            old_target_repo4_id,
        ] {
            next = datastore
                .target_release_insert(
                    opctx,
                    TargetRelease::new_system_version(&next, repo_id.into()),
                )
                .await
                .expect("setting target release");
        }

        // We should be able to prune the following releases.
        for repo_id in
            [old_target_repo1_id, new_upload1, new_upload2, new_upload3]
        {
            let repos = datastore
                .tuf_list_repos_unpruned_batched(opctx)
                .await
                .expect("listing all repos");
            assert!(repos.iter().any(|r| r.id() == repo_id));

            let tuf_generation = datastore
                .tuf_get_generation(opctx)
                .await
                .expect("fetching TUF generation");
            let recent = datastore
                .target_release_fetch_recent_distinct(opctx, 3)
                .await
                .expect("fetching recent target releases");

            // If we supply the initial TUF generation OR the initial "recent
            // releases", this should fail because things have changed.
            let error = datastore
                .tuf_repo_mark_pruned(opctx, tuf_generation1, &recent, repo_id)
                .await
                .expect_err(
                    "unexpectedly succeeded in pruning release with old \
                     tuf_generation",
                );
            eprintln!(
                "got error (expected one): {}",
                InlineErrorChain::new(&error)
            );
            let error = datastore
                .tuf_repo_mark_pruned(opctx, tuf_generation, &recent1, repo_id)
                .await
                .expect_err(
                    "unexpectedly succeeded in pruning release with old \
                     recent_releases",
                );
            eprintln!(
                "got error (expected one): {}",
                InlineErrorChain::new(&error)
            );

            // It should still be there.
            let repos = datastore
                .tuf_list_repos_unpruned_batched(opctx)
                .await
                .expect("listing all repos");
            assert!(repos.iter().any(|r| r.id() == repo_id));

            // With up-to-date info, this should succeed.
            datastore
                .tuf_repo_mark_pruned(opctx, tuf_generation, &recent, repo_id)
                .await
                .expect("pruning release");
            let repos = datastore
                .tuf_list_repos_unpruned_batched(opctx)
                .await
                .expect("listing all repos");
            assert!(!repos.iter().any(|r| r.id() == repo_id));
        }

        // It should be illegal to prune the following releases because they're
        // too recent target releases.
        for repo_id in
            [old_target_repo2_id, old_target_repo3_id, old_target_repo4_id]
        {
            let repos = datastore
                .tuf_list_repos_unpruned_batched(opctx)
                .await
                .expect("listing all repos");
            assert!(repos.iter().any(|r| r.id() == repo_id));
            let tuf_generation = datastore
                .tuf_get_generation(opctx)
                .await
                .expect("fetching TUF generation");
            let recent = datastore
                .target_release_fetch_recent_distinct(opctx, 3)
                .await
                .expect("fetching recent target releases");
            let error = datastore
                .tuf_repo_mark_pruned(opctx, tuf_generation, &recent, repo_id)
                .await
                .expect_err("unexpectedly pruned recent target release repo");
            eprintln!(
                "found error (expected one): {}",
                InlineErrorChain::new(&error)
            );
            let repos = datastore
                .tuf_list_repos_unpruned_batched(opctx)
                .await
                .expect("listing all repos");
            assert!(repos.iter().any(|r| r.id() == repo_id));
        }

        db.terminate().await;
        logctx.cleanup_successful();
    }

    /// Tests pagination behavior around `tuf_list_repos_unpruned_batched()`.
    ///
    /// The behavior of filtering out pruned repos is tested in
    /// test_repo_mark_pruned().
    #[tokio::test]
    async fn test_list_unpruned() {
        let logctx = dev::test_setup_log("test_list_unpruned");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        let repos = datastore
            .tuf_list_repos_unpruned_batched(opctx)
            .await
            .expect("listing all repos");
        assert!(repos.is_empty());

        // Make sure we have more than a page worth of TUF repos.
        let count = SQL_BATCH_SIZE.get() + 3;
        let mut expected_repos = BTreeSet::new();
        for i in 0..count {
            assert!(
                expected_repos.insert(
                    insert_test_tuf_repo(opctx, datastore, i + 1).await
                )
            );
        }

        // Fetch them.  Make sure we got them all and nothing else.
        let repos = datastore
            .tuf_list_repos_unpruned_batched(opctx)
            .await
            .expect("listing all repos");
        assert_eq!(repos.len(), usize::try_from(count).unwrap());
        for repo in repos {
            assert!(expected_repos.remove(&repo.id()));
        }
        assert!(expected_repos.is_empty());

        db.terminate().await;
        logctx.cleanup_successful();
    }
}
