// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods to get/set the current [`TargetRelease`].

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::model::{
    Generation, SemverVersion, TargetRelease, TargetReleaseSource,
};
use async_bb8_diesel::AsyncRunQueryDsl as _;
use diesel::insert_into;
use diesel::prelude::*;
use diesel::sql_types;
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_schema::enums::TargetReleaseSourceEnum;
use nexus_db_schema::schema::target_release::dsl;
use nexus_types::external_api::views;
use omicron_common::api::external::{CreateResult, Error, LookupResult};

impl DataStore {
    /// Fetch the current target release, i.e., the row with the largest
    /// generation number.
    pub async fn target_release_get_current(
        &self,
        opctx: &OpContext,
    ) -> LookupResult<TargetRelease> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let current = dsl::target_release
            .select(TargetRelease::as_select())
            .order_by(dsl::generation.desc())
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))?;

        // We expect there to always be a current target release,
        // since the database migration `create-target-release/up3.sql`
        // adds an initial row.
        let current = current
            .ok_or_else(|| Error::internal_error("no target release"))?;

        Ok(current)
    }

    /// Fetch a target release by generation number.
    pub async fn target_release_get_generation(
        &self,
        opctx: &OpContext,
        generation: Generation,
    ) -> LookupResult<Option<TargetRelease>> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        dsl::target_release
            .select(TargetRelease::as_select())
            .filter(dsl::generation.eq(generation))
            .first_async(&*conn)
            .await
            .optional()
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
    }

    /// Insert a new target release row and return it. It will only become
    /// the current target release if its generation is larger than any
    /// existing row.
    pub async fn target_release_insert(
        &self,
        opctx: &OpContext,
        target_release: TargetRelease,
    ) -> CreateResult<TargetRelease> {
        opctx
            .authorize(authz::Action::Modify, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;

        // If we have a TUF repo ID, we need a more complex query to confirm
        // (transactionally) that it isn't the pruned as we make it the target
        // release.
        if let Some(tuf_repo_id) = target_release.tuf_repo_id {
            let selection = {
                use nexus_db_schema::schema::tuf_repo::dsl as repo_dsl;

                // This statement is just here to force a compilation error if
                // the set of columns in `target_release` changes, because that
                // will affect the correctness of the query below.
                //
                // If you're here because of a compile error, you might be
                // changing the `target_release` table. Update the statement
                // below to match!
                let _: (
                    dsl::generation,
                    dsl::time_requested,
                    dsl::release_source,
                    dsl::tuf_repo_id,
                ) = dsl::target_release::all_columns();

                // What we want to write here is a query that confirms
                // `tuf_repo_id` is not pruned and avoids performing an insert
                // otherwise. We'll do that via an `INSERT SELECT ...` where the
                // `SELECT` is:
                //
                // ```
                // SELECT $target_release WHERE EXISTS (
                //     SELECT 1 FROM tuf_repo WHERE
                //         id = $tuf_repo_id
                //         AND time_pruned IS NULL
                // )
                // ```
                //
                // but with a couple of diesel quirks:
                //
                // 1. We can't splat the `$target_release` value directly into a
                //    SELECT, so we select each of its columns individually. See
                //    the above check that the columns of this table haven't
                //    changed.
                // 2. We don't bother getting it to `SELECT 1 ...` in the
                //    subquery. diesel defaults to `SELECT * ...` there instead,
                //    but that should be fine since it's inside a `WHERE
                //    EXISTS`.
                diesel::select((
                    target_release.generation.into_sql::<sql_types::Int8>(),
                    target_release
                        .time_requested
                        .into_sql::<sql_types::Timestamptz>(),
                    target_release
                        .release_source
                        .into_sql::<TargetReleaseSourceEnum>(),
                    tuf_repo_id
                        .into_sql::<sql_types::Nullable<sql_types::Uuid>>(),
                ))
                .filter(diesel::dsl::exists(
                    repo_dsl::tuf_repo
                        .filter(repo_dsl::id.eq(tuf_repo_id))
                        .filter(repo_dsl::time_pruned.is_null()),
                ))
            };

            // Attempt the insert; use `.optional()` so we can attach a custom
            // error message if we get back no rows.
            let result = insert_into(dsl::target_release)
                .values(selection)
                .returning(TargetRelease::as_returning())
                .get_result_async(&*conn)
                .await
                .optional()
                .map_err(|e| {
                    public_error_from_diesel(e, ErrorHandler::Server)
                })?;

            match result {
                Some(target_release) => {
                    // Insertion succeeded and returned the newly-inserted
                    // target release.
                    Ok(target_release)
                }
                None => {
                    // Insertion succeeded but didn't return any rows: we tried
                    // to insert a target release for a pruned repo.
                    Err(Error::invalid_request(format!(
                        "cannot make TUF repo {tuf_repo_id} the \
                         target release: it has been pruned from the system"
                    )))
                }
            }
        } else {
            insert_into(dsl::target_release)
                .values(target_release)
                .returning(TargetRelease::as_returning())
                .get_result_async(&*conn)
                .await
                .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
        }
    }

    /// Convert a model-level target release to an external view.
    /// This method lives here because we have to look up the version
    /// corresponding to the TUF repo.
    pub async fn target_release_view(
        &self,
        opctx: &OpContext,
        target_release: &TargetRelease,
    ) -> LookupResult<views::TargetRelease> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;
        let conn = self.pool_connection_authorized(opctx).await?;
        let release_source = match target_release.release_source {
            TargetReleaseSource::Unspecified => {
                views::TargetReleaseSource::Unspecified
            }
            TargetReleaseSource::SystemVersion => {
                use nexus_db_schema::schema::tuf_repo;
                if let Some(tuf_repo_id) = target_release.tuf_repo_id {
                    views::TargetReleaseSource::SystemVersion {
                        version: tuf_repo::table
                            .select(tuf_repo::system_version)
                            .filter(tuf_repo::id.eq(tuf_repo_id))
                            .first_async::<SemverVersion>(&*conn)
                            .await
                            .map_err(|e| {
                                public_error_from_diesel(
                                    e,
                                    ErrorHandler::Server,
                                )
                            })?
                            .into(),
                    }
                } else {
                    return Err(Error::internal_error(
                        "missing TUF repo ID for specified system version",
                    ));
                }
            }
        };
        Ok(target_release.into_external(release_source))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::model::{Generation, TargetReleaseSource};
    use crate::db::pub_test_utils::TestDatabase;
    use chrono::{TimeDelta, Utc};
    use nexus_db_model::TufRepo;
    use omicron_common::api::external::{
        TufArtifactMeta, TufRepoDescription, TufRepoMeta,
    };
    use omicron_common::update::ArtifactId;
    use omicron_test_utils::dev;
    use semver::Version;
    use sha2::Digest;
    use sha2::Sha256;
    use slog_error_chain::InlineErrorChain;
    use tufaceous_artifact::{ArtifactHash, ArtifactKind, ArtifactVersion};

    async fn insert_tuf_repo(
        opctx: &OpContext,
        datastore: &DataStore,
        version: &Version,
    ) -> TufRepo {
        let artifact_version = ArtifactVersion::new(version.to_string())
            .expect("version is valid for artifacts too");
        let hash = ArtifactHash(Sha256::digest(version.to_string()).into());

        datastore
            .tuf_repo_insert(
                opctx,
                &TufRepoDescription {
                    repo: TufRepoMeta {
                        hash,
                        targets_role_version: 0,
                        valid_until: Utc::now(),
                        system_version: version.clone(),
                        file_name: String::new(),
                    },
                    artifacts: vec![TufArtifactMeta {
                        id: ArtifactId {
                            name: String::new(),
                            version: artifact_version,
                            kind: ArtifactKind::from_static("empty"),
                        },
                        hash,
                        size: 0,
                        board: None,
                        sign: None,
                    }],
                },
            )
            .await
            .expect("inserted TUF repo description")
            .recorded
            .repo
    }

    #[tokio::test]
    async fn target_release_datastore() {
        let logctx = dev::test_setup_log("target_release_datastore");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // There should always be a target release.
        // This is ensured by the schema migration.
        let initial_target_release = datastore
            .target_release_get_current(opctx)
            .await
            .expect("should be a target release");
        assert_eq!(initial_target_release.generation, Generation(1.into()));
        assert!(initial_target_release.time_requested < Utc::now());
        assert_eq!(
            initial_target_release.release_source,
            TargetReleaseSource::Unspecified
        );
        assert!(initial_target_release.tuf_repo_id.is_none());

        // We should be able to set a new generation just like the first.
        // We allow some slack in the timestamp comparison because the
        // database only stores timestamps with μsec precision.
        let target_release =
            TargetRelease::new_unspecified(&initial_target_release);
        let target_release = datastore
            .target_release_insert(opctx, target_release)
            .await
            .unwrap();
        assert_eq!(target_release.generation, Generation(2.into()));
        assert!(
            (target_release.time_requested - target_release.time_requested)
                .abs()
                < TimeDelta::new(0, 1_000).expect("1 μsec")
        );
        assert!(target_release.tuf_repo_id.is_none());

        // Trying to reuse a generation should fail.
        assert!(
            datastore
                .target_release_insert(
                    opctx,
                    TargetRelease::new_unspecified(&initial_target_release),
                )
                .await
                .is_err()
        );

        // But inserting a new unspecified target should be fine.
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_unspecified(&target_release),
            )
            .await
            .unwrap();
        assert_eq!(target_release.generation, Generation(3.into()));

        // Now add a new TUF repo and use it as the source.
        let version = Version::new(0, 0, 1);
        let repo = insert_tuf_repo(opctx, datastore, &version).await;
        assert_eq!(repo.system_version, version.into());
        let tuf_repo_id = repo.id;

        let before = Utc::now();
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(&target_release, tuf_repo_id),
            )
            .await
            .unwrap();
        let after = Utc::now();
        assert_eq!(target_release.generation, Generation(4.into()));
        assert!(target_release.time_requested >= before);
        assert!(target_release.time_requested <= after);
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::SystemVersion
        );
        assert_eq!(target_release.tuf_repo_id, Some(tuf_repo_id));

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn reject_target_release_if_repo_pruned() {
        let logctx =
            dev::test_setup_log("reject_target_release_if_repo_pruned");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // Insert two TUF repos.
        let repo1 =
            insert_tuf_repo(opctx, datastore, &Version::new(0, 0, 1)).await;
        let repo2 =
            insert_tuf_repo(opctx, datastore, &Version::new(0, 0, 2)).await;

        // Manually prune the second one.
        {
            use nexus_db_schema::schema::tuf_repo::dsl;

            let conn = datastore
                .pool_connection_for_tests()
                .await
                .expect("got connection");
            let n = diesel::update(dsl::tuf_repo)
                .filter(dsl::id.eq(repo2.id))
                .set(dsl::time_pruned.eq(Some(Utc::now())))
                .execute_async(&*conn)
                .await
                .expect("pruned repo2");
            assert_eq!(n, 1, "should have only pruned 1 repo");
        }

        // There should always be an initial target release.
        let target_release = datastore
            .target_release_get_current(opctx)
            .await
            .expect("should be a target release");

        // Make repo1 the target release. This should succeed.
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(&target_release, repo1.id),
            )
            .await
            .expect("made repo1 the target release");
        assert_eq!(target_release.generation, Generation(2.into()));

        // Attempting to make repo1 the target release again should fail with a
        // reasonable error message (we need a higher generation).
        let err = datastore
            .target_release_insert(opctx, target_release.clone())
            .await
            .expect_err("making repo1 the target release again should fail");
        let err = InlineErrorChain::new(&err).to_string();
        assert!(
            err.contains("violates unique constraint"),
            "unexpected error: {err}"
        );

        // Attempt to make repo2 the target release. This should fail with a
        // reasonable error message (it's been pruned).
        let err = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(&target_release, repo2.id),
            )
            .await
            .expect_err("making repo2 the target release should fail");
        let err = InlineErrorChain::new(&err).to_string();
        assert!(
            err.contains("cannot make TUF repo")
                && err.contains(&repo2.id.to_string())
                && err.contains("target release")
                && err.contains("pruned"),
            "unexpected error: {err}"
        );

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
