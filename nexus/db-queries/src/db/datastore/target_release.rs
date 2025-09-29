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
use nexus_db_errors::{ErrorHandler, public_error_from_diesel};
use nexus_db_schema::schema::target_release::dsl;
use nexus_types::external_api::views;
use omicron_common::api::external::{CreateResult, Error, LookupResult};
use omicron_uuid_kinds::TufRepoUuid;
use std::collections::BTreeSet;

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
        insert_into(dsl::target_release)
            .values(target_release)
            .returning(TargetRelease::as_returning())
            .get_result_async(&*conn)
            .await
            .map_err(|e| public_error_from_diesel(e, ErrorHandler::Server))
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

    /// Lists the most recent N distinct target releases
    pub async fn target_release_fetch_recent_distinct(
        &self,
        opctx: &OpContext,
        count: u8,
    ) -> Result<RecentTargetReleases, Error> {
        opctx
            .authorize(authz::Action::Read, &authz::TARGET_RELEASE_CONFIG)
            .await?;

        // Fetch recent rows from `target_release`.
        //
        // In almost all cases, `count` = 2 and we only need to look back two
        // rows to find the most recent target releases.  But we do allow this
        // to be configurable, and there are cases where the same release can
        // appear in sequential `target_release` rows (e.g., after a MUPdate).
        //
        // We want to avoid a loop so that this function can be used in contexts
        // that don't want to take an arbitrary amount of time.
        //
        // The solution: `count` is a `u8`, so it can be at most 255.  We'll
        // multiply this by 4 to account for an unbelievable number of MUPdates.
        // That's still small enough to do in one go.  If we're wrong and can't
        // find enough distinct releases, we'll return an error.  (This seems
        // extremely unlikely.)
        let limit = 4 * u16::from(count);
        let conn = self.pool_connection_authorized(opctx).await?;
        let rows = dsl::target_release
            .select(TargetRelease::as_select())
            .order_by(dsl::generation.desc())
            .limit(i64::from(limit))
            .load_async(&*conn)
            .await
            .map_err(|err| {
                public_error_from_diesel(err, ErrorHandler::Server)
            })?;
        if rows.is_empty() {
            return Err(Error::internal_error(
                "unexpectedly found no rows in `target_release` table",
            ));
        }

        let target_release_generation = rows[0].generation.0;
        let nfound = rows.len();
        let mut releases = BTreeSet::new();
        for target_release in rows {
            if let Some(repo_id) = target_release.tuf_repo_id {
                releases.insert(repo_id.into());
                if releases.len() >= usize::from(count) {
                    return Ok(RecentTargetReleases {
                        releases,
                        count,
                        target_release_generation,
                    });
                }
            }
        }

        // We ran out of rows before finding enough distinct target releases.
        // If we got `limit` rows, there may have been more that we didn't
        // search.  This is the case we called "extremely unlikely" above.
        // Return an error in that case.
        if nfound == usize::from(limit) {
            return Err(Error::internal_error(&format!(
                "looking for {} distinct releases in the most recent {} \
                 target_release rows, but found only {} before giving up",
                count,
                limit,
                releases.len(),
            )));
        }

        // Otherwise, that's it: we found all the releases that were there.
        Ok(RecentTargetReleases { releases, count, target_release_generation })
    }
}

/// Represents information fetched about recent target releases
#[derive(Debug)]
pub struct RecentTargetReleases {
    /// distinct target releases found
    pub releases: BTreeSet<TufRepoUuid>,
    /// how many releases we tried to find
    pub(crate) count: u8,
    /// latest target_release generation when we fetched these releases
    /// (used to notice if a new target release has been set that could
    /// invalidate this information)
    pub(crate) target_release_generation:
        omicron_common::api::external::Generation,
}

#[cfg(test)]
pub(crate) mod test {
    use crate::db::DataStore;
    use crate::db::model::{Generation, TargetReleaseSource};
    use crate::db::pub_test_utils::TestDatabase;
    use chrono::{TimeDelta, Utc};
    use nexus_auth::context::OpContext;
    use nexus_db_model::TargetRelease;
    use omicron_common::api::external::{
        TufArtifactMeta, TufRepoDescription, TufRepoMeta,
    };
    use omicron_common::update::ArtifactId;
    use omicron_test_utils::dev;
    use omicron_uuid_kinds::TufRepoUuid;
    use semver::Version;
    use tufaceous_artifact::ArtifactHash;
    use tufaceous_artifact::{ArtifactKind, ArtifactVersion};

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
        const ARTIFACT_VERSION: ArtifactVersion =
            ArtifactVersion::new_const("0.0.1");
        let hash =
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .expect("SHA256('')");
        let repo = datastore
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
                            version: ARTIFACT_VERSION,
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
            .unwrap()
            .recorded
            .repo;
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

    pub(crate) fn make_test_repo(version: u32) -> TufRepoDescription {
        // We just need a unique hash for each repo.  We'll key it on the
        // version for determinism.
        let version_bytes = version.to_le_bytes();
        let hash_bytes: [u8; 32] =
            std::array::from_fn(|i| version_bytes[i % 4]);
        let hash = ArtifactHash(hash_bytes);
        let version = semver::Version::new(u64::from(version), 0, 0);
        let artifact_version = ArtifactVersion::new(version.to_string())
            .expect("valid artifact version");
        TufRepoDescription {
            repo: TufRepoMeta {
                hash,
                targets_role_version: 0,
                valid_until: chrono::Utc::now(),
                system_version: version,
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
        }
    }

    pub(crate) async fn make_and_insert(
        opctx: &OpContext,
        datastore: &DataStore,
        version: u32,
    ) -> TufRepoUuid {
        let repo = make_test_repo(version);
        datastore
            .tuf_repo_insert(opctx, &repo)
            .await
            .expect("inserting repo")
            .recorded
            .repo
            .id()
    }

    #[tokio::test]
    async fn test_recent_distinct() {
        let logctx = dev::test_setup_log("target_release_datastore");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // From initial conditions, this should succeed but find nothing of
        // note.  That's because on a freshly installed system, there's a row in
        // the target_release table, but it has no system version in it.
        let generation = datastore
            .target_release_get_current(opctx)
            .await
            .unwrap()
            .generation
            .0;
        let recent = datastore
            .target_release_fetch_recent_distinct(opctx, 3)
            .await
            .unwrap();
        assert_eq!(recent.count, 3);
        assert!(recent.releases.is_empty());
        assert_eq!(recent.target_release_generation, generation);

        // Now insert a TUF repo and try again.  That alone shouldn't change
        // anything.
        let repo1id = make_and_insert(opctx, datastore, 1).await;
        let target_release =
            datastore.target_release_get_current(opctx).await.unwrap();
        let last_generation = generation;
        let generation = target_release.generation.0;
        let recent = datastore
            .target_release_fetch_recent_distinct(opctx, 3)
            .await
            .unwrap();
        assert_eq!(recent.count, 3);
        assert!(recent.releases.is_empty());
        assert_eq!(last_generation, generation);
        assert_eq!(recent.target_release_generation, generation);

        // Now insert a target release and try again.
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(
                    &target_release,
                    repo1id.into(),
                ),
            )
            .await
            .unwrap();
        let last_generation = generation;
        let generation = target_release.generation.0;
        assert_ne!(last_generation, generation);
        let recent = datastore
            .target_release_fetch_recent_distinct(opctx, 3)
            .await
            .unwrap();
        assert_eq!(recent.count, 3);
        assert_eq!(recent.releases.len(), 1);
        assert!(recent.releases.contains(&repo1id));
        assert_eq!(recent.target_release_generation, generation);

        // Now insert a second distinct target release and try again.
        let repo2id = make_and_insert(opctx, datastore, 2).await;
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(
                    &target_release,
                    repo2id.into(),
                ),
            )
            .await
            .unwrap();
        let last_generation = generation;
        let generation = target_release.generation.0;
        assert_ne!(last_generation, generation);
        let recent = datastore
            .target_release_fetch_recent_distinct(opctx, 3)
            .await
            .unwrap();
        assert_eq!(recent.count, 3);
        assert_eq!(recent.releases.len(), 2);
        assert!(recent.releases.contains(&repo1id));
        assert!(recent.releases.contains(&repo2id));
        assert_eq!(recent.target_release_generation, generation);

        // If we only look back far enough for one, we'll only find one.
        let recent = datastore
            .target_release_fetch_recent_distinct(opctx, 1)
            .await
            .unwrap();
        assert_eq!(recent.count, 1);
        assert_eq!(recent.releases.len(), 1);
        assert!(recent.releases.contains(&repo2id));
        assert_eq!(recent.target_release_generation, generation);

        // Set the target release to the same value again.  We'll use this to
        // test that it looks back further than two rows when necessary.
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_system_version(
                    &target_release,
                    repo2id.into(),
                ),
            )
            .await
            .unwrap();
        let generation = target_release.generation.0;
        let recent = datastore
            .target_release_fetch_recent_distinct(opctx, 3)
            .await
            .unwrap();
        assert_eq!(recent.count, 3);
        assert_eq!(recent.releases.len(), 2);
        assert!(recent.releases.contains(&repo1id));
        assert!(recent.releases.contains(&repo2id));
        assert_eq!(recent.target_release_generation, generation);

        // If we do that a total of 7 times (so, five more times), it will have
        // to look back 8 rows to find two distinct releases, and it won't know
        // if it's looked back far enough.
        let mut target_release = target_release;
        for i in 0..6 {
            target_release = datastore
                .target_release_insert(
                    opctx,
                    TargetRelease::new_system_version(
                        &target_release,
                        repo2id.into(),
                    ),
                )
                .await
                .unwrap();
            let generation = target_release.generation.0;
            if i < 5 {
                let recent = datastore
                    .target_release_fetch_recent_distinct(opctx, 2)
                    .await
                    .unwrap();
                assert_eq!(recent.count, 2);
                assert_eq!(recent.releases.len(), 2);
                assert!(recent.releases.contains(&repo1id));
                assert!(recent.releases.contains(&repo2id));
                assert_eq!(recent.target_release_generation, generation);
            } else {
                let error = datastore
                    .target_release_fetch_recent_distinct(opctx, 2)
                    .await
                    .unwrap_err();
                eprintln!("got error (expected one): {}", error);
                // It'll look further if we're looking for more distinct
                // releases.
                let recent = datastore
                    .target_release_fetch_recent_distinct(opctx, 3)
                    .await
                    .unwrap();
                assert_eq!(recent.count, 3);
                assert_eq!(recent.releases.len(), 2);
                assert!(recent.releases.contains(&repo1id));
                assert!(recent.releases.contains(&repo2id));
                assert_eq!(recent.target_release_generation, generation);
            }
        }

        // Clean up.
        db.terminate().await;
        logctx.cleanup_successful();
    }
}
