// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! [`DataStore`] methods to get/set the current [`TargetRelease`].

use super::DataStore;
use crate::authz;
use crate::context::OpContext;
use crate::db::error::{public_error_from_diesel, ErrorHandler};
use crate::db::model::{SemverVersion, TargetRelease, TargetReleaseSource};
use crate::db::schema::target_release::dsl;
use async_bb8_diesel::AsyncRunQueryDsl as _;
use diesel::insert_into;
use diesel::prelude::*;
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
        let current = current.ok_or_else(|| Error::InternalError {
            internal_message: "no target release".to_string(),
        })?;

        Ok(current)
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
                use crate::db::schema::tuf_repo;
                if let Some(tuf_repo_id) = target_release.tuf_repo_id {
                    views::TargetReleaseSource::SystemVersion(
                        tuf_repo::table
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
                    )
                } else {
                    return Err(Error::invalid_request(
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
    use crate::db::model::{
        ArtifactHash, Generation, SemverVersion, TargetReleaseSource,
        TufArtifact, TufRepo, TufRepoDescription,
    };
    use crate::db::pub_test_utils::TestDatabase;
    use chrono::{TimeDelta, Utc};
    use omicron_common::update::{ArtifactId, ArtifactKind};
    use omicron_test_utils::dev;

    #[tokio::test]
    async fn target_release_datastore() {
        let logctx = dev::test_setup_log("target_release_datastore");
        let db = TestDatabase::new_with_datastore(&logctx.log).await;
        let (opctx, datastore) = (db.opctx(), db.datastore());

        // There should always be a target release.
        // This is ensured by the schema migration.
        let target_release = datastore
            .target_release_get_current(opctx)
            .await
            .expect("should be a target release");
        assert_eq!(target_release.generation, Generation(1.into()));
        assert!(target_release.time_requested < Utc::now());
        assert_eq!(
            target_release.release_source,
            TargetReleaseSource::Unspecified
        );
        assert!(target_release.tuf_repo_id.is_none());

        // We should be able to set a new generation just like the first,
        // with some (very small) fuzz allowed in the timestamp reported
        // by the database.
        let initial_target_release =
            TargetRelease::new_unspecified(&target_release);
        let target_release = datastore
            .target_release_insert(opctx, initial_target_release.clone())
            .await
            .unwrap();
        assert_eq!(
            target_release.generation,
            initial_target_release.generation
        );
        assert!(
            (target_release.time_requested
                - initial_target_release.time_requested)
                .abs()
                < TimeDelta::new(0, 1_000).expect("1 μsec")
        );
        assert!(target_release.tuf_repo_id.is_none());

        // Inserting a new "unspecified" target should be fine.
        let target_release = datastore
            .target_release_insert(
                opctx,
                TargetRelease::new_unspecified(&target_release),
            )
            .await
            .unwrap();

        // Now add a new TUF repo and use it as the source.
        let version = SemverVersion::new(0, 0, 1);
        let hash = ArtifactHash(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .parse()
                .expect("SHA256('')"),
        );
        let repo = datastore
            .update_tuf_repo_insert(
                opctx,
                TufRepoDescription {
                    repo: TufRepo::new(
                        hash,
                        0,
                        Utc::now(),
                        version.clone(),
                        String::from(""),
                    ),
                    artifacts: vec![TufArtifact::new(
                        ArtifactId {
                            name: String::from(""),
                            version: version.clone().into(),
                            kind: ArtifactKind::from_static("empty"),
                        },
                        hash,
                        0,
                    )],
                },
            )
            .await
            .unwrap()
            .recorded
            .repo;
        assert_eq!(repo.system_version, version);
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
}
